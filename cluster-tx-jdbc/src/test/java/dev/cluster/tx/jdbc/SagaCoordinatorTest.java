package dev.cluster.tx.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.cluster.tx.core.ActionResult;
import dev.cluster.tx.core.OperationState;
import dev.cluster.tx.core.ParticipantAction;
import dev.cluster.tx.core.SagaAction;
import dev.cluster.tx.core.SagaCommand;
import dev.cluster.tx.core.SagaDefinition;
import dev.cluster.tx.core.SagaHandle;
import dev.cluster.tx.core.SagaStatus;
import dev.cluster.tx.core.SagaStep;
import java.util.List;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.support.TransactionTemplate;

class SagaCoordinatorTest {
    private JdbcTemplate coordinatorJdbc;
    private DataSourceTransactionManager coordinatorManager;
    private SagaJdbcRepository store;
    private JdbcTemplate inventoryJdbc;
    private JdbcTemplate paymentJdbc;
    private ParticipantExecutor inventory;
    private ParticipantExecutor payment;

    @BeforeEach
    void setUp() {
        JdbcDataSource coordinatorDataSource = database();
        coordinatorJdbc = new JdbcTemplate(coordinatorDataSource);
        coordinatorManager = new DataSourceTransactionManager(coordinatorDataSource);
        store = new SagaJdbcRepository(coordinatorJdbc, coordinatorManager);
        coordinatorJdbc.execute("CREATE TABLE orders (id VARCHAR(100) PRIMARY KEY, status VARCHAR(20))");

        JdbcDataSource inventoryDataSource = database();
        inventoryJdbc = new JdbcTemplate(inventoryDataSource);
        inventoryJdbc.execute("CREATE TABLE stock (id INTEGER PRIMARY KEY, quantity INTEGER)");
        inventoryJdbc.update("INSERT INTO stock VALUES (1, 10)");
        inventory = new ParticipantExecutor(inventoryJdbc, new DataSourceTransactionManager(inventoryDataSource));

        JdbcDataSource paymentDataSource = database();
        paymentJdbc = new JdbcTemplate(paymentDataSource);
        paymentJdbc.execute("CREATE TABLE charges (id INTEGER PRIMARY KEY, amount INTEGER)");
        paymentJdbc.update("INSERT INTO charges VALUES (1, 0)");
        payment = new ParticipantExecutor(paymentJdbc, new DataSourceTransactionManager(paymentDataSource));
    }

    @Test
    void completesAcrossThreeIndependentDatabasesAndDeduplicatesStart() {
        SagaCoordinator coordinator = coordinator(inventoryAction(), paymentAction(false));
        SagaHandle first = coordinator.start("order", "order-1", "{\"amount\":1}");
        SagaHandle duplicate = coordinator.start("order", "order-1", "{\"amount\":1}");
        assertEquals(first.id(), duplicate.id());
        assertThrows(IllegalArgumentException.class,
                () -> coordinator.start("order", "order-1", "{\"amount\":2}"));

        drain(coordinator, 3);
        assertEquals(SagaStatus.SUCCEEDED, coordinator.get(first.id()).status());
        assertEquals(9, stock());
        assertEquals(1, charges());
    }

    @Test
    void explicitRejectionCompensatesPreviouslyCommittedStep() {
        SagaCoordinator coordinator = coordinator(inventoryAction(), paymentAction(true));
        String id = coordinator.start("order", "order-2", "{}").id();
        drain(coordinator, 4);
        assertEquals(SagaStatus.COMPENSATED, coordinator.get(id).status());
        assertEquals(10, stock());
        assertEquals(0, charges());
        assertEquals(OperationState.COMPENSATED, inventory.query(id + ":0"));
    }

    @Test
    void lostExecuteResponseIsResolvedByQueryWithoutDuplicateCharge() {
        AtomicBoolean responseLost = new AtomicBoolean();
        SagaAction paymentAction = adapter(payment, paymentBusiness(false));
        SagaAction lossyPayment = new SagaAction() {
            @Override
            public ActionResult execute(SagaCommand command) {
                ActionResult result = paymentAction.execute(command);
                if (responseLost.compareAndSet(false, true)) {
                    throw new IllegalStateException("response lost after commit");
                }
                return result;
            }

            @Override public OperationState query(SagaCommand command) { return paymentAction.query(command); }
            @Override public OperationState compensate(SagaCommand command) { return paymentAction.compensate(command); }
        };
        SagaCoordinator coordinator = coordinator(inventoryAction(), lossyPayment);
        String id = coordinator.start("order", "order-3", "{}").id();
        drain(coordinator, 4);
        assertEquals(SagaStatus.SUCCEEDED, coordinator.get(id).status());
        assertEquals(1, charges());
    }

    @Test
    void lostCompensationResponseIsResolvedByQuery() {
        AtomicBoolean responseLost = new AtomicBoolean();
        SagaAction stockAction = inventoryAction();
        SagaAction lossyStock = new SagaAction() {
            @Override public ActionResult execute(SagaCommand command) { return stockAction.execute(command); }
            @Override public OperationState query(SagaCommand command) { return stockAction.query(command); }
            @Override public OperationState compensate(SagaCommand command) {
                OperationState result = stockAction.compensate(command);
                if (responseLost.compareAndSet(false, true)) {
                    throw new IllegalStateException("compensation response lost");
                }
                return result;
            }
        };
        SagaCoordinator coordinator = coordinator(lossyStock, paymentAction(true));
        String id = coordinator.start("order", "order-4", "{}").id();
        drain(coordinator, 5);
        assertEquals(SagaStatus.COMPENSATED, coordinator.get(id).status());
        assertEquals(10, stock());
    }

    @Test
    void businessStartAndSagaRecordRollBackTogether() {
        SagaCoordinator coordinator = coordinator(inventoryAction(), paymentAction(false));
        TransactionTemplate transaction = new TransactionTemplate(coordinatorManager);
        assertThrows(IllegalStateException.class, () -> transaction.execute(status -> {
            coordinatorJdbc.update("INSERT INTO orders VALUES ('order-5', 'PENDING')");
            coordinator.start("order", "order-5", "{}");
            throw new IllegalStateException("caller failed");
        }));
        assertEquals(0, coordinatorJdbc.queryForObject("SELECT COUNT(*) FROM orders", Integer.class));
        assertEquals(null, store.findByBusinessKey("order", "order-5"));
    }

    @Test
    void secondWorkerCannotClaimAnActiveLease() {
        AtomicReference<SagaCoordinator> second = new AtomicReference<>();
        SagaAction guarded = new SagaAction() {
            @Override public ActionResult execute(SagaCommand command) {
                assertFalse(second.get().process(command.sagaId()));
                return inventoryAction().execute(command);
            }
            @Override public OperationState query(SagaCommand command) { return inventoryAction().query(command); }
            @Override public OperationState compensate(SagaCommand command) { return inventoryAction().compensate(command); }
        };
        SagaDefinition definition = new SagaDefinition("order", 1, List.of(new SagaStep("inventory", guarded)));
        SagaOptions options = new SagaOptions(30_000, 0, 5, 32);
        SagaCoordinator first = new SagaCoordinator(store, List.of(definition), options);
        second.set(new SagaCoordinator(store, List.of(definition), options));
        String id = first.start("order", "order-6", "{}").id();
        assertEquals(1, first.tick());
        assertEquals(SagaStatus.SUCCEEDED, first.get(id).status());
        assertEquals(9, stock());
    }

    @Test
    void compensatesThreeCommittedStepsInReverseOrder() {
        List<String> events = new ArrayList<>();
        SagaAction first = recordingAction("first", events);
        SagaAction second = recordingAction("second", events);
        SagaAction third = recordingAction("third", events);
        SagaAction rejected = new SagaAction() {
            @Override public ActionResult execute(SagaCommand command) { return ActionResult.REJECTED; }
            @Override public OperationState query(SagaCommand command) { return OperationState.REJECTED; }
            @Override public OperationState compensate(SagaCommand command) { return OperationState.CANCELLED; }
        };
        SagaDefinition definition = new SagaDefinition("ordered", 1, List.of(
                new SagaStep("first", first), new SagaStep("second", second),
                new SagaStep("third", third), new SagaStep("reject", rejected)));
        SagaCoordinator coordinator = new SagaCoordinator(store, List.of(definition),
                new SagaOptions(30_000, 0, 5, 32));
        String id = coordinator.start("ordered", "reverse-1", "{}").id();
        drain(coordinator, 7);
        assertEquals(SagaStatus.COMPENSATED, coordinator.get(id).status());
        assertEquals(List.of("execute-first", "execute-second", "execute-third",
                "compensate-third", "compensate-second", "compensate-first"), events);
    }

    @Test
    void anotherCoordinatorResumesPersistedProgress() {
        SagaAction stock = inventoryAction();
        SagaAction charge = paymentAction(false);
        SagaDefinition definition = new SagaDefinition("restart", 1,
                List.of(new SagaStep("stock", stock), new SagaStep("charge", charge)));
        SagaOptions options = new SagaOptions(30_000, 0, 5, 32);
        SagaCoordinator before = new SagaCoordinator(store, List.of(definition), options);
        String id = before.start("restart", "restart-1", "{}").id();
        assertEquals(1, before.tick());
        SagaCoordinator after = new SagaCoordinator(store, List.of(definition), options);
        assertEquals(1, after.tick());
        assertEquals(SagaStatus.SUCCEEDED, after.get(id).status());
        assertEquals(9, stock());
        assertEquals(1, charges());
    }

    @Test
    void expiredWorkerCannotOverwriteNewOwnerAfterTakeover() {
        SagaCoordinator coordinator = coordinator(inventoryAction(), paymentAction(false));
        String id = coordinator.start("order", "takeover-1", "{}").id();
        long now = store.now() + 1_000;
        SagaInstanceRow first = store.claim(id, "first", now, 1);
        SagaInstanceRow second = store.claim(id, "second", now + 2, 30_000);
        assertEquals("first", first.leaseOwner());
        assertEquals("second", second.leaseOwner());
        assertThrows(IllegalStateException.class, () -> store.progress(first,
                new SagaProgress(SagaStatus.SUCCEEDED, 2, now, null, -1,
                        null, null, 0, 0), now));
        assertEquals("second", store.instance(id).leaseOwner());
        assertEquals(SagaStatus.RUNNING, coordinator.get(id).status());
    }

    private static SagaAction recordingAction(String name, List<String> events) {
        return new SagaAction() {
            @Override public ActionResult execute(SagaCommand command) {
                events.add("execute-" + name);
                return ActionResult.APPLIED;
            }
            @Override public OperationState query(SagaCommand command) { return OperationState.APPLIED; }
            @Override public OperationState compensate(SagaCommand command) {
                events.add("compensate-" + name);
                return OperationState.COMPENSATED;
            }
        };
    }

    private SagaCoordinator coordinator(SagaAction first, SagaAction second) {
        SagaDefinition definition = new SagaDefinition("order", 1,
                List.of(new SagaStep("inventory", first), new SagaStep("payment", second)));
        return new SagaCoordinator(store, List.of(definition), new SagaOptions(30_000, 0, 5, 32));
    }

    private SagaAction inventoryAction() {
        ParticipantAction business = new ParticipantAction() {
            @Override public ActionResult execute(SagaCommand command) {
                inventoryJdbc.update("UPDATE stock SET quantity = quantity - 1 WHERE id = 1 AND quantity > 0");
                return ActionResult.APPLIED;
            }
            @Override public void compensate(SagaCommand command) {
                inventoryJdbc.update("UPDATE stock SET quantity = quantity + 1 WHERE id = 1");
            }
        };
        return adapter(inventory, business);
    }

    private SagaAction paymentAction(boolean reject) {
        return adapter(payment, paymentBusiness(reject));
    }

    private ParticipantAction paymentBusiness(boolean reject) {
        return new ParticipantAction() {
            @Override public ActionResult execute(SagaCommand command) {
                if (reject) return ActionResult.REJECTED;
                paymentJdbc.update("UPDATE charges SET amount = amount + 1 WHERE id = 1");
                return ActionResult.APPLIED;
            }
            @Override public void compensate(SagaCommand command) {
                paymentJdbc.update("UPDATE charges SET amount = amount - 1 WHERE id = 1");
            }
        };
    }

    private static SagaAction adapter(ParticipantExecutor executor, ParticipantAction business) {
        return new SagaAction() {
            @Override public ActionResult execute(SagaCommand command) {
                return executor.execute(command, business);
            }
            @Override public OperationState query(SagaCommand command) {
                return executor.query(command.operationId());
            }
            @Override public OperationState compensate(SagaCommand command) {
                return executor.compensate(command, business);
            }
        };
    }

    private static JdbcDataSource database() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:tx-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        new ResourceDatabasePopulator(new ClassPathResource("db/cluster-tx/schema.sql")).execute(dataSource);
        return dataSource;
    }

    private static void drain(SagaCoordinator coordinator, int count) {
        for (int i = 0; i < count; i++) coordinator.tick();
    }

    private int stock() {
        return inventoryJdbc.queryForObject("SELECT quantity FROM stock WHERE id = 1", Integer.class);
    }

    private int charges() {
        return paymentJdbc.queryForObject("SELECT amount FROM charges WHERE id = 1", Integer.class);
    }
}
