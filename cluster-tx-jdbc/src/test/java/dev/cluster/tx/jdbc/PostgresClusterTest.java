package dev.cluster.tx.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import dev.cluster.tx.core.ActionResult;
import dev.cluster.tx.core.OperationState;
import dev.cluster.tx.core.ParticipantAction;
import dev.cluster.tx.core.SagaAction;
import dev.cluster.tx.core.SagaCommand;
import dev.cluster.tx.core.SagaDefinition;
import dev.cluster.tx.core.SagaStatus;
import dev.cluster.tx.core.SagaStep;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

class PostgresClusterTest {
    @Test
    void twoCoordinatorsAndParticipantReplicasShareOneDurableResult() throws Exception {
        String url = System.getenv("CLUSTER_TX_POSTGRES_URL");
        assumeTrue(url != null && !url.isBlank(), "PostgreSQL service is configured in CI");
        String user = System.getenv().getOrDefault("CLUSTER_TX_POSTGRES_USER", "postgres");
        String password = System.getenv().getOrDefault("CLUSTER_TX_POSTGRES_PASSWORD", "postgres");
        JdbcTemplate admin = new JdbcTemplate(dataSource(url, user, password));
        String coordinatorSchema = "c_" + UUID.randomUUID().toString().replace("-", "");
        String participantSchema = "p_" + UUID.randomUUID().toString().replace("-", "");
        admin.execute("CREATE SCHEMA " + coordinatorSchema);
        admin.execute("CREATE SCHEMA " + participantSchema);

        DataSource coordinatorData = dataSource(url + "?currentSchema=" + coordinatorSchema, user, password);
        DataSource coordinatorReplicaData = dataSource(url + "?currentSchema=" + coordinatorSchema, user, password);
        DataSource participantData = dataSource(url + "?currentSchema=" + participantSchema, user, password);
        DataSource participantReplicaData = dataSource(url + "?currentSchema=" + participantSchema, user, password);
        ResourceDatabasePopulator schema = new ResourceDatabasePopulator(
                new ClassPathResource("db/cluster-tx/schema.sql"));
        schema.execute(coordinatorData);
        schema.execute(participantData);
        JdbcTemplate business = new JdbcTemplate(participantData);
        business.execute("CREATE TABLE inventory (id INTEGER PRIMARY KEY, reserved INTEGER NOT NULL)");
        business.update("INSERT INTO inventory VALUES (1, 0)");

        ParticipantExecutor participantOne = new ParticipantExecutor(business,
                new DataSourceTransactionManager(participantData));
        ParticipantExecutor participantTwo = new ParticipantExecutor(new JdbcTemplate(participantReplicaData),
                new DataSourceTransactionManager(participantReplicaData));
        AtomicInteger businessCalls = new AtomicInteger();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ParticipantAction reserve = new ParticipantAction() {
            @Override public ActionResult execute(SagaCommand command) {
                businessCalls.incrementAndGet();
                business.update("UPDATE inventory SET reserved = reserved + 1 WHERE id = 1");
                entered.countDown();
                try {
                    if (!release.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("Timed out waiting for peer");
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
                return ActionResult.APPLIED;
            }
            @Override public void compensate(SagaCommand command) {
                business.update("UPDATE inventory SET reserved = reserved - 1 WHERE id = 1");
            }
        };
        SagaAction reserveAction = adapter(participantOne, reserve);
        ParticipantAction reject = new ParticipantAction() {
            @Override public ActionResult execute(SagaCommand command) { return ActionResult.REJECTED; }
            @Override public void compensate(SagaCommand command) { }
        };
        SagaAction rejectAction = adapter(participantTwo, reject);
        SagaDefinition workflow = new SagaDefinition("cluster-order", 1,
                List.of(new SagaStep("reserve", reserveAction), new SagaStep("reject", rejectAction)));
        SagaOptions options = new SagaOptions(30_000, 0, 5, 32);
        SagaCoordinator first = new SagaCoordinator(new SagaJdbcRepository(
                new JdbcTemplate(coordinatorData), new DataSourceTransactionManager(coordinatorData)),
                List.of(workflow), options);
        SagaCoordinator second = new SagaCoordinator(new SagaJdbcRepository(
                new JdbcTemplate(coordinatorReplicaData), new DataSourceTransactionManager(coordinatorReplicaData)),
                List.of(workflow), options);
        String id = first.start("cluster-order", "order-pg", "{}").id();

        ExecutorService threads = Executors.newSingleThreadExecutor();
        try {
            Future<Integer> running = threads.submit(first::tick);
            assertTrue(entered.await(10, TimeUnit.SECONDS));
            assertEquals(0, second.tick());
            release.countDown();
            assertEquals(1, running.get(10, TimeUnit.SECONDS));
        } finally {
            release.countDown();
            threads.shutdownNow();
        }

        SagaCommand duplicate = new SagaCommand(id, id + ":0", "order-pg", "{}");
        assertEquals(ActionResult.APPLIED, participantTwo.execute(duplicate, reserve));
        assertEquals(1, businessCalls.get());
        assertEquals(1, second.tick());
        assertEquals(1, first.tick());
        assertEquals(SagaStatus.COMPENSATED, second.get(id).status());
        assertEquals(OperationState.COMPENSATED, participantTwo.query(id + ":0"));
        assertEquals(0, business.queryForObject("SELECT reserved FROM inventory WHERE id = 1", Integer.class));
    }

    private static DataSource dataSource(String url, String user, String password) {
        return new DriverManagerDataSource(url, user, password);
    }

    private static SagaAction adapter(ParticipantExecutor executor, ParticipantAction action) {
        return new SagaAction() {
            @Override public ActionResult execute(SagaCommand command) { return executor.execute(command, action); }
            @Override public OperationState query(SagaCommand command) { return executor.query(command.operationId()); }
            @Override public OperationState compensate(SagaCommand command) {
                return executor.compensate(command, action);
            }
        };
    }
}
