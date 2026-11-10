package dev.cluster.tx.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.cluster.tx.core.ActionResult;
import dev.cluster.tx.core.OperationState;
import dev.cluster.tx.core.ParticipantAction;
import dev.cluster.tx.core.SagaCommand;
import java.util.UUID;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

class ParticipantExecutorTest {
    private JdbcTemplate jdbc;
    private ParticipantExecutor executor;

    @BeforeEach
    void setUp() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:participant-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        new ResourceDatabasePopulator(new ClassPathResource("db/cluster-tx/schema.sql")).execute(dataSource);
        jdbc = new JdbcTemplate(dataSource);
        jdbc.execute("CREATE TABLE balance (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)");
        jdbc.update("INSERT INTO balance VALUES (1, 0)");
        executor = new ParticipantExecutor(jdbc, new DataSourceTransactionManager(dataSource));
    }

    @Test
    void duplicateExecutionAndCompensationChangeBusinessDataOnce() {
        SagaCommand command = command("op-1");
        ParticipantAction action = balanceAction();
        assertEquals(ActionResult.APPLIED, executor.execute(command, action));
        assertEquals(ActionResult.APPLIED, executor.execute(command, action));
        assertEquals(10, balance());
        assertEquals(OperationState.COMPENSATED, executor.compensate(command, action));
        assertEquals(OperationState.COMPENSATED, executor.compensate(command, action));
        assertEquals(0, balance());
        assertEquals(OperationState.COMPENSATED, executor.query(command.operationId()));
    }

    @Test
    void cancellationBeforeLateExecutionLeavesAFence() {
        SagaCommand command = command("op-2");
        ParticipantAction action = balanceAction();
        assertEquals(OperationState.CANCELLED, executor.compensate(command, action));
        assertEquals(ActionResult.REJECTED, executor.execute(command, action));
        assertEquals(OperationState.CANCELLED, executor.query(command.operationId()));
        assertEquals(0, balance());
    }

    @Test
    void sameOperationIdCannotBeReusedWithDifferentPayload() {
        SagaCommand original = command("op-mismatch");
        executor.execute(original, balanceAction());
        SagaCommand changed = new SagaCommand(original.sagaId(), original.operationId(),
                original.businessKey(), "{\"different\":true}");
        assertThrows(IllegalArgumentException.class,
                () -> executor.execute(changed, balanceAction()));
        assertEquals(10, balance());
    }

    @Test
    void failedBusinessActionRollsBackDataAndOperationRecord() {
        SagaCommand command = command("op-3");
        ParticipantAction action = new ParticipantAction() {
            @Override
            public ActionResult execute(SagaCommand ignored) {
                jdbc.update("UPDATE balance SET amount = amount + 10 WHERE id = 1");
                throw new IllegalStateException("fail after write");
            }

            @Override
            public void compensate(SagaCommand ignored) { }
        };
        assertThrows(IllegalStateException.class, () -> executor.execute(command, action));
        assertEquals(OperationState.NOT_FOUND, executor.query(command.operationId()));
        assertEquals(0, balance());
    }

    private ParticipantAction balanceAction() {
        return new ParticipantAction() {
            @Override
            public ActionResult execute(SagaCommand ignored) {
                jdbc.update("UPDATE balance SET amount = amount + 10 WHERE id = 1");
                return ActionResult.APPLIED;
            }

            @Override
            public void compensate(SagaCommand ignored) {
                jdbc.update("UPDATE balance SET amount = amount - 10 WHERE id = 1");
            }
        };
    }

    private int balance() {
        return jdbc.queryForObject("SELECT amount FROM balance WHERE id = 1", Integer.class);
    }

    private static SagaCommand command(String operationId) {
        return new SagaCommand("saga-1", operationId, "order-1", "{}");
    }
}
