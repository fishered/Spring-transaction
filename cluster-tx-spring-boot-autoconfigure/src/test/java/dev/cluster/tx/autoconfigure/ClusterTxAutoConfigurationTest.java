package dev.cluster.tx.autoconfigure;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.cluster.tx.core.ActionResult;
import dev.cluster.tx.core.OperationState;
import dev.cluster.tx.core.SagaAction;
import dev.cluster.tx.core.SagaCommand;
import dev.cluster.tx.core.SagaDefinition;
import dev.cluster.tx.core.SagaStep;
import dev.cluster.tx.jdbc.ParticipantExecutor;
import dev.cluster.tx.jdbc.SagaCoordinator;
import java.util.List;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

class ClusterTxAutoConfigurationTest {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    ClusterTxAutoConfiguration.class, SagaCoordinatorAutoConfiguration.class))
            .withUserConfiguration(JdbcConfiguration.class)
            .withPropertyValues("cluster.tx.scheduler-enabled=false");

    @Test
    void participantOnlyApplicationGetsExecutorWithoutCoordinator() {
        runner.run(context -> {
            assertTrue(context.containsBean("participantExecutor"));
            assertTrue(context.getBean(ParticipantExecutor.class) != null);
            assertFalse(context.containsBean("sagaCoordinator"));
        });
    }

    @Test
    void registeredWorkflowActivatesCoordinator() {
        runner.withUserConfiguration(WorkflowConfiguration.class).run(context -> {
            assertTrue(context.getBean(SagaCoordinator.class) != null);
            assertFalse(context.containsBean("sagaRecoveryWorker"));
        });
    }

    @Configuration(proxyBeanMethods = false)
    static class JdbcConfiguration {
        @Bean DataSource dataSource() {
            JdbcDataSource source = new JdbcDataSource();
            source.setURL("jdbc:h2:mem:auto-config-test;DB_CLOSE_DELAY=-1");
            return source;
        }
        @Bean JdbcTemplate jdbcTemplate(DataSource dataSource) { return new JdbcTemplate(dataSource); }
        @Bean PlatformTransactionManager transactionManager(DataSource dataSource) {
            return new DataSourceTransactionManager(dataSource);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class WorkflowConfiguration {
        @Bean SagaDefinition demoWorkflow() {
            SagaAction action = new SagaAction() {
                @Override public ActionResult execute(SagaCommand command) { return ActionResult.APPLIED; }
                @Override public OperationState query(SagaCommand command) { return OperationState.APPLIED; }
                @Override public OperationState compensate(SagaCommand command) {
                    return OperationState.COMPENSATED;
                }
            };
            return new SagaDefinition("demo", 1, List.of(new SagaStep("step", action)));
        }
    }
}
