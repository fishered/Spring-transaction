package dev.cluster.tx.autoconfigure;

import dev.cluster.tx.core.SagaStateMachine;
import dev.cluster.tx.jdbc.ParticipantExecutor;
import dev.cluster.tx.jdbc.SagaJdbcRepository;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

/** Participant and persistence services shared by coordinator and participant apps. */
@AutoConfiguration(afterName = {
        "org.springframework.boot.jdbc.autoconfigure.JdbcTemplateAutoConfiguration",
        "org.springframework.boot.jdbc.autoconfigure.DataSourceTransactionManagerAutoConfiguration"
})
@EnableConfigurationProperties(ClusterTxProperties.class)
public class ClusterTxAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    SagaStateMachine sagaStateMachine() {
        return new SagaStateMachine();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(JdbcTemplate.class)
    @ConditionalOnSingleCandidate(PlatformTransactionManager.class)
    SagaJdbcRepository sagaJdbcRepository(JdbcTemplate jdbc, PlatformTransactionManager manager) {
        return new SagaJdbcRepository(jdbc, manager);
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(JdbcTemplate.class)
    @ConditionalOnSingleCandidate(PlatformTransactionManager.class)
    ParticipantExecutor participantExecutor(JdbcTemplate jdbc, PlatformTransactionManager manager) {
        return new ParticipantExecutor(jdbc, manager);
    }
}
