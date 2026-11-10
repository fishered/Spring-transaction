package dev.cluster.tx.autoconfigure;

import dev.cluster.tx.core.SagaDefinition;
import dev.cluster.tx.jdbc.SagaCoordinator;
import dev.cluster.tx.jdbc.SagaJdbcRepository;
import java.util.List;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Activated only in applications that register at least one SagaDefinition bean. */
@AutoConfiguration(after = ClusterTxAutoConfiguration.class)
@ConditionalOnBean(SagaDefinition.class)
@EnableScheduling
public class SagaCoordinatorAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    SagaCoordinator sagaCoordinator(SagaJdbcRepository store, List<SagaDefinition> definitions,
                                    ClusterTxProperties properties) {
        return new SagaCoordinator(store, definitions, properties.toOptions());
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "cluster.tx", name = "scheduler-enabled",
            havingValue = "true", matchIfMissing = true)
    SagaRecoveryWorker sagaRecoveryWorker(SagaCoordinator coordinator) {
        return new SagaRecoveryWorker(coordinator);
    }
}
