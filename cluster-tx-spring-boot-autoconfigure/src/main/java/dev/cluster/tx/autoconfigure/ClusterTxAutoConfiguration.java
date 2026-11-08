package dev.cluster.tx.autoconfigure;

import dev.cluster.tx.core.SagaStateMachine;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/** Initial, non-executing auto-configuration. The coordinator is added later. */
@AutoConfiguration
public class ClusterTxAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    SagaStateMachine sagaStateMachine() {
        return new SagaStateMachine();
    }
}
