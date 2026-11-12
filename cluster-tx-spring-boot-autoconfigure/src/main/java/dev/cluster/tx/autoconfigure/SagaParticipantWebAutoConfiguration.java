package dev.cluster.tx.autoconfigure;

import dev.cluster.tx.core.ParticipantAction;
import dev.cluster.tx.core.SagaParticipant;
import dev.cluster.tx.http.ParticipantHttpHandler;
import dev.cluster.tx.jdbc.ParticipantExecutor;
import java.util.HashMap;
import java.util.Map;
import org.springframework.aop.support.AopUtils;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.web.servlet.DispatcherServlet;

/** HTTP endpoints are enabled explicitly in servlet applications. */
@AutoConfiguration(after = ClusterTxAutoConfiguration.class)
@ConditionalOnClass(DispatcherServlet.class)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnProperty(prefix = "cluster.tx.http", name = "enabled", havingValue = "true")
@ConditionalOnBean(ParticipantAction.class)
public class SagaParticipantWebAutoConfiguration {
    @Bean
    SagaParticipantController sagaParticipantController(ParticipantExecutor executor,
                                                         Map<String, ParticipantAction> actions) {
        Map<String, ParticipantHttpHandler> handlers = new HashMap<>();
        for (ParticipantAction action : actions.values()) {
            SagaParticipant annotation = AnnotationUtils.findAnnotation(
                    AopUtils.getTargetClass(action), SagaParticipant.class);
            if (annotation == null) {
                continue;
            }
            String name = annotation.value();
            if (!name.matches("[a-zA-Z0-9][a-zA-Z0-9_-]{0,99}")) {
                throw new IllegalArgumentException("Invalid Saga participant name: " + name);
            }
            if (handlers.putIfAbsent(name, new ParticipantHttpHandler(executor, action)) != null) {
                throw new IllegalArgumentException("Duplicate Saga participant name: " + name);
            }
        }
        if (handlers.isEmpty()) {
            throw new IllegalStateException("HTTP enabled but no @SagaParticipant action found");
        }
        return new SagaParticipantController(Map.copyOf(handlers));
    }
}
