package dev.cluster.tx.sample;

import dev.cluster.tx.core.ActionResult;
import dev.cluster.tx.core.OperationState;
import dev.cluster.tx.core.ParticipantAction;
import dev.cluster.tx.core.SagaAction;
import dev.cluster.tx.core.SagaClient;
import dev.cluster.tx.core.SagaCommand;
import dev.cluster.tx.core.SagaDefinition;
import dev.cluster.tx.core.SagaHandle;
import dev.cluster.tx.core.SagaSnapshot;
import dev.cluster.tx.core.SagaStep;
import dev.cluster.tx.http.HttpSagaAction;
import dev.cluster.tx.jdbc.ParticipantExecutor;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClient;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "sample.role", havingValue = "coordinator")
class SampleCoordinator {
    @Bean
    SagaDefinition orderWorkflow(ParticipantExecutor executor, JdbcTemplate jdbc,
                                 @Value("${sample.participant-url}") String participantUrl) {
        ParticipantAction order = new ParticipantAction() {
            @Override public ActionResult execute(SagaCommand command) {
                jdbc.update("INSERT INTO orders (id, state) VALUES (?, 'PENDING')", command.businessKey());
                return ActionResult.APPLIED;
            }
            @Override public void compensate(SagaCommand command) {
                jdbc.update("UPDATE orders SET state = 'CANCELLED' WHERE id = ?", command.businessKey());
            }
        };
        SagaAction localOrder = new SagaAction() {
            @Override public ActionResult execute(SagaCommand command) { return executor.execute(command, order); }
            @Override public OperationState query(SagaCommand command) { return executor.query(command.operationId()); }
            @Override public OperationState compensate(SagaCommand command) { return executor.compensate(command, order); }
        };
        ParticipantAction confirm = new ParticipantAction() {
            @Override public ActionResult execute(SagaCommand command) {
                int changed = jdbc.update("UPDATE orders SET state = 'CONFIRMED' WHERE id = ? AND state = 'PENDING'",
                        command.businessKey());
                return changed == 1 ? ActionResult.APPLIED : ActionResult.REJECTED;
            }
            @Override public void compensate(SagaCommand command) {
                jdbc.update("UPDATE orders SET state = 'CANCELLED' WHERE id = ?", command.businessKey());
            }
        };
        SagaAction confirmOrder = new SagaAction() {
            @Override public ActionResult execute(SagaCommand command) { return executor.execute(command, confirm); }
            @Override public OperationState query(SagaCommand command) { return executor.query(command.operationId()); }
            @Override public OperationState compensate(SagaCommand command) { return executor.compensate(command, confirm); }
        };
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(2_000);
        requestFactory.setReadTimeout(5_000);
        SagaAction inventory = new HttpSagaAction(RestClient.builder().baseUrl(participantUrl)
                .requestFactory(requestFactory).build(), "/tx/inventory");
        return new SagaDefinition("order", 1,
                List.of(new SagaStep("create-order", localOrder), new SagaStep("reserve-stock", inventory),
                        new SagaStep("confirm-order", confirmOrder)));
    }

    @RestController
    @ConditionalOnProperty(name = "sample.role", havingValue = "coordinator")
    static class OrdersController {
        private final SagaClient sagas;
        private final JdbcTemplate jdbc;

        OrdersController(SagaClient sagas, JdbcTemplate jdbc) {
            this.sagas = sagas;
            this.jdbc = jdbc;
        }

        @PostMapping("/orders/{orderId}")
        SagaHandle start(@PathVariable String orderId, @RequestBody String payload) {
            return sagas.start("order", orderId, payload);
        }

        @GetMapping("/sagas/{sagaId}")
        SagaSnapshot saga(@PathVariable String sagaId) {
            return sagas.get(sagaId);
        }

        @GetMapping("/orders/{orderId}")
        List<String> order(@PathVariable String orderId) {
            return jdbc.query("SELECT state FROM orders WHERE id = ?", (rs, row) -> rs.getString(1), orderId);
        }
    }
}
