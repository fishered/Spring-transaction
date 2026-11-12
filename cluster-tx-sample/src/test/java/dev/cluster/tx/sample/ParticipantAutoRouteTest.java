package dev.cluster.tx.sample;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.cluster.tx.core.ActionResult;
import dev.cluster.tx.core.OperationState;
import dev.cluster.tx.core.SagaCommand;
import dev.cluster.tx.http.ExecutionReply;
import dev.cluster.tx.http.OperationReply;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"sample.role=participant", "spring.datasource.url=jdbc:h2:mem:auto-route-test;DB_CLOSE_DELAY=-1"})
class ParticipantAutoRouteTest {
    @Value("${local.server.port}")
    private int port;

    @Test
    void annotatedParticipantIsExposedWithIdempotentExecutionAndCompensation() {
        RestClient client = RestClient.builder().baseUrl("http://localhost:" + port).build();
        String base = "/cluster-tx/participants/inventory";
        SagaCommand command = new SagaCommand("saga-test", "operation-test", "order-test", "ok");
        for (int i = 0; i < 2; i++) {
            ExecutionReply reply = client.post().uri(base + "/execute")
                    .contentType(MediaType.APPLICATION_JSON).body(command)
                    .retrieve().body(ExecutionReply.class);
            assertEquals(ActionResult.APPLIED, reply.result());
        }
        assertEquals(9, client.get().uri("/stock").retrieve().body(Integer.class));
        OperationReply before = client.get().uri(base + "/operations/{id}", command.operationId())
                .retrieve().body(OperationReply.class);
        assertEquals(OperationState.APPLIED, before.state());
        OperationReply compensated = client.post().uri(base + "/compensate")
                .contentType(MediaType.APPLICATION_JSON).body(command)
                .retrieve().body(OperationReply.class);
        assertEquals(OperationState.COMPENSATED, compensated.state());
        assertEquals(10, client.get().uri("/stock").retrieve().body(Integer.class));
    }
}
