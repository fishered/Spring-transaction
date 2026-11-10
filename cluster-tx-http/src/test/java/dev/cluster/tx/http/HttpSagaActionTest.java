package dev.cluster.tx.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

import dev.cluster.tx.core.ActionResult;
import dev.cluster.tx.core.OperationState;
import dev.cluster.tx.core.SagaCommand;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

class HttpSagaActionTest {
    @Test
    void mapsExecuteQueryAndCompensateOverJson() {
        RestClient.Builder builder = RestClient.builder().baseUrl("https://participant.example");
        MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
        HttpSagaAction action = new HttpSagaAction(builder.build(), "/tx/inventory");
        SagaCommand command = new SagaCommand("saga-1", "op-1", "order-1", "{}");

        server.expect(requestTo("https://participant.example/tx/inventory/execute"))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withSuccess("{\"result\":\"APPLIED\"}", MediaType.APPLICATION_JSON));
        server.expect(requestTo("https://participant.example/tx/inventory/operations/op-1"))
                .andExpect(method(HttpMethod.GET))
                .andRespond(withSuccess("{\"state\":\"APPLIED\"}", MediaType.APPLICATION_JSON));
        server.expect(requestTo("https://participant.example/tx/inventory/compensate"))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withSuccess("{\"state\":\"COMPENSATED\"}", MediaType.APPLICATION_JSON));
        assertEquals(ActionResult.APPLIED, action.execute(command));
        assertEquals(OperationState.APPLIED, action.query(command));
        assertEquals(OperationState.COMPENSATED, action.compensate(command));
        server.verify();
    }
}
