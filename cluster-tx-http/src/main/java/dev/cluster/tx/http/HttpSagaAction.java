package dev.cluster.tx.http;

import dev.cluster.tx.core.ActionResult;
import dev.cluster.tx.core.OperationState;
import dev.cluster.tx.core.SagaAction;
import dev.cluster.tx.core.SagaCommand;
import java.util.Objects;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;

/**
 * JSON wire adapter. Supply a RestClient with authentication and a request timeout
 * shorter than the coordinator lease. The participant controls endpoint exposure.
 */
public final class HttpSagaAction implements SagaAction {
    private final RestClient client;
    private final String path;

    public HttpSagaAction(RestClient client, String path) {
        this.client = Objects.requireNonNull(client, "client");
        if (path == null || path.isBlank() || !path.startsWith("/")) {
            throw new IllegalArgumentException("Action path must start with /");
        }
        this.path = path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
    }

    @Override
    public ActionResult execute(SagaCommand command) {
        ExecutionReply reply = client.post().uri(path + "/execute")
                .contentType(MediaType.APPLICATION_JSON).body(command)
                .retrieve().body(ExecutionReply.class);
        return Objects.requireNonNull(reply, "execute reply").result();
    }

    @Override
    public OperationState query(SagaCommand command) {
        OperationReply reply = client.get().uri(path + "/operations/{operationId}", command.operationId())
                .retrieve().body(OperationReply.class);
        return Objects.requireNonNull(reply, "query reply").state();
    }

    @Override
    public OperationState compensate(SagaCommand command) {
        OperationReply reply = client.post().uri(path + "/compensate")
                .contentType(MediaType.APPLICATION_JSON).body(command)
                .retrieve().body(OperationReply.class);
        return Objects.requireNonNull(reply, "compensate reply").state();
    }
}
