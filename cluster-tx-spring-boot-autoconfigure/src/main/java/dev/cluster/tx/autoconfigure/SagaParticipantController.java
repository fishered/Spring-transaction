package dev.cluster.tx.autoconfigure;

import dev.cluster.tx.core.SagaCommand;
import dev.cluster.tx.http.ExecutionReply;
import dev.cluster.tx.http.OperationReply;
import dev.cluster.tx.http.ParticipantHttpHandler;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SagaParticipantController {
    private final Map<String, ParticipantHttpHandler> handlers;

    SagaParticipantController(Map<String, ParticipantHttpHandler> handlers) {
        this.handlers = handlers;
    }

    @PostMapping(path = "/cluster-tx/participants/{name}/execute", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ExecutionReply execute(@PathVariable String name, @RequestBody SagaCommand command) {
        return handler(name).execute(command);
    }

    @GetMapping("/cluster-tx/participants/{name}/operations/{operationId}")
    public OperationReply query(@PathVariable String name, @PathVariable String operationId) {
        return handler(name).query(operationId);
    }

    @PostMapping(path = "/cluster-tx/participants/{name}/compensate", consumes = MediaType.APPLICATION_JSON_VALUE)
    public OperationReply compensate(@PathVariable String name, @RequestBody SagaCommand command) {
        return handler(name).compensate(command);
    }

    private ParticipantHttpHandler handler(String name) {
        ParticipantHttpHandler handler = handlers.get(name);
        if (handler == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Unknown Saga participant: " + name);
        }
        return handler;
    }
}
