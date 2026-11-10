package dev.cluster.tx.core;

import java.util.Objects;

/** The same operation id is used for all retries, queries and compensation. */
public record SagaCommand(String sagaId, String operationId, String businessKey, String payload) {
    public SagaCommand {
        Objects.requireNonNull(sagaId, "sagaId");
        Objects.requireNonNull(operationId, "operationId");
        Objects.requireNonNull(businessKey, "businessKey");
        Objects.requireNonNull(payload, "payload");
    }
}
