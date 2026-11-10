package dev.cluster.tx.core;

public interface SagaClient {
    /** Joins an existing local transaction when the configured manager owns it. */
    SagaHandle start(String workflowName, String businessKey, String payload);

    SagaSnapshot get(String sagaId);

    /** Operator action after investigating an instance in NEEDS_ATTENTION. */
    void resume(String sagaId, SagaStatus direction);
}
