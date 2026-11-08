package dev.cluster.tx.core;

/** Global workflow status. A step can progress while the workflow remains RUNNING. */
public enum SagaStatus {
    RUNNING,
    COMPENSATING,
    SUCCEEDED,
    COMPENSATED,
    NEEDS_ATTENTION;

    public boolean isTerminal() {
        return this == SUCCEEDED || this == COMPENSATED;
    }
}
