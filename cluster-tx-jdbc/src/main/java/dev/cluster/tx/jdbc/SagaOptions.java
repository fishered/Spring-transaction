package dev.cluster.tx.jdbc;

/** Runtime limits. The action's network timeout should be shorter than the lease. */
public record SagaOptions(long leaseMillis, long retryDelayMillis, int maxAttempts, int batchSize) {
    public SagaOptions {
        if (leaseMillis < 1 || retryDelayMillis < 0 || maxAttempts < 1 || batchSize < 1) {
            throw new IllegalArgumentException("Invalid Saga options");
        }
    }

    public static SagaOptions defaults() {
        return new SagaOptions(30_000, 1_000, 12, 32);
    }
}
