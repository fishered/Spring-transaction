package dev.cluster.tx.autoconfigure;

import dev.cluster.tx.jdbc.SagaOptions;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "cluster.tx")
public class ClusterTxProperties {
    private boolean schedulerEnabled = true;
    private long pollDelayMs = 1_000;
    private long leaseMs = 30_000;
    private long retryDelayMs = 1_000;
    private int maxAttempts = 12;
    private int batchSize = 32;

    public SagaOptions toOptions() {
        return new SagaOptions(leaseMs, retryDelayMs, maxAttempts, batchSize);
    }

    public boolean isSchedulerEnabled() { return schedulerEnabled; }
    public void setSchedulerEnabled(boolean schedulerEnabled) { this.schedulerEnabled = schedulerEnabled; }
    public long getPollDelayMs() { return pollDelayMs; }
    public void setPollDelayMs(long pollDelayMs) { this.pollDelayMs = pollDelayMs; }
    public long getLeaseMs() { return leaseMs; }
    public void setLeaseMs(long leaseMs) { this.leaseMs = leaseMs; }
    public long getRetryDelayMs() { return retryDelayMs; }
    public void setRetryDelayMs(long retryDelayMs) { this.retryDelayMs = retryDelayMs; }
    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }
}
