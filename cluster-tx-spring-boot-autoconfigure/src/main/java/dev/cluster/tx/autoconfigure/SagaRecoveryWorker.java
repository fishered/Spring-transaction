package dev.cluster.tx.autoconfigure;

import dev.cluster.tx.jdbc.SagaCoordinator;
import org.springframework.scheduling.annotation.Scheduled;

public final class SagaRecoveryWorker {
    private final SagaCoordinator coordinator;

    public SagaRecoveryWorker(SagaCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    @Scheduled(fixedDelayString = "${cluster.tx.poll-delay-ms:1000}")
    public void poll() {
        coordinator.tick();
    }
}
