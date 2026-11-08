package dev.cluster.tx.core;

import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;

/** Validates global workflow transitions; persistence and execution are separate concerns. */
public final class SagaStateMachine {
    private static final Map<SagaStatus, EnumSet<SagaStatus>> ALLOWED = Map.of(
            SagaStatus.RUNNING, EnumSet.of(SagaStatus.COMPENSATING, SagaStatus.SUCCEEDED,
                    SagaStatus.NEEDS_ATTENTION),
            SagaStatus.COMPENSATING, EnumSet.of(SagaStatus.COMPENSATED, SagaStatus.NEEDS_ATTENTION),
            SagaStatus.NEEDS_ATTENTION, EnumSet.of(SagaStatus.RUNNING, SagaStatus.COMPENSATING)
    );

    public boolean canTransition(SagaStatus from, SagaStatus to) {
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");
        return ALLOWED.getOrDefault(from, EnumSet.noneOf(SagaStatus.class)).contains(to);
    }

    public void requireTransition(SagaStatus from, SagaStatus to) {
        if (!canTransition(from, to)) {
            throw new IllegalStateException("Invalid Saga transition: " + from + " -> " + to);
        }
    }
}
