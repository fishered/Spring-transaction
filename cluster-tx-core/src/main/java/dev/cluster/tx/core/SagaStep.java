package dev.cluster.tx.core;

import java.util.Objects;

public record SagaStep(String name, SagaAction action) {
    public SagaStep {
        if (name == null || name.isBlank() || name.length() > 100) {
            throw new IllegalArgumentException("Step name must contain 1 to 100 characters");
        }
        Objects.requireNonNull(action, "action");
    }
}
