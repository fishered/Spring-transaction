package dev.cluster.tx.core;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Immutable, versioned workflow definition registered as a Spring bean. */
public record SagaDefinition(String name, int version, List<SagaStep> steps) {
    public SagaDefinition {
        if (name == null || name.isBlank() || name.length() > 100) {
            throw new IllegalArgumentException("Workflow name must contain 1 to 100 characters");
        }
        if (version < 1) {
            throw new IllegalArgumentException("Workflow version must be positive");
        }
        Objects.requireNonNull(steps, "steps");
        steps = List.copyOf(steps);
        if (steps.isEmpty()) {
            throw new IllegalArgumentException("Workflow must contain a step");
        }
        Set<String> names = new HashSet<>();
        for (SagaStep step : steps) {
            if (!names.add(step.name())) {
                throw new IllegalArgumentException("Duplicate step name: " + step.name());
            }
        }
    }
}
