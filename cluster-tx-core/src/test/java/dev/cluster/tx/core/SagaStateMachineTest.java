package dev.cluster.tx.core;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SagaStateMachineTest {
    private final SagaStateMachine machine = new SagaStateMachine();

    @Test
    void completedWorkflowCannotBeReopenedOrCompensated() {
        for (SagaStatus target : SagaStatus.values()) {
            assertFalse(machine.canTransition(SagaStatus.SUCCEEDED, target));
            assertFalse(machine.canTransition(SagaStatus.COMPENSATED, target));
        }
        assertThrows(IllegalStateException.class,
                () -> machine.requireTransition(SagaStatus.SUCCEEDED, SagaStatus.COMPENSATING));
    }

    @Test
    void attentionStateCanResumeInThePreviousDirection() {
        assertTrue(machine.canTransition(SagaStatus.NEEDS_ATTENTION, SagaStatus.RUNNING));
        assertTrue(machine.canTransition(SagaStatus.NEEDS_ATTENTION, SagaStatus.COMPENSATING));
        assertFalse(machine.canTransition(SagaStatus.NEEDS_ATTENTION, SagaStatus.SUCCEEDED));
    }
}
