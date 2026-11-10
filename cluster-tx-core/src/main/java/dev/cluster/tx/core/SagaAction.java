package dev.cluster.tx.core;

/** Adapter implemented by the coordinator application to call a participant. */
public interface SagaAction {
    ActionResult execute(SagaCommand command);

    OperationState query(SagaCommand command);

    /** Returns CANCELLED if execution never happened, or COMPENSATED if it was undone. */
    OperationState compensate(SagaCommand command);
}
