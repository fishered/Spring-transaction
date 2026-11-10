package dev.cluster.tx.core;

/**
 * Business code executed inside the participant's local database transaction.
 * It must use the same database and transaction manager as the operation log.
 * Returning REJECTED must leave no business side effects; throw to roll back
 * if a partial change has already occurred. Compensation must reverse the
 * applied business change and may be invoked after an uncertain response.
 */
public interface ParticipantAction {
    ActionResult execute(SagaCommand command);

    void compensate(SagaCommand command);
}
