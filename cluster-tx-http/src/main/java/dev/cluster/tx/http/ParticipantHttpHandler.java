package dev.cluster.tx.http;

import dev.cluster.tx.core.ParticipantAction;
import dev.cluster.tx.core.SagaCommand;
import dev.cluster.tx.jdbc.ParticipantExecutor;
import java.util.Objects;

/** Bind these methods to authenticated HTTP routes in the participant application. */
public final class ParticipantHttpHandler {
    private final ParticipantExecutor executor;
    private final ParticipantAction action;

    public ParticipantHttpHandler(ParticipantExecutor executor, ParticipantAction action) {
        this.executor = Objects.requireNonNull(executor, "executor");
        this.action = Objects.requireNonNull(action, "action");
    }

    public ExecutionReply execute(SagaCommand command) {
        return new ExecutionReply(executor.execute(command, action));
    }

    public OperationReply query(String operationId) {
        return new OperationReply(executor.query(operationId));
    }

    public OperationReply compensate(SagaCommand command) {
        return new OperationReply(executor.compensate(command, action));
    }
}
