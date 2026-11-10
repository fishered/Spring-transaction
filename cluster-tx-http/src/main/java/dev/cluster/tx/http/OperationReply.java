package dev.cluster.tx.http;

import dev.cluster.tx.core.OperationState;

public record OperationReply(OperationState state) { }
