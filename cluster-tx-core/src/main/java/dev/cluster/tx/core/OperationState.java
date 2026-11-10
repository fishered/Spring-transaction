package dev.cluster.tx.core;

/** Durable result reported by a participant for one stable operation id. */
public enum OperationState {
    NOT_FOUND,
    APPLIED,
    REJECTED,
    CANCELLED,
    COMPENSATED
}
