package dev.cluster.tx.core;

/** A definite result of a forward business action. Transport failures are not a rejection. */
public enum ActionResult {
    APPLIED,
    REJECTED
}
