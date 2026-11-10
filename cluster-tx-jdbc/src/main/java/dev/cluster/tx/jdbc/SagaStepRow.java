package dev.cluster.tx.jdbc;

record SagaStepRow(String sagaId, int index, String name, String operationId,
                   String executeState, String compensateState,
                   int executeAttempts, int compensateAttempts) { }
