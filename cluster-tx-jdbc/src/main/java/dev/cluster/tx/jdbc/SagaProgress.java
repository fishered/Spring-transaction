package dev.cluster.tx.jdbc;

import dev.cluster.tx.core.SagaStatus;

record SagaProgress(SagaStatus status, int currentStep, long nextRunAt,
                    String lastError, int stepIndex, String executeState,
                    String compensateState, int executeAttempts,
                    int compensateAttempts) { }
