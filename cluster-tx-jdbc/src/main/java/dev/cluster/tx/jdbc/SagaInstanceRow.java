package dev.cluster.tx.jdbc;

import dev.cluster.tx.core.SagaStatus;

record SagaInstanceRow(String id, String workflowName, int workflowVersion,
                       String businessKey, String payload, String payloadHash,
                       SagaStatus status, int currentStep, long version,
                       String leaseOwner, long leaseEpoch, String lastError) { }
