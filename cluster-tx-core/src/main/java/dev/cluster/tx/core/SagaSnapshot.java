package dev.cluster.tx.core;

public record SagaSnapshot(String id, String workflowName, int workflowVersion,
                           String businessKey, SagaStatus status, int currentStep,
                           String lastError) { }
