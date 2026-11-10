package dev.cluster.tx.jdbc;

import dev.cluster.tx.core.ActionResult;
import dev.cluster.tx.core.OperationState;
import dev.cluster.tx.core.SagaAction;
import dev.cluster.tx.core.SagaClient;
import dev.cluster.tx.core.SagaCommand;
import dev.cluster.tx.core.SagaDefinition;
import dev.cluster.tx.core.SagaHandle;
import dev.cluster.tx.core.SagaSnapshot;
import dev.cluster.tx.core.SagaStatus;
import dev.cluster.tx.core.SagaStep;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Recoverable, sequential Saga coordinator. Each call to {@link #tick()} processes one
 * step per due instance, so no database transaction is held across a remote call.
 */
public final class SagaCoordinator implements SagaClient {
    private static final Logger LOG = Logger.getLogger(SagaCoordinator.class.getName());

    private final SagaJdbcRepository store;
    private final SagaOptions options;
    private final String workerId = UUID.randomUUID().toString();
    private final Map<String, SagaDefinition> definitions = new HashMap<>();
    private final Map<String, SagaDefinition> latest = new HashMap<>();

    public SagaCoordinator(SagaJdbcRepository store, Collection<SagaDefinition> definitions,
                           SagaOptions options) {
        this.store = Objects.requireNonNull(store, "store");
        this.options = Objects.requireNonNull(options, "options");
        for (SagaDefinition definition : definitions) {
            String key = definition.name() + "#" + definition.version();
            if (this.definitions.putIfAbsent(key, definition) != null) {
                throw new IllegalArgumentException("Duplicate workflow definition: " + key);
            }
            latest.merge(definition.name(), definition,
                    (oldValue, newValue) -> oldValue.version() > newValue.version() ? oldValue : newValue);
        }
    }

    @Override
    public SagaHandle start(String workflowName, String businessKey, String payload) {
        SagaDefinition definition = latest.get(workflowName);
        if (definition == null) {
            throw new IllegalArgumentException("Unknown workflow: " + workflowName);
        }
        if (businessKey == null || businessKey.isBlank() || businessKey.length() > 200) {
            throw new IllegalArgumentException("Business key must contain 1 to 200 characters");
        }
        if (payload == null || payload.length() > 8192) {
            throw new IllegalArgumentException("Payload must contain at most 8192 characters");
        }
        String digest = sha256(payload);
        SagaInstanceRow existing = store.findByBusinessKey(workflowName, businessKey);
        if (existing != null) {
            return existingHandle(existing, digest);
        }
        String id = UUID.randomUUID().toString();
        long now = store.now();
        SagaInstanceRow row = new SagaInstanceRow(id, workflowName, definition.version(),
                businessKey, payload, digest, SagaStatus.RUNNING, 0, 0, null, 0, null);
        try {
            store.insert(row, definition.steps().stream().map(SagaStep::name).toList(), now);
            return new SagaHandle(id, SagaStatus.RUNNING);
        } catch (DuplicateKeyException race) {
            // A duplicate insert aborts the ambient transaction on some databases.
            if (TransactionSynchronizationManager.isActualTransactionActive()) {
                throw race;
            }
            SagaInstanceRow winner = store.findByBusinessKey(workflowName, businessKey);
            if (winner == null) {
                throw race;
            }
            return existingHandle(winner, digest);
        }
    }

    @Override
    public SagaSnapshot get(String sagaId) {
        return store.snapshot(sagaId);
    }

    @Override
    public void resume(String sagaId, SagaStatus direction) {
        if (direction != SagaStatus.RUNNING && direction != SagaStatus.COMPENSATING) {
            throw new IllegalArgumentException("Resume direction must be RUNNING or COMPENSATING");
        }
        store.resume(sagaId, direction, store.now());
    }

    /** Scans and processes a bounded number of due instances. Safe to call on every node. */
    public int tick() {
        int processed = 0;
        for (String id : store.due(store.now(), options.batchSize())) {
            try {
                if (process(id)) {
                    processed++;
                }
            } catch (RuntimeException failure) {
                LOG.log(Level.WARNING, "Saga processing failed for " + id + "; lease expiry will recover it", failure);
            }
        }
        return processed;
    }

    /** Claims and processes one due instance. Returns false if another node owns it. */
    public boolean process(String sagaId) {
        SagaInstanceRow lease = store.claim(sagaId, workerId, store.now(), options.leaseMillis());
        if (lease == null) {
            return false;
        }
        SagaDefinition definition = definitions.get(lease.workflowName() + "#" + lease.workflowVersion());
        if (definition == null) {
            attention(lease, null, "Workflow version is not registered");
            return true;
        }
        if (lease.status() == SagaStatus.RUNNING) {
            forward(lease, definition);
        } else {
            backward(lease, definition);
        }
        return true;
    }

    private void forward(SagaInstanceRow lease, SagaDefinition definition) {
        int index = lease.currentStep();
        if (index >= definition.steps().size()) {
            save(lease, null, SagaStatus.SUCCEEDED, index, store.now(), null,
                    null, null, 0, 0);
            return;
        }
        SagaStepRow step = store.step(lease.id(), index);
        SagaStep declared = definition.steps().get(index);
        if (!declared.name().equals(step.name())) {
            attention(lease, step, "Workflow step differs from persisted definition");
            return;
        }
        SagaCommand command = command(lease, step);
        if (!"UNKNOWN".equals(step.executeState()) && step.executeAttempts() >= options.maxAttempts()) {
            attention(lease, step, "Forward attempt limit reached; result needs investigation");
            return;
        }
        if ("UNKNOWN".equals(step.executeState())) {
            OperationState state;
            try {
                state = declared.action().query(command);
            } catch (RuntimeException unknown) {
                if (step.executeAttempts() >= options.maxAttempts()) {
                    attention(lease, step, "Forward result query failed after attempt limit: " + error(unknown));
                } else {
                    forwardUnknown(lease, step, unknown);
                }
                return;
            }
            if (state == OperationState.APPLIED) {
                forwardApplied(lease, step, definition, step.executeAttempts());
            } else if (state == OperationState.REJECTED) {
                forwardRejected(lease, step, step.executeAttempts());
            } else if (state == OperationState.NOT_FOUND) {
                if (step.executeAttempts() >= options.maxAttempts()) {
                    attention(lease, step, "Forward attempt limit reached after result query");
                } else {
                    save(lease, step, SagaStatus.RUNNING, index,
                            store.now() + retryDelay(step.executeAttempts() + 1), null,
                            "PENDING", step.compensateState(), step.executeAttempts(),
                            step.compensateAttempts());
                }
            } else {
                attention(lease, step, "Unexpected forward result: " + state);
            }
        } else {
            ActionResult result;
            try {
                result = declared.action().execute(command);
            } catch (RuntimeException unknown) {
                forwardUnknown(lease, step, unknown);
                return;
            }
            if (result == ActionResult.APPLIED) {
                forwardApplied(lease, step, definition, step.executeAttempts() + 1);
            } else if (result == ActionResult.REJECTED) {
                forwardRejected(lease, step, step.executeAttempts() + 1);
            } else {
                attention(lease, step, "Action returned no definite result");
            }
        }
    }

    private void forwardUnknown(SagaInstanceRow lease, SagaStepRow step, RuntimeException unknown) {
        save(lease, step, SagaStatus.RUNNING, step.index(),
                store.now() + retryDelay(step.executeAttempts() + 1), error(unknown),
                "UNKNOWN", step.compensateState(), step.executeAttempts() + 1,
                step.compensateAttempts());
    }

    private void forwardApplied(SagaInstanceRow lease, SagaStepRow step,
                                SagaDefinition definition, int attempts) {
        int next = step.index() + 1;
        SagaStatus status = next == definition.steps().size() ? SagaStatus.SUCCEEDED : SagaStatus.RUNNING;
        save(lease, step, status, next, store.now(), null,
                "APPLIED", step.compensateState(), attempts, step.compensateAttempts());
    }

    private void forwardRejected(SagaInstanceRow lease, SagaStepRow step, int attempts) {
        int previous = step.index() - 1;
        SagaStatus status = previous < 0 ? SagaStatus.COMPENSATED : SagaStatus.COMPENSATING;
        save(lease, step, status, previous, store.now(), "Business action rejected",
                "REJECTED", step.compensateState(), attempts, step.compensateAttempts());
    }

    private void backward(SagaInstanceRow lease, SagaDefinition definition) {
        int index = lease.currentStep();
        if (index < 0) {
            save(lease, null, SagaStatus.COMPENSATED, index, store.now(), null,
                    null, null, 0, 0);
            return;
        }
        if (index >= definition.steps().size()) {
            attention(lease, null, "Compensation step is outside the workflow definition");
            return;
        }
        SagaStepRow step = store.step(lease.id(), index);
        if (!definition.steps().get(index).name().equals(step.name())
                || !"APPLIED".equals(step.executeState())) {
            attention(lease, step, "Cannot compensate an unconfirmed step");
            return;
        }
        if (!"UNKNOWN".equals(step.compensateState()) && step.compensateAttempts() >= options.maxAttempts()) {
            attention(lease, step, "Compensation attempt limit reached");
            return;
        }
        SagaAction action = definition.steps().get(index).action();
        SagaCommand command = command(lease, step);
        if ("UNKNOWN".equals(step.compensateState())) {
            OperationState state;
            try {
                state = action.query(command);
            } catch (RuntimeException unknown) {
                if (step.compensateAttempts() >= options.maxAttempts()) {
                    attention(lease, step, "Compensation result query failed after attempt limit: " + error(unknown));
                } else {
                    backwardUnknown(lease, step, unknown);
                }
                return;
            }
            if (state == OperationState.COMPENSATED || state == OperationState.CANCELLED) {
                compensated(lease, step, step.compensateAttempts());
            } else if (state == OperationState.APPLIED) {
                if (step.compensateAttempts() >= options.maxAttempts()) {
                    attention(lease, step, "Compensation attempt limit reached after result query");
                } else {
                    save(lease, step, SagaStatus.COMPENSATING, index,
                            store.now() + retryDelay(step.compensateAttempts() + 1), null,
                            step.executeState(), "PENDING", step.executeAttempts(),
                            step.compensateAttempts());
                }
            } else {
                attention(lease, step, "Unexpected compensation result: " + state);
            }
        } else {
            OperationState state;
            try {
                state = action.compensate(command);
            } catch (RuntimeException unknown) {
                backwardUnknown(lease, step, unknown);
                return;
            }
            if (state == OperationState.COMPENSATED || state == OperationState.CANCELLED) {
                compensated(lease, step, step.compensateAttempts() + 1);
            } else {
                attention(lease, step, "Compensation returned: " + state);
            }
        }
    }

    private void backwardUnknown(SagaInstanceRow lease, SagaStepRow step, RuntimeException unknown) {
        save(lease, step, SagaStatus.COMPENSATING, step.index(),
                store.now() + retryDelay(step.compensateAttempts() + 1), error(unknown),
                step.executeState(), "UNKNOWN", step.executeAttempts(),
                step.compensateAttempts() + 1);
    }

    private void compensated(SagaInstanceRow lease, SagaStepRow step, int attempts) {
        int previous = step.index() - 1;
        SagaStatus status = previous < 0 ? SagaStatus.COMPENSATED : SagaStatus.COMPENSATING;
        save(lease, step, status, previous, store.now(), null,
                step.executeState(), "DONE", step.executeAttempts(), attempts);
    }

    private void attention(SagaInstanceRow lease, SagaStepRow step, String reason) {
        save(lease, step, SagaStatus.NEEDS_ATTENTION, lease.currentStep(),
                store.now(), reason,
                step == null ? null : step.executeState(),
                step == null ? null : step.compensateState(),
                step == null ? 0 : step.executeAttempts(),
                step == null ? 0 : step.compensateAttempts());
    }

    private void save(SagaInstanceRow lease, SagaStepRow step, SagaStatus status,
                      int currentStep, long nextRunAt, String lastError,
                      String executeState, String compensateState,
                      int executeAttempts, int compensateAttempts) {
        store.progress(lease, new SagaProgress(status, currentStep, nextRunAt,
                lastError, step == null ? -1 : step.index(), executeState,
                compensateState, executeAttempts, compensateAttempts), store.now());
    }

    private long retryDelay(int attempts) {
        long multiplier = 1L << Math.min(attempts - 1, 6);
        return Math.min(options.retryDelayMillis(), 60_000L / multiplier) * multiplier;
    }

    private static SagaCommand command(SagaInstanceRow instance, SagaStepRow step) {
        return new SagaCommand(instance.id(), step.operationId(),
                instance.businessKey(), instance.payload());
    }

    private static SagaHandle existingHandle(SagaInstanceRow existing, String digest) {
        if (!existing.payloadHash().equals(digest)) {
            throw new IllegalArgumentException("Business key is already used with a different payload");
        }
        return new SagaHandle(existing.id(), existing.status());
    }

    private static String sha256(String payload) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(payload.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static String error(RuntimeException failure) {
        String message = failure.getClass().getSimpleName() + ": " + failure.getMessage();
        return message.length() > 1024 ? message.substring(0, 1024) : message;
    }
}
