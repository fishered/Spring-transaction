package dev.cluster.tx.jdbc;

import dev.cluster.tx.core.ActionResult;
import dev.cluster.tx.core.OperationState;
import dev.cluster.tx.core.ParticipantAction;
import dev.cluster.tx.core.SagaCommand;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.HexFormat;
import java.util.Objects;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Executes participant business code and its idempotency fence in one local transaction.
 * The supplied action must use the same transaction manager and database.
 */
public final class ParticipantExecutor {
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;
    private final Clock clock;

    public ParticipantExecutor(JdbcTemplate jdbc, PlatformTransactionManager manager) {
        this(jdbc, manager, Clock.systemUTC());
    }

    public ParticipantExecutor(JdbcTemplate jdbc, PlatformTransactionManager manager, Clock clock) {
        this.jdbc = Objects.requireNonNull(jdbc, "jdbc");
        this.transactions = new TransactionTemplate(Objects.requireNonNull(manager, "manager"));
        this.transactions.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public ActionResult execute(SagaCommand command, ParticipantAction action) {
        Objects.requireNonNull(command, "command");
        Objects.requireNonNull(action, "action");
        for (int retry = 0; retry < 3; retry++) {
            try {
                return transactions.execute(status -> {
                    OperationRow existing = lockedOperation(command.operationId());
                    if (existing != null) {
                        verifyFingerprint(existing, command);
                        return existing.state() == OperationState.APPLIED
                                ? ActionResult.APPLIED : ActionResult.REJECTED;
                    }
                    insertOperation(command, "PENDING");
                    ActionResult result = Objects.requireNonNull(action.execute(command), "action result");
                    jdbc.update("UPDATE participant_operation SET state = ?, updated_at = ? WHERE operation_id = ?",
                            result.name(), clock.millis(), command.operationId());
                    return result;
                });
            } catch (OperationRaceException race) {
                // A concurrent execute or cancellation won. The failed transaction is rolled back.
                OperationRow winner = operation(command.operationId());
                if (winner != null) {
                    verifyFingerprint(winner, command);
                    return winner.state() == OperationState.APPLIED
                            ? ActionResult.APPLIED : ActionResult.REJECTED;
                }
            }
        }
        throw new IllegalStateException("Cannot resolve participant operation race: " + command.operationId());
    }

    public OperationState compensate(SagaCommand command, ParticipantAction action) {
        Objects.requireNonNull(command, "command");
        Objects.requireNonNull(action, "action");
        for (int retry = 0; retry < 3; retry++) {
            try {
                return transactions.execute(status -> {
                    OperationRow row = lockedOperation(command.operationId());
                    if (row == null) {
                        insertOperation(command, "CANCELLED");
                        return OperationState.CANCELLED;
                    }
                    verifyFingerprint(row, command);
                    OperationState existing = row.state();
                    if (existing == OperationState.APPLIED) {
                        action.compensate(command);
                        jdbc.update("UPDATE participant_operation SET state = 'COMPENSATED', updated_at = ? "
                                        + "WHERE operation_id = ?",
                                clock.millis(), command.operationId());
                        return OperationState.COMPENSATED;
                    }
                    if (existing == OperationState.REJECTED) {
                        jdbc.update("UPDATE participant_operation SET state = 'CANCELLED', updated_at = ? "
                                        + "WHERE operation_id = ?",
                                clock.millis(), command.operationId());
                        return OperationState.CANCELLED;
                    }
                    return existing;
                });
            } catch (OperationRaceException race) {
                OperationRow winner = operation(command.operationId());
                if (winner != null) {
                    verifyFingerprint(winner, command);
                    if (winner.state() == OperationState.APPLIED) {
                        continue;
                    }
                    return winner.state();
                }
            }
        }
        throw new IllegalStateException("Cannot resolve participant compensation race: " + command.operationId());
    }

    public OperationState query(String operationId) {
        Objects.requireNonNull(operationId, "operationId");
        OperationRow row = operation(operationId);
        return row == null ? OperationState.NOT_FOUND : row.state();
    }

    private OperationRow lockedOperation(String operationId) {
        return jdbc.query("SELECT state, payload_hash FROM participant_operation "
                        + "WHERE operation_id = ? FOR UPDATE",
                (rs, rowNum) -> new OperationRow(OperationState.valueOf(rs.getString(1)),
                        rs.getString(2)), operationId)
                .stream().findFirst().orElse(null);
    }

    private OperationRow operation(String operationId) {
        return jdbc.query("SELECT state, payload_hash FROM participant_operation WHERE operation_id = ?",
                (rs, rowNum) -> new OperationRow(OperationState.valueOf(rs.getString(1)),
                        rs.getString(2)), operationId).stream().findFirst().orElse(null);
    }

    private void insertOperation(SagaCommand command, String state) {
        try {
            jdbc.update("INSERT INTO participant_operation (operation_id, payload_hash, state, updated_at) "
                            + "VALUES (?, ?, ?, ?)", command.operationId(), fingerprint(command),
                    state, clock.millis());
        } catch (DuplicateKeyException race) {
            throw new OperationRaceException(race);
        }
    }

    private static void verifyFingerprint(OperationRow row, SagaCommand command) {
        if (!row.payloadHash().equals(fingerprint(command))) {
            throw new IllegalArgumentException("Operation id is already used with a different command");
        }
    }

    private static String fingerprint(SagaCommand command) {
        try {
            byte[] bytes = (command.sagaId() + "\u0000" + command.businessKey() + "\u0000"
                    + command.payload()).getBytes(StandardCharsets.UTF_8);
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private record OperationRow(OperationState state, String payloadHash) { }

    private static final class OperationRaceException extends RuntimeException {
        private OperationRaceException(DuplicateKeyException cause) {
            super(cause);
        }
    }
}
