package dev.cluster.tx.jdbc;

import dev.cluster.tx.core.SagaSnapshot;
import dev.cluster.tx.core.SagaStatus;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.NoSuchElementException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** JDBC state store. Every progress update checks the lease epoch before changing a step. */
public final class SagaJdbcRepository {
    private static final RowMapper<SagaInstanceRow> INSTANCE_MAPPER = (rs, rowNum) -> instance(rs);
    private static final RowMapper<SagaStepRow> STEP_MAPPER = (rs, rowNum) -> step(rs);

    private final JdbcTemplate jdbc;
    private final TransactionTemplate transactions;

    public SagaJdbcRepository(JdbcTemplate jdbc, PlatformTransactionManager manager) {
        this.jdbc = jdbc;
        this.transactions = new TransactionTemplate(manager);
    }

    public SagaInstanceRow findByBusinessKey(String name, String key) {
        return jdbc.query("SELECT * FROM saga_instance WHERE workflow_name = ? AND business_key = ?",
                INSTANCE_MAPPER, name, key).stream().findFirst().orElse(null);
    }

    /** Use the coordinator database clock for leases and retry deadlines. */
    public long now() {
        return jdbc.queryForObject("SELECT CURRENT_TIMESTAMP", (rs, rowNum) -> rs.getTimestamp(1).getTime());
    }

    public void insert(SagaInstanceRow instance, List<String> stepNames, long now) {
        transactions.executeWithoutResult(status -> {
            jdbc.update("INSERT INTO saga_instance (id, workflow_name, workflow_version, business_key, "
                    + "payload, payload_hash, status, current_step, version, lease_owner, lease_until, "
                    + "lease_epoch, next_run_at, last_error, created_at, updated_at) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, NULL, 0, 0, ?, NULL, ?, ?)",
                    instance.id(), instance.workflowName(), instance.workflowVersion(),
                    instance.businessKey(), instance.payload(), instance.payloadHash(),
                    SagaStatus.RUNNING.name(), now, now, now);
            for (int i = 0; i < stepNames.size(); i++) {
                jdbc.update("INSERT INTO saga_step (saga_id, step_index, step_name, operation_id, "
                        + "execute_state, compensate_state, execute_attempts, compensate_attempts) "
                        + "VALUES (?, ?, ?, ?, 'PENDING', 'PENDING', 0, 0)",
                        instance.id(), i, stepNames.get(i), instance.id() + ":" + i);
            }
        });
    }

    public SagaInstanceRow instance(String id) {
        return jdbc.query("SELECT * FROM saga_instance WHERE id = ?", INSTANCE_MAPPER, id)
                .stream().findFirst().orElseThrow(() -> new NoSuchElementException("Saga not found: " + id));
    }

    public SagaSnapshot snapshot(String id) {
        SagaInstanceRow row = instance(id);
        return new SagaSnapshot(row.id(), row.workflowName(), row.workflowVersion(),
                row.businessKey(), row.status(), row.currentStep(), row.lastError());
    }

    public SagaStepRow step(String id, int index) {
        return jdbc.query("SELECT * FROM saga_step WHERE saga_id = ? AND step_index = ?",
                STEP_MAPPER, id, index).stream().findFirst()
                .orElseThrow(() -> new NoSuchElementException("Saga step not found: " + id + ":" + index));
    }

    public List<String> due(long now, int limit) {
        return jdbc.query(connection -> {
            PreparedStatement statement = connection.prepareStatement(
                    "SELECT id FROM saga_instance WHERE status IN ('RUNNING','COMPENSATING') "
                            + "AND next_run_at <= ? AND lease_until <= ? ORDER BY next_run_at, created_at");
            statement.setLong(1, now);
            statement.setLong(2, now);
            statement.setMaxRows(limit);
            return statement;
        }, (rs, rowNum) -> rs.getString(1));
    }

    public SagaInstanceRow claim(String id, String owner, long now, long leaseMillis) {
        return transactions.execute(status -> {
            int changed = jdbc.update("UPDATE saga_instance SET lease_owner = ?, lease_until = ?, "
                            + "lease_epoch = lease_epoch + 1 WHERE id = ? "
                            + "AND status IN ('RUNNING','COMPENSATING') AND next_run_at <= ? AND lease_until <= ?",
                    owner, now + leaseMillis, id, now, now);
            if (changed != 1) {
                return null;
            }
            return jdbc.query("SELECT * FROM saga_instance WHERE id = ? AND lease_owner = ?",
                    INSTANCE_MAPPER, id, owner).stream().findFirst()
                    .orElseThrow(() -> new IllegalStateException("Claimed Saga lease disappeared: " + id));
        });
    }

    public void progress(SagaInstanceRow lease, SagaProgress next, long now) {
        transactions.executeWithoutResult(status -> {
            int changed = jdbc.update("UPDATE saga_instance SET status = ?, current_step = ?, "
                            + "next_run_at = ?, last_error = ?, version = version + 1, "
                            + "lease_owner = NULL, lease_until = 0, updated_at = ? "
                            + "WHERE id = ? AND lease_owner = ? AND lease_epoch = ?",
                    next.status().name(), next.currentStep(), next.nextRunAt(), next.lastError(),
                    now, lease.id(), lease.leaseOwner(), lease.leaseEpoch());
            if (changed != 1) {
                throw new IllegalStateException("Saga lease lost: " + lease.id());
            }
            if (next.stepIndex() >= 0) {
                int updatedStep = jdbc.update("UPDATE saga_step SET execute_state = ?, compensate_state = ?, "
                                + "execute_attempts = ?, compensate_attempts = ? "
                                + "WHERE saga_id = ? AND step_index = ?",
                        next.executeState(), next.compensateState(), next.executeAttempts(),
                        next.compensateAttempts(), lease.id(), next.stepIndex());
                if (updatedStep != 1) {
                    throw new IllegalStateException("Saga step disappeared: " + lease.id());
                }
            }
        });
    }

    public void resume(String id, SagaStatus direction, long now) {
        transactions.executeWithoutResult(status -> {
            int changed = jdbc.update("UPDATE saga_instance SET status = ?, next_run_at = ?, "
                            + "last_error = NULL, lease_until = 0, lease_owner = NULL, "
                            + "version = version + 1, updated_at = ? "
                            + "WHERE id = ? AND status = 'NEEDS_ATTENTION'",
                    direction.name(), now, now, id);
            if (changed != 1) {
                throw new IllegalStateException("Saga is not in NEEDS_ATTENTION: " + id);
            }
            SagaInstanceRow row = instance(id);
            if (row.currentStep() >= 0) {
                if (direction == SagaStatus.RUNNING) {
                    jdbc.update("UPDATE saga_step SET execute_attempts = 0 WHERE saga_id = ? AND step_index = ?",
                            id, row.currentStep());
                } else {
                    jdbc.update("UPDATE saga_step SET compensate_attempts = 0 WHERE saga_id = ? AND step_index = ?",
                            id, row.currentStep());
                }
            }
        });
    }

    private static SagaInstanceRow instance(ResultSet rs) throws SQLException {
        return new SagaInstanceRow(rs.getString("id"), rs.getString("workflow_name"),
                rs.getInt("workflow_version"), rs.getString("business_key"),
                rs.getString("payload"), rs.getString("payload_hash"),
                SagaStatus.valueOf(rs.getString("status")), rs.getInt("current_step"),
                rs.getLong("version"), rs.getString("lease_owner"),
                rs.getLong("lease_epoch"), rs.getString("last_error"));
    }

    private static SagaStepRow step(ResultSet rs) throws SQLException {
        return new SagaStepRow(rs.getString("saga_id"), rs.getInt("step_index"),
                rs.getString("step_name"), rs.getString("operation_id"),
                rs.getString("execute_state"), rs.getString("compensate_state"),
                rs.getInt("execute_attempts"), rs.getInt("compensate_attempts"));
    }
}
