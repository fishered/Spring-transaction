-- Apply once to the coordinator database and to each participant database.
-- A participant-only database only needs participant_operation.
CREATE TABLE saga_instance (
    id VARCHAR(36) PRIMARY KEY,
    workflow_name VARCHAR(100) NOT NULL,
    workflow_version INTEGER NOT NULL,
    business_key VARCHAR(200) NOT NULL,
    payload VARCHAR(8192) NOT NULL,
    payload_hash VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL,
    current_step INTEGER NOT NULL,
    version BIGINT NOT NULL,
    lease_owner VARCHAR(36),
    lease_until BIGINT NOT NULL,
    lease_epoch BIGINT NOT NULL,
    next_run_at BIGINT NOT NULL,
    last_error VARCHAR(1024),
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    CONSTRAINT uq_saga_business UNIQUE (workflow_name, business_key)
);

CREATE INDEX ix_saga_due ON saga_instance (status, next_run_at, lease_until);

CREATE TABLE saga_step (
    saga_id VARCHAR(36) NOT NULL,
    step_index INTEGER NOT NULL,
    step_name VARCHAR(100) NOT NULL,
    operation_id VARCHAR(64) NOT NULL,
    execute_state VARCHAR(16) NOT NULL,
    compensate_state VARCHAR(16) NOT NULL,
    execute_attempts INTEGER NOT NULL,
    compensate_attempts INTEGER NOT NULL,
    PRIMARY KEY (saga_id, step_index),
    CONSTRAINT uq_saga_operation UNIQUE (operation_id)
);

CREATE TABLE participant_operation (
    operation_id VARCHAR(64) PRIMARY KEY,
    payload_hash VARCHAR(64) NOT NULL,
    state VARCHAR(16) NOT NULL,
    updated_at BIGINT NOT NULL
);
