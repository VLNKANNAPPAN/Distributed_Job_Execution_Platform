CREATE TABLE jobs (
    id UUID PRIMARY KEY,
    idempotency_key VARCHAR(128) NOT NULL,
    request_fingerprint VARCHAR(64) NOT NULL,
    type VARCHAR(64) NOT NULL,
    priority INTEGER NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(32) NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    execution_generation BIGINT NOT NULL DEFAULT 1,
    dispatch_generation BIGINT NOT NULL DEFAULT 0,
    version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT uk_jobs_idempotency_key UNIQUE (idempotency_key),
    CONSTRAINT ck_jobs_request_fingerprint CHECK (char_length(request_fingerprint) = 64),
    CONSTRAINT ck_jobs_priority CHECK (priority BETWEEN 0 AND 100),
    CONSTRAINT ck_jobs_attempt CHECK (attempt >= 0 AND attempt <= max_attempts),
    CONSTRAINT ck_jobs_generation CHECK (execution_generation >= 1 AND dispatch_generation >= 0),
    CONSTRAINT ck_jobs_status CHECK (status IN (
        'SUBMITTED', 'SCHEDULED', 'QUEUED', 'RUNNING', 'RETRY_WAIT',
        'SUCCEEDED', 'FAILED', 'DEAD_LETTERED', 'CANCELLED'
    ))
);

CREATE INDEX ix_jobs_status_created ON jobs (status, created_at, id);

CREATE TABLE job_history (
    id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
    from_status VARCHAR(32),
    to_status VARCHAR(32) NOT NULL,
    actor VARCHAR(64) NOT NULL,
    reason VARCHAR(256) NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT ck_job_history_to_status CHECK (to_status IN (
        'SUBMITTED', 'SCHEDULED', 'QUEUED', 'RUNNING', 'RETRY_WAIT',
        'SUCCEEDED', 'FAILED', 'DEAD_LETTERED', 'CANCELLED'
    )),
    CONSTRAINT ck_job_history_from_status CHECK (
        from_status IS NULL OR from_status IN (
            'SUBMITTED', 'SCHEDULED', 'QUEUED', 'RUNNING', 'RETRY_WAIT',
            'SUCCEEDED', 'FAILED', 'DEAD_LETTERED', 'CANCELLED'
        )
    )
);

CREATE INDEX ix_job_history_job_time ON job_history (job_id, occurred_at, id);
