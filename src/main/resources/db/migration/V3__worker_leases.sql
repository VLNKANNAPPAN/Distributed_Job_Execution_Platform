ALTER TABLE jobs ADD COLUMN eligible_at TIMESTAMPTZ;
UPDATE jobs SET eligible_at = created_at;
ALTER TABLE jobs ALTER COLUMN eligible_at SET NOT NULL;

ALTER TABLE jobs
    ADD COLUMN lease_owner VARCHAR(64),
    ADD COLUMN lease_token UUID,
    ADD COLUMN lease_version BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN lease_expires_at TIMESTAMPTZ;

ALTER TABLE jobs ADD CONSTRAINT ck_jobs_lease_version CHECK (lease_version >= 0);
ALTER TABLE jobs ADD CONSTRAINT ck_jobs_lease_tuple CHECK (
    (
        status = 'RUNNING'
        AND lease_owner IS NOT NULL
        AND lease_token IS NOT NULL
        AND lease_expires_at IS NOT NULL
    )
    OR
    (
        status <> 'RUNNING'
        AND lease_owner IS NULL
        AND lease_token IS NULL
        AND lease_expires_at IS NULL
    )
);

CREATE INDEX ix_jobs_claimable
    ON jobs (priority DESC, created_at, id)
    WHERE status = 'QUEUED';

CREATE INDEX ix_jobs_expired_lease
    ON jobs (lease_expires_at, id)
    WHERE status = 'RUNNING';

CREATE TABLE job_attempts (
    id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
    execution_generation BIGINT NOT NULL,
    attempt_number INTEGER NOT NULL,
    worker_id VARCHAR(64) NOT NULL,
    lease_token UUID NOT NULL,
    lease_version BIGINT NOT NULL,
    status VARCHAR(32) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    error_type VARCHAR(64),
    error_message VARCHAR(1000),
    CONSTRAINT uk_job_attempt_number UNIQUE (job_id, execution_generation, attempt_number),
    CONSTRAINT uk_job_attempt_token UNIQUE (lease_token),
    CONSTRAINT ck_job_attempt_numbers CHECK (
        execution_generation >= 1 AND attempt_number >= 1 AND lease_version >= 1
    ),
    CONSTRAINT ck_job_attempt_status CHECK (status IN (
        'RUNNING', 'SUCCEEDED', 'RETRYABLE_FAILED', 'NON_RETRYABLE_FAILED',
        'LEASE_EXPIRED', 'DEAD_LETTERED'
    )),
    CONSTRAINT ck_job_attempt_completion CHECK (
        (status = 'RUNNING' AND finished_at IS NULL)
        OR
        (status <> 'RUNNING' AND finished_at IS NOT NULL)
    )
);

CREATE INDEX ix_job_attempts_job_time
    ON job_attempts (job_id, execution_generation, attempt_number);
