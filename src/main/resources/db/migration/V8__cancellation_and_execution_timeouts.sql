ALTER TABLE jobs
    ADD COLUMN cancellation_requested_at TIMESTAMPTZ,
    ADD COLUMN cancellation_requested_by VARCHAR(64),
    ADD COLUMN cancellation_reason VARCHAR(256),
    ADD COLUMN execution_timeout_seconds INTEGER,
    ADD COLUMN execution_timeout_at TIMESTAMPTZ;

ALTER TABLE jobs ADD CONSTRAINT ck_jobs_cancellation_request CHECK (
    (cancellation_requested_at IS NULL
        AND cancellation_requested_by IS NULL
        AND cancellation_reason IS NULL)
    OR
    (cancellation_requested_at IS NOT NULL
        AND cancellation_requested_by IS NOT NULL
        AND cancellation_reason IS NOT NULL)
);

ALTER TABLE jobs ADD CONSTRAINT ck_jobs_execution_timeout CHECK (
    execution_timeout_seconds IS NULL OR execution_timeout_seconds BETWEEN 1 AND 86400
);

ALTER TABLE jobs ADD CONSTRAINT ck_jobs_execution_deadline CHECK (
    (execution_timeout_seconds IS NULL AND execution_timeout_at IS NULL)
    OR execution_timeout_seconds IS NOT NULL
);

ALTER TABLE job_attempts DROP CONSTRAINT ck_job_attempt_status;
ALTER TABLE job_attempts ADD CONSTRAINT ck_job_attempt_status CHECK (status IN (
    'RUNNING', 'SUCCEEDED', 'RETRYABLE_FAILED', 'NON_RETRYABLE_FAILED',
    'LEASE_EXPIRED', 'DEAD_LETTERED', 'CANCELLED', 'TIMED_OUT'
));

CREATE INDEX ix_jobs_running_cancellation
    ON jobs (cancellation_requested_at, id)
    WHERE status = 'RUNNING' AND cancellation_requested_at IS NOT NULL;
