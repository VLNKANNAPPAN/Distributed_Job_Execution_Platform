ALTER TABLE jobs
    ADD COLUMN next_attempt_at TIMESTAMPTZ,
    ADD COLUMN last_error VARCHAR(1000);

ALTER TABLE jobs ADD CONSTRAINT ck_jobs_retry_time CHECK (
    (status = 'RETRY_WAIT' AND next_attempt_at IS NOT NULL)
    OR
    (status <> 'RETRY_WAIT' AND next_attempt_at IS NULL)
);

CREATE INDEX ix_jobs_due_retry
    ON jobs (next_attempt_at, priority DESC, created_at, id)
    WHERE status = 'RETRY_WAIT';
