ALTER TABLE jobs ADD COLUMN queued_at TIMESTAMPTZ;

UPDATE jobs
SET queued_at = created_at
WHERE status IN ('QUEUED', 'RUNNING');

ALTER TABLE jobs ADD CONSTRAINT ck_jobs_queued_time CHECK (
    status <> 'QUEUED' OR queued_at IS NOT NULL
);

DROP INDEX ix_jobs_claimable;

CREATE INDEX ix_jobs_claimable
    ON jobs (priority DESC, queued_at, created_at, id)
    WHERE status = 'QUEUED';

CREATE INDEX ix_jobs_due_schedule
    ON jobs (eligible_at, priority DESC, created_at, id)
    WHERE status = 'SCHEDULED';
