ALTER TABLE jobs
    ADD COLUMN last_error_type VARCHAR(64),
    ADD COLUMN dead_lettered_at TIMESTAMPTZ;

UPDATE jobs
SET dead_lettered_at = updated_at
WHERE status = 'DEAD_LETTERED';

ALTER TABLE jobs ADD CONSTRAINT ck_jobs_dead_letter_time CHECK (
    (status = 'DEAD_LETTERED' AND dead_lettered_at IS NOT NULL)
    OR
    (status <> 'DEAD_LETTERED' AND dead_lettered_at IS NULL)
);

CREATE INDEX ix_jobs_dead_lettered
    ON jobs (dead_lettered_at, id)
    WHERE status = 'DEAD_LETTERED';

CREATE TABLE job_replays (
    id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
    from_execution_generation BIGINT NOT NULL,
    to_execution_generation BIGINT NOT NULL,
    requested_by VARCHAR(64) NOT NULL,
    reason VARCHAR(256) NOT NULL,
    previous_error_type VARCHAR(64),
    previous_error_message VARCHAR(1000),
    requested_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT uk_job_replay_generation UNIQUE (job_id, to_execution_generation),
    CONSTRAINT ck_job_replay_generation CHECK (
        from_execution_generation >= 1
        AND to_execution_generation = from_execution_generation + 1
    )
);

CREATE INDEX ix_job_replays_job_time
    ON job_replays (job_id, requested_at, id);
