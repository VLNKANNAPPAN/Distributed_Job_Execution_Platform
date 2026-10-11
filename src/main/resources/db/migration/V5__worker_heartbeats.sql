CREATE TABLE worker_heartbeats (
    worker_id VARCHAR(64) PRIMARY KEY,
    session_id UUID NOT NULL,
    registered_at TIMESTAMPTZ NOT NULL,
    last_heartbeat_at TIMESTAMPTZ NOT NULL,
    stopped_at TIMESTAMPTZ,
    CONSTRAINT ck_worker_heartbeat_time CHECK (
        last_heartbeat_at >= registered_at
        AND (stopped_at IS NULL OR stopped_at >= registered_at)
    )
);

CREATE INDEX ix_worker_heartbeats_liveness
    ON worker_heartbeats (last_heartbeat_at, worker_id)
    WHERE stopped_at IS NULL;
