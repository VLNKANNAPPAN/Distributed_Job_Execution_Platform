CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE RESTRICT,
    event_type VARCHAR(64) NOT NULL,
    schema_version INTEGER NOT NULL,
    execution_generation BIGINT NOT NULL,
    dispatch_generation BIGINT NOT NULL,
    payload JSONB NOT NULL,
    available_at TIMESTAMPTZ NOT NULL,
    next_attempt_at TIMESTAMPTZ NOT NULL,
    publish_attempts INTEGER NOT NULL DEFAULT 0,
    claim_owner VARCHAR(128),
    claim_token UUID,
    claim_expires_at TIMESTAMPTZ,
    published_at TIMESTAMPTZ,
    last_error VARCHAR(1000),
    created_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT uk_outbox_logical_event UNIQUE (
        job_id, execution_generation, dispatch_generation, event_type
    ),
    CONSTRAINT ck_outbox_schema_version CHECK (schema_version >= 1),
    CONSTRAINT ck_outbox_generation CHECK (
        execution_generation >= 1 AND dispatch_generation >= 1
    ),
    CONSTRAINT ck_outbox_attempts CHECK (publish_attempts >= 0),
    CONSTRAINT ck_outbox_claim_tuple CHECK (
        (claim_owner IS NULL AND claim_token IS NULL AND claim_expires_at IS NULL)
        OR
        (claim_owner IS NOT NULL AND claim_token IS NOT NULL AND claim_expires_at IS NOT NULL)
    )
);

CREATE INDEX ix_outbox_dispatch ON outbox_events (
    next_attempt_at, available_at, created_at
) WHERE published_at IS NULL;
