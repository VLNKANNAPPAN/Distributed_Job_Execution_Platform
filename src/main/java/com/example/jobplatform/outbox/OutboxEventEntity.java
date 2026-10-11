package com.example.jobplatform.outbox;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Entity
@Table(name = "outbox_events")
public class OutboxEventEntity {

    @Id
    private UUID id;

    @Column(name = "job_id", nullable = false)
    private UUID jobId;

    @Column(name = "event_type", nullable = false, length = 64)
    private String eventType;

    @Column(name = "schema_version", nullable = false)
    private int schemaVersion;

    @Column(name = "execution_generation", nullable = false)
    private long executionGeneration;

    @Column(name = "dispatch_generation", nullable = false)
    private long dispatchGeneration;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, columnDefinition = "jsonb")
    private JsonNode payload;

    @Column(name = "available_at", nullable = false)
    private Instant availableAt;

    @Column(name = "next_attempt_at", nullable = false)
    private Instant nextAttemptAt;

    @Column(name = "publish_attempts", nullable = false)
    private int publishAttempts;

    @Column(name = "claim_owner", length = 128)
    private String claimOwner;

    @Column(name = "claim_token")
    private UUID claimToken;

    @Column(name = "claim_expires_at")
    private Instant claimExpiresAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    @Column(name = "last_error", length = 1000)
    private String lastError;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected OutboxEventEntity() {}

    private OutboxEventEntity(
            UUID id,
            UUID jobId,
            String eventType,
            int schemaVersion,
            long executionGeneration,
            long dispatchGeneration,
            JsonNode payload,
            Instant availableAt) {
        this.id = id;
        this.jobId = jobId;
        this.eventType = eventType;
        this.schemaVersion = schemaVersion;
        this.executionGeneration = executionGeneration;
        this.dispatchGeneration = dispatchGeneration;
        this.payload = payload.deepCopy();
        this.availableAt = availableAt;
        this.nextAttemptAt = availableAt;
        this.publishAttempts = 0;
        this.createdAt = availableAt;
    }

    public static OutboxEventEntity workAvailable(
            UUID id,
            UUID jobId,
            long executionGeneration,
            long dispatchGeneration,
            JsonNode payload,
            Instant availableAt) {
        return new OutboxEventEntity(
                id,
                jobId,
                "JOB_WORK_AVAILABLE",
                1,
                executionGeneration,
                dispatchGeneration,
                payload,
                availableAt);
    }

    public UUID getId() {
        return id;
    }

    public UUID getJobId() {
        return jobId;
    }

    public String getEventType() {
        return eventType;
    }

    public long getExecutionGeneration() {
        return executionGeneration;
    }

    public long getDispatchGeneration() {
        return dispatchGeneration;
    }

    public JsonNode getPayload() {
        return payload.deepCopy();
    }
}
