package com.example.jobplatform.job;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.time.Instant;
import java.util.UUID;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Entity
@Table(name = "jobs")
public class JobEntity {

    @Id
    private UUID id;

    @Column(name = "idempotency_key", nullable = false, length = 128, unique = true)
    private String idempotencyKey;

    @Column(name = "request_fingerprint", nullable = false, length = 64)
    private String requestFingerprint;

    @Column(nullable = false, length = 64)
    private String type;

    @Column(nullable = false)
    private int priority;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, columnDefinition = "jsonb")
    private JsonNode payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    private JobStatus status;

    @Column(nullable = false)
    private int attempt;

    @Column(name = "max_attempts", nullable = false)
    private int maxAttempts;

    @Column(name = "execution_generation", nullable = false)
    private long executionGeneration;

    @Column(name = "dispatch_generation", nullable = false)
    private long dispatchGeneration;

    @Column(name = "eligible_at", nullable = false)
    private Instant eligibleAt;

    @Column(name = "next_attempt_at")
    private Instant nextAttemptAt;

    @Column(name = "lease_owner", length = 64)
    private String leaseOwner;

    @Column(name = "lease_token")
    private UUID leaseToken;

    @Column(name = "lease_version", nullable = false)
    private long leaseVersion;

    @Column(name = "lease_expires_at")
    private Instant leaseExpiresAt;

    @Column(name = "last_error", length = 1000)
    private String lastError;

    @Column(name = "last_error_type", length = 64)
    private String lastErrorType;

    @Column(name = "dead_lettered_at")
    private Instant deadLetteredAt;

    @Column(name = "queued_at")
    private Instant queuedAt;

    @Column(name = "cancellation_requested_at")
    private Instant cancellationRequestedAt;

    @Column(name = "cancellation_requested_by", length = 64)
    private String cancellationRequestedBy;

    @Column(name = "cancellation_reason", length = 256)
    private String cancellationReason;

    @Column(name = "execution_timeout_seconds")
    private Integer executionTimeoutSeconds;

    @Column(name = "execution_timeout_at")
    private Instant executionTimeoutAt;

    @Version
    @Column(nullable = false)
    private long version;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected JobEntity() {}

    private JobEntity(
            UUID id,
            String idempotencyKey,
            String requestFingerprint,
            String type,
            int priority,
            JsonNode payload,
            Instant now,
            Instant eligibleAt,
            Integer executionTimeoutSeconds) {
        this.id = id;
        this.idempotencyKey = idempotencyKey;
        this.requestFingerprint = requestFingerprint;
        this.type = type;
        this.priority = priority;
        this.payload = payload.deepCopy();
        this.status = JobStatus.SUBMITTED;
        this.attempt = 0;
        this.maxAttempts = 3;
        this.executionGeneration = 1;
        this.dispatchGeneration = 0;
        this.eligibleAt = eligibleAt;
        this.executionTimeoutSeconds = executionTimeoutSeconds;
        this.leaseVersion = 0;
        this.createdAt = now;
        this.updatedAt = now;
    }

    public static JobEntity submitted(
            UUID id,
            String idempotencyKey,
            String requestFingerprint,
            String type,
            int priority,
            JsonNode payload,
            Instant now,
            Instant eligibleAt,
            Integer executionTimeoutSeconds) {
        return new JobEntity(
                id,
                idempotencyKey,
                requestFingerprint,
                type,
                priority,
                payload,
                now,
                eligibleAt,
                executionTimeoutSeconds);
    }

    public void transitionTo(JobStatus target, JobStateMachine stateMachine, Instant now) {
        stateMachine.requireTransition(status, target);
        status = target;
        updatedAt = now;
        if (target == JobStatus.QUEUED) {
            queuedAt = now;
        }
    }

    public void advanceDispatchGeneration() {
        dispatchGeneration++;
    }

    public UUID getId() {
        return id;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public String getRequestFingerprint() {
        return requestFingerprint;
    }

    public String getType() {
        return type;
    }

    public int getPriority() {
        return priority;
    }

    public JsonNode getPayload() {
        return payload.deepCopy();
    }

    public JobStatus getStatus() {
        return status;
    }

    public int getAttempt() {
        return attempt;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public long getExecutionGeneration() {
        return executionGeneration;
    }

    public long getDispatchGeneration() {
        return dispatchGeneration;
    }

    public Instant getEligibleAt() {
        return eligibleAt;
    }

    public Instant getNextAttemptAt() {
        return nextAttemptAt;
    }

    public String getLeaseOwner() {
        return leaseOwner;
    }

    public long getLeaseVersion() {
        return leaseVersion;
    }

    public Instant getLeaseExpiresAt() {
        return leaseExpiresAt;
    }

    public String getLastError() {
        return lastError;
    }

    public String getLastErrorType() {
        return lastErrorType;
    }

    public Instant getDeadLetteredAt() {
        return deadLetteredAt;
    }

    public Instant getQueuedAt() {
        return queuedAt;
    }

    public Instant getCancellationRequestedAt() {
        return cancellationRequestedAt;
    }

    public String getCancellationRequestedBy() {
        return cancellationRequestedBy;
    }

    public String getCancellationReason() {
        return cancellationReason;
    }

    public Integer getExecutionTimeoutSeconds() {
        return executionTimeoutSeconds;
    }

    public Instant getExecutionTimeoutAt() {
        return executionTimeoutAt;
    }

    public long getVersion() {
        return version;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
