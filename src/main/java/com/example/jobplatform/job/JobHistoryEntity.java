package com.example.jobplatform.job;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "job_history")
public class JobHistoryEntity {

    @Id
    private UUID id;

    @Column(name = "job_id", nullable = false)
    private UUID jobId;

    @Enumerated(EnumType.STRING)
    @Column(name = "from_status", length = 32)
    private JobStatus fromStatus;

    @Enumerated(EnumType.STRING)
    @Column(name = "to_status", nullable = false, length = 32)
    private JobStatus toStatus;

    @Column(nullable = false, length = 64)
    private String actor;

    @Column(nullable = false, length = 256)
    private String reason;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;

    protected JobHistoryEntity() {}

    private JobHistoryEntity(
            UUID id,
            UUID jobId,
            JobStatus fromStatus,
            JobStatus toStatus,
            String actor,
            String reason,
            Instant occurredAt) {
        this.id = id;
        this.jobId = jobId;
        this.fromStatus = fromStatus;
        this.toStatus = toStatus;
        this.actor = actor;
        this.reason = reason;
        this.occurredAt = occurredAt;
    }

    public static JobHistoryEntity record(
            UUID jobId,
            JobStatus fromStatus,
            JobStatus toStatus,
            String actor,
            String reason,
            Instant occurredAt) {
        return new JobHistoryEntity(
                UUID.randomUUID(), jobId, fromStatus, toStatus, actor, reason, occurredAt);
    }
}
