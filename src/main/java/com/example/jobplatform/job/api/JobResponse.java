package com.example.jobplatform.job.api;

import com.example.jobplatform.job.JobEntity;
import com.example.jobplatform.job.JobStatus;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

public record JobResponse(
        UUID id,
        String type,
        int priority,
        JsonNode payload,
        JobStatus status,
        int attempt,
        int maxAttempts,
        long executionGeneration,
        Instant runAt,
        Integer executionTimeoutSeconds,
        Instant executionTimeoutAt,
        Instant cancellationRequestedAt,
        String cancellationRequestedBy,
        String cancellationReason,
        Instant nextAttemptAt,
        String lastErrorType,
        String lastError,
        Instant deadLetteredAt,
        Instant createdAt,
        Instant updatedAt) {

    public static JobResponse from(JobEntity job) {
        return new JobResponse(
                job.getId(),
                job.getType(),
                job.getPriority(),
                job.getPayload(),
                job.getStatus(),
                job.getAttempt(),
                job.getMaxAttempts(),
                job.getExecutionGeneration(),
                job.getEligibleAt(),
                job.getExecutionTimeoutSeconds(),
                job.getExecutionTimeoutAt(),
                job.getCancellationRequestedAt(),
                job.getCancellationRequestedBy(),
                job.getCancellationReason(),
                job.getNextAttemptAt(),
                job.getLastErrorType(),
                job.getLastError(),
                job.getDeadLetteredAt(),
                job.getCreatedAt(),
                job.getUpdatedAt());
    }
}
