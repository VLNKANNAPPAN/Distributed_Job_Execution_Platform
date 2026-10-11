package com.example.jobplatform.lease;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

public record JobLease(
        UUID jobId,
        String workerId,
        UUID leaseToken,
        long leaseVersion,
        long executionGeneration,
        int attempt,
        int maxAttempts,
        String jobType,
        JsonNode payload,
        Instant queuedAt,
        Instant acquiredAt,
        Instant leaseExpiresAt,
        Instant executionTimeoutAt) {

    public JobLease {
        payload = payload.deepCopy();
    }

    @Override
    public JsonNode payload() {
        return payload.deepCopy();
    }
}
