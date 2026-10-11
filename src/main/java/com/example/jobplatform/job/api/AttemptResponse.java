package com.example.jobplatform.job.api;

import java.time.Instant;
import java.util.UUID;

public record AttemptResponse(
        UUID id,
        long executionGeneration,
        int attemptNumber,
        String workerId,
        long leaseVersion,
        String status,
        Instant startedAt,
        Instant finishedAt,
        String errorType,
        String errorMessage) {}
