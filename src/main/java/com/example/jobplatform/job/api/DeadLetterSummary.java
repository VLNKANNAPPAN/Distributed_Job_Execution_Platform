package com.example.jobplatform.job.api;

import java.time.Instant;
import java.util.UUID;

public record DeadLetterSummary(
        UUID jobId,
        String type,
        int priority,
        int attempts,
        int maxAttempts,
        long executionGeneration,
        String failureType,
        String failureReason,
        Instant deadLetteredAt) {}
