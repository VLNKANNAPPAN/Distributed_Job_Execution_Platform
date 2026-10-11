package com.example.jobplatform.job.api;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record DeadLetterJobResponse(
        UUID jobId,
        String type,
        JsonNode payload,
        int priority,
        int attempts,
        int maxAttempts,
        long executionGeneration,
        String failureType,
        String failureReason,
        Instant deadLetteredAt,
        List<AttemptResponse> attemptHistory,
        List<HistoryResponse> stateHistory) {}
