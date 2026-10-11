package com.example.jobplatform.job;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;

public record SubmissionCommand(
        String idempotencyKey,
        String requestFingerprint,
        String type,
        int priority,
        JsonNode payload,
        Instant runAt,
        Integer executionTimeoutSeconds) {}
