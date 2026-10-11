package com.example.jobplatform.outbox;

import java.time.Instant;
import java.util.UUID;

public record WorkAvailableEvent(
        UUID eventId,
        UUID jobId,
        String eventType,
        int schemaVersion,
        long executionGeneration,
        long dispatchGeneration,
        Instant eligibleAt,
        UUID correlationId) {}
