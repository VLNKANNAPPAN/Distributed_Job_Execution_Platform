package com.example.jobplatform.outbox;

import java.util.UUID;

public record OutboxClaim(
        UUID eventId,
        UUID jobId,
        String eventType,
        String payload,
        UUID claimToken,
        int publishAttempts) {}
