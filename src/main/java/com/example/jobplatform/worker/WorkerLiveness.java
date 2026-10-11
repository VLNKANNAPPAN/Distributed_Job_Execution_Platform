package com.example.jobplatform.worker;

import java.time.Instant;
import java.util.UUID;

public record WorkerLiveness(
        String workerId,
        UUID sessionId,
        Instant registeredAt,
        Instant lastHeartbeatAt,
        Instant stoppedAt,
        Instant databaseTime,
        boolean apparentlyAlive) {}
