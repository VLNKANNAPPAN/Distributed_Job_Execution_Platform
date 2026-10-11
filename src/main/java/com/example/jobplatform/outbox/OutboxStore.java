package com.example.jobplatform.outbox;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

public interface OutboxStore {

    List<OutboxClaim> claimBatch(String owner, int batchSize, Duration claimDuration);

    boolean markPublished(UUID eventId, UUID claimToken);

    boolean recordFailure(UUID eventId, UUID claimToken, Duration retryDelay, String error);
}
