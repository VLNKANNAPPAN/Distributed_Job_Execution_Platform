package com.example.jobplatform.outbox;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "app.outbox", name = "dispatcher-enabled", havingValue = "true")
public class OutboxDispatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(OutboxDispatcher.class);

    private final OutboxStore store;
    private final OutboxEventPublisher publisher;
    private final OutboxProperties properties;
    private final String ownerId;

    @Autowired
    public OutboxDispatcher(
            OutboxStore store, OutboxEventPublisher publisher, OutboxProperties properties) {
        this(store, publisher, properties, defaultOwnerId());
    }

    OutboxDispatcher(
            OutboxStore store,
            OutboxEventPublisher publisher,
            OutboxProperties properties,
            String ownerId) {
        this.store = store;
        this.publisher = publisher;
        this.properties = properties;
        this.ownerId = ownerId;
    }

    @Scheduled(
            fixedDelayString = "${app.outbox.fixed-delay-ms:500}",
            initialDelayString = "${app.outbox.initial-delay-ms:0}")
    public int dispatchOnce() {
        List<OutboxClaim> claims =
                store.claimBatch(ownerId, properties.getBatchSize(), properties.claimDuration());
        for (OutboxClaim claim : claims) {
            publishOne(claim);
        }
        return claims.size();
    }

    private void publishOne(OutboxClaim claim) {
        try {
            publisher.publish(claim);
            if (!store.markPublished(claim.eventId(), claim.claimToken())) {
                LOGGER.warn(
                        "Outbox event {} was published but its expired/stale claim could not be marked;"
                                + " duplicate publication is expected",
                        claim.eventId());
            }
        } catch (Exception exception) {
            Duration delay = retryDelay(claim.publishAttempts());
            store.recordFailure(
                    claim.eventId(), claim.claimToken(), delay, exception.getMessage());
            LOGGER.warn(
                    "Outbox publication failed eventId={} attempt={} retryInMs={}",
                    claim.eventId(),
                    claim.publishAttempts(),
                    delay.toMillis());
        }
    }

    private static Duration retryDelay(int attempt) {
        int exponent = Math.max(0, Math.min(6, attempt - 1));
        return Duration.ofSeconds(Math.min(60, 1L << exponent));
    }

    private static String defaultOwnerId() {
        try {
            return InetAddress.getLocalHost().getHostName() + ":" + UUID.randomUUID();
        } catch (UnknownHostException exception) {
            return "unknown-host:" + UUID.randomUUID();
        }
    }
}
