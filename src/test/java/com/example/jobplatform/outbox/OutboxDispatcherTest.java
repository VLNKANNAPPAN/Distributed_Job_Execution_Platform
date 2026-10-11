package com.example.jobplatform.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class OutboxDispatcherTest {

    @Mock
    private OutboxStore store;

    @Mock
    private OutboxEventPublisher publisher;

    private OutboxDispatcher dispatcher;
    private OutboxClaim claim;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        OutboxProperties properties = new OutboxProperties();
        properties.setBatchSize(10);
        properties.setClaimSeconds(30);
        dispatcher = new OutboxDispatcher(store, publisher, properties, "dispatcher-test");
        claim = new OutboxClaim(
                UUID.randomUUID(),
                UUID.randomUUID(),
                "JOB_WORK_AVAILABLE",
                "{}",
                UUID.randomUUID(),
                1);
    }

    @Test
    void marksPublicationOnlyAfterPublisherReturns() throws Exception {
        when(store.claimBatch(eq("dispatcher-test"), eq(10), eq(Duration.ofSeconds(30))))
                .thenReturn(List.of(claim));
        when(store.markPublished(claim.eventId(), claim.claimToken())).thenReturn(true);

        assertThat(dispatcher.dispatchOnce()).isEqualTo(1);

        verify(publisher).publish(claim);
        verify(store).markPublished(claim.eventId(), claim.claimToken());
        verify(store, never()).recordFailure(any(), any(), any(), any());
    }

    @Test
    void failedKafkaSendLeavesEventRetryable() throws Exception {
        when(store.claimBatch(eq("dispatcher-test"), eq(10), eq(Duration.ofSeconds(30))))
                .thenReturn(List.of(claim));
        doThrow(new IllegalStateException("Kafka unavailable")).when(publisher).publish(claim);

        dispatcher.dispatchOnce();

        verify(store, never()).markPublished(any(), any());
        verify(store).recordFailure(
                eq(claim.eventId()),
                eq(claim.claimToken()),
                eq(Duration.ofSeconds(1)),
                eq("Kafka unavailable"));
    }

    @Test
    void publishSuccessWithLostDatabaseMarkIsAllowedToRepublish() throws Exception {
        OutboxClaim reclaimed = new OutboxClaim(
                claim.eventId(),
                claim.jobId(),
                claim.eventType(),
                claim.payload(),
                UUID.randomUUID(),
                2);
        when(store.claimBatch(eq("dispatcher-test"), eq(10), eq(Duration.ofSeconds(30))))
                .thenReturn(List.of(claim), List.of(reclaimed));
        when(store.markPublished(claim.eventId(), claim.claimToken())).thenReturn(false);
        when(store.markPublished(reclaimed.eventId(), reclaimed.claimToken())).thenReturn(true);

        dispatcher.dispatchOnce();
        dispatcher.dispatchOnce();

        verify(publisher).publish(claim);
        verify(publisher).publish(reclaimed);
    }
}
