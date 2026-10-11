package com.example.jobplatform.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.jobplatform.job.JobSubmissionService;
import com.example.jobplatform.support.PostgresIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class OutboxStoreIntegrationTest extends PostgresIntegrationTest {

    @Autowired
    private JdbcOutboxStore store;

    @Autowired
    private JobSubmissionService submissionService;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void jobHistoryAndOutboxCommitAtomically() {
        var result = submissionService.submit(
                "atomic-job", "GENERATE_REPORT", 5, objectMapper.createObjectNode().put("value", 1));

        assertThat(jdbcTemplate.queryForObject(
                        "SELECT count(*) FROM jobs WHERE id = ?", Long.class, result.job().getId()))
                .isEqualTo(1);
        assertThat(jdbcTemplate.queryForObject(
                        "SELECT count(*) FROM job_history WHERE job_id = ?",
                        Long.class,
                        result.job().getId()))
                .isEqualTo(2);
        assertThat(jdbcTemplate.queryForObject(
                        "SELECT count(*) FROM outbox_events WHERE job_id = ?",
                        Long.class,
                        result.job().getId()))
                .isEqualTo(1);
    }

    @Test
    void expiredDispatcherClaimIsReclaimedAfterRestart() {
        submitJobs(1, "restart");
        OutboxClaim abandoned = store.claimBatch("dispatcher-a", 1, Duration.ofSeconds(30)).get(0);
        jdbcTemplate.update(
                "UPDATE outbox_events SET claim_expires_at = clock_timestamp() - interval '1 second' WHERE id = ?",
                abandoned.eventId());

        OutboxClaim reclaimed = store.claimBatch("dispatcher-b", 1, Duration.ofSeconds(30)).get(0);

        assertThat(reclaimed.eventId()).isEqualTo(abandoned.eventId());
        assertThat(reclaimed.claimToken()).isNotEqualTo(abandoned.claimToken());
        assertThat(reclaimed.publishAttempts()).isEqualTo(2);
        assertThat(store.markPublished(reclaimed.eventId(), reclaimed.claimToken())).isTrue();
        assertThat(store.markPublished(abandoned.eventId(), abandoned.claimToken())).isFalse();
    }

    @Test
    void twoDispatchersClaimDisjointEvents() throws Exception {
        submitJobs(20, "multi-dispatch");
        CyclicBarrier barrier = new CyclicBarrier(2);
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<List<OutboxClaim>> first = executor.submit(() -> {
                barrier.await();
                return store.claimBatch("dispatcher-a", 20, Duration.ofSeconds(30));
            });
            Future<List<OutboxClaim>> second = executor.submit(() -> {
                barrier.await();
                return store.claimBatch("dispatcher-b", 20, Duration.ofSeconds(30));
            });

            List<OutboxClaim> firstClaims = first.get();
            List<OutboxClaim> secondClaims = second.get();
            Set<java.util.UUID> ids = new HashSet<>();
            firstClaims.forEach(claim -> ids.add(claim.eventId()));
            int firstSize = ids.size();
            secondClaims.forEach(claim -> ids.add(claim.eventId()));

            assertThat(ids).hasSize(20);
            assertThat(firstSize + secondClaims.size()).isEqualTo(20);
        }
    }

    @Test
    void failedPublicationRecordsBackoffAndCanBeRetried() {
        submitJobs(1, "failure");
        OutboxClaim claim = store.claimBatch("dispatcher-a", 1, Duration.ofSeconds(30)).get(0);

        assertThat(store.recordFailure(
                        claim.eventId(), claim.claimToken(), Duration.ofMillis(1), "Kafka unavailable"))
                .isTrue();
        jdbcTemplate.update(
                "UPDATE outbox_events SET next_attempt_at = clock_timestamp() - interval '1 second' WHERE id = ?",
                claim.eventId());

        OutboxClaim retry = store.claimBatch("dispatcher-b", 1, Duration.ofSeconds(30)).get(0);
        assertThat(retry.eventId()).isEqualTo(claim.eventId());
        assertThat(retry.publishAttempts()).isEqualTo(2);
    }

    private void submitJobs(int count, String prefix) {
        for (int index = 0; index < count; index++) {
            submissionService.submit(
                    prefix + "-" + index,
                    "GENERATE_REPORT",
                    5,
                    objectMapper.createObjectNode().put("index", index));
        }
    }
}
