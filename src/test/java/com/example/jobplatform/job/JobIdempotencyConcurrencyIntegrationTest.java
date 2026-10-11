package com.example.jobplatform.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.example.jobplatform.support.PostgresIntegrationTest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class JobIdempotencyConcurrencyIntegrationTest extends PostgresIntegrationTest {

    @Autowired
    private JobSubmissionService submissionService;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void twoSimultaneousRequestsCreateOneJob() throws Exception {
        assertConcurrentSameRequest(2, "race-two");
    }

    @Test
    void tenSimultaneousRequestsCreateOneJob() throws Exception {
        assertConcurrentSameRequest(10, "race-ten");
    }

    @Test
    void practicalBurstCreatesOneJobAndOneOutboxEvent() throws Exception {
        assertConcurrentSameRequest(50, "race-fifty");
    }

    @Test
    void simultaneousDifferentPayloadsProduceOneWinnerAndOneConflict() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<Object> first = executor.submit(() -> submitAfterBarrier(
                    barrier, "different-race", objectMapper.readTree("{\"value\":1}")));
            Future<Object> second = executor.submit(() -> submitAfterBarrier(
                    barrier, "different-race", objectMapper.readTree("{\"value\":2}")));

            List<Object> outcomes = List.of(first.get(), second.get());
            assertThat(outcomes.stream().filter(SubmissionResult.class::isInstance).count()).isEqualTo(1);
            assertThat(outcomes.stream().filter(IdempotencyConflictException.class::isInstance).count())
                    .isEqualTo(1);
            assertThat(jobRepository.count()).isEqualTo(1);
        }
    }

    @Test
    void outboxFailureRollsBackJobAndHistory() {
        jdbcTemplate.execute("""
                CREATE FUNCTION test_fail_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    RAISE EXCEPTION 'injected outbox failure';
                END;
                $$
                """);
        jdbcTemplate.execute("""
                CREATE TRIGGER test_fail_outbox_trigger
                BEFORE INSERT ON outbox_events
                FOR EACH ROW EXECUTE FUNCTION test_fail_outbox()
                """);
        try {
            JsonNode payload = objectMapper.createObjectNode().put("value", 1);
            assertThatThrownBy(() -> submissionService.submit(
                            "rollback-key", "GENERATE_REPORT", 5, payload))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("injected outbox failure");
            assertThat(jobRepository.count()).isZero();
            assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM job_history", Long.class))
                    .isZero();
            assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM outbox_events", Long.class))
                    .isZero();
        } finally {
            jdbcTemplate.execute("DROP TRIGGER test_fail_outbox_trigger ON outbox_events");
            jdbcTemplate.execute("DROP FUNCTION test_fail_outbox()");
        }
    }

    private void assertConcurrentSameRequest(int requestCount, String key) throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(requestCount);
        JsonNode payload = objectMapper.readTree("{\"report\":\"daily\",\"format\":\"PDF\"}");
        List<Callable<SubmissionResult>> tasks = new ArrayList<>();
        for (int index = 0; index < requestCount; index++) {
            tasks.add(() -> {
                barrier.await();
                return submissionService.submit(key, "GENERATE_REPORT", 5, payload);
            });
        }

        List<SubmissionResult> results = new ArrayList<>();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (Future<SubmissionResult> future : executor.invokeAll(tasks)) {
                results.add(future.get());
            }
        }

        assertThat(results).extracting(result -> result.job().getId()).containsOnly(results.get(0).job().getId());
        assertThat(results.stream().filter(SubmissionResult::created).count()).isEqualTo(1);
        assertThat(jobRepository.count()).isEqualTo(1);
        assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM outbox_events", Long.class))
                .isEqualTo(1);
        assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM job_history", Long.class))
                .isEqualTo(2);
    }

    private Object submitAfterBarrier(CyclicBarrier barrier, String key, JsonNode payload) {
        try {
            barrier.await();
            return submissionService.submit(key, "GENERATE_REPORT", 5, payload);
        } catch (IdempotencyConflictException exception) {
            return exception;
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }
}
