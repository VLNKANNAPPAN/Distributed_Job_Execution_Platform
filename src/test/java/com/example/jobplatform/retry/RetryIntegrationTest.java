package com.example.jobplatform.retry;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.jobplatform.job.JobRepository;
import com.example.jobplatform.job.JobStatus;
import com.example.jobplatform.job.JobSubmissionService;
import com.example.jobplatform.lease.FailureResult;
import com.example.jobplatform.lease.JobLease;
import com.example.jobplatform.lease.JobLeaseService;
import com.example.jobplatform.support.PostgresIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class RetryIntegrationTest extends PostgresIntegrationTest {

    @Autowired
    private JobSubmissionService submissionService;

    @Autowired
    private JobLeaseService leaseService;

    @Autowired
    private RetryScheduler retryScheduler;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void retryableFailureWaitsThenCreatesANewAttempt() {
        java.util.UUID jobId = submit("retryable");
        JobLease first = claim("worker-a");

        assertThat(leaseService.fail(first, FailureKind.RETRYABLE, "TEMPORARY", "try later"))
                .isEqualTo(FailureResult.RETRY_WAIT);
        assertThat(jobRepository.findById(jobId).orElseThrow().getStatus())
                .isEqualTo(JobStatus.RETRY_WAIT);
        assertThat(jobRepository.findById(jobId).orElseThrow().getNextAttemptAt()).isNotNull();

        makeDue(jobId);
        assertThat(retryScheduler.promoteDue(10)).isEqualTo(1);
        JobLease second = claim("worker-b");
        assertThat(second.attempt()).isEqualTo(2);
        assertThat(second.leaseVersion()).isGreaterThan(first.leaseVersion());
    }

    @Test
    void nonRetryableFailureIsTerminal() {
        java.util.UUID jobId = submit("non-retryable");
        JobLease lease = claim("worker-a");

        assertThat(leaseService.fail(lease, FailureKind.NON_RETRYABLE, "VALIDATION", "bad input"))
                .isEqualTo(FailureResult.FAILED);
        assertThat(jobRepository.findById(jobId).orElseThrow().getStatus())
                .isEqualTo(JobStatus.FAILED);
        assertThat(retryScheduler.promoteDue(10)).isZero();
    }

    @Test
    void exhaustedRetryBudgetDeadLetters() {
        java.util.UUID jobId = submit("max-attempts");
        for (int attempt = 1; attempt <= 3; attempt++) {
            JobLease lease = claim("worker-" + attempt);
            FailureResult result = leaseService.fail(
                    lease, FailureKind.RETRYABLE, "TEMPORARY", "attempt " + attempt);
            if (attempt < 3) {
                assertThat(result).isEqualTo(FailureResult.RETRY_WAIT);
                makeDue(jobId);
                assertThat(retryScheduler.promoteDue(10)).isEqualTo(1);
            } else {
                assertThat(result).isEqualTo(FailureResult.DEAD_LETTERED);
            }
        }

        assertThat(jobRepository.findById(jobId).orElseThrow().getStatus())
                .isEqualTo(JobStatus.DEAD_LETTERED);
        assertThat(jdbcTemplate.queryForObject(
                        "SELECT count(*) FROM job_attempts WHERE job_id = ?", Long.class, jobId))
                .isEqualTo(3);
    }

    @Test
    void duplicateRetryAndFailureEventsAreNoOps() {
        java.util.UUID jobId = submit("duplicate-retry");
        JobLease lease = claim("worker-a");

        assertThat(leaseService.fail(lease, FailureKind.RETRYABLE, "TEMPORARY", "first"))
                .isEqualTo(FailureResult.RETRY_WAIT);
        assertThat(leaseService.fail(lease, FailureKind.RETRYABLE, "TEMPORARY", "duplicate"))
                .isEqualTo(FailureResult.STALE);
        makeDue(jobId);
        assertThat(retryScheduler.promoteDue(10)).isEqualTo(1);
        assertThat(retryScheduler.promoteDue(10)).isZero();
        assertThat(jdbcTemplate.queryForObject(
                        "SELECT count(*) FROM outbox_events WHERE job_id = ?", Long.class, jobId))
                .isEqualTo(2);
    }

    @Test
    void twoWorkersRacingForPromotedRetryProduceOneAttempt() throws Exception {
        java.util.UUID jobId = submit("retry-race");
        JobLease first = claim("worker-original");
        leaseService.fail(first, FailureKind.RETRYABLE, "TEMPORARY", "retry");
        makeDue(jobId);
        retryScheduler.promoteDue(10);

        CyclicBarrier barrier = new CyclicBarrier(2);
        List<Callable<Optional<JobLease>>> tasks = List.of(
                () -> claimAfterBarrier(barrier, "worker-a"),
                () -> claimAfterBarrier(barrier, "worker-b"));
        List<JobLease> winners = new ArrayList<>();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (Future<Optional<JobLease>> result : executor.invokeAll(tasks)) {
                result.get().ifPresent(winners::add);
            }
        }

        assertThat(winners).hasSize(1);
        assertThat(winners.getFirst().attempt()).isEqualTo(2);
    }

    @Test
    void oldAttemptCannotCompleteAfterRetryAttemptStarts() {
        java.util.UUID jobId = submit("old-attempt");
        JobLease first = claim("worker-a");
        leaseService.fail(first, FailureKind.RETRYABLE, "TEMPORARY", "retry");
        makeDue(jobId);
        retryScheduler.promoteDue(10);
        JobLease second = claim("worker-b");

        assertThat(leaseService.complete(first)).isFalse();
        assertThat(leaseService.complete(second)).isTrue();
    }

    private Optional<JobLease> claimAfterBarrier(CyclicBarrier barrier, String worker) throws Exception {
        barrier.await();
        return leaseService.acquire(worker, Duration.ofMinutes(1));
    }

    private JobLease claim(String worker) {
        return leaseService.acquire(worker, Duration.ofMinutes(1)).orElseThrow();
    }

    private java.util.UUID submit(String key) {
        return submissionService
                .submit(
                        key,
                        "GENERATE_REPORT",
                        5,
                        objectMapper.createObjectNode().put("report", key))
                .job()
                .getId();
    }

    private void makeDue(java.util.UUID jobId) {
        jdbcTemplate.update(
                "UPDATE jobs SET next_attempt_at = clock_timestamp() - interval '1 second' WHERE id = ?",
                jobId);
    }
}
