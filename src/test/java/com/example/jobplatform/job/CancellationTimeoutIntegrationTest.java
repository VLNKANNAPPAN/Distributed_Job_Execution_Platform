package com.example.jobplatform.job;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.jobplatform.lease.JobLease;
import com.example.jobplatform.lease.JobLeaseService;
import com.example.jobplatform.retry.ExpiredLeaseReaper;
import com.example.jobplatform.retry.RetryScheduler;
import com.example.jobplatform.support.PostgresIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class CancellationTimeoutIntegrationTest extends PostgresIntegrationTest {

    @Autowired
    private JobSubmissionService submissionService;

    @Autowired
    private JobCancellationService cancellationService;

    @Autowired
    private JobLeaseService leaseService;

    @Autowired
    private ExpiredLeaseReaper reaper;

    @Autowired
    private RetryScheduler retryScheduler;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void pendingCancellationIsTerminalAndCannotBeClaimed() {
        JobEntity job = submit("cancel-pending", Instant.now().plusSeconds(3600), null);

        assertThat(cancellationService.cancel(job.getId(), "operator", "no longer needed"))
                .isEqualTo(CancellationResult.CANCELLED);
        assertThat(leaseService.acquire("worker", Duration.ofMinutes(1))).isEmpty();
        assertThat(jobRepository.findById(job.getId()).orElseThrow().getStatus())
                .isEqualTo(JobStatus.CANCELLED);
        assertThat(cancellationService.cancel(job.getId(), "operator", "repeat"))
                .isEqualTo(CancellationResult.ALREADY_CANCELLED);
    }

    @Test
    void runningOwnerCooperativelyFinalizesCancellation() {
        JobEntity job = submit("cancel-running", null, null);
        JobLease lease = leaseService.acquire("worker", Duration.ofMinutes(1)).orElseThrow();

        assertThat(cancellationService.cancel(job.getId(), "operator", "stop"))
                .isEqualTo(CancellationResult.CANCELLATION_REQUESTED);
        assertThat(leaseService.renew(lease, Duration.ofMinutes(1))).isFalse();
        assertThat(leaseService.complete(lease)).isFalse();
        assertThat(leaseService.cancelIfRequested(lease)).isTrue();
        assertThat(jobRepository.findById(job.getId()).orElseThrow().getStatus())
                .isEqualTo(JobStatus.CANCELLED);
    }

    @Test
    void cancellationAndCompletionRaceHasOneLegalOutcome() throws Exception {
        JobEntity job = submit("cancel-complete-race", null, null);
        JobLease lease = leaseService.acquire("worker", Duration.ofMinutes(1)).orElseThrow();
        CyclicBarrier barrier = new CyclicBarrier(2);

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<Boolean> completion = executor.submit(() -> {
                barrier.await();
                return leaseService.complete(lease);
            });
            Future<?> cancellation = executor.submit(() -> {
                barrier.await();
                try {
                    return cancellationService.cancel(job.getId(), "operator", "race");
                } catch (CancellationConflictException conflict) {
                    return conflict;
                }
            });
            completion.get();
            cancellation.get();
        }

        JobStatus status = jobRepository.findById(job.getId()).orElseThrow().getStatus();
        if (status == JobStatus.RUNNING) {
            assertThat(leaseService.cancelIfRequested(lease)).isTrue();
            status = jobRepository.findById(job.getId()).orElseThrow().getStatus();
        }
        assertThat(status).isIn(JobStatus.SUCCEEDED, JobStatus.CANCELLED);
        assertThat(jdbcTemplate.queryForObject(
                        "SELECT count(*) FROM job_attempts WHERE status <> 'RUNNING'", Long.class))
                .isEqualTo(1);
    }

    @Test
    void uncooperativeRunningCancellationIsFinalizedAfterLeaseExpiry() {
        JobEntity job = submit("cancel-expiry", null, null);
        JobLease lease = leaseService.acquire("worker", Duration.ofMinutes(1)).orElseThrow();
        cancellationService.cancel(job.getId(), "operator", "stop eventually");
        jdbcTemplate.update(
                "UPDATE jobs SET lease_expires_at = clock_timestamp() WHERE id = ?", job.getId());

        assertThat(reaper.reapExpired(10)).isEqualTo(1);
        assertThat(jobRepository.findById(job.getId()).orElseThrow().getStatus())
                .isEqualTo(JobStatus.CANCELLED);
        assertThat(leaseService.complete(lease)).isFalse();
    }

    @Test
    void exactExecutionTimeoutRejectsOwnerAndEntersDurableRetry() {
        JobEntity job = submit("execution-timeout", null, 30);
        JobLease lease = leaseService.acquire("worker", Duration.ofMinutes(1)).orElseThrow();
        assertThat(lease.executionTimeoutAt()).isNotNull();
        jdbcTemplate.update(
                """
                UPDATE jobs
                SET execution_timeout_at = clock_timestamp(),
                    lease_expires_at = clock_timestamp()
                WHERE id = ?
                """,
                job.getId());

        assertThat(leaseService.complete(lease)).isFalse();
        assertThat(reaper.reapExpired(10)).isEqualTo(1);
        JobEntity timedOut = jobRepository.findById(job.getId()).orElseThrow();
        assertThat(timedOut.getStatus()).isEqualTo(JobStatus.RETRY_WAIT);
        assertThat(timedOut.getLastErrorType()).isEqualTo("EXECUTION_TIMEOUT");
        assertThat(jdbcTemplate.queryForObject(
                        "SELECT status FROM job_attempts WHERE job_id = ?",
                        String.class,
                        job.getId()))
                .isEqualTo("TIMED_OUT");
        assertThat(retryScheduler.promoteDue(10)).isZero();
    }

    private JobEntity submit(String key, Instant runAt, Integer timeoutSeconds) {
        return submissionService
                .submit(
                        key,
                        "GENERATE_REPORT",
                        5,
                        objectMapper.createObjectNode().put("report", key),
                        runAt,
                        timeoutSeconds)
                .job();
    }
}
