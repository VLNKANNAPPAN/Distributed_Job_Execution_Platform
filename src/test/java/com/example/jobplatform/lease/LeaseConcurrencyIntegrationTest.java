package com.example.jobplatform.lease;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.jobplatform.job.JobRepository;
import com.example.jobplatform.job.JobStatus;
import com.example.jobplatform.job.JobSubmissionService;
import com.example.jobplatform.retry.ExpiredLeaseReaper;
import com.example.jobplatform.retry.FailureKind;
import com.example.jobplatform.retry.RetryScheduler;
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

class LeaseConcurrencyIntegrationTest extends PostgresIntegrationTest {

    @Autowired
    private JobSubmissionService submissionService;

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
    void twoWorkersRaceAndOnlyOneOwnsTheJob() throws Exception {
        submit("lease-two");

        List<JobLease> winners = concurrentClaims(2);

        assertThat(winners).hasSize(1);
        assertThat(jobRepository.findById(winners.getFirst().jobId()).orElseThrow().getStatus())
                .isEqualTo(JobStatus.RUNNING);
        assertThat(count("job_attempts")).isEqualTo(1);
    }

    @Test
    void manySimultaneousClaimsStillProduceOneOwner() throws Exception {
        submit("lease-many");

        List<JobLease> winners = concurrentClaims(40);

        assertThat(winners).hasSize(1);
        assertThat(count("job_attempts")).isEqualTo(1);
    }

    @Test
    void duplicateWakeupCannotCreateAnotherAttempt() {
        submit("duplicate-wakeup");

        JobLease first = leaseService.acquire("worker-a", Duration.ofMinutes(1)).orElseThrow();
        Optional<JobLease> duplicate = leaseService.acquire("worker-b", Duration.ofMinutes(1));

        assertThat(duplicate).isEmpty();
        assertThat(first.attempt()).isEqualTo(1);
        assertThat(count("job_attempts")).isEqualTo(1);
    }

    @Test
    void expiredOwnerCannotCorruptReplacementAndReplacementCompletes() {
        submit("lease-reclaim");
        JobLease first = leaseService.acquire("worker-a", Duration.ofMinutes(1)).orElseThrow();
        expire(first);

        assertThat(reaper.reapExpired(10)).isEqualTo(1);
        makeRetryDue(first.jobId());
        assertThat(retryScheduler.promoteDue(10)).isEqualTo(1);
        JobLease second = leaseService.acquire("worker-b", Duration.ofMinutes(1)).orElseThrow();

        assertThat(second.leaseVersion()).isGreaterThan(first.leaseVersion());
        assertThat(second.leaseToken()).isNotEqualTo(first.leaseToken());
        assertThat(second.attempt()).isEqualTo(2);
        assertThat(leaseService.complete(first)).isFalse();
        assertThat(leaseService.fail(first, FailureKind.RETRYABLE, "late", "late failure"))
                .isEqualTo(FailureResult.STALE);
        assertThat(leaseService.complete(second)).isTrue();
        assertThat(jobRepository.findById(first.jobId()).orElseThrow().getStatus())
                .isEqualTo(JobStatus.SUCCEEDED);
    }

    @Test
    void exactExpiryRejectsRenewalCompletionAndFailure() {
        submit("lease-boundary");
        JobLease lease = leaseService.acquire("worker-a", Duration.ofMinutes(1)).orElseThrow();
        jdbcTemplate.update(
                "UPDATE jobs SET lease_expires_at = clock_timestamp() WHERE id = ?", lease.jobId());

        assertThat(leaseService.renew(lease, Duration.ofMinutes(1))).isFalse();
        assertThat(leaseService.complete(lease)).isFalse();
        assertThat(leaseService.fail(lease, FailureKind.RETRYABLE, "late", "at expiry"))
                .isEqualTo(FailureResult.STALE);
        assertThat(reaper.reapExpired(10)).isEqualTo(1);
    }

    @Test
    void reusedWorkerIdDoesNotCreateAbaAuthority() {
        submit("lease-aba");
        JobLease first = leaseService.acquire("worker-same", Duration.ofMinutes(1)).orElseThrow();
        expire(first);
        reaper.reapExpired(10);
        makeRetryDue(first.jobId());
        retryScheduler.promoteDue(10);

        JobLease second = leaseService.acquire("worker-same", Duration.ofMinutes(1)).orElseThrow();

        assertThat(second.leaseVersion()).isEqualTo(first.leaseVersion() + 1);
        assertThat(second.leaseToken()).isNotEqualTo(first.leaseToken());
        assertThat(leaseService.complete(first)).isFalse();
        assertThat(leaseService.complete(second)).isTrue();
    }

    @Test
    void forgedExecutionGenerationIsRejected() {
        submit("lease-generation");
        JobLease lease = leaseService.acquire("worker-a", Duration.ofMinutes(1)).orElseThrow();
        JobLease forged = new JobLease(
                lease.jobId(),
                lease.workerId(),
                lease.leaseToken(),
                lease.leaseVersion(),
                lease.executionGeneration() + 1,
                lease.attempt(),
                lease.maxAttempts(),
                lease.jobType(),
                lease.payload(),
                lease.queuedAt(),
                lease.acquiredAt(),
                lease.leaseExpiresAt(),
                lease.executionTimeoutAt());

        assertThat(leaseService.renew(forged, Duration.ofMinutes(1))).isFalse();
        assertThat(leaseService.complete(forged)).isFalse();
        assertThat(leaseService.complete(lease)).isTrue();
    }

    @Test
    void completionAndFailureRaceHasOneCommittedOutcome() throws Exception {
        submit("lease-terminal-race");
        JobLease lease = leaseService.acquire("worker-a", Duration.ofMinutes(1)).orElseThrow();
        CyclicBarrier barrier = new CyclicBarrier(2);
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<Boolean> completion = executor.submit(() -> {
                barrier.await();
                return leaseService.complete(lease);
            });
            Future<FailureResult> failure = executor.submit(() -> {
                barrier.await();
                return leaseService.fail(
                        lease, FailureKind.NON_RETRYABLE, "business", "invalid request");
            });

            boolean completed = completion.get();
            FailureResult failed = failure.get();
            assertThat((completed ? 1 : 0) + (failed == FailureResult.FAILED ? 1 : 0))
                    .isEqualTo(1);
        }
        assertThat(countWhere("job_attempts", "status <> 'RUNNING'")).isEqualTo(1);
    }

    private List<JobLease> concurrentClaims(int workers) throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(workers);
        List<Callable<Optional<JobLease>>> tasks = new ArrayList<>();
        for (int index = 0; index < workers; index++) {
            String worker = "worker-" + index;
            tasks.add(() -> {
                barrier.await();
                return leaseService.acquire(worker, Duration.ofMinutes(1));
            });
        }
        List<JobLease> winners = new ArrayList<>();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (Future<Optional<JobLease>> result : executor.invokeAll(tasks)) {
                result.get().ifPresent(winners::add);
            }
        }
        return winners;
    }

    private void submit(String key) {
        submissionService.submit(
                key,
                "GENERATE_REPORT",
                5,
                objectMapper.createObjectNode().put("report", key));
    }

    private void expire(JobLease lease) {
        jdbcTemplate.update(
                "UPDATE jobs SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE id = ?",
                lease.jobId());
    }

    private void makeRetryDue(java.util.UUID jobId) {
        jdbcTemplate.update(
                "UPDATE jobs SET next_attempt_at = clock_timestamp() - interval '1 second' WHERE id = ?",
                jobId);
    }

    private long count(String table) {
        return jdbcTemplate.queryForObject("SELECT count(*) FROM " + table, Long.class);
    }

    private long countWhere(String table, String predicate) {
        return jdbcTemplate.queryForObject(
                "SELECT count(*) FROM " + table + " WHERE " + predicate, Long.class);
    }
}
