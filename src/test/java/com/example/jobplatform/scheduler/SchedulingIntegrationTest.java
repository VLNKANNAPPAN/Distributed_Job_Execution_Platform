package com.example.jobplatform.scheduler;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.jobplatform.job.JobEntity;
import com.example.jobplatform.job.JobRepository;
import com.example.jobplatform.job.JobStatus;
import com.example.jobplatform.job.JobSubmissionService;
import com.example.jobplatform.lease.JobLease;
import com.example.jobplatform.lease.JobLeaseService;
import com.example.jobplatform.support.PostgresIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class SchedulingIntegrationTest extends PostgresIntegrationTest {

    @Autowired
    private JobSubmissionService submissionService;

    @Autowired
    private ScheduledJobScheduler scheduler;

    @Autowired
    private JobLeaseService leaseService;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void futureJobHasNoEarlySignalOrClaimAndTwoSchedulersPromoteItOnce() throws Exception {
        UUID jobId = submit("future-job", 50, Instant.now().plus(Duration.ofHours(1)));
        JobEntity scheduled = jobRepository.findById(jobId).orElseThrow();

        assertThat(scheduled.getStatus()).isEqualTo(JobStatus.SCHEDULED);
        assertThat(countOutbox(jobId)).isZero();
        assertThat(scheduler.promoteDue(10)).isZero();
        assertThat(leaseService.acquire("worker-early", Duration.ofMinutes(1))).isEmpty();

        jdbcTemplate.update(
                """
                UPDATE jobs
                SET eligible_at = clock_timestamp() - interval '1 second'
                WHERE id = ?
                """,
                jobId);
        CyclicBarrier barrier = new CyclicBarrier(2);
        List<Callable<Integer>> calls = List.of(
                () -> promoteAfterBarrier(barrier), () -> promoteAfterBarrier(barrier));
        int total;
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<Integer>> results = executor.invokeAll(calls);
            total = results.get(0).get() + results.get(1).get();
        }

        assertThat(total).isEqualTo(1);
        assertThat(scheduler.promoteDue(10)).isZero();
        assertThat(countOutbox(jobId)).isEqualTo(1);
        assertThat(leaseService.acquire("worker-due", Duration.ofMinutes(1)))
                .isPresent()
                .get()
                .extracting(JobLease::jobId)
                .isEqualTo(jobId);
    }

    @Test
    void priorityOrdersFreshEligibleJobs() {
        UUID low = submit("priority-low", 10, null);
        UUID high = submit("priority-high", 90, null);

        JobLease first = leaseService.acquire("worker-priority", Duration.ofMinutes(1)).orElseThrow();

        assertThat(first.jobId()).isEqualTo(high);
        assertThat(first.jobId()).isNotEqualTo(low);
    }

    @Test
    void agingAllowsOldLowPriorityWorkToBeatFreshMaximumPriorityWork() {
        UUID low = submit("aged-low", 0, null);
        UUID high = submit("fresh-high", 100, null);
        jdbcTemplate.update(
                """
                UPDATE jobs
                SET queued_at = clock_timestamp() - interval '2 hours'
                WHERE id = ?
                """,
                low);
        jdbcTemplate.update(
                "UPDATE jobs SET queued_at = clock_timestamp() WHERE id = ?", high);

        JobLease first = leaseService.acquire("worker-aging", Duration.ofMinutes(1)).orElseThrow();

        assertThat(first.jobId()).isEqualTo(low);
    }

    private int promoteAfterBarrier(CyclicBarrier barrier) throws Exception {
        barrier.await();
        return scheduler.promoteDue(10);
    }

    private UUID submit(String key, int priority, Instant runAt) {
        return submissionService
                .submit(
                        key,
                        "GENERATE_REPORT",
                        priority,
                        objectMapper.createObjectNode().put("report", key),
                        runAt)
                .job()
                .getId();
    }

    private long countOutbox(UUID jobId) {
        return jdbcTemplate.queryForObject(
                "SELECT count(*) FROM outbox_events WHERE job_id = ?", Long.class, jobId);
    }
}
