package com.example.jobplatform.worker;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.jobplatform.job.JobSubmissionService;
import com.example.jobplatform.lease.JobLease;
import com.example.jobplatform.lease.JobLeaseService;
import com.example.jobplatform.retry.ExpiredLeaseReaper;
import com.example.jobplatform.retry.RetryScheduler;
import com.example.jobplatform.support.PostgresIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class WorkerHeartbeatIntegrationTest extends PostgresIntegrationTest {

    @Autowired
    private WorkerHeartbeatService heartbeatService;

    @Autowired
    private JobSubmissionService submissionService;

    @Autowired
    private JobLeaseService leaseService;

    @Autowired
    private ExpiredLeaseReaper reaper;

    @Autowired
    private RetryScheduler retryScheduler;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void registrationHeartbeatDeathDetectionAndSessionFencingAreDiagnostic() {
        UUID firstSession = UUID.randomUUID();
        heartbeatService.register("worker-a", firstSession);

        assertThat(heartbeatService.list(Duration.ofSeconds(10)))
                .singleElement()
                .satisfies(worker -> assertThat(worker.apparentlyAlive()).isTrue());

        jdbcTemplate.update(
                """
                UPDATE worker_heartbeats
                SET registered_at = clock_timestamp() - interval '2 minutes',
                    last_heartbeat_at = clock_timestamp() - interval '1 minute'
                WHERE worker_id = 'worker-a'
                """);
        assertThat(heartbeatService.list(Duration.ofSeconds(10)))
                .singleElement()
                .satisfies(worker -> assertThat(worker.apparentlyAlive()).isFalse());

        UUID replacementSession = UUID.randomUUID();
        heartbeatService.register("worker-a", replacementSession);
        assertThat(heartbeatService.heartbeat("worker-a", firstSession)).isFalse();
        assertThat(heartbeatService.heartbeat("worker-a", replacementSession)).isTrue();
        assertThat(heartbeatService.stop("worker-a", replacementSession)).isTrue();
        assertThat(heartbeatService.list(Duration.ofSeconds(10)))
                .singleElement()
                .satisfies(worker -> assertThat(worker.apparentlyAlive()).isFalse());
    }

    @Test
    void deadHeartbeatDoesNotReclaimButExpiredLeaseDoesAndOldWorkerIsFenced() {
        UUID session = UUID.randomUUID();
        heartbeatService.register("worker-a", session);
        submissionService.submit(
                "heartbeat-owned-job",
                "GENERATE_REPORT",
                5,
                objectMapper.createObjectNode().put("report", "heartbeat"));
        JobLease first = leaseService.acquire("worker-a", Duration.ofMinutes(1)).orElseThrow();

        jdbcTemplate.update(
                """
                UPDATE worker_heartbeats
                SET registered_at = clock_timestamp() - interval '2 minutes',
                    last_heartbeat_at = clock_timestamp() - interval '1 minute'
                WHERE worker_id = 'worker-a'
                """);

        assertThat(heartbeatService.list(Duration.ofSeconds(10)).getFirst().apparentlyAlive())
                .isFalse();
        assertThat(reaper.reapExpired(10)).isZero();
        assertThat(leaseService.renew(first, Duration.ofMinutes(1))).isTrue();

        jdbcTemplate.update(
                """
                UPDATE jobs
                SET lease_expires_at = clock_timestamp() - interval '1 second'
                WHERE id = ?
                """,
                first.jobId());
        assertThat(reaper.reapExpired(10)).isEqualTo(1);
        jdbcTemplate.update(
                """
                UPDATE jobs
                SET next_attempt_at = clock_timestamp() - interval '1 second'
                WHERE id = ?
                """,
                first.jobId());
        assertThat(retryScheduler.promoteDue(10)).isEqualTo(1);
        JobLease replacement =
                leaseService.acquire("worker-b", Duration.ofMinutes(1)).orElseThrow();

        assertThat(leaseService.complete(first)).isFalse();
        assertThat(leaseService.complete(replacement)).isTrue();
    }
}
