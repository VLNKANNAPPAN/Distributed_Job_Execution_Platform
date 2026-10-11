package com.example.jobplatform;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.jobplatform.job.JobSubmissionService;
import com.example.jobplatform.lease.JobLease;
import com.example.jobplatform.lease.JobLeaseService;
import com.example.jobplatform.retry.ExpiredLeaseReaper;
import com.example.jobplatform.retry.RetryScheduler;
import com.example.jobplatform.support.PostgresIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class FailureInjectionIntegrationTest extends PostgresIntegrationTest {

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
    void externalEffectCanRepeatButStaleOwnerCannotCorruptPlatformState() {
        submissionService.submit(
                "failure-after-effect",
                "GENERATE_REPORT",
                5,
                objectMapper.createObjectNode().put("report", "failure injection"));
        List<String> externalEffects = new ArrayList<>();

        JobLease first = leaseService.acquire("worker-a", Duration.ofMinutes(1)).orElseThrow();
        externalEffects.add(first.jobId() + ":" + first.executionGeneration());
        jdbcTemplate.update(
                "UPDATE jobs SET lease_expires_at = clock_timestamp() WHERE id = ?",
                first.jobId());
        assertThat(reaper.reapExpired(10)).isEqualTo(1);
        jdbcTemplate.update(
                "UPDATE jobs SET next_attempt_at = clock_timestamp() WHERE id = ?",
                first.jobId());
        assertThat(retryScheduler.promoteDue(10)).isEqualTo(1);

        JobLease second = leaseService.acquire("worker-b", Duration.ofMinutes(1)).orElseThrow();
        externalEffects.add(second.jobId() + ":" + second.executionGeneration());
        assertThat(leaseService.complete(second)).isTrue();
        assertThat(leaseService.complete(first)).isFalse();

        assertThat(externalEffects).hasSize(2).containsOnly(first.jobId() + ":1");
        assertThat(jdbcTemplate.queryForObject(
                        "SELECT status FROM jobs WHERE id = ?", String.class, first.jobId()))
                .isEqualTo("SUCCEEDED");
        assertThat(jdbcTemplate.queryForObject(
                        "SELECT count(*) FROM job_attempts WHERE job_id = ?",
                        Long.class,
                        first.jobId()))
                .isEqualTo(2);
    }
}
