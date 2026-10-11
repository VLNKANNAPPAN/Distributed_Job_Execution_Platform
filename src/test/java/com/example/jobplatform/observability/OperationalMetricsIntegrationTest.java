package com.example.jobplatform.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.jobplatform.job.JobSubmissionService;
import com.example.jobplatform.support.PostgresIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = "app.observability.sampling-delay-ms=3600000")
class OperationalMetricsIntegrationTest extends PostgresIntegrationTest {

    @Autowired
    private JobSubmissionService submissionService;

    @Autowired
    private OperationalMetricsSampler sampler;

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void onlyCountsJobsThatArePastTheirStateSpecificEligibilityBoundary() {
        var queued = submissionService.submit(
                "stuck-queued",
                "GENERATE_REPORT",
                5,
                objectMapper.createObjectNode().put("report", "daily"));
        submissionService.submit(
                "future-scheduled",
                "GENERATE_REPORT",
                5,
                objectMapper.createObjectNode().put("report", "weekly"),
                Instant.now().plusSeconds(3600));

        jdbcTemplate.update(
                "UPDATE jobs SET queued_at = clock_timestamp() - interval '10 minutes' WHERE id = ?",
                queued.job().getId());

        sampler.sample();

        assertThat(meterRegistry.get("job_platform_stuck_jobs").gauge().value()).isEqualTo(1);
    }
}
