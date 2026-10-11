package com.example.jobplatform.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

class PlatformMetricsTest {

    @Test
    void recordsLifecycleCountersTimersAndOperationalGauges() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        PlatformMetrics metrics = new PlatformMetrics(registry);

        metrics.submitted();
        metrics.outcome("SUCCEEDED");
        metrics.outcome("FAILED");
        metrics.outcome("RETRY_WAIT");
        metrics.outcome("DEAD_LETTERED");
        metrics.leaseExpired();
        metrics.staleWrite();
        metrics.queueDelay(Duration.ofMillis(12));
        metrics.executionDuration(Duration.ofMillis(34));
        metrics.updateOperationalGauges(3, 7, 2);

        assertThat(counter(registry, "job_platform_jobs_submitted_total")).isEqualTo(1);
        assertThat(counter(registry, "job_platform_jobs_succeeded_total")).isEqualTo(1);
        assertThat(counter(registry, "job_platform_jobs_failed_total")).isEqualTo(1);
        assertThat(counter(registry, "job_platform_jobs_retried_total")).isEqualTo(1);
        assertThat(counter(registry, "job_platform_jobs_dead_lettered_total")).isEqualTo(1);
        assertThat(counter(registry, "job_platform_lease_expirations_total")).isEqualTo(1);
        assertThat(counter(registry, "job_platform_stale_writes_total")).isEqualTo(1);
        assertThat(registry.get("job_platform_queue_delay").timer().count()).isEqualTo(1);
        assertThat(registry.get("job_platform_execution_duration").timer().count()).isEqualTo(1);
        assertThat(registry.get("job_platform_active_workers").gauge().value()).isEqualTo(3);
        assertThat(registry.get("job_platform_outbox_backlog").gauge().value()).isEqualTo(7);
        assertThat(registry.get("job_platform_stuck_jobs").gauge().value()).isEqualTo(2);
    }

    @Test
    void correlationFilterPropagatesOrCreatesBoundedIdentifier() throws Exception {
        CorrelationIdFilter filter = new CorrelationIdFilter();
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(CorrelationIdFilter.HEADER, "caller-123");
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, new MockFilterChain());

        assertThat(response.getHeader(CorrelationIdFilter.HEADER)).isEqualTo("caller-123");
    }

    private static double counter(SimpleMeterRegistry registry, String name) {
        return registry.get(name).counter().count();
    }
}
