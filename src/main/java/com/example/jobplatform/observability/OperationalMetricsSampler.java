package com.example.jobplatform.observability;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class OperationalMetricsSampler {

    private final JdbcTemplate jdbcTemplate;
    private final PlatformMetrics metrics;
    private final long stuckJobSeconds;
    private final long workerLivenessSeconds;

    public OperationalMetricsSampler(
            JdbcTemplate jdbcTemplate,
            PlatformMetrics metrics,
            @Value("${app.observability.stuck-job-seconds:300}") long stuckJobSeconds,
            @Value("${app.worker.liveness-timeout-seconds:20}") long workerLivenessSeconds) {
        this.jdbcTemplate = jdbcTemplate;
        this.metrics = metrics;
        this.stuckJobSeconds = stuckJobSeconds;
        this.workerLivenessSeconds = workerLivenessSeconds;
    }

    @Scheduled(fixedDelayString = "${app.observability.sampling-delay-ms:10000}")
    public void sample() {
        long workers = jdbcTemplate.queryForObject("""
                SELECT count(*) FROM worker_heartbeats
                WHERE stopped_at IS NULL
                  AND last_heartbeat_at > clock_timestamp() - (? * interval '1 second')
                """, Long.class, workerLivenessSeconds);
        long backlog = count("SELECT count(*) FROM outbox_events WHERE published_at IS NULL");
        long stuck = jdbcTemplate.queryForObject(
                """
                SELECT count(*) FROM jobs
                WHERE (status = 'QUEUED'
                        AND queued_at < clock_timestamp() - (? * interval '1 second'))
                   OR (status = 'RUNNING' AND lease_expires_at <= clock_timestamp())
                   OR (status = 'RETRY_WAIT'
                        AND next_attempt_at < clock_timestamp() - (? * interval '1 second'))
                   OR (status = 'SCHEDULED'
                        AND eligible_at < clock_timestamp() - (? * interval '1 second'))
                """,
                Long.class,
                stuckJobSeconds,
                stuckJobSeconds,
                stuckJobSeconds);
        metrics.updateOperationalGauges(workers, backlog, stuck);
    }

    private long count(String sql) {
        return jdbcTemplate.queryForObject(sql, Long.class);
    }
}
