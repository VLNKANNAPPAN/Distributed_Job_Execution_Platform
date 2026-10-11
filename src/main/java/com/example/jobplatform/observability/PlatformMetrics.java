package com.example.jobplatform.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Component
public class PlatformMetrics {

    private final Counter submitted;
    private final Counter succeeded;
    private final Counter failed;
    private final Counter retried;
    private final Counter deadLettered;
    private final Counter leaseExpirations;
    private final Counter staleWrites;
    private final Timer queueDelay;
    private final Timer executionDuration;
    private final AtomicLong activeWorkers = new AtomicLong();
    private final AtomicLong outboxBacklog = new AtomicLong();
    private final AtomicLong stuckJobs = new AtomicLong();

    public PlatformMetrics(MeterRegistry registry) {
        submitted = registry.counter("job_platform_jobs_submitted_total");
        succeeded = registry.counter("job_platform_jobs_succeeded_total");
        failed = registry.counter("job_platform_jobs_failed_total");
        retried = registry.counter("job_platform_jobs_retried_total");
        deadLettered = registry.counter("job_platform_jobs_dead_lettered_total");
        leaseExpirations = registry.counter("job_platform_lease_expirations_total");
        staleWrites = registry.counter("job_platform_stale_writes_total");
        queueDelay = registry.timer("job_platform_queue_delay");
        executionDuration = registry.timer("job_platform_execution_duration");
        Gauge.builder("job_platform_active_workers", activeWorkers, AtomicLong::get)
                .register(registry);
        Gauge.builder("job_platform_outbox_backlog", outboxBacklog, AtomicLong::get)
                .register(registry);
        Gauge.builder("job_platform_stuck_jobs", stuckJobs, AtomicLong::get)
                .register(registry);
    }

    public void submitted() {
        submitted.increment();
    }

    public void outcome(String outcome) {
        switch (outcome) {
            case "SUCCEEDED" -> succeeded.increment();
            case "FAILED" -> failed.increment();
            case "RETRY_WAIT" -> retried.increment();
            case "DEAD_LETTERED" -> deadLettered.increment();
            default -> {
                // Cancellation, no-work and stale paths have dedicated operational signals.
            }
        }
    }

    public void leaseExpired() {
        leaseExpirations.increment();
    }

    public void staleWrite() {
        staleWrites.increment();
    }

    public void queueDelay(Duration duration) {
        queueDelay.record(nonNegative(duration));
    }

    public void executionDuration(Duration duration) {
        executionDuration.record(nonNegative(duration));
    }

    public void updateOperationalGauges(long workers, long backlog, long stuck) {
        activeWorkers.set(workers);
        outboxBacklog.set(backlog);
        stuckJobs.set(stuck);
    }

    public void afterCommit(Runnable action) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(
                    new TransactionSynchronization() {
                        @Override
                        public void afterCommit() {
                            action.run();
                        }
                    });
        } else {
            action.run();
        }
    }

    private static Duration nonNegative(Duration duration) {
        return duration.isNegative() ? Duration.ZERO : duration;
    }
}
