package com.example.jobplatform.worker;

import com.example.jobplatform.lease.JobLease;
import com.example.jobplatform.lease.JobLeaseService;
import com.example.jobplatform.lease.FailureResult;
import com.example.jobplatform.retry.FailureKind;
import com.example.jobplatform.observability.PlatformMetrics;
import jakarta.annotation.PreDestroy;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.time.Duration;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class WorkerProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerProcessor.class);

    private final JobLeaseService leaseService;
    private final JobHandlerRegistry handlers;
    private final WorkerIdentity identity;
    private final WorkerProperties properties;
    private final PlatformMetrics metrics;
    private final ScheduledExecutorService renewalExecutor =
            Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().factory());

    public WorkerProcessor(
            JobLeaseService leaseService,
            JobHandlerRegistry handlers,
            WorkerIdentity identity,
            WorkerProperties properties,
            PlatformMetrics metrics) {
        this.leaseService = leaseService;
        this.handlers = handlers;
        this.identity = identity;
        this.properties = properties;
        this.metrics = metrics;
    }

    public WorkerProcessingResult processOneSignal() {
        Optional<JobLease> claimed = claimOne();
        if (claimed.isEmpty()) {
            return WorkerProcessingResult.NO_WORK;
        }
        return executeClaimed(claimed.get());
    }

    public Optional<JobLease> claimOne() {
        Optional<JobLease> claimed = leaseService.acquire(identity.value(), properties.leaseDuration());
        claimed.ifPresent(lease ->
                metrics.queueDelay(Duration.between(lease.queuedAt(), lease.acquiredAt())));
        return claimed;
    }

    public WorkerProcessingResult executeClaimed(JobLease lease) {
        return execute(lease);
    }

    private WorkerProcessingResult execute(JobLease lease) {
        LOGGER.atInfo()
                .addKeyValue("job_id", lease.jobId())
                .addKeyValue("worker_id", lease.workerId())
                .addKeyValue("attempt", lease.attempt())
                .addKeyValue("lease_token", lease.leaseToken())
                .addKeyValue("lease_version", lease.leaseVersion())
                .log("job execution started");
        AtomicBoolean stale = new AtomicBoolean(false);
        AtomicBoolean cancellationRequested = new AtomicBoolean(false);
        long safeUpperBound = Math.max(1, properties.leaseDuration().toMillis() / 2);
        long interval = Math.min(
                Math.max(1, properties.getRenewalIntervalMs()), safeUpperBound);
        ScheduledFuture<?> renewal = renewalExecutor.scheduleAtFixedRate(
                () -> {
                    try {
                        if (leaseService.isCancellationRequested(lease)) {
                            cancellationRequested.set(true);
                            return;
                        }
                        if (!leaseService.renew(lease, properties.leaseDuration())) {
                            stale.set(true);
                        }
                    } catch (RuntimeException exception) {
                        stale.set(true);
                    }
                },
                interval,
                interval,
                TimeUnit.MILLISECONDS);
        try {
            JobHandler handler = handlers.find(lease.jobType()).orElseThrow(
                    () -> new NonRetryableJobException("No handler for job type " + lease.jobType()));
            JobExecutionContext context = JobExecutionContext.from(
                    lease,
                    () -> cancellationRequested.get()
                            || leaseService.isCancellationRequested(lease));
            handler.execute(context);
            if (context.isCancellationRequested()) {
                return finish(lease, leaseService.cancelIfRequested(lease)
                        ? WorkerProcessingResult.CANCELLED
                        : WorkerProcessingResult.STALE);
            }
            if (stale.get()) {
                return finish(lease, WorkerProcessingResult.STALE);
            }
            WorkerProcessingResult result = leaseService.complete(lease)
                    ? WorkerProcessingResult.SUCCEEDED
                    : WorkerProcessingResult.STALE;
            return finish(lease, result);
        } catch (JobCancellationException exception) {
            return finish(lease, leaseService.cancelIfRequested(lease)
                    ? WorkerProcessingResult.CANCELLED
                    : WorkerProcessingResult.STALE);
        } catch (NonRetryableJobException exception) {
            return recordFailure(lease, FailureKind.NON_RETRYABLE, exception);
        } catch (RuntimeException exception) {
            return recordFailure(lease, FailureKind.RETRYABLE, exception);
        } finally {
            renewal.cancel(false);
        }
    }

    private WorkerProcessingResult recordFailure(
            JobLease lease, FailureKind kind, RuntimeException exception) {
        FailureResult result = leaseService.fail(
                lease, kind, exception.getClass().getSimpleName(), exception.getMessage());
        WorkerProcessingResult processingResult = WorkerProcessingResult.valueOf(result.name());
        return finish(lease, processingResult);
    }

    private WorkerProcessingResult finish(JobLease lease, WorkerProcessingResult result) {
        metrics.executionDuration(Duration.between(lease.acquiredAt(), Instant.now()));
        metrics.outcome(result.name());
        if (result == WorkerProcessingResult.STALE) {
            metrics.staleWrite();
        }
        LOGGER.atInfo()
                .addKeyValue("job_id", lease.jobId())
                .addKeyValue("worker_id", lease.workerId())
                .addKeyValue("attempt", lease.attempt())
                .addKeyValue("lease_token", lease.leaseToken())
                .addKeyValue("lease_version", lease.leaseVersion())
                .addKeyValue("outcome", result)
                .log("job execution finished");
        return result;
    }

    @PreDestroy
    void close() {
        renewalExecutor.shutdownNow();
    }
}
