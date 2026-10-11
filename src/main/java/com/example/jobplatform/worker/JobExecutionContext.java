package com.example.jobplatform.worker;

import com.example.jobplatform.lease.JobLease;
import java.util.function.BooleanSupplier;

public final class JobExecutionContext {

    private final JobLease lease;
    private final String effectId;
    private final BooleanSupplier cancellationRequested;

    private JobExecutionContext(
            JobLease lease, String effectId, BooleanSupplier cancellationRequested) {
        this.lease = lease;
        this.effectId = effectId;
        this.cancellationRequested = cancellationRequested;
    }

    public static JobExecutionContext from(JobLease lease) {
        return from(lease, () -> false);
    }

    public static JobExecutionContext from(JobLease lease, BooleanSupplier cancellationRequested) {
        String effectId = lease.jobId() + ":" + lease.executionGeneration() + ":primary";
        return new JobExecutionContext(lease, effectId, cancellationRequested);
    }

    public JobLease lease() {
        return lease;
    }

    public String effectId() {
        return effectId;
    }

    public boolean isCancellationRequested() {
        return cancellationRequested.getAsBoolean();
    }

    public void throwIfCancellationRequested() {
        if (isCancellationRequested()) {
            throw new JobCancellationException("Job cancellation requested");
        }
    }
}
