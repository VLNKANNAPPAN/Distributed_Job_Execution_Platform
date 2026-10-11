package com.example.jobplatform.job;

import java.util.UUID;

public class CancellationConflictException extends RuntimeException {

    public CancellationConflictException(UUID jobId, JobStatus status) {
        super("Job " + jobId + " cannot be cancelled from " + status);
    }
}
