package com.example.jobplatform.job;

public enum JobStatus {
    SUBMITTED,
    SCHEDULED,
    QUEUED,
    RUNNING,
    RETRY_WAIT,
    SUCCEEDED,
    FAILED,
    DEAD_LETTERED,
    CANCELLED
}
