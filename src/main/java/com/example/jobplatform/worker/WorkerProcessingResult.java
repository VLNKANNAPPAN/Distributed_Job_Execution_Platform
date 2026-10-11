package com.example.jobplatform.worker;

public enum WorkerProcessingResult {
    NO_WORK,
    SUCCEEDED,
    RETRY_WAIT,
    FAILED,
    DEAD_LETTERED,
    STALE,
    CANCELLED
}
