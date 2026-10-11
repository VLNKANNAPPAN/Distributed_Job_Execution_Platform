package com.example.jobplatform.lease;

public enum FailureResult {
    RETRY_WAIT,
    FAILED,
    DEAD_LETTERED,
    STALE
}
