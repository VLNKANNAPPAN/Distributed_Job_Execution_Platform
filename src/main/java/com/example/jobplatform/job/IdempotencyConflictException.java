package com.example.jobplatform.job;

public class IdempotencyConflictException extends RuntimeException {

    public IdempotencyConflictException(String key) {
        super("Idempotency key is already associated with a different request: " + key);
    }
}
