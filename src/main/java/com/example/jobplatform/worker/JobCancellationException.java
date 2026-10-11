package com.example.jobplatform.worker;

public class JobCancellationException extends RuntimeException {

    public JobCancellationException(String message) {
        super(message);
    }
}
