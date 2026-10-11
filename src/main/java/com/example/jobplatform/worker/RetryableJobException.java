package com.example.jobplatform.worker;

public class RetryableJobException extends RuntimeException {

    public RetryableJobException(String message) {
        super(message);
    }
}
