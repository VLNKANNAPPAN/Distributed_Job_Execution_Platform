package com.example.jobplatform.job;

public class IllegalJobTransitionException extends RuntimeException {

    public IllegalJobTransitionException(JobStatus from, JobStatus to) {
        super("Illegal job state transition: " + from + " -> " + to);
    }
}
