package com.example.jobplatform.job;

import java.util.UUID;

public class DeadLetterReplayConflictException extends RuntimeException {

    public DeadLetterReplayConflictException(UUID jobId) {
        super("Job " + jobId + " is not currently dead-lettered or was already replayed");
    }
}
