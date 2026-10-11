package com.example.jobplatform.job.api;

import com.example.jobplatform.job.JobStatus;
import java.time.Instant;
import java.util.UUID;

public record ReplayResponse(
        UUID jobId,
        long previousExecutionGeneration,
        long executionGeneration,
        JobStatus status,
        Instant replayedAt) {}
