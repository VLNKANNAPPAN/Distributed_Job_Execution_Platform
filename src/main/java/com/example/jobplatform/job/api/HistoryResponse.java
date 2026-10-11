package com.example.jobplatform.job.api;

import com.example.jobplatform.job.JobStatus;
import java.time.Instant;
import java.util.UUID;

public record HistoryResponse(
        UUID id,
        JobStatus fromStatus,
        JobStatus toStatus,
        String actor,
        String reason,
        Instant occurredAt) {}
