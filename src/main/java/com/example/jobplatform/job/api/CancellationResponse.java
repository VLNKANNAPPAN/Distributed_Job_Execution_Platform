package com.example.jobplatform.job.api;

import com.example.jobplatform.job.CancellationResult;

public record CancellationResponse(CancellationResult result, JobResponse job) {}
