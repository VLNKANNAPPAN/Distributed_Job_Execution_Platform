package com.example.jobplatform.job.api;

import java.time.Instant;

import com.fasterxml.jackson.databind.JsonNode;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

public record CreateJobRequest(
        @NotBlank
                @Pattern(
                        regexp = "[A-Z][A-Z0-9_]{0,63}",
                        message = "must start with A-Z and contain only A-Z, 0-9, or underscore")
                String type,
        @Min(0) @Max(100) int priority,
        @NotNull JsonNode payload,
        Instant runAt,
        @Min(1) @Max(86400) Integer executionTimeoutSeconds) {}
