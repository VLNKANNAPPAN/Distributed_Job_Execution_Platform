package com.example.jobplatform.job.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record ReplayDeadLetterRequest(
        @NotBlank @Size(max = 64) String requestedBy,
        @NotBlank @Size(max = 220) String reason) {}
