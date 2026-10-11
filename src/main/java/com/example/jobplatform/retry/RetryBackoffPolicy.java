package com.example.jobplatform.retry;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class RetryBackoffPolicy {

    private final RetryProperties properties;

    public RetryBackoffPolicy(RetryProperties properties) {
        this.properties = properties;
    }

    public Duration delay(UUID jobId, long executionGeneration, int failedAttempt) {
        int exponent = Math.max(0, Math.min(30, failedAttempt - 1));
        long multiplier = 1L << exponent;
        long base = saturatedMultiply(properties.getInitialDelayMs(), multiplier);
        base = Math.min(base, properties.getMaxDelayMs());

        double jitter = Math.max(0.0, Math.min(1.0, properties.getJitterFactor()));
        int hash = Objects.hash(jobId, executionGeneration, failedAttempt);
        double unit = (hash & 0x7fffffff) / (double) Integer.MAX_VALUE;
        double factor = (1.0 - jitter) + (2.0 * jitter * unit);
        return Duration.ofMillis(Math.max(1, Math.round(base * factor)));
    }

    private static long saturatedMultiply(long value, long multiplier) {
        if (value <= 0 || multiplier <= 0) {
            return 1;
        }
        if (value > Long.MAX_VALUE / multiplier) {
            return Long.MAX_VALUE;
        }
        return value * multiplier;
    }
}
