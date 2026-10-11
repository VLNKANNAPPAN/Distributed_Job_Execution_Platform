package com.example.jobplatform.retry;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class RetryBackoffPolicyTest {

    @Test
    void doublesAndCapsWithoutJitter() {
        RetryProperties properties = new RetryProperties();
        properties.setInitialDelayMs(1000);
        properties.setMaxDelayMs(8000);
        properties.setJitterFactor(0);
        RetryBackoffPolicy policy = new RetryBackoffPolicy(properties);
        UUID jobId = UUID.randomUUID();

        assertThat(policy.delay(jobId, 1, 1)).isEqualTo(Duration.ofSeconds(1));
        assertThat(policy.delay(jobId, 1, 2)).isEqualTo(Duration.ofSeconds(2));
        assertThat(policy.delay(jobId, 1, 4)).isEqualTo(Duration.ofSeconds(8));
        assertThat(policy.delay(jobId, 1, 10)).isEqualTo(Duration.ofSeconds(8));
    }

    @Test
    void jitterIsDeterministicForOneAttemptAndBounded() {
        RetryProperties properties = new RetryProperties();
        properties.setInitialDelayMs(1000);
        properties.setMaxDelayMs(10000);
        properties.setJitterFactor(0.2);
        RetryBackoffPolicy policy = new RetryBackoffPolicy(properties);
        UUID jobId = UUID.randomUUID();

        Duration first = policy.delay(jobId, 1, 2);
        Duration repeated = policy.delay(jobId, 1, 2);

        assertThat(repeated).isEqualTo(first);
        assertThat(first.toMillis()).isBetween(1600L, 2400L);
    }
}
