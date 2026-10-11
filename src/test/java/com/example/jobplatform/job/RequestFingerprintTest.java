package com.example.jobplatform.job;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class RequestFingerprintTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final RequestFingerprint fingerprint = new RequestFingerprint(objectMapper);

    @Test
    void ignoresJsonObjectFieldOrder() throws Exception {
        String first = fingerprint.calculate(
                "GENERATE_REPORT", 5, objectMapper.readTree("{\"b\":2,\"a\":{\"y\":2,\"x\":1}}"));
        String second = fingerprint.calculate(
                "GENERATE_REPORT", 5, objectMapper.readTree("{\"a\":{\"x\":1,\"y\":2},\"b\":2}"));

        assertThat(first).isEqualTo(second).hasSize(64);
    }

    @Test
    void changesWhenExecutionRelevantInputChanges() throws Exception {
        String base = fingerprint.calculate(
                "GENERATE_REPORT", 5, objectMapper.readTree("{\"report\":\"daily\"}"));

        assertThat(fingerprint.calculate(
                        "GENERATE_REPORT", 6, objectMapper.readTree("{\"report\":\"daily\"}")))
                .isNotEqualTo(base);
        assertThat(fingerprint.calculate(
                        "GENERATE_REPORT", 5, objectMapper.readTree("{\"report\":\"weekly\"}")))
                .isNotEqualTo(base);
        assertThat(fingerprint.calculate(
                        "GENERATE_REPORT",
                        5,
                        objectMapper.readTree("{\"report\":\"daily\"}"),
                        Instant.parse("2030-01-01T00:00:00Z")))
                .isNotEqualTo(base);
    }
}
