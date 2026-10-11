package com.example.jobplatform.job.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.example.jobplatform.support.PostgresIntegrationTest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

@AutoConfigureMockMvc
class JobApiIntegrationTest extends PostgresIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void createsRetrievesAndListsJob() throws Exception {
        String body = """
                {"type":"GENERATE_REPORT","priority":5,"payload":{"report":"daily"}}
                """;
        String response = mockMvc.perform(post("/jobs")
                        .header("Idempotency-Key", "create-retrieve-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body))
                .andExpect(status().isCreated())
                .andExpect(header().string("Location", org.hamcrest.Matchers.startsWith("/jobs/")))
                .andExpect(jsonPath("$.status").value("QUEUED"))
                .andExpect(jsonPath("$.attempt").value(0))
                .andReturn()
                .getResponse()
                .getContentAsString();
        UUID id = UUID.fromString(objectMapper.readTree(response).get("id").asText());

        mockMvc.perform(get("/jobs/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(id.toString()))
                .andExpect(jsonPath("$.status").value("QUEUED"))
                .andExpect(jsonPath("$.payload.report").value("daily"));

        mockMvc.perform(get("/jobs").param("page", "0").param("size", "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalElements").value(1))
                .andExpect(jsonPath("$.items[0].id").value(id.toString()));
    }

    @Test
    void repeatedSameKeyAndCanonicalPayloadReturnsOriginalJob() throws Exception {
        String first = submit("same-key", "{\"b\":2,\"a\":1}", 201);
        String second = submit("same-key", "{\"a\":1,\"b\":2}", 200);

        assertThat(objectMapper.readTree(second).get("id").asText())
                .isEqualTo(objectMapper.readTree(first).get("id").asText());
        assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM jobs", Long.class)).isEqualTo(1);
        assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM outbox_events", Long.class))
                .isEqualTo(1);
    }

    @Test
    void sameKeyWithDifferentPayloadReturnsConflict() throws Exception {
        submit("conflicting-key", "{\"report\":\"daily\"}", 201);

        mockMvc.perform(post("/jobs")
                        .header("Idempotency-Key", "conflicting-key")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"type":"GENERATE_REPORT","priority":5,
                                 "payload":{"report":"weekly"}}
                                """))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.title").value("Idempotency conflict"));
    }

    @Test
    void invalidRequestsUseProblemDetails() throws Exception {
        mockMvc.perform(post("/jobs")
                        .header("Idempotency-Key", "invalid-type")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"type":"bad type","priority":101,"payload":[]}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").value("Request validation failed"));

        mockMvc.perform(post("/jobs")
                        .header("Idempotency-Key", "payload-array")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"type":"GENERATE_REPORT","priority":5,"payload":[]}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.title").value("Invalid request"));

        mockMvc.perform(post("/jobs")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"type":"GENERATE_REPORT","priority":5,"payload":{}}
                                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    void missingJobReturnsProblemDetails() throws Exception {
        mockMvc.perform(get("/jobs/{id}", UUID.randomUUID()))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.title").value("Job not found"));
    }

    @Test
    void futureRunAtCreatesScheduledJobWithoutOutboxSignal() throws Exception {
        String response = mockMvc.perform(post("/jobs")
                        .header("Idempotency-Key", "future-api-job")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"type":"GENERATE_REPORT","priority":5,"payload":{},
                                 "runAt":"2035-01-01T00:00:00Z"}
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("SCHEDULED"))
                .andExpect(jsonPath("$.runAt").value("2035-01-01T00:00:00Z"))
                .andReturn()
                .getResponse()
                .getContentAsString();
        UUID id = UUID.fromString(objectMapper.readTree(response).get("id").asText());

        assertThat(jdbcTemplate.queryForObject(
                        "SELECT count(*) FROM outbox_events WHERE job_id = ?", Long.class, id))
                .isZero();
    }

    private String submit(String key, String payload, int expectedStatus) throws Exception {
        JsonNode request = objectMapper.readTree("""
                {"type":"GENERATE_REPORT","priority":5,"payload":{}}
                """);
        ((com.fasterxml.jackson.databind.node.ObjectNode) request)
                .set("payload", objectMapper.readTree(payload));
        return mockMvc.perform(post("/jobs")
                        .header("Idempotency-Key", key)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsBytes(request)))
                .andExpect(status().is(expectedStatus))
                .andReturn()
                .getResponse()
                .getContentAsString();
    }
}
