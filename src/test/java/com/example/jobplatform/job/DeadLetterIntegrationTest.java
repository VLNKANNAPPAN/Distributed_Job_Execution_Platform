package com.example.jobplatform.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.example.jobplatform.job.api.ReplayDeadLetterRequest;
import com.example.jobplatform.job.api.ReplayResponse;
import com.example.jobplatform.lease.FailureResult;
import com.example.jobplatform.lease.JobLease;
import com.example.jobplatform.lease.JobLeaseService;
import com.example.jobplatform.retry.FailureKind;
import com.example.jobplatform.retry.RetryScheduler;
import com.example.jobplatform.support.PostgresIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

@AutoConfigureMockMvc
class DeadLetterIntegrationTest extends PostgresIntegrationTest {

    @Autowired
    private JobSubmissionService submissionService;

    @Autowired
    private JobLeaseService leaseService;

    @Autowired
    private RetryScheduler retryScheduler;

    @Autowired
    private DeadLetterService deadLetterService;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private MockMvc mockMvc;

    @Test
    void exhaustedJobIsInspectableNotAutomaticallyClaimableAndExplicitlyReplayable()
            throws Exception {
        UUID jobId = exhaust("dead-letter-api");

        assertThat(leaseService.acquire("worker-extra", Duration.ofMinutes(1))).isEmpty();
        mockMvc.perform(get("/operator/dead-letter-jobs"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].jobId").value(jobId.toString()))
                .andExpect(jsonPath("$[0].failureType").value("TRANSIENT"));
        mockMvc.perform(get("/operator/dead-letter-jobs/{jobId}", jobId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.failureReason").value("failure 3"))
                .andExpect(jsonPath("$.attemptHistory.length()").value(3))
                .andExpect(jsonPath("$.attemptHistory[2].status").value("DEAD_LETTERED"));

        mockMvc.perform(post("/operator/dead-letter-jobs/{jobId}/replay", jobId)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"requestedBy":"operator-a","reason":"dependency repaired"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.previousExecutionGeneration").value(1))
                .andExpect(jsonPath("$.executionGeneration").value(2))
                .andExpect(jsonPath("$.status").value("QUEUED"));

        JobEntity replayed = jobRepository.findById(jobId).orElseThrow();
        assertThat(replayed.getStatus()).isEqualTo(JobStatus.QUEUED);
        assertThat(replayed.getExecutionGeneration()).isEqualTo(2);
        assertThat(replayed.getAttempt()).isZero();
        assertThat(replayed.getLastError()).isNull();
        assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM job_replays", Long.class))
                .isEqualTo(1);

        JobLease replayLease =
                leaseService.acquire("worker-replay", Duration.ofMinutes(1)).orElseThrow();
        assertThat(replayLease.executionGeneration()).isEqualTo(2);
        assertThat(replayLease.attempt()).isEqualTo(1);
    }

    @Test
    void concurrentReplayRequestsProduceOneNewGenerationAndOneAuditRecord() throws Exception {
        UUID jobId = exhaust("dead-letter-race");
        ReplayDeadLetterRequest request =
                new ReplayDeadLetterRequest("operator-race", "confirmed safe to replay");
        CyclicBarrier barrier = new CyclicBarrier(2);
        List<Callable<Object>> calls = List.of(
                () -> replayAfterBarrier(jobId, request, barrier),
                () -> replayAfterBarrier(jobId, request, barrier));
        List<Object> results = new ArrayList<>();

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (Future<Object> result : executor.invokeAll(calls)) {
                results.add(result.get());
            }
        }

        assertThat(results.stream().filter(ReplayResponse.class::isInstance)).hasSize(1);
        assertThat(results.stream().filter(DeadLetterReplayConflictException.class::isInstance))
                .hasSize(1);
        assertThat(jobRepository.findById(jobId).orElseThrow().getExecutionGeneration())
                .isEqualTo(2);
        assertThat(jdbcTemplate.queryForObject("SELECT count(*) FROM job_replays", Long.class))
                .isEqualTo(1);
    }

    private Object replayAfterBarrier(
            UUID jobId, ReplayDeadLetterRequest request, CyclicBarrier barrier) throws Exception {
        barrier.await();
        try {
            return deadLetterService.replay(jobId, request);
        } catch (DeadLetterReplayConflictException exception) {
            return exception;
        }
    }

    private UUID exhaust(String key) {
        UUID jobId = submissionService
                .submit(
                        key,
                        "GENERATE_REPORT",
                        5,
                        objectMapper.createObjectNode().put("report", key))
                .job()
                .getId();
        for (int attempt = 1; attempt <= 3; attempt++) {
            JobLease lease = leaseService
                    .acquire("worker-" + attempt, Duration.ofMinutes(1))
                    .orElseThrow();
            FailureResult result = leaseService.fail(
                    lease, FailureKind.RETRYABLE, "TRANSIENT", "failure " + attempt);
            if (attempt < 3) {
                assertThat(result).isEqualTo(FailureResult.RETRY_WAIT);
                jdbcTemplate.update(
                        """
                        UPDATE jobs
                        SET next_attempt_at = clock_timestamp() - interval '1 second'
                        WHERE id = ?
                        """,
                        jobId);
                assertThat(retryScheduler.promoteDue(10)).isEqualTo(1);
            } else {
                assertThat(result).isEqualTo(FailureResult.DEAD_LETTERED);
            }
        }
        return jobId;
    }
}
