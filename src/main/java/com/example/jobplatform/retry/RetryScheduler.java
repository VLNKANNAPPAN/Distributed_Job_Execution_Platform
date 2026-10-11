package com.example.jobplatform.retry;

import com.example.jobplatform.job.JobStateMachine;
import com.example.jobplatform.job.JobStatus;
import com.example.jobplatform.outbox.WorkAvailableEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class RetryScheduler {

    private static final String SELECT_DUE_SQL = """
            SELECT id
            FROM jobs
            WHERE status = 'RETRY_WAIT'
              AND next_attempt_at <= clock_timestamp()
              AND attempt < max_attempts
            ORDER BY next_attempt_at, priority DESC, created_at, id
            FOR UPDATE SKIP LOCKED
            LIMIT :batchSize
            """;

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final JobStateMachine stateMachine;

    public RetryScheduler(
            NamedParameterJdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            JobStateMachine stateMachine) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.stateMachine = stateMachine;
    }

    @Transactional
    public int promoteDue(int batchSize) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        stateMachine.requireTransition(JobStatus.RETRY_WAIT, JobStatus.QUEUED);
        List<UUID> candidates = jdbcTemplate.query(
                SELECT_DUE_SQL,
                Map.of("batchSize", batchSize),
                (resultSet, rowNumber) -> resultSet.getObject("id", UUID.class));
        int promoted = 0;
        for (UUID jobId : candidates) {
            List<PromotedJob> result = promote(jobId);
            if (!result.isEmpty()) {
                writeCompanions(result.getFirst());
                promoted++;
            }
        }
        return promoted;
    }

    private List<PromotedJob> promote(UUID jobId) {
        String sql = """
                UPDATE jobs
                SET status = 'QUEUED',
                    eligible_at = clock_timestamp(),
                    queued_at = clock_timestamp(),
                    next_attempt_at = NULL,
                    dispatch_generation = dispatch_generation + 1,
                    updated_at = clock_timestamp(),
                    version = version + 1
                WHERE id = :jobId
                  AND status = 'RETRY_WAIT'
                  AND next_attempt_at <= clock_timestamp()
                  AND attempt < max_attempts
                RETURNING id, execution_generation, dispatch_generation, updated_at
                """;
        return jdbcTemplate.query(
                sql,
                Map.of("jobId", jobId),
                (resultSet, rowNumber) -> new PromotedJob(
                        resultSet.getObject("id", UUID.class),
                        resultSet.getLong("execution_generation"),
                        resultSet.getLong("dispatch_generation"),
                        resultSet.getObject("updated_at", OffsetDateTime.class).toInstant()));
    }

    private void writeCompanions(PromotedJob job) {
        jdbcTemplate.update(
                """
                INSERT INTO job_history (
                    id, job_id, from_status, to_status, actor, reason, occurred_at
                ) VALUES (
                    :id, :jobId, 'RETRY_WAIT', 'QUEUED',
                    'RETRY_SCHEDULER', 'retry became due', :occurredAt
                )
                """,
                Map.of(
                        "id", UUID.randomUUID(),
                        "jobId", job.jobId(),
                        "occurredAt", Timestamp.from(job.occurredAt())));

        UUID eventId = UUID.randomUUID();
        WorkAvailableEvent event = new WorkAvailableEvent(
                eventId,
                job.jobId(),
                "JOB_WORK_AVAILABLE",
                1,
                job.executionGeneration(),
                job.dispatchGeneration(),
                job.occurredAt(),
                job.jobId());
        jdbcTemplate.update(
                """
                INSERT INTO outbox_events (
                    id, job_id, event_type, schema_version, execution_generation,
                    dispatch_generation, payload, available_at, next_attempt_at,
                    publish_attempts, created_at
                ) VALUES (
                    :id, :jobId, 'JOB_WORK_AVAILABLE', 1, :executionGeneration,
                    :dispatchGeneration, CAST(:payload AS jsonb), :availableAt,
                    :availableAt, 0, :availableAt
                )
                """,
                Map.of(
                        "id", eventId,
                        "jobId", job.jobId(),
                        "executionGeneration", job.executionGeneration(),
                        "dispatchGeneration", job.dispatchGeneration(),
                        "payload", serialize(event),
                        "availableAt", Timestamp.from(job.occurredAt())));
    }

    private String serialize(WorkAvailableEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Could not serialize retry event", exception);
        }
    }

    private record PromotedJob(
            UUID jobId, long executionGeneration, long dispatchGeneration, Instant occurredAt) {}
}
