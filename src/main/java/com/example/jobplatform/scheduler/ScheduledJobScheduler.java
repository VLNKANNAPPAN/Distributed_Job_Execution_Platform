package com.example.jobplatform.scheduler;

import com.example.jobplatform.job.JobStateMachine;
import com.example.jobplatform.job.JobStatus;
import com.example.jobplatform.outbox.WorkAvailableEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ScheduledJobScheduler {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final JobStateMachine stateMachine;

    public ScheduledJobScheduler(
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
        stateMachine.requireTransition(JobStatus.SCHEDULED, JobStatus.QUEUED);
        List<UUID> candidates = jdbcTemplate.query(
                """
                SELECT id
                FROM jobs
                WHERE status = 'SCHEDULED'
                  AND eligible_at <= clock_timestamp()
                ORDER BY eligible_at, priority DESC, created_at, id
                FOR UPDATE SKIP LOCKED
                LIMIT :batchSize
                """,
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
        return jdbcTemplate.query(
                """
                UPDATE jobs
                SET status = 'QUEUED',
                    queued_at = clock_timestamp(),
                    dispatch_generation = dispatch_generation + 1,
                    updated_at = clock_timestamp(),
                    version = version + 1
                WHERE id = :jobId
                  AND status = 'SCHEDULED'
                  AND eligible_at <= clock_timestamp()
                RETURNING id, execution_generation, dispatch_generation, updated_at
                """,
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
                    :id, :jobId, 'SCHEDULED', 'QUEUED',
                    'SCHEDULER', 'scheduled time became due', :occurredAt
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
            throw new IllegalStateException("Could not serialize scheduled event", exception);
        }
    }

    private record PromotedJob(
            UUID jobId, long executionGeneration, long dispatchGeneration, Instant occurredAt) {}
}
