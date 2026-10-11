package com.example.jobplatform.job;

import com.example.jobplatform.job.api.AttemptResponse;
import com.example.jobplatform.job.api.DeadLetterJobResponse;
import com.example.jobplatform.job.api.DeadLetterSummary;
import com.example.jobplatform.job.api.HistoryResponse;
import com.example.jobplatform.job.api.ReplayDeadLetterRequest;
import com.example.jobplatform.job.api.ReplayResponse;
import com.example.jobplatform.outbox.WorkAvailableEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class DeadLetterService {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final JobStateMachine stateMachine;

    public DeadLetterService(
            NamedParameterJdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            JobStateMachine stateMachine) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.stateMachine = stateMachine;
    }

    @Transactional(readOnly = true)
    public List<DeadLetterSummary> list(int limit) {
        if (limit < 1 || limit > 100) {
            throw new InvalidSubmissionException("limit must be between 1 and 100");
        }
        return jdbcTemplate.query(
                """
                SELECT id, type, priority, attempt, max_attempts, execution_generation,
                       last_error_type, last_error, dead_lettered_at
                FROM jobs
                WHERE status = 'DEAD_LETTERED'
                ORDER BY dead_lettered_at, id
                LIMIT :limit
                """,
                Map.of("limit", limit),
                (resultSet, rowNumber) -> new DeadLetterSummary(
                        resultSet.getObject("id", UUID.class),
                        resultSet.getString("type"),
                        resultSet.getInt("priority"),
                        resultSet.getInt("attempt"),
                        resultSet.getInt("max_attempts"),
                        resultSet.getLong("execution_generation"),
                        resultSet.getString("last_error_type"),
                        resultSet.getString("last_error"),
                        instant(resultSet.getObject("dead_lettered_at", OffsetDateTime.class))));
    }

    @Transactional(readOnly = true)
    public DeadLetterJobResponse inspect(UUID jobId) {
        List<DeadLetterJobResponse> jobs = jdbcTemplate.query(
                """
                SELECT id, type, payload::text, priority, attempt, max_attempts,
                       execution_generation, last_error_type, last_error, dead_lettered_at
                FROM jobs
                WHERE id = :jobId AND status = 'DEAD_LETTERED'
                """,
                Map.of("jobId", jobId),
                (resultSet, rowNumber) -> new DeadLetterJobResponse(
                        resultSet.getObject("id", UUID.class),
                        resultSet.getString("type"),
                        readJson(resultSet.getString("payload")),
                        resultSet.getInt("priority"),
                        resultSet.getInt("attempt"),
                        resultSet.getInt("max_attempts"),
                        resultSet.getLong("execution_generation"),
                        resultSet.getString("last_error_type"),
                        resultSet.getString("last_error"),
                        instant(resultSet.getObject("dead_lettered_at", OffsetDateTime.class)),
                        attempts(jobId),
                        history(jobId)));
        if (jobs.isEmpty()) {
            throw new JobNotFoundException(jobId);
        }
        return jobs.getFirst();
    }

    @Transactional
    public ReplayResponse replay(UUID jobId, ReplayDeadLetterRequest request) {
        stateMachine.requireTransition(JobStatus.DEAD_LETTERED, JobStatus.QUEUED);
        List<DeadLetterRow> current = jdbcTemplate.query(
                """
                SELECT execution_generation, last_error_type, last_error
                FROM jobs
                WHERE id = :jobId AND status = 'DEAD_LETTERED'
                FOR UPDATE
                """,
                Map.of("jobId", jobId),
                (resultSet, rowNumber) -> new DeadLetterRow(
                        resultSet.getLong("execution_generation"),
                        resultSet.getString("last_error_type"),
                        resultSet.getString("last_error")));
        if (current.isEmpty()) {
            throw new DeadLetterReplayConflictException(jobId);
        }
        DeadLetterRow previous = current.getFirst();
        List<ReplayCandidate> replayed = jdbcTemplate.query(
                """
                UPDATE jobs
                SET status = 'QUEUED',
                    execution_generation = execution_generation + 1,
                    dispatch_generation = dispatch_generation + 1,
                    attempt = 0,
                    eligible_at = clock_timestamp(),
                    queued_at = clock_timestamp(),
                    next_attempt_at = NULL,
                    last_error_type = NULL,
                    last_error = NULL,
                    dead_lettered_at = NULL,
                    updated_at = clock_timestamp(),
                    version = version + 1
                WHERE id = :jobId
                  AND status = 'DEAD_LETTERED'
                  AND execution_generation = :previousGeneration
                RETURNING id, execution_generation, dispatch_generation, updated_at
                """,
                Map.of(
                        "jobId", jobId,
                        "previousGeneration", previous.executionGeneration()),
                (resultSet, rowNumber) -> new ReplayCandidate(
                        resultSet.getObject("id", UUID.class),
                        previous.executionGeneration(),
                        resultSet.getLong("execution_generation"),
                        resultSet.getLong("dispatch_generation"),
                        instant(resultSet.getObject("updated_at", OffsetDateTime.class)),
                        previous.errorType(),
                        previous.errorMessage()));
        if (replayed.isEmpty()) {
            throw new DeadLetterReplayConflictException(jobId);
        }
        ReplayCandidate replay = replayed.getFirst();
        writeReplayAudit(replay, request);
        writeHistory(replay, request);
        writeOutbox(replay);
        return new ReplayResponse(
                replay.jobId(),
                replay.previousGeneration(),
                replay.executionGeneration(),
                JobStatus.QUEUED,
                replay.replayedAt());
    }

    private void writeReplayAudit(ReplayCandidate replay, ReplayDeadLetterRequest request) {
        jdbcTemplate.update(
                """
                INSERT INTO job_replays (
                    id, job_id, from_execution_generation, to_execution_generation,
                    requested_by, reason, previous_error_type, previous_error_message,
                    requested_at
                ) VALUES (
                    :id, :jobId, :fromGeneration, :toGeneration, :requestedBy,
                    :reason, :errorType, :errorMessage, :requestedAt
                )
                """,
                nullableMap(
                        "id", UUID.randomUUID(),
                        "jobId", replay.jobId(),
                        "fromGeneration", replay.previousGeneration(),
                        "toGeneration", replay.executionGeneration(),
                        "requestedBy", request.requestedBy().strip(),
                        "reason", request.reason().strip(),
                        "errorType", replay.previousErrorType(),
                        "errorMessage", replay.previousError(),
                        "requestedAt", Timestamp.from(replay.replayedAt())));
    }

    private void writeHistory(ReplayCandidate replay, ReplayDeadLetterRequest request) {
        jdbcTemplate.update(
                """
                INSERT INTO job_history (
                    id, job_id, from_status, to_status, actor, reason, occurred_at
                ) VALUES (
                    :id, :jobId, 'DEAD_LETTERED', 'QUEUED',
                    :actor, :reason, :occurredAt
                )
                """,
                Map.of(
                        "id", UUID.randomUUID(),
                        "jobId", replay.jobId(),
                        "actor", request.requestedBy().strip(),
                        "reason", "operator replay: " + request.reason().strip(),
                        "occurredAt", Timestamp.from(replay.replayedAt())));
    }

    private void writeOutbox(ReplayCandidate replay) {
        UUID eventId = UUID.randomUUID();
        WorkAvailableEvent event = new WorkAvailableEvent(
                eventId,
                replay.jobId(),
                "JOB_WORK_AVAILABLE",
                1,
                replay.executionGeneration(),
                replay.dispatchGeneration(),
                replay.replayedAt(),
                replay.jobId());
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
                        "jobId", replay.jobId(),
                        "executionGeneration", replay.executionGeneration(),
                        "dispatchGeneration", replay.dispatchGeneration(),
                        "payload", serialize(event),
                        "availableAt", Timestamp.from(replay.replayedAt())));
    }

    private List<AttemptResponse> attempts(UUID jobId) {
        return jdbcTemplate.query(
                """
                SELECT id, execution_generation, attempt_number, worker_id,
                       lease_version, status, started_at, finished_at,
                       error_type, error_message
                FROM job_attempts
                WHERE job_id = :jobId
                ORDER BY execution_generation, attempt_number
                """,
                Map.of("jobId", jobId),
                (resultSet, rowNumber) -> new AttemptResponse(
                        resultSet.getObject("id", UUID.class),
                        resultSet.getLong("execution_generation"),
                        resultSet.getInt("attempt_number"),
                        resultSet.getString("worker_id"),
                        resultSet.getLong("lease_version"),
                        resultSet.getString("status"),
                        instant(resultSet.getObject("started_at", OffsetDateTime.class)),
                        instant(resultSet.getObject("finished_at", OffsetDateTime.class)),
                        resultSet.getString("error_type"),
                        resultSet.getString("error_message")));
    }

    private List<HistoryResponse> history(UUID jobId) {
        return jdbcTemplate.query(
                """
                SELECT id, from_status, to_status, actor, reason, occurred_at
                FROM job_history
                WHERE job_id = :jobId
                ORDER BY occurred_at, id
                """,
                Map.of("jobId", jobId),
                (resultSet, rowNumber) -> new HistoryResponse(
                        resultSet.getObject("id", UUID.class),
                        resultSet.getString("from_status") == null
                                ? null
                                : JobStatus.valueOf(resultSet.getString("from_status")),
                        JobStatus.valueOf(resultSet.getString("to_status")),
                        resultSet.getString("actor"),
                        resultSet.getString("reason"),
                        instant(resultSet.getObject("occurred_at", OffsetDateTime.class))));
    }

    private com.fasterxml.jackson.databind.JsonNode readJson(String value) {
        try {
            return objectMapper.readTree(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Stored payload is invalid JSON", exception);
        }
    }

    private String serialize(WorkAvailableEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Could not serialize replay event", exception);
        }
    }

    private static Instant instant(OffsetDateTime value) {
        return value == null ? null : value.toInstant();
    }

    private static Map<String, Object> nullableMap(Object... values) {
        Map<String, Object> result = new HashMap<>();
        for (int index = 0; index < values.length; index += 2) {
            result.put((String) values[index], values[index + 1]);
        }
        return result;
    }

    private record ReplayCandidate(
            UUID jobId,
            long previousGeneration,
            long executionGeneration,
            long dispatchGeneration,
            Instant replayedAt,
            String previousErrorType,
            String previousError) {}

    private record DeadLetterRow(
            long executionGeneration, String errorType, String errorMessage) {}
}
