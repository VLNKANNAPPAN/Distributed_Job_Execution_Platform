package com.example.jobplatform.job;

import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JobCancellationService {

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final JobStateMachine stateMachine;

    public JobCancellationService(
            NamedParameterJdbcTemplate jdbcTemplate, JobStateMachine stateMachine) {
        this.jdbcTemplate = jdbcTemplate;
        this.stateMachine = stateMachine;
    }

    @Transactional
    public CancellationResult cancel(UUID jobId, String requestedBy, String reason) {
        String actor = requireText(requestedBy, "requestedBy", 64);
        String normalizedReason = requireText(reason, "reason", 256);
        JobStatus status = jdbcTemplate.query(
                        "SELECT status FROM jobs WHERE id = :jobId FOR UPDATE",
                        Map.of("jobId", jobId),
                        (rs, row) -> JobStatus.valueOf(rs.getString(1)))
                .stream()
                .findFirst()
                .orElseThrow(() -> new JobNotFoundException(jobId));

        if (status == JobStatus.CANCELLED) {
            return CancellationResult.ALREADY_CANCELLED;
        }
        if (status == JobStatus.RUNNING) {
            int updated = jdbcTemplate.update(
                    """
                    UPDATE jobs
                    SET cancellation_requested_at = clock_timestamp(),
                        cancellation_requested_by = :actor,
                        cancellation_reason = :reason,
                        updated_at = clock_timestamp(),
                        version = version + 1
                    WHERE id = :jobId
                      AND status = 'RUNNING'
                      AND cancellation_requested_at IS NULL
                    """,
                    Map.of("jobId", jobId, "actor", actor, "reason", normalizedReason));
            return updated == 1
                    ? CancellationResult.CANCELLATION_REQUESTED
                    : CancellationResult.CANCELLATION_REQUESTED;
        }
        if (status == JobStatus.SUBMITTED
                || status == JobStatus.SCHEDULED
                || status == JobStatus.QUEUED
                || status == JobStatus.RETRY_WAIT) {
            stateMachine.requireTransition(status, JobStatus.CANCELLED);
            int updated = jdbcTemplate.update(
                    """
                    UPDATE jobs
                    SET status = 'CANCELLED',
                        cancellation_requested_at = clock_timestamp(),
                        cancellation_requested_by = :actor,
                        cancellation_reason = :reason,
                        next_attempt_at = NULL,
                        updated_at = clock_timestamp(),
                        version = version + 1
                    WHERE id = :jobId AND status = :status
                    """,
                    Map.of(
                            "jobId", jobId,
                            "actor", actor,
                            "reason", normalizedReason,
                            "status", status.name()));
            if (updated != 1) {
                throw new IllegalStateException("Locked cancellation update did not affect one row");
            }
            insertHistory(jobId, status, actor, normalizedReason);
            return CancellationResult.CANCELLED;
        }
        throw new CancellationConflictException(jobId, status);
    }

    private void insertHistory(UUID jobId, JobStatus from, String actor, String reason) {
        jdbcTemplate.update(
                """
                INSERT INTO job_history (
                    id, job_id, from_status, to_status, actor, reason, occurred_at
                ) VALUES (
                    :id, :jobId, :fromStatus, 'CANCELLED', :actor, :reason, clock_timestamp()
                )
                """,
                Map.of(
                        "id", UUID.randomUUID(),
                        "jobId", jobId,
                        "fromStatus", from.name(),
                        "actor", actor,
                        "reason", reason));
    }

    private static String requireText(String value, String field, int maxLength) {
        if (value == null || value.isBlank() || value.length() > maxLength) {
            throw new InvalidSubmissionException(
                    field + " must contain 1 to " + maxLength + " characters");
        }
        return value.strip();
    }
}
