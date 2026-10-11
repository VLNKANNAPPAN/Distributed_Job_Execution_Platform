package com.example.jobplatform.retry;

import com.example.jobplatform.job.JobStateMachine;
import com.example.jobplatform.job.JobStatus;
import com.example.jobplatform.observability.PlatformMetrics;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ExpiredLeaseReaper {

    private static final String SELECT_EXPIRED_SQL = """
            SELECT id, execution_generation, attempt, max_attempts,
                   lease_owner, lease_token, lease_version, lease_expires_at,
                   cancellation_requested_at, cancellation_requested_by,
                   cancellation_reason, execution_timeout_at,
                   execution_timeout_at IS NOT NULL
                       AND execution_timeout_at <= clock_timestamp() AS execution_timed_out
            FROM jobs
            WHERE status = 'RUNNING'
              AND lease_expires_at <= clock_timestamp()
            ORDER BY lease_expires_at, id
            FOR UPDATE SKIP LOCKED
            LIMIT :batchSize
            """;

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final RetryBackoffPolicy backoffPolicy;
    private final JobStateMachine stateMachine;
    private final PlatformMetrics metrics;

    public ExpiredLeaseReaper(
            NamedParameterJdbcTemplate jdbcTemplate,
            RetryBackoffPolicy backoffPolicy,
            JobStateMachine stateMachine,
            PlatformMetrics metrics) {
        this.jdbcTemplate = jdbcTemplate;
        this.backoffPolicy = backoffPolicy;
        this.stateMachine = stateMachine;
        this.metrics = metrics;
    }

    @Transactional
    public int reapExpired(int batchSize) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        List<ExpiredLease> expired = jdbcTemplate.query(
                SELECT_EXPIRED_SQL,
                Map.of("batchSize", batchSize),
                (resultSet, rowNumber) -> new ExpiredLease(
                        resultSet.getObject("id", UUID.class),
                        resultSet.getLong("execution_generation"),
                        resultSet.getInt("attempt"),
                        resultSet.getInt("max_attempts"),
                        resultSet.getString("lease_owner"),
                        resultSet.getObject("lease_token", UUID.class),
                        resultSet.getLong("lease_version"),
                        resultSet.getObject("lease_expires_at", OffsetDateTime.class).toInstant(),
                        toInstant(resultSet.getObject(
                                "cancellation_requested_at", OffsetDateTime.class)),
                        resultSet.getString("cancellation_requested_by"),
                        resultSet.getString("cancellation_reason"),
                        toInstant(resultSet.getObject("execution_timeout_at", OffsetDateTime.class)),
                        resultSet.getBoolean("execution_timed_out")));
        int recovered = 0;
        for (ExpiredLease lease : expired) {
            if (recover(lease)) {
                recovered++;
            }
        }
        return recovered;
    }

    private boolean recover(ExpiredLease lease) {
        boolean cancelled = lease.cancellationRequestedAt() != null;
        boolean timedOut = !cancelled && lease.executionTimedOut();
        JobStatus target = cancelled
                ? JobStatus.CANCELLED
                : (lease.attempt() >= lease.maxAttempts()
                        ? JobStatus.DEAD_LETTERED
                        : JobStatus.RETRY_WAIT);
        stateMachine.requireTransition(JobStatus.RUNNING, target);
        Duration delay = target == JobStatus.RETRY_WAIT
                ? backoffPolicy.delay(lease.jobId(), lease.executionGeneration(), lease.attempt())
                : Duration.ZERO;
        String sql = """
                UPDATE jobs
                SET status = :targetStatus,
                    lease_owner = NULL,
                    lease_token = NULL,
                    lease_expires_at = NULL,
                    next_attempt_at = CASE
                        WHEN :targetStatus = 'RETRY_WAIT'
                            THEN clock_timestamp() + (:delayMillis * interval '1 millisecond')
                        ELSE NULL
                    END,
                    last_error_type = :errorType,
                    last_error = :errorMessage,
                    dead_lettered_at = CASE
                        WHEN :targetStatus = 'DEAD_LETTERED' THEN clock_timestamp()
                        ELSE NULL
                    END,
                    updated_at = clock_timestamp(),
                    version = version + 1
                WHERE id = :jobId
                  AND execution_generation = :executionGeneration
                  AND status = 'RUNNING'
                  AND lease_owner = :workerId
                  AND lease_token = :leaseToken
                  AND lease_version = :leaseVersion
                  AND lease_expires_at <= clock_timestamp()
                """;
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("targetStatus", target.name());
        parameters.put("delayMillis", delay.toMillis());
        parameters.put("errorType", cancelled ? "CANCELLED" : timedOut ? "EXECUTION_TIMEOUT" : "LEASE_EXPIRED");
        parameters.put(
                "errorMessage",
                cancelled ? "cancellation requested" : timedOut ? "execution timed out" : "worker lease expired");
        parameters.put("jobId", lease.jobId());
        parameters.put("executionGeneration", lease.executionGeneration());
        parameters.put("workerId", lease.workerId());
        parameters.put("leaseToken", lease.leaseToken());
        parameters.put("leaseVersion", lease.leaseVersion());
        if (jdbcTemplate.update(sql, parameters) != 1) {
            return false;
        }

        closeAttempt(lease, target, cancelled, timedOut);
        insertHistory(lease, target, cancelled, timedOut);
        metrics.afterCommit(() -> {
            metrics.leaseExpired();
            metrics.outcome(target.name());
        });
        return true;
    }

    private void closeAttempt(
            ExpiredLease lease, JobStatus target, boolean cancelled, boolean timedOut) {
        String sql = """
                UPDATE job_attempts
                SET status = :attemptStatus,
                    finished_at = clock_timestamp(),
                    error_type = :errorType,
                    error_message = :errorMessage
                WHERE job_id = :jobId
                  AND execution_generation = :executionGeneration
                  AND attempt_number = :attempt
                  AND lease_token = :leaseToken
                  AND lease_version = :leaseVersion
                  AND status = 'RUNNING'
                """;
        int updated = jdbcTemplate.update(sql, Map.of(
                "attemptStatus",
                cancelled
                        ? "CANCELLED"
                        : timedOut
                                ? "TIMED_OUT"
                                : target == JobStatus.DEAD_LETTERED
                                        ? "DEAD_LETTERED"
                                        : "LEASE_EXPIRED",
                "errorType",
                cancelled ? "CANCELLED" : timedOut ? "EXECUTION_TIMEOUT" : "LEASE_EXPIRED",
                "errorMessage",
                cancelled ? "cancellation requested" : timedOut ? "execution timed out" : "worker lease expired",
                "jobId",
                lease.jobId(),
                "executionGeneration",
                lease.executionGeneration(),
                "attempt",
                lease.attempt(),
                "leaseToken",
                lease.leaseToken(),
                "leaseVersion",
                lease.leaseVersion()));
        if (updated != 1) {
            throw new IllegalStateException("Expired running attempt was not closed");
        }
    }

    private void insertHistory(
            ExpiredLease lease, JobStatus target, boolean cancelled, boolean timedOut) {
        String sql = """
                INSERT INTO job_history (
                    id, job_id, from_status, to_status, actor, reason, occurred_at
                ) VALUES (
                    :id, :jobId, 'RUNNING', :toStatus,
                    :actor, :reason, clock_timestamp()
                )
                """;
        jdbcTemplate.update(sql, Map.of(
                "id",
                UUID.randomUUID(),
                "jobId",
                lease.jobId(),
                "toStatus",
                target.name(),
                "actor",
                cancelled ? lease.cancellationRequestedBy() : "LEASE_REAPER",
                "reason",
                cancelled
                        ? lease.cancellationReason()
                        : timedOut ? "execution timed out" : "worker lease expired"));
    }

    private record ExpiredLease(
            UUID jobId,
            long executionGeneration,
            int attempt,
            int maxAttempts,
            String workerId,
            UUID leaseToken,
            long leaseVersion,
            java.time.Instant leaseExpiresAt,
            java.time.Instant cancellationRequestedAt,
            String cancellationRequestedBy,
            String cancellationReason,
            java.time.Instant executionTimeoutAt,
            boolean executionTimedOut) {}

    private static java.time.Instant toInstant(OffsetDateTime value) {
        return value == null ? null : value.toInstant();
    }
}
