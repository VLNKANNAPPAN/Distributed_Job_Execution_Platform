package com.example.jobplatform.lease;

import com.example.jobplatform.job.JobStateMachine;
import com.example.jobplatform.job.JobStatus;
import com.example.jobplatform.retry.FailureKind;
import com.example.jobplatform.retry.RetryBackoffPolicy;
import com.example.jobplatform.scheduler.SchedulingProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JobLeaseService {

    private static final String CLAIM_SQL = """
            WITH candidate AS (
                SELECT id
                FROM jobs
                WHERE status = 'QUEUED'
                  AND eligible_at <= clock_timestamp()
                  AND attempt < max_attempts
                ORDER BY LEAST(
                            100,
                            priority + FLOOR(
                                EXTRACT(EPOCH FROM (clock_timestamp() - queued_at))
                                / :priorityAgingSeconds
                            )
                         ) DESC,
                         queued_at, created_at, id
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE jobs job
            SET status = 'RUNNING',
                attempt = job.attempt + 1,
                lease_owner = :workerId,
                lease_token = :leaseToken,
                lease_version = job.lease_version + 1,
                execution_timeout_at = CASE
                    WHEN job.execution_timeout_seconds IS NULL THEN NULL
                    ELSE clock_timestamp()
                        + (job.execution_timeout_seconds * interval '1 second')
                END,
                lease_expires_at = LEAST(
                    clock_timestamp() + (:leaseMillis * interval '1 millisecond'),
                    COALESCE(
                        clock_timestamp()
                            + (job.execution_timeout_seconds * interval '1 second'),
                        'infinity'::timestamptz
                    )
                ),
                updated_at = clock_timestamp(),
                version = job.version + 1
            FROM candidate
            WHERE job.id = candidate.id
            RETURNING job.id, job.execution_generation, job.attempt, job.max_attempts,
                      job.type, job.payload::text, job.lease_version, job.lease_expires_at,
                      job.queued_at, job.updated_at, job.execution_timeout_at
            """;

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final JobStateMachine stateMachine;
    private final RetryBackoffPolicy retryBackoffPolicy;
    private final SchedulingProperties schedulingProperties;

    public JobLeaseService(
            NamedParameterJdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            JobStateMachine stateMachine,
            RetryBackoffPolicy retryBackoffPolicy,
            SchedulingProperties schedulingProperties) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.stateMachine = stateMachine;
        this.retryBackoffPolicy = retryBackoffPolicy;
        this.schedulingProperties = schedulingProperties;
    }

    @Transactional
    public Optional<JobLease> acquire(String workerId, Duration leaseDuration) {
        validateWorker(workerId);
        long leaseMillis = positiveMillis(leaseDuration);
        stateMachine.requireTransition(JobStatus.QUEUED, JobStatus.RUNNING);
        UUID leaseToken = UUID.randomUUID();
        Map<String, Object> parameters = Map.of(
                "workerId", workerId,
                "leaseToken", leaseToken,
                "leaseMillis", leaseMillis,
                "priorityAgingSeconds", positiveAgingSeconds());
        List<JobLease> claimed = jdbcTemplate.query(
                CLAIM_SQL,
                parameters,
                (resultSet, rowNumber) -> mapLease(resultSet, workerId, leaseToken));
        if (claimed.isEmpty()) {
            return Optional.empty();
        }

        JobLease lease = claimed.getFirst();
        insertAttempt(lease);
        insertHistory(
                lease.jobId(), JobStatus.QUEUED, JobStatus.RUNNING, workerId, "lease acquired");
        return Optional.of(lease);
    }

    public boolean renew(JobLease lease, Duration leaseDuration) {
        long leaseMillis = positiveMillis(leaseDuration);
        String sql = """
                UPDATE jobs
                SET lease_expires_at = LEAST(
                        clock_timestamp() + (:leaseMillis * interval '1 millisecond'),
                        COALESCE(execution_timeout_at, 'infinity'::timestamptz)
                    ),
                    updated_at = clock_timestamp(),
                    version = version + 1
                WHERE id = :jobId
                  AND execution_generation = :executionGeneration
                  AND status = 'RUNNING'
                  AND lease_owner = :workerId
                  AND lease_token = :leaseToken
                  AND lease_version = :leaseVersion
                  AND lease_expires_at > clock_timestamp()
                  AND cancellation_requested_at IS NULL
                  AND (execution_timeout_at IS NULL OR execution_timeout_at > clock_timestamp())
                """;
        return jdbcTemplate.update(sql, leaseParameters(lease, Map.of("leaseMillis", leaseMillis))) == 1;
    }

    @Transactional
    public boolean complete(JobLease lease) {
        stateMachine.requireTransition(JobStatus.RUNNING, JobStatus.SUCCEEDED);
        String sql = """
                UPDATE jobs
                SET status = 'SUCCEEDED',
                    lease_owner = NULL,
                    lease_token = NULL,
                    lease_expires_at = NULL,
                    next_attempt_at = NULL,
                    last_error_type = NULL,
                    last_error = NULL,
                    dead_lettered_at = NULL,
                    updated_at = clock_timestamp(),
                    version = version + 1
                WHERE id = :jobId
                  AND execution_generation = :executionGeneration
                  AND status = 'RUNNING'
                  AND lease_owner = :workerId
                  AND lease_token = :leaseToken
                  AND lease_version = :leaseVersion
                  AND lease_expires_at > clock_timestamp()
                  AND cancellation_requested_at IS NULL
                  AND (execution_timeout_at IS NULL OR execution_timeout_at > clock_timestamp())
                RETURNING updated_at
                """;
        if (jdbcTemplate.query(sql, leaseParameters(lease, Map.of()), (rs, row) -> rs.getObject(1))
                .isEmpty()) {
            return false;
        }
        closeAttempt(lease, "SUCCEEDED", null, null);
        insertHistory(
                lease.jobId(), JobStatus.RUNNING, JobStatus.SUCCEEDED, lease.workerId(), "job completed");
        return true;
    }

    @Transactional
    public FailureResult fail(
            JobLease lease, FailureKind kind, String errorType, String errorMessage) {
        JobStatus target = failureTarget(lease, kind);
        stateMachine.requireTransition(JobStatus.RUNNING, target);
        Duration delay = target == JobStatus.RETRY_WAIT
                ? retryBackoffPolicy.delay(lease.jobId(), lease.executionGeneration(), lease.attempt())
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
                  AND lease_expires_at > clock_timestamp()
                  AND cancellation_requested_at IS NULL
                  AND (execution_timeout_at IS NULL OR execution_timeout_at > clock_timestamp())
                RETURNING updated_at
                """;
        Map<String, Object> additions = new java.util.HashMap<>();
        additions.put("targetStatus", target.name());
        additions.put("delayMillis", delay.toMillis());
        additions.put("errorType", truncate(errorType, 64));
        additions.put("errorMessage", truncate(errorMessage, 1000));
        if (jdbcTemplate.query(sql, leaseParameters(lease, additions), (rs, row) -> rs.getObject(1))
                .isEmpty()) {
            return FailureResult.STALE;
        }

        String attemptStatus = switch (target) {
            case RETRY_WAIT -> "RETRYABLE_FAILED";
            case FAILED -> "NON_RETRYABLE_FAILED";
            case DEAD_LETTERED -> "DEAD_LETTERED";
            default -> throw new IllegalStateException("Unexpected failure target " + target);
        };
        closeAttempt(lease, attemptStatus, errorType, errorMessage);
        insertHistory(
                lease.jobId(),
                JobStatus.RUNNING,
                target,
                lease.workerId(),
                target == JobStatus.RETRY_WAIT ? "retryable failure" : "terminal failure");
        return FailureResult.valueOf(target.name());
    }

    private JobLease mapLease(ResultSet resultSet, String workerId, UUID token) throws SQLException {
        try {
            return new JobLease(
                    resultSet.getObject("id", UUID.class),
                    workerId,
                    token,
                    resultSet.getLong("lease_version"),
                    resultSet.getLong("execution_generation"),
                    resultSet.getInt("attempt"),
                    resultSet.getInt("max_attempts"),
                    resultSet.getString("type"),
                    objectMapper.readTree(resultSet.getString("payload")),
                    resultSet.getObject("queued_at", OffsetDateTime.class).toInstant(),
                    resultSet.getObject("updated_at", OffsetDateTime.class).toInstant(),
                    resultSet.getObject("lease_expires_at", OffsetDateTime.class).toInstant(),
                    toInstant(resultSet.getObject("execution_timeout_at", OffsetDateTime.class)));
        } catch (JsonProcessingException exception) {
            throw new SQLException("Stored job payload is not valid JSON", exception);
        }
    }

    private void insertAttempt(JobLease lease) {
        String sql = """
                INSERT INTO job_attempts (
                    id, job_id, execution_generation, attempt_number, worker_id,
                    lease_token, lease_version, status, started_at
                ) VALUES (
                    :id, :jobId, :executionGeneration, :attempt, :workerId,
                    :leaseToken, :leaseVersion, 'RUNNING', :startedAt
                )
                """;
        jdbcTemplate.update(sql, Map.of(
                "id", UUID.randomUUID(),
                "jobId", lease.jobId(),
                "executionGeneration", lease.executionGeneration(),
                "attempt", lease.attempt(),
                "workerId", lease.workerId(),
                "leaseToken", lease.leaseToken(),
                "leaseVersion", lease.leaseVersion(),
                "startedAt", Timestamp.from(lease.acquiredAt())));
    }

    private void closeAttempt(
            JobLease lease, String status, String errorType, String errorMessage) {
        String sql = """
                UPDATE job_attempts
                SET status = :status,
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
        Map<String, Object> additions = new java.util.HashMap<>();
        additions.put("status", status);
        additions.put("attempt", lease.attempt());
        additions.put("errorType", truncate(errorType, 64));
        additions.put("errorMessage", truncate(errorMessage, 1000));
        int updated = jdbcTemplate.update(sql, leaseParameters(lease, additions));
        if (updated != 1) {
            throw new IllegalStateException("Matching running attempt was not closed");
        }
    }

    private void insertHistory(
            UUID jobId, JobStatus from, JobStatus to, String actor, String reason) {
        String sql = """
                INSERT INTO job_history (
                    id, job_id, from_status, to_status, actor, reason, occurred_at
                ) VALUES (
                    :id, :jobId, :fromStatus, :toStatus, :actor, :reason, clock_timestamp()
                )
                """;
        jdbcTemplate.update(sql, Map.of(
                "id", UUID.randomUUID(),
                "jobId", jobId,
                "fromStatus", from.name(),
                "toStatus", to.name(),
                "actor", actor,
                "reason", reason));
    }

    private static Map<String, Object> leaseParameters(
            JobLease lease, Map<String, Object> additions) {
        Map<String, Object> parameters = new java.util.HashMap<>(additions);
        parameters.put("jobId", lease.jobId());
        parameters.put("executionGeneration", lease.executionGeneration());
        parameters.put("workerId", lease.workerId());
        parameters.put("leaseToken", lease.leaseToken());
        parameters.put("leaseVersion", lease.leaseVersion());
        return parameters;
    }

    private static JobStatus failureTarget(JobLease lease, FailureKind kind) {
        if (kind == FailureKind.NON_RETRYABLE) {
            return JobStatus.FAILED;
        }
        return lease.attempt() >= lease.maxAttempts()
                ? JobStatus.DEAD_LETTERED
                : JobStatus.RETRY_WAIT;
    }

    private static void validateWorker(String workerId) {
        if (workerId == null || workerId.isBlank() || workerId.length() > 64) {
            throw new IllegalArgumentException("workerId must contain 1 to 64 characters");
        }
    }

    private static long positiveMillis(Duration duration) {
        if (duration == null || duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException("lease duration must be positive");
        }
        return Math.max(1, duration.toMillis());
    }

    private long positiveAgingSeconds() {
        if (schedulingProperties.getPriorityAgingSeconds() < 1) {
            throw new IllegalArgumentException("priority aging seconds must be positive");
        }
        return schedulingProperties.getPriorityAgingSeconds();
    }

    private static String truncate(String value, int length) {
        if (value == null) {
            return null;
        }
        return value.length() <= length ? value : value.substring(0, length);
    }

    public boolean isCancellationRequested(JobLease lease) {
        String sql = """
                SELECT cancellation_requested_at IS NOT NULL
                FROM jobs
                WHERE id = :jobId
                  AND execution_generation = :executionGeneration
                  AND status = 'RUNNING'
                  AND lease_owner = :workerId
                  AND lease_token = :leaseToken
                  AND lease_version = :leaseVersion
                  AND lease_expires_at > clock_timestamp()
                """;
        return jdbcTemplate.query(
                        sql,
                        leaseParameters(lease, Map.of()),
                        (rs, row) -> rs.getBoolean(1))
                .stream()
                .findFirst()
                .orElse(true);
    }

    @Transactional
    public boolean cancelIfRequested(JobLease lease) {
        stateMachine.requireTransition(JobStatus.RUNNING, JobStatus.CANCELLED);
        String sql = """
                UPDATE jobs
                SET status = 'CANCELLED',
                    lease_owner = NULL,
                    lease_token = NULL,
                    lease_expires_at = NULL,
                    next_attempt_at = NULL,
                    updated_at = clock_timestamp(),
                    version = version + 1
                WHERE id = :jobId
                  AND execution_generation = :executionGeneration
                  AND status = 'RUNNING'
                  AND lease_owner = :workerId
                  AND lease_token = :leaseToken
                  AND lease_version = :leaseVersion
                  AND lease_expires_at > clock_timestamp()
                  AND cancellation_requested_at IS NOT NULL
                RETURNING cancellation_requested_by, cancellation_reason
                """;
        List<Map<String, Object>> rows = jdbcTemplate.query(
                sql,
                leaseParameters(lease, Map.of()),
                (rs, row) -> Map.of(
                        "actor", rs.getString("cancellation_requested_by"),
                        "reason", rs.getString("cancellation_reason")));
        if (rows.isEmpty()) {
            return false;
        }
        closeAttempt(lease, "CANCELLED", "CANCELLED", "cancellation requested");
        Map<String, Object> row = rows.getFirst();
        insertHistory(
                lease.jobId(),
                JobStatus.RUNNING,
                JobStatus.CANCELLED,
                row.get("actor").toString(),
                row.get("reason").toString());
        return true;
    }

    private static java.time.Instant toInstant(OffsetDateTime value) {
        return value == null ? null : value.toInstant();
    }
}
