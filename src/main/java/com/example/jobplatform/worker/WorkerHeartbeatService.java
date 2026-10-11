package com.example.jobplatform.worker;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class WorkerHeartbeatService {

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public WorkerHeartbeatService(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional
    public void register(String workerId, UUID sessionId) {
        validate(workerId, sessionId);
        jdbcTemplate.update(
                """
                INSERT INTO worker_heartbeats (
                    worker_id, session_id, registered_at, last_heartbeat_at, stopped_at
                ) VALUES (
                    :workerId, :sessionId, clock_timestamp(), clock_timestamp(), NULL
                )
                ON CONFLICT (worker_id) DO UPDATE
                SET session_id = EXCLUDED.session_id,
                    registered_at = EXCLUDED.registered_at,
                    last_heartbeat_at = EXCLUDED.last_heartbeat_at,
                    stopped_at = NULL
                """,
                Map.of("workerId", workerId, "sessionId", sessionId));
    }

    public boolean heartbeat(String workerId, UUID sessionId) {
        validate(workerId, sessionId);
        return jdbcTemplate.update(
                        """
                        UPDATE worker_heartbeats
                        SET last_heartbeat_at = clock_timestamp()
                        WHERE worker_id = :workerId
                          AND session_id = :sessionId
                          AND stopped_at IS NULL
                        """,
                        Map.of("workerId", workerId, "sessionId", sessionId))
                == 1;
    }

    public boolean stop(String workerId, UUID sessionId) {
        validate(workerId, sessionId);
        return jdbcTemplate.update(
                        """
                        UPDATE worker_heartbeats
                        SET stopped_at = clock_timestamp(),
                            last_heartbeat_at = clock_timestamp()
                        WHERE worker_id = :workerId
                          AND session_id = :sessionId
                          AND stopped_at IS NULL
                        """,
                        Map.of("workerId", workerId, "sessionId", sessionId))
                == 1;
    }

    @Transactional(readOnly = true)
    public List<WorkerLiveness> list(Duration timeout) {
        long timeoutMillis = positiveMillis(timeout);
        return jdbcTemplate.query(
                """
                SELECT worker_id, session_id, registered_at, last_heartbeat_at,
                       stopped_at, clock_timestamp() AS database_time,
                       stopped_at IS NULL
                           AND last_heartbeat_at > clock_timestamp()
                               - (:timeoutMillis * interval '1 millisecond')
                           AS apparently_alive
                FROM worker_heartbeats
                ORDER BY worker_id
                """,
                Map.of("timeoutMillis", timeoutMillis),
                (resultSet, rowNumber) -> new WorkerLiveness(
                        resultSet.getString("worker_id"),
                        resultSet.getObject("session_id", UUID.class),
                        resultSet.getObject("registered_at", OffsetDateTime.class).toInstant(),
                        resultSet.getObject("last_heartbeat_at", OffsetDateTime.class).toInstant(),
                        resultSet.getObject("stopped_at", OffsetDateTime.class) == null
                                ? null
                                : resultSet.getObject("stopped_at", OffsetDateTime.class).toInstant(),
                        resultSet.getObject("database_time", OffsetDateTime.class).toInstant(),
                        resultSet.getBoolean("apparently_alive")));
    }

    private static void validate(String workerId, UUID sessionId) {
        if (workerId == null || workerId.isBlank() || workerId.length() > 64) {
            throw new IllegalArgumentException("workerId must contain 1 to 64 characters");
        }
        if (sessionId == null) {
            throw new IllegalArgumentException("sessionId is required");
        }
    }

    private static long positiveMillis(Duration duration) {
        if (duration == null || duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException("liveness timeout must be positive");
        }
        return Math.max(1, duration.toMillis());
    }
}
