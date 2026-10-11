package com.example.jobplatform.outbox;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class JdbcOutboxStore implements OutboxStore {

    private static final String CLAIM_SQL = """
            WITH candidates AS (
                SELECT id
                FROM outbox_events
                WHERE published_at IS NULL
                  AND available_at <= clock_timestamp()
                  AND next_attempt_at <= clock_timestamp()
                  AND (claim_expires_at IS NULL OR claim_expires_at <= clock_timestamp())
                ORDER BY created_at, id
                FOR UPDATE SKIP LOCKED
                LIMIT :batchSize
            )
            UPDATE outbox_events event
            SET claim_owner = :owner,
                claim_token = :claimToken,
                claim_expires_at = clock_timestamp() + (:claimSeconds * interval '1 second'),
                publish_attempts = publish_attempts + 1
            FROM candidates
            WHERE event.id = candidates.id
            RETURNING event.id, event.job_id, event.event_type, event.payload::text,
                      event.claim_token, event.publish_attempts
            """;

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public JdbcOutboxStore(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public List<OutboxClaim> claimBatch(String owner, int batchSize, Duration claimDuration) {
        UUID claimToken = UUID.randomUUID();
        Map<String, Object> parameters = Map.of(
                "owner", owner,
                "claimToken", claimToken,
                "claimSeconds", Math.max(1, claimDuration.toSeconds()),
                "batchSize", batchSize);
        return jdbcTemplate.query(CLAIM_SQL, parameters, JdbcOutboxStore::mapClaim);
    }

    @Override
    public boolean markPublished(UUID eventId, UUID claimToken) {
        String sql = """
                UPDATE outbox_events
                SET published_at = clock_timestamp(),
                    claim_owner = NULL,
                    claim_token = NULL,
                    claim_expires_at = NULL,
                    last_error = NULL
                WHERE id = :eventId
                  AND claim_token = :claimToken
                  AND claim_expires_at > clock_timestamp()
                  AND published_at IS NULL
                """;
        int updated = jdbcTemplate.update(
                sql, Map.of("eventId", eventId, "claimToken", claimToken));
        return updated == 1;
    }

    @Override
    public boolean recordFailure(
            UUID eventId, UUID claimToken, Duration retryDelay, String error) {
        String sql = """
                UPDATE outbox_events
                SET next_attempt_at = clock_timestamp() + (:retryMillis * interval '1 millisecond'),
                    claim_owner = NULL,
                    claim_token = NULL,
                    claim_expires_at = NULL,
                    last_error = :error
                WHERE id = :eventId
                  AND claim_token = :claimToken
                  AND published_at IS NULL
                """;
        Map<String, Object> parameters = Map.of(
                "eventId", eventId,
                "claimToken", claimToken,
                "retryMillis", Math.max(1, retryDelay.toMillis()),
                "error", truncate(error));
        return jdbcTemplate.update(sql, parameters) == 1;
    }

    private static OutboxClaim mapClaim(ResultSet resultSet, int rowNumber) throws SQLException {
        return new OutboxClaim(
                resultSet.getObject("id", UUID.class),
                resultSet.getObject("job_id", UUID.class),
                resultSet.getString("event_type"),
                resultSet.getString("payload"),
                resultSet.getObject("claim_token", UUID.class),
                resultSet.getInt("publish_attempts"));
    }

    private static String truncate(String value) {
        if (value == null || value.isBlank()) {
            return "unknown publication failure";
        }
        return value.length() <= 1000 ? value : value.substring(0, 1000);
    }
}
