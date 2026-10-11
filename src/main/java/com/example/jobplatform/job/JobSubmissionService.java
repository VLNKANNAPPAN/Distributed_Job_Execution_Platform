package com.example.jobplatform.job;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.Optional;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import com.example.jobplatform.observability.PlatformMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class JobSubmissionService {

    private static final int MAX_IDEMPOTENCY_KEY_LENGTH = 128;
    private static final Logger LOGGER = LoggerFactory.getLogger(JobSubmissionService.class);

    private final JobCreationTransaction creationTransaction;
    private final JobLookupTransaction lookupTransaction;
    private final RequestFingerprint requestFingerprint;
    private final PlatformMetrics metrics;

    public JobSubmissionService(
            JobCreationTransaction creationTransaction,
            JobLookupTransaction lookupTransaction,
            RequestFingerprint requestFingerprint,
            PlatformMetrics metrics) {
        this.creationTransaction = creationTransaction;
        this.lookupTransaction = lookupTransaction;
        this.requestFingerprint = requestFingerprint;
        this.metrics = metrics;
    }

    public SubmissionResult submit(String rawIdempotencyKey, String type, int priority, JsonNode payload) {
        return submit(rawIdempotencyKey, type, priority, payload, null, null);
    }

    public SubmissionResult submit(
            String rawIdempotencyKey,
            String type,
            int priority,
            JsonNode payload,
            Instant runAt) {
        return submit(rawIdempotencyKey, type, priority, payload, runAt, null);
    }

    public SubmissionResult submit(
            String rawIdempotencyKey,
            String type,
            int priority,
            JsonNode payload,
            Instant runAt,
            Integer executionTimeoutSeconds) {
        String key = validateAndNormalizeKey(rawIdempotencyKey);
        if (payload == null || !payload.isObject()) {
            throw new InvalidSubmissionException("payload must be a JSON object");
        }
        if (executionTimeoutSeconds != null
                && (executionTimeoutSeconds < 1 || executionTimeoutSeconds > 86400)) {
            throw new InvalidSubmissionException(
                    "executionTimeoutSeconds must be between 1 and 86400");
        }
        String fingerprint = requestFingerprint.calculate(
                type, priority, payload, runAt, executionTimeoutSeconds);
        Optional<JobEntity> existing = lookupTransaction.findByIdempotencyKey(key);
        if (existing.isPresent()) {
            return compareExisting(existing.get(), fingerprint, key);
        }

        SubmissionCommand command =
                new SubmissionCommand(
                        key, fingerprint, type, priority, payload, runAt, executionTimeoutSeconds);
        try {
            JobEntity created = creationTransaction.create(command);
            metrics.submitted();
            LOGGER.atInfo()
                    .addKeyValue("job_id", created.getId())
                    .addKeyValue("execution_generation", created.getExecutionGeneration())
                    .addKeyValue("status", created.getStatus())
                    .log("job submitted");
            return new SubmissionResult(created, true);
        } catch (DataIntegrityViolationException race) {
            JobEntity winner = lookupTransaction
                    .findByIdempotencyKey(key)
                    .orElseThrow(() -> race);
            return compareExisting(winner, fingerprint, key);
        }
    }

    private SubmissionResult compareExisting(JobEntity existing, String fingerprint, String key) {
        if (!existing.getRequestFingerprint().equals(fingerprint)) {
            throw new IdempotencyConflictException(key);
        }
        return new SubmissionResult(existing, false);
    }

    private String validateAndNormalizeKey(String rawKey) {
        if (rawKey == null) {
            throw new InvalidSubmissionException("Idempotency-Key header is required");
        }
        String key = rawKey.strip();
        if (key.isEmpty() || key.length() > MAX_IDEMPOTENCY_KEY_LENGTH) {
            throw new InvalidSubmissionException(
                    "Idempotency-Key must contain 1 to " + MAX_IDEMPOTENCY_KEY_LENGTH + " characters");
        }
        if (key.chars().anyMatch(Character::isISOControl)) {
            throw new InvalidSubmissionException("Idempotency-Key must not contain control characters");
        }
        return key;
    }
}
