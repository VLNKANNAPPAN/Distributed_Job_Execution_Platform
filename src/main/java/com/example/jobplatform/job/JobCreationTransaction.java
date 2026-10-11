package com.example.jobplatform.job;

import com.example.jobplatform.outbox.OutboxEventEntity;
import com.example.jobplatform.outbox.OutboxEventRepository;
import com.example.jobplatform.outbox.WorkAvailableEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.EntityManager;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JobCreationTransaction {

    private final JobRepository jobRepository;
    private final JobHistoryRepository historyRepository;
    private final OutboxEventRepository outboxRepository;
    private final JobStateMachine stateMachine;
    private final ObjectMapper objectMapper;
    private final JdbcTemplate jdbcTemplate;
    private final EntityManager entityManager;

    public JobCreationTransaction(
            JobRepository jobRepository,
            JobHistoryRepository historyRepository,
            OutboxEventRepository outboxRepository,
            JobStateMachine stateMachine,
            ObjectMapper objectMapper,
            JdbcTemplate jdbcTemplate,
            EntityManager entityManager) {
        this.jobRepository = jobRepository;
        this.historyRepository = historyRepository;
        this.outboxRepository = outboxRepository;
        this.stateMachine = stateMachine;
        this.objectMapper = objectMapper;
        this.jdbcTemplate = jdbcTemplate;
        this.entityManager = entityManager;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public JobEntity create(SubmissionCommand command) {
        Instant now = jdbcTemplate
                .queryForObject("SELECT clock_timestamp()", OffsetDateTime.class)
                .toInstant();
        Instant eligibleAt = command.runAt() == null ? now : command.runAt();
        UUID jobId = UUID.randomUUID();
        JobEntity job = JobEntity.submitted(
                jobId,
                command.idempotencyKey(),
                command.requestFingerprint(),
                command.type(),
                command.priority(),
                command.payload(),
                now,
                eligibleAt,
                command.executionTimeoutSeconds());

        job = jobRepository.saveAndFlush(job);
        historyRepository.save(JobHistoryEntity.record(
                jobId, null, JobStatus.SUBMITTED, "API", "submission accepted", now));

        JobStatus target = eligibleAt.isAfter(now) ? JobStatus.SCHEDULED : JobStatus.QUEUED;
        job.transitionTo(target, stateMachine, now);
        historyRepository.save(JobHistoryEntity.record(
                jobId,
                JobStatus.SUBMITTED,
                target,
                "API",
                target == JobStatus.QUEUED ? "immediately eligible" : "scheduled for future execution",
                now));

        if (target == JobStatus.QUEUED) {
            job.advanceDispatchGeneration();
            UUID eventId = UUID.randomUUID();
            WorkAvailableEvent event = new WorkAvailableEvent(
                    eventId,
                    jobId,
                    "JOB_WORK_AVAILABLE",
                    1,
                    job.getExecutionGeneration(),
                    job.getDispatchGeneration(),
                    job.getEligibleAt(),
                    jobId);
            outboxRepository.save(OutboxEventEntity.workAvailable(
                    eventId,
                    jobId,
                    job.getExecutionGeneration(),
                    job.getDispatchGeneration(),
                    objectMapper.valueToTree(event),
                    now));
        }

        entityManager.flush();
        return job;
    }
}
