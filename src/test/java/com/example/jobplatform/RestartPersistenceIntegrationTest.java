package com.example.jobplatform;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.jobplatform.job.JobRepository;
import com.example.jobplatform.job.JobSubmissionService;
import com.example.jobplatform.job.SubmissionResult;
import com.example.jobplatform.lease.JobLease;
import com.example.jobplatform.lease.JobLeaseService;
import com.example.jobplatform.retry.FailureKind;
import com.example.jobplatform.retry.RetryScheduler;
import com.example.jobplatform.scheduler.ScheduledJobScheduler;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class RestartPersistenceIntegrationTest {

    @Container
    private static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16.4-alpine")
                    .withDatabaseName("restart_test")
                    .withUsername("test")
                    .withPassword("test");

    @BeforeEach
    void cleanDatabase() throws SQLException {
        try (Connection connection = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                Statement statement = connection.createStatement()) {
            try (ResultSet result = statement.executeQuery("SELECT to_regclass('public.jobs')")) {
                result.next();
                if (result.getString(1) == null) {
                    return;
                }
            }
            statement.executeUpdate("DELETE FROM job_replays");
            statement.executeUpdate("DELETE FROM outbox_events");
            statement.executeUpdate("DELETE FROM job_attempts");
            statement.executeUpdate("DELETE FROM job_history");
            statement.executeUpdate("DELETE FROM jobs");
            statement.executeUpdate("DELETE FROM worker_heartbeats");
        }
    }

    @Test
    void idempotencySurvivesApplicationRestart() {
        java.util.UUID originalId;
        try (ConfigurableApplicationContext first = startApplication()) {
            JobSubmissionService service = first.getBean(JobSubmissionService.class);
            ObjectMapper mapper = first.getBean(ObjectMapper.class);
            SubmissionResult created = service.submit(
                    "restart-idempotency",
                    "GENERATE_REPORT",
                    5,
                    mapper.createObjectNode().put("report", "daily"));
            assertThat(created.created()).isTrue();
            originalId = created.job().getId();
        }

        try (ConfigurableApplicationContext second = startApplication()) {
            JobSubmissionService service = second.getBean(JobSubmissionService.class);
            ObjectMapper mapper = second.getBean(ObjectMapper.class);
            SubmissionResult repeated = service.submit(
                    "restart-idempotency",
                    "GENERATE_REPORT",
                    5,
                    mapper.createObjectNode().put("report", "daily"));

            assertThat(repeated.created()).isFalse();
            assertThat(repeated.job().getId()).isEqualTo(originalId);
            assertThat(second.getBean(JobRepository.class).count()).isEqualTo(1);
        }
    }

    @Test
    void retryWaitSurvivesApplicationRestart() {
        java.util.UUID jobId;
        try (ConfigurableApplicationContext first = startApplication()) {
            JobSubmissionService submission = first.getBean(JobSubmissionService.class);
            ObjectMapper mapper = first.getBean(ObjectMapper.class);
            jobId = submission
                    .submit(
                            "restart-retry-wait",
                            "GENERATE_REPORT",
                            5,
                            mapper.createObjectNode().put("report", "restart"))
                    .job()
                    .getId();
            JobLease lease = first.getBean(JobLeaseService.class)
                    .acquire("worker-before-restart", Duration.ofMinutes(1))
                    .orElseThrow();
            first.getBean(JobLeaseService.class)
                    .fail(lease, FailureKind.RETRYABLE, "TEMPORARY", "restart required");
        }

        try (ConfigurableApplicationContext second = startApplication()) {
            JobRepository jobs = second.getBean(JobRepository.class);
            assertThat(jobs.findById(jobId).orElseThrow().getStatus().name()).isEqualTo("RETRY_WAIT");
            second.getBean(org.springframework.jdbc.core.JdbcTemplate.class)
                    .update(
                            "UPDATE jobs SET next_attempt_at = clock_timestamp() - interval '1 second'"
                                    + " WHERE id = ?",
                            jobId);
            assertThat(second.getBean(RetryScheduler.class).promoteDue(10)).isEqualTo(1);
            assertThat(jobs.findById(jobId).orElseThrow().getStatus().name()).isEqualTo("QUEUED");
        }
    }

    @Test
    void scheduledJobSurvivesApplicationRestartAndIsPromotedExactlyOnceWhenDue() {
        java.util.UUID jobId;
        try (ConfigurableApplicationContext first = startApplication()) {
            JobSubmissionService submission = first.getBean(JobSubmissionService.class);
            ObjectMapper mapper = first.getBean(ObjectMapper.class);
            jobId = submission
                    .submit(
                            "restart-scheduled",
                            "GENERATE_REPORT",
                            5,
                            mapper.createObjectNode().put("report", "scheduled"),
                            Instant.now().plus(Duration.ofHours(1)))
                    .job()
                    .getId();
            assertThat(first.getBean(JobRepository.class)
                            .findById(jobId)
                            .orElseThrow()
                            .getStatus()
                            .name())
                    .isEqualTo("SCHEDULED");
        }

        try (ConfigurableApplicationContext second = startApplication()) {
            org.springframework.jdbc.core.JdbcTemplate jdbc =
                    second.getBean(org.springframework.jdbc.core.JdbcTemplate.class);
            assertThat(jdbc.queryForObject(
                            "SELECT count(*) FROM outbox_events WHERE job_id = ?",
                            Long.class,
                            jobId))
                    .isZero();
            jdbc.update(
                    """
                    UPDATE jobs
                    SET eligible_at = clock_timestamp() - interval '1 second'
                    WHERE id = ?
                    """,
                    jobId);
            ScheduledJobScheduler scheduler = second.getBean(ScheduledJobScheduler.class);
            assertThat(scheduler.promoteDue(10)).isEqualTo(1);
            assertThat(scheduler.promoteDue(10)).isZero();
            assertThat(jdbc.queryForObject(
                            "SELECT count(*) FROM outbox_events WHERE job_id = ?",
                            Long.class,
                            jobId))
                    .isEqualTo(1);
        }
    }

    private ConfigurableApplicationContext startApplication() {
        return new SpringApplicationBuilder(JobPlatformApplication.class)
                .web(WebApplicationType.NONE)
                .profiles("test")
                .run(
                        "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                        "--spring.datasource.username=" + POSTGRES.getUsername(),
                        "--spring.datasource.password=" + POSTGRES.getPassword(),
                        "--spring.datasource.hikari.maximum-pool-size=4",
                        "--logging.level.root=WARN");
    }
}
