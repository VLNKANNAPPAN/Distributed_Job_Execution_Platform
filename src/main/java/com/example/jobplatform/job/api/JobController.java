package com.example.jobplatform.job.api;

import com.example.jobplatform.job.JobQueryService;
import com.example.jobplatform.job.JobCancellationService;
import com.example.jobplatform.job.CancellationResult;
import com.example.jobplatform.job.JobSubmissionService;
import com.example.jobplatform.job.SubmissionResult;
import jakarta.validation.Valid;
import java.net.URI;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/jobs")
public class JobController {

    private final JobSubmissionService submissionService;
    private final JobQueryService queryService;
    private final JobCancellationService cancellationService;

    public JobController(
            JobSubmissionService submissionService,
            JobQueryService queryService,
            JobCancellationService cancellationService) {
        this.submissionService = submissionService;
        this.queryService = queryService;
        this.cancellationService = cancellationService;
    }

    @PostMapping
    public ResponseEntity<JobResponse> submit(
            @RequestHeader(name = "Idempotency-Key") String idempotencyKey,
            @Valid @RequestBody CreateJobRequest request) {
        SubmissionResult result = submissionService.submit(
                idempotencyKey,
                request.type(),
                request.priority(),
                request.payload(),
                request.runAt(),
                request.executionTimeoutSeconds());
        JobResponse response = JobResponse.from(result.job());
        if (result.created()) {
            return ResponseEntity.created(URI.create("/jobs/" + response.id())).body(response);
        }
        return ResponseEntity.ok(response);
    }

    @GetMapping("/{jobId}")
    public JobResponse get(@PathVariable UUID jobId) {
        return JobResponse.from(queryService.get(jobId));
    }

    @PostMapping("/{jobId}/cancel")
    public CancellationResponse cancel(
            @PathVariable UUID jobId, @Valid @RequestBody CancelJobRequest request) {
        CancellationResult result =
                cancellationService.cancel(jobId, request.requestedBy(), request.reason());
        return new CancellationResponse(result, JobResponse.from(queryService.get(jobId)));
    }

    @GetMapping
    public JobPageResponse list(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        return JobPageResponse.from(queryService.list(page, size));
    }
}
