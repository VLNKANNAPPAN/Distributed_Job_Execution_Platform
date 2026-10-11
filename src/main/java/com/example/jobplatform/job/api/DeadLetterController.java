package com.example.jobplatform.job.api;

import com.example.jobplatform.job.DeadLetterService;
import jakarta.validation.Valid;
import java.util.List;
import java.util.UUID;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/operator/dead-letter-jobs")
public class DeadLetterController {

    private final DeadLetterService deadLetterService;

    public DeadLetterController(DeadLetterService deadLetterService) {
        this.deadLetterService = deadLetterService;
    }

    @GetMapping
    public List<DeadLetterSummary> list(@RequestParam(defaultValue = "50") int limit) {
        return deadLetterService.list(limit);
    }

    @GetMapping("/{jobId}")
    public DeadLetterJobResponse inspect(@PathVariable UUID jobId) {
        return deadLetterService.inspect(jobId);
    }

    @PostMapping("/{jobId}/replay")
    public ReplayResponse replay(
            @PathVariable UUID jobId, @Valid @RequestBody ReplayDeadLetterRequest request) {
        return deadLetterService.replay(jobId, request);
    }
}
