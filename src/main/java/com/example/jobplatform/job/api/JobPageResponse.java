package com.example.jobplatform.job.api;

import java.util.List;
import org.springframework.data.domain.Page;

public record JobPageResponse(
        List<JobResponse> items,
        int page,
        int size,
        long totalElements,
        int totalPages) {

    public static JobPageResponse from(Page<com.example.jobplatform.job.JobEntity> jobs) {
        return new JobPageResponse(
                jobs.stream().map(JobResponse::from).toList(),
                jobs.getNumber(),
                jobs.getSize(),
                jobs.getTotalElements(),
                jobs.getTotalPages());
    }
}
