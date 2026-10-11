package com.example.jobplatform.job;

import java.util.Optional;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JobLookupTransaction {

    private final JobRepository jobRepository;

    public JobLookupTransaction(JobRepository jobRepository) {
        this.jobRepository = jobRepository;
    }

    @Transactional(readOnly = true, propagation = Propagation.REQUIRES_NEW)
    public Optional<JobEntity> findByIdempotencyKey(String idempotencyKey) {
        return jobRepository.findByIdempotencyKey(idempotencyKey);
    }
}
