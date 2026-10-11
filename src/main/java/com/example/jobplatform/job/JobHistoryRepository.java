package com.example.jobplatform.job;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface JobHistoryRepository extends JpaRepository<JobHistoryEntity, UUID> {

    long countByJobId(UUID jobId);
}
