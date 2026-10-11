package com.example.jobplatform.retry;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "app.retry", name = "maintenance-enabled", havingValue = "true")
public class RetryMaintenanceLoop {

    private final ExpiredLeaseReaper leaseReaper;
    private final RetryScheduler retryScheduler;
    private final RetryProperties properties;

    public RetryMaintenanceLoop(
            ExpiredLeaseReaper leaseReaper,
            RetryScheduler retryScheduler,
            RetryProperties properties) {
        this.leaseReaper = leaseReaper;
        this.retryScheduler = retryScheduler;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${app.retry.maintenance-delay-ms:1000}")
    public void runOnce() {
        leaseReaper.reapExpired(properties.getMaintenanceBatchSize());
        retryScheduler.promoteDue(properties.getMaintenanceBatchSize());
    }
}
