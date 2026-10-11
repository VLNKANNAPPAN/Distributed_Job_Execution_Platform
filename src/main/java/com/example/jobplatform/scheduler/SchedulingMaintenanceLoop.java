package com.example.jobplatform.scheduler;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "app.scheduling", name = "maintenance-enabled", havingValue = "true")
public class SchedulingMaintenanceLoop {

    private final ScheduledJobScheduler scheduler;
    private final SchedulingProperties properties;

    public SchedulingMaintenanceLoop(
            ScheduledJobScheduler scheduler, SchedulingProperties properties) {
        this.scheduler = scheduler;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${app.scheduling.maintenance-delay-ms:1000}")
    public void runOnce() {
        scheduler.promoteDue(properties.getBatchSize());
    }
}
