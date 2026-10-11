package com.example.jobplatform.scheduler;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.scheduling")
public class SchedulingProperties {

    private int batchSize = 25;
    private long priorityAgingSeconds = 60;

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public long getPriorityAgingSeconds() {
        return priorityAgingSeconds;
    }

    public void setPriorityAgingSeconds(long priorityAgingSeconds) {
        this.priorityAgingSeconds = priorityAgingSeconds;
    }
}
