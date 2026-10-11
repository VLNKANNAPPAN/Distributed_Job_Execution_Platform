package com.example.jobplatform.retry;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.retry")
public class RetryProperties {

    private long initialDelayMs = 1000;
    private long maxDelayMs = 60000;
    private double jitterFactor = 0.2;
    private int maintenanceBatchSize = 25;

    public long getInitialDelayMs() {
        return initialDelayMs;
    }

    public void setInitialDelayMs(long initialDelayMs) {
        this.initialDelayMs = initialDelayMs;
    }

    public long getMaxDelayMs() {
        return maxDelayMs;
    }

    public void setMaxDelayMs(long maxDelayMs) {
        this.maxDelayMs = maxDelayMs;
    }

    public double getJitterFactor() {
        return jitterFactor;
    }

    public void setJitterFactor(double jitterFactor) {
        this.jitterFactor = jitterFactor;
    }

    public int getMaintenanceBatchSize() {
        return maintenanceBatchSize;
    }

    public void setMaintenanceBatchSize(int maintenanceBatchSize) {
        this.maintenanceBatchSize = maintenanceBatchSize;
    }
}
