package com.example.jobplatform.outbox;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.outbox")
public class OutboxProperties {

    private boolean dispatcherEnabled;
    private String topic = "job.work-available.v1";
    private int batchSize = 25;
    private long claimSeconds = 30;
    private long sendTimeoutSeconds = 10;

    public boolean isDispatcherEnabled() {
        return dispatcherEnabled;
    }

    public void setDispatcherEnabled(boolean dispatcherEnabled) {
        this.dispatcherEnabled = dispatcherEnabled;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public long getClaimSeconds() {
        return claimSeconds;
    }

    public void setClaimSeconds(long claimSeconds) {
        this.claimSeconds = claimSeconds;
    }

    public long getSendTimeoutSeconds() {
        return sendTimeoutSeconds;
    }

    public void setSendTimeoutSeconds(long sendTimeoutSeconds) {
        this.sendTimeoutSeconds = sendTimeoutSeconds;
    }

    public Duration claimDuration() {
        return Duration.ofSeconds(claimSeconds);
    }

    public Duration sendTimeout() {
        return Duration.ofSeconds(sendTimeoutSeconds);
    }
}
