package com.example.jobplatform.worker;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.worker")
public class WorkerProperties {

    private boolean enabled;
    private String id;
    private String groupId = "job-workers-v1";
    private long leaseSeconds = 30;
    private long renewalIntervalMs = 10000;
    private long heartbeatIntervalMs = 5000;
    private long livenessTimeoutSeconds = 20;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public long getLeaseSeconds() {
        return leaseSeconds;
    }

    public void setLeaseSeconds(long leaseSeconds) {
        this.leaseSeconds = leaseSeconds;
    }

    public long getRenewalIntervalMs() {
        return renewalIntervalMs;
    }

    public void setRenewalIntervalMs(long renewalIntervalMs) {
        this.renewalIntervalMs = renewalIntervalMs;
    }

    public long getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    public void setHeartbeatIntervalMs(long heartbeatIntervalMs) {
        this.heartbeatIntervalMs = heartbeatIntervalMs;
    }

    public long getLivenessTimeoutSeconds() {
        return livenessTimeoutSeconds;
    }

    public void setLivenessTimeoutSeconds(long livenessTimeoutSeconds) {
        this.livenessTimeoutSeconds = livenessTimeoutSeconds;
    }

    public Duration leaseDuration() {
        return Duration.ofSeconds(Math.max(1, leaseSeconds));
    }

    public Duration livenessTimeout() {
        return Duration.ofSeconds(Math.max(1, livenessTimeoutSeconds));
    }
}
