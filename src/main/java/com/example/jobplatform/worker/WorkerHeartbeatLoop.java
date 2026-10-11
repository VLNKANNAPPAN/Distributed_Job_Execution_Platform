package com.example.jobplatform.worker;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "app.worker", name = "enabled", havingValue = "true")
public class WorkerHeartbeatLoop {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerHeartbeatLoop.class);

    private final WorkerHeartbeatService heartbeatService;
    private final WorkerIdentity identity;
    private final UUID sessionId = UUID.randomUUID();
    private boolean registered;
    private boolean superseded;

    public WorkerHeartbeatLoop(
            WorkerHeartbeatService heartbeatService, WorkerIdentity identity) {
        this.heartbeatService = heartbeatService;
        this.identity = identity;
    }

    @PostConstruct
    public void start() {
        recordHeartbeat();
    }

    @Scheduled(fixedDelayString = "${app.worker.heartbeat-interval-ms:5000}")
    public void recordHeartbeat() {
        if (superseded) {
            return;
        }
        try {
            if (!registered) {
                heartbeatService.register(identity.value(), sessionId);
                registered = true;
                return;
            }
            if (!heartbeatService.heartbeat(identity.value(), sessionId)) {
                superseded = true;
                LOGGER.warn("Worker heartbeat session was superseded for {}", identity.value());
            }
        } catch (RuntimeException exception) {
            LOGGER.warn("Worker heartbeat store is unavailable for {}", identity.value());
        }
    }

    @PreDestroy
    public void stop() {
        if (!registered || superseded) {
            return;
        }
        try {
            heartbeatService.stop(identity.value(), sessionId);
        } catch (RuntimeException exception) {
            LOGGER.warn("Could not record worker shutdown for {}", identity.value());
        }
    }
}
