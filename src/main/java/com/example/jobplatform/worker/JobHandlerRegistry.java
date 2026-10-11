package com.example.jobplatform.worker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.stereotype.Component;

@Component
public class JobHandlerRegistry {

    private final Map<String, JobHandler> handlers;

    public JobHandlerRegistry(List<JobHandler> handlers) {
        Map<String, JobHandler> indexed = new HashMap<>();
        for (JobHandler handler : handlers) {
            JobHandler existing = indexed.putIfAbsent(handler.jobType(), handler);
            if (existing != null) {
                throw new IllegalStateException("Duplicate handler for job type " + handler.jobType());
            }
        }
        this.handlers = Map.copyOf(indexed);
    }

    public Optional<JobHandler> find(String jobType) {
        return Optional.ofNullable(handlers.get(jobType));
    }
}
