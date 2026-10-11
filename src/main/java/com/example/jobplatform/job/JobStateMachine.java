package com.example.jobplatform.job;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import org.springframework.stereotype.Component;

@Component
public class JobStateMachine {

    private static final Map<JobStatus, Set<JobStatus>> LEGAL_TRANSITIONS = legalTransitions();

    public void requireTransition(JobStatus from, JobStatus to) {
        if (!LEGAL_TRANSITIONS.getOrDefault(from, Set.of()).contains(to)) {
            throw new IllegalJobTransitionException(from, to);
        }
    }

    public boolean canTransition(JobStatus from, JobStatus to) {
        return LEGAL_TRANSITIONS.getOrDefault(from, Set.of()).contains(to);
    }

    private static Map<JobStatus, Set<JobStatus>> legalTransitions() {
        EnumMap<JobStatus, Set<JobStatus>> transitions = new EnumMap<>(JobStatus.class);
        transitions.put(
                JobStatus.SUBMITTED,
                EnumSet.of(JobStatus.QUEUED, JobStatus.SCHEDULED, JobStatus.CANCELLED));
        transitions.put(JobStatus.SCHEDULED, EnumSet.of(JobStatus.QUEUED, JobStatus.CANCELLED));
        transitions.put(JobStatus.QUEUED, EnumSet.of(JobStatus.RUNNING, JobStatus.CANCELLED));
        transitions.put(
                JobStatus.RUNNING,
                EnumSet.of(
                        JobStatus.SUCCEEDED,
                        JobStatus.RETRY_WAIT,
                        JobStatus.FAILED,
                        JobStatus.DEAD_LETTERED,
                        JobStatus.CANCELLED));
        transitions.put(
                JobStatus.RETRY_WAIT, EnumSet.of(JobStatus.QUEUED, JobStatus.CANCELLED, JobStatus.DEAD_LETTERED));
        transitions.put(JobStatus.FAILED, EnumSet.noneOf(JobStatus.class));
        transitions.put(JobStatus.DEAD_LETTERED, EnumSet.of(JobStatus.QUEUED));
        transitions.put(JobStatus.CANCELLED, EnumSet.noneOf(JobStatus.class));
        transitions.put(JobStatus.SUCCEEDED, EnumSet.noneOf(JobStatus.class));
        return Map.copyOf(transitions);
    }
}
