package com.example.jobplatform.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;
import org.junit.jupiter.api.Test;

class JobStateMachineTest {

    private final JobStateMachine stateMachine = new JobStateMachine();

    @Test
    void acceptsRepresentativeLegalTransitions() {
        assertThat(stateMachine.canTransition(JobStatus.SUBMITTED, JobStatus.QUEUED)).isTrue();
        assertThat(stateMachine.canTransition(JobStatus.QUEUED, JobStatus.RUNNING)).isTrue();
        assertThat(stateMachine.canTransition(JobStatus.RUNNING, JobStatus.SUCCEEDED)).isTrue();
        assertThat(stateMachine.canTransition(JobStatus.RUNNING, JobStatus.RETRY_WAIT)).isTrue();
        assertThat(stateMachine.canTransition(JobStatus.RETRY_WAIT, JobStatus.QUEUED)).isTrue();
        assertThat(stateMachine.canTransition(JobStatus.DEAD_LETTERED, JobStatus.QUEUED)).isTrue();
    }

    @Test
    void rejectsEveryUndocumentedTransition() {
        Set<String> documented = Set.of(
                edge(JobStatus.SUBMITTED, JobStatus.QUEUED),
                edge(JobStatus.SUBMITTED, JobStatus.SCHEDULED),
                edge(JobStatus.SUBMITTED, JobStatus.CANCELLED),
                edge(JobStatus.SCHEDULED, JobStatus.QUEUED),
                edge(JobStatus.SCHEDULED, JobStatus.CANCELLED),
                edge(JobStatus.QUEUED, JobStatus.RUNNING),
                edge(JobStatus.QUEUED, JobStatus.CANCELLED),
                edge(JobStatus.RUNNING, JobStatus.SUCCEEDED),
                edge(JobStatus.RUNNING, JobStatus.RETRY_WAIT),
                edge(JobStatus.RUNNING, JobStatus.FAILED),
                edge(JobStatus.RUNNING, JobStatus.DEAD_LETTERED),
                edge(JobStatus.RUNNING, JobStatus.CANCELLED),
                edge(JobStatus.RETRY_WAIT, JobStatus.QUEUED),
                edge(JobStatus.RETRY_WAIT, JobStatus.CANCELLED),
                edge(JobStatus.RETRY_WAIT, JobStatus.DEAD_LETTERED),
                edge(JobStatus.DEAD_LETTERED, JobStatus.QUEUED));

        for (JobStatus from : JobStatus.values()) {
            for (JobStatus to : JobStatus.values()) {
                if (!documented.contains(edge(from, to))) {
                    assertThatThrownBy(() -> stateMachine.requireTransition(from, to))
                            .isInstanceOf(IllegalJobTransitionException.class);
                }
            }
        }
    }

    private static String edge(JobStatus from, JobStatus to) {
        return from + "->" + to;
    }
}
