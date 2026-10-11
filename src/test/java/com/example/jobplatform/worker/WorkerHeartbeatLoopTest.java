package com.example.jobplatform.worker;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

class WorkerHeartbeatLoopTest {

    @Test
    void heartbeatStoreFailureDoesNotEscapeIntoWorkerCorrectnessPath() {
        WorkerHeartbeatService heartbeatService = mock(WorkerHeartbeatService.class);
        WorkerIdentity identity = mock(WorkerIdentity.class);
        when(identity.value()).thenReturn("worker-isolated");
        doThrow(new IllegalStateException("database unavailable"))
                .when(heartbeatService)
                .register(org.mockito.ArgumentMatchers.eq("worker-isolated"),
                        org.mockito.ArgumentMatchers.any());
        WorkerHeartbeatLoop loop = new WorkerHeartbeatLoop(heartbeatService, identity);

        assertThatCode(loop::recordHeartbeat).doesNotThrowAnyException();
    }
}
