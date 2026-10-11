package com.example.jobplatform.worker;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.jobplatform.lease.JobLease;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.springframework.kafka.support.Acknowledgment;

class KafkaWorkerConsumerTest {

    @Test
    void duplicateDeliveriesEachBecomeHarmlessDatabaseWakeups() {
        WorkerProcessor processor = mock(WorkerProcessor.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        when(processor.claimOne()).thenReturn(Optional.empty());
        KafkaWorkerConsumer consumer = new KafkaWorkerConsumer(processor);

        consumer.onWorkAvailable("same-event", acknowledgment);
        consumer.onWorkAvailable("same-event", acknowledgment);

        verify(processor, times(2)).claimOne();
        verify(acknowledgment, times(2)).acknowledge();
    }

    @Test
    void databaseFailureEscapesListenerSoKafkaDoesNotTreatTheRecordAsSuccessful() {
        WorkerProcessor processor = mock(WorkerProcessor.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        when(processor.claimOne()).thenThrow(new IllegalStateException("database unavailable"));
        KafkaWorkerConsumer consumer = new KafkaWorkerConsumer(processor);

        assertThatThrownBy(() -> consumer.onWorkAvailable("event", acknowledgment))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("database unavailable");
        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void acknowledgesAfterClaimCommitAndBeforeHandlerExecution() {
        WorkerProcessor processor = mock(WorkerProcessor.class);
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        JobLease lease = mock(JobLease.class);
        when(processor.claimOne()).thenReturn(Optional.of(lease));
        KafkaWorkerConsumer consumer = new KafkaWorkerConsumer(processor);

        consumer.onWorkAvailable("event", acknowledgment);

        InOrder order = inOrder(processor, acknowledgment);
        order.verify(processor).claimOne();
        order.verify(acknowledgment).acknowledge();
        order.verify(processor).executeClaimed(lease);
    }
}
