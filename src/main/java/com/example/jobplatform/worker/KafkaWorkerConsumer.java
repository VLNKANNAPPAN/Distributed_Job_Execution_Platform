package com.example.jobplatform.worker;

import com.example.jobplatform.lease.JobLease;
import java.util.Optional;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "app.worker", name = "enabled", havingValue = "true")
public class KafkaWorkerConsumer {

    private final WorkerProcessor processor;

    public KafkaWorkerConsumer(WorkerProcessor processor) {
        this.processor = processor;
    }

    @KafkaListener(
            topics = "${app.outbox.topic:job.work-available.v1}",
            groupId = "${app.worker.group-id:job-workers-v1}")
    public void onWorkAvailable(String ignoredEvent, Acknowledgment acknowledgment) {
        Optional<JobLease> claimed = processor.claimOne();
        acknowledgment.acknowledge();
        claimed.ifPresent(processor::executeClaimed);
    }
}
