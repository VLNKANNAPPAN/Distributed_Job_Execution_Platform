package com.example.jobplatform.outbox;

import java.util.concurrent.TimeUnit;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(prefix = "app.outbox", name = "dispatcher-enabled", havingValue = "true")
public class KafkaOutboxEventPublisher implements OutboxEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OutboxProperties properties;

    public KafkaOutboxEventPublisher(
            KafkaTemplate<String, String> kafkaTemplate, OutboxProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
    }

    @Override
    public void publish(OutboxClaim claim) throws Exception {
        kafkaTemplate
                .send(properties.getTopic(), claim.jobId().toString(), claim.payload())
                .get(properties.sendTimeout().toMillis(), TimeUnit.MILLISECONDS);
    }
}
