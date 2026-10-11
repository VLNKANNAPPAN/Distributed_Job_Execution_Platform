package com.example.jobplatform.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.jobplatform.job.JobSubmissionService;
import com.example.jobplatform.support.PostgresIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

@EmbeddedKafka(
        kraft = true,
        partitions = 1,
        topics = "job.work-available.v1",
        brokerProperties = "auto.create.topics.enable=false")
@SpringBootTest(
        properties = {
            "app.outbox.dispatcher-enabled=true",
            "app.outbox.initial-delay-ms=3600000",
            "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}"
        })
class OutboxKafkaIntegrationTest extends PostgresIntegrationTest {

    @Autowired
    private OutboxDispatcher dispatcher;

    @Autowired
    private JobSubmissionService submissionService;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Test
    void eventuallyPublishesCommittedOutboxEventToKafka() {
        var submitted = submissionService.submit(
                "kafka-publication",
                "GENERATE_REPORT",
                5,
                objectMapper.createObjectNode().put("report", "daily"));
        Map<String, Object> consumerProperties =
                KafkaTestUtils.consumerProps("outbox-integration", "false", broker);
        try (Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(
                        consumerProperties, new StringDeserializer(), new StringDeserializer())
                .createConsumer()) {
            TopicPartition partition = new TopicPartition("job.work-available.v1", 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));

            assertThat(dispatcher.dispatchOnce()).isEqualTo(1);
            ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(
                    consumer, "job.work-available.v1", Duration.ofSeconds(10));

            assertThat(record.key()).isEqualTo(submitted.job().getId().toString());
            assertThat(record.value()).contains(submitted.job().getId().toString());
            assertThat(jdbcTemplate.queryForObject(
                            "SELECT count(*) FROM outbox_events WHERE published_at IS NOT NULL",
                            Long.class))
                    .isEqualTo(1);
        }
    }
}
