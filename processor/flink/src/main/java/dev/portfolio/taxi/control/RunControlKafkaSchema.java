package dev.portfolio.taxi.control;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

public final class RunControlKafkaSchema
        implements KafkaRecordSerializationSchema<RunControlRecord>, Serializable {
    private final String topic;

    public RunControlKafkaSchema(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            RunControlRecord record, KafkaSinkContext context, Long ignoredEventTimestamp) {
        return new ProducerRecord<>(
                topic,
                record.runId().getBytes(StandardCharsets.UTF_8),
                record.kafkaJson().getBytes(StandardCharsets.UTF_8));
    }
}
