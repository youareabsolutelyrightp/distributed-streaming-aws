package dev.portfolio.taxi.measurement;

import dev.portfolio.taxi.query1.Query1Result;
import dev.portfolio.taxi.query2.Query2Result;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

public final class KafkaResultSchemas {
    private KafkaResultSchemas() {}

    public static final class Query1
            implements KafkaRecordSerializationSchema<Query1Result>, Serializable {
        private final String topic;

        public Query1(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                Query1Result result, KafkaSinkContext context, Long ignoredEventTimestamp) {
            return result(
                    topic, result.runId(), result.kafkaJson(),
                    result.triggerType(), result.terminal(), result.transport());
        }
    }

    public static final class Query2
            implements KafkaRecordSerializationSchema<Query2Result>, Serializable {
        private final String topic;

        public Query2(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                Query2Result result, KafkaSinkContext context, Long ignoredEventTimestamp) {
            return result(
                    topic, result.runId(), result.kafkaJson(),
                    result.triggerType(), result.terminal(), result.transport());
        }
    }

    public static final class AppendTimeString
            implements KafkaRecordSerializationSchema<String>, Serializable {
        private final String topic;

        public AppendTimeString(String topic) {
            this.topic = topic;
        }

        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                String value, KafkaSinkContext context, Long ignoredEventTimestamp) {
            return new ProducerRecord<>(topic, value.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static ProducerRecord<byte[], byte[]> result(
            String topic,
            String runId,
            String json,
            String triggerType,
            boolean terminal,
            ResultTransport transport) {
        long serializationStartedEpochMicros = ResultTransport.nowEpochMicros();
        byte[] key = runId.getBytes(StandardCharsets.UTF_8);
        byte[] value = json.getBytes(StandardCharsets.UTF_8);
        long serializationCompletedEpochMicros = ResultTransport.nowEpochMicros();
        var headers = TransportHeaders.full(
                transport,
                triggerType,
                terminal,
                serializationStartedEpochMicros,
                serializationCompletedEpochMicros);
        return new ProducerRecord<>(topic, null, null, key, value, headers);
    }
}
