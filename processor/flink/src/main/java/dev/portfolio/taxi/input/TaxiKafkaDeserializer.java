package dev.portfolio.taxi.input;

import java.nio.charset.StandardCharsets;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public final class TaxiKafkaDeserializer
        implements KafkaRecordDeserializationSchema<KafkaInputRecord> {
    @Override
    public void deserialize(
            ConsumerRecord<byte[], byte[]> record, Collector<KafkaInputRecord> output) {
        String raw = new String(record.value(), StandardCharsets.UTF_8);
        long sourceReadEpochMicros = System.currentTimeMillis() * 1000L;
        long kafkaAppendEpochMicros = Math.max(0L, record.timestamp()) * 1000L;
        try {
            output.collect(new KafkaInputRecord(
                    record.partition(), record.offset(), raw,
                    InputJson.parse(
                            raw,
                            record.partition(),
                            kafkaAppendEpochMicros,
                            sourceReadEpochMicros),
                    null));
        } catch (Exception error) {
            String reason = error.getMessage() == null
                    ? error.getClass().getSimpleName()
                    : error.getMessage();
            output.collect(new KafkaInputRecord(
                    record.partition(), record.offset(), raw, null, reason));
        }
    }

    @Override
    public TypeInformation<KafkaInputRecord> getProducedType() {
        return TypeInformation.of(KafkaInputRecord.class);
    }
}
