package dev.portfolio.taxi.boundary;

import static dev.portfolio.taxi.input.InputModels.EosEvent;
import static dev.portfolio.taxi.input.InputModels.InputEnvelope;
import static dev.portfolio.taxi.input.InputModels.TripEvent;

import java.io.Serializable;

public record PartitionBoundaryInput(
        byte recordKind,
        long eventTimeMillis,
        int kafkaPartition,
        long sourceLine,
        String runId,
        String datasetId,
        long producerSendEpochMicros,
        long inputKafkaAppendEpochMicros,
        long flinkSourceReadEpochMicros,
        int expectedPartitionCount)
        implements Serializable {
    public static final byte TRIP = 1;
    public static final byte EOS = 2;
    private static final String PARTITION_KEY_PREFIX = "partition-";

    public static PartitionBoundaryInput fromEnvelope(
            InputEnvelope value, int inputPartitions) {
        if (value instanceof TripEvent trip) {
            return new PartitionBoundaryInput(
                    TRIP,
                    trip.eventTimeMillis(),
                    normalizePartition(trip.kafkaPartition(), inputPartitions),
                    trip.sourceLine(),
                    trip.runId(),
                    trip.datasetId(),
                    trip.producerSendEpochMicros(),
                    trip.inputKafkaAppendEpochMicros(),
                    trip.flinkSourceReadEpochMicros(),
                    inputPartitions);
        }
        if (value instanceof EosEvent eos) {
            return new PartitionBoundaryInput(
                    EOS,
                    eos.eventTimeMillis(),
                    normalizePartition(eos.partition(), inputPartitions),
                    0L,
                    eos.runId(),
                    eos.datasetId(),
                    0L,
                    0L,
                    0L,
                    eos.expectedPartitionCount());
        }
        throw new IllegalArgumentException("unsupported input boundary envelope");
    }

    public boolean trip() {
        return recordKind == TRIP;
    }

    public boolean eos() {
        return recordKind == EOS;
    }

    public String stablePartitionKey() {
        return PARTITION_KEY_PREFIX + kafkaPartition;
    }

    static int partitionFromKey(String key) {
        if (key == null || !key.startsWith(PARTITION_KEY_PREFIX)) {
            throw new IllegalArgumentException("invalid input boundary partition key");
        }
        return Integer.parseInt(key.substring(PARTITION_KEY_PREFIX.length()));
    }

    private static int normalizePartition(int partition, int inputPartitions) {
        if (partition < 0 && inputPartitions == 1) partition = 0;
        if (partition < 0 || partition >= inputPartitions) {
            throw new IllegalArgumentException("input boundary has invalid Kafka partition");
        }
        return partition;
    }
}
