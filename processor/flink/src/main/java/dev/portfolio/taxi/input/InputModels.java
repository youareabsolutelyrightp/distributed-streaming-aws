package dev.portfolio.taxi.input;

import java.io.Serializable;

public final class InputModels {
    private InputModels() {}

    public sealed interface InputEnvelope extends Serializable permits TripEvent, EosEvent {
        long eventTimeMillis();
        String runId();
        String datasetId();
    }

    public record TripEvent(
            String runId,
            String datasetId,
            String eventId,
            String joinedRecordId,
            long sourceLine,
            int kafkaPartition,
            String medallion,
            long eventTimeMillis,
            String pickupLongitude,
            String pickupLatitude,
            String dropoffLongitude,
            String dropoffLatitude,
            String fareAmount,
            String tipAmount,
            long producerSendEpochMicros,
            long inputKafkaAppendEpochMicros,
            long flinkSourceReadEpochMicros)
            implements InputEnvelope {
        public TripEvent(
                String runId,
                String datasetId,
                String eventId,
                String joinedRecordId,
                long sourceLine,
                String medallion,
                long eventTimeMillis,
                String pickupLongitude,
                String pickupLatitude,
                String dropoffLongitude,
                String dropoffLatitude,
                String fareAmount,
                String tipAmount,
                long producerSendEpochMicros,
                long inputKafkaAppendEpochMicros,
                long flinkSourceReadEpochMicros) {
            this(
                    runId,
                    datasetId,
                    eventId,
                    joinedRecordId,
                    sourceLine,
                    -1,
                    medallion,
                    eventTimeMillis,
                    pickupLongitude,
                    pickupLatitude,
                    dropoffLongitude,
                    dropoffLatitude,
                    fareAmount,
                    tipAmount,
                    producerSendEpochMicros,
                    inputKafkaAppendEpochMicros,
                    flinkSourceReadEpochMicros);
        }

        public SemanticTrip semantic() {
            return new SemanticTrip(
                    runId, datasetId, eventId, sourceLine, medallion, eventTimeMillis,
                    pickupLongitude, pickupLatitude, dropoffLongitude, dropoffLatitude,
                    fareAmount, tipAmount);
        }
    }

    public record SemanticTrip(
            String runId,
            String datasetId,
            String eventId,
            long sourceLine,
            String medallion,
            long eventTimeMillis,
            String pickupLongitude,
            String pickupLatitude,
            String dropoffLongitude,
            String dropoffLatitude,
            String fareAmount,
            String tipAmount)
            implements Serializable {}

    public record EosEvent(
            String runId,
            String datasetId,
            int partition,
            int expectedPartitionCount,
            long expectedSourceCount,
            long eventTimeMillis)
            implements InputEnvelope {}
}
