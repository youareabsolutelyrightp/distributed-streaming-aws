package dev.portfolio.taxi.query1;

import java.io.Serializable;
import java.util.List;

import dev.portfolio.taxi.boundary.BoundaryFields;

public final class Query1Models {
    private Query1Models() {}

    public record RouteUpdate(
            long routeId,
            int pickupCell,
            int dropoffCell,
            long eventTimeMillis,
            long sourceLine,
            String eventId,
            String runId,
            String datasetId,
            long producerSendEpochMicros)
            implements Serializable {}

    public record RouteAggregate(
            long routeId,
            int pickupCell,
            int dropoffCell,
            long count,
            long freshnessTimeMillis,
            long freshnessSourceLine,
            long effectiveTimeMillis,
            String triggerType,
            long triggerSourceLine,
            String triggerEventId,
            String runId,
            String datasetId,
            long inputProducerSendEpochMicros,
            boolean active)
            implements Serializable {}

    public record RouteRankEntry(
            long routeId,
            int pickupCell,
            int dropoffCell,
            long count,
            long freshnessTimeMillis,
            long freshnessSourceLine)
            implements Serializable {}

    public sealed interface GlobalMessage extends Serializable
            permits ShardTopMessage, BoundaryMessage, EosMessage {
        long effectiveTimeMillis();
    }

    public record ShardTopMessage(
            int shardId,
            List<RouteRankEntry> top,
            long effectiveTimeMillis,
            String runId,
            String datasetId,
            long inputProducerSendEpochMicros)
            implements GlobalMessage {}

    public record BoundaryMessage(
            long effectiveTimeMillis,
            boolean input,
            boolean timer,
            String runId,
            String datasetId,
            long inputProducerSendEpochMicros,
            long inputKafkaAppendEpochMicros,
            long flinkSourceReadEpochMicros,
            long boundaryFirstInputAppendEpochMicros,
            int triggerInputPartition,
            long triggerInputSourceLine,
            long triggerInputSourceReadEpochMicros,
            long partitionBoundaryEmittedEpochMicros,
            long sharedBoundaryEmittedEpochMicros,
            long boundarySignalArrivalEpochMicros)
            implements GlobalMessage, BoundaryFields {
        public BoundaryMessage(
                long effectiveTimeMillis,
                boolean input,
                boolean timer,
                String runId,
                String datasetId,
                long inputProducerSendEpochMicros,
                long inputKafkaAppendEpochMicros,
                long flinkSourceReadEpochMicros) {
            this(
                    effectiveTimeMillis,
                    input,
                    timer,
                    runId,
                    datasetId,
                    inputProducerSendEpochMicros,
                    inputKafkaAppendEpochMicros,
                    flinkSourceReadEpochMicros,
                    inputKafkaAppendEpochMicros,
                    -1,
                    0L,
                    flinkSourceReadEpochMicros,
                    0L,
                    0L,
                    0L);
        }

        public static BoundaryMessage from(BoundaryFields value) {
            return new BoundaryMessage(
                    value.effectiveTimeMillis(),
                    value.input(),
                    value.timer(),
                    value.runId(),
                    value.datasetId(),
                    value.producerSendEpochMicros(),
                    value.inputKafkaAppendEpochMicros(),
                    value.flinkSourceReadEpochMicros(),
                    value.boundaryFirstInputAppendEpochMicros(),
                    value.triggerInputPartition(),
                    value.triggerInputSourceLine(),
                    value.triggerInputSourceReadEpochMicros(),
                    value.partitionBoundaryEmittedEpochMicros(),
                    value.sharedBoundaryEmittedEpochMicros(),
                    value.boundarySignalArrivalEpochMicros());
        }

        @Override
        public long producerSendEpochMicros() {
            return inputProducerSendEpochMicros;
        }
    }

    public record EosMessage(
            long effectiveTimeMillis,
            int partition,
            int expectedPartitionCount,
            String runId,
            String datasetId)
            implements GlobalMessage {}
}
