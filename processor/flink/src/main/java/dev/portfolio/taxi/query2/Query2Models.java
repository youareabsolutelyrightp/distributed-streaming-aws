package dev.portfolio.taxi.query2;

import java.io.Serializable;
import java.util.List;

import dev.portfolio.taxi.boundary.BoundaryFields;

public final class Query2Models {
    private Query2Models() {}

    public record CellDelta(
            int cellId,
            int delta,
            long effectiveTimeMillis,
            int phase,
            long sourceLine,
            String runId,
            String datasetId,
            long producerSendEpochMicros)
            implements Serializable {}

    public record FareInput(
            int cellId,
            long moneyCents,
            long eventTimeMillis,
            long sourceLine,
            String runId,
            String datasetId,
            long producerSendEpochMicros)
            implements Serializable {}

    public sealed interface ProfitComponentUpdate extends Serializable
            permits EmptyCountUpdate, MedianUpdate {
        int cellId();
        long effectiveTimeMillis();
        int phase();
        long freshnessSourceLine();
        String runId();
        String datasetId();
        long producerSendEpochMicros();
    }

    public record EmptyCountUpdate(
            int cellId,
            long emptyTaxis,
            long effectiveTimeMillis,
            boolean input,
            boolean timer,
            int phase,
            long freshnessSourceLine,
            String runId,
            String datasetId,
            long producerSendEpochMicros)
            implements ProfitComponentUpdate {}

    public record MedianUpdate(
            int cellId,
            long medianTwiceCents,
            boolean active,
            long effectiveTimeMillis,
            boolean input,
            boolean timer,
            int phase,
            long freshnessSourceLine,
            String runId,
            String datasetId,
            long producerSendEpochMicros)
            implements ProfitComponentUpdate {}

    public record ProfitAggregate(
            int cellId,
            long medianTwiceCents,
            long emptyTaxis,
            long freshnessTimeMillis,
            long freshnessSourceLine,
            long effectiveTimeMillis,
            boolean input,
            boolean timer,
            String runId,
            String datasetId,
            long producerSendEpochMicros,
            boolean active)
            implements Serializable {}

    public record ProfitRankEntry(
            int cellId,
            long medianTwiceCents,
            long emptyTaxis,
            long freshnessTimeMillis,
            long freshnessSourceLine)
            implements Serializable {}

    public sealed interface GlobalMessage extends Serializable
            permits ShardTopMessage, BoundaryMessage, EosMessage {
        long effectiveTimeMillis();
    }

    public record ShardTopMessage(
            int shardId,
            List<ProfitRankEntry> top,
            long effectiveTimeMillis,
            String runId,
            String datasetId,
            long producerSendEpochMicros)
            implements GlobalMessage {}

    public record BoundaryMessage(
            long effectiveTimeMillis,
            boolean input,
            boolean timer,
            String runId,
            String datasetId,
            long producerSendEpochMicros,
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
                long producerSendEpochMicros,
                long inputKafkaAppendEpochMicros,
                long flinkSourceReadEpochMicros) {
            this(
                    effectiveTimeMillis,
                    input,
                    timer,
                    runId,
                    datasetId,
                    producerSendEpochMicros,
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
    }

    public record EosMessage(
            long effectiveTimeMillis,
            int partition,
            int expectedPartitionCount,
            String runId,
            String datasetId)
            implements GlobalMessage {}
}
