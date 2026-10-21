package dev.portfolio.taxi.boundary;

import java.io.Serializable;

public final class SharedBoundaryModels {
    private SharedBoundaryModels() {}

    public enum Origin {
        INPUT,
        QUERY1_TIMER,
        QUERY2_TIMER
    }

    public record BoundarySignal(
            long effectiveTimeMillis,
            Origin origin,
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
            long boundarySignalArrivalEpochMicros)
            implements BoundaryFields {
        public BoundarySignal(
                long effectiveTimeMillis,
                Origin origin,
                String runId,
                String datasetId,
                long producerSendEpochMicros,
                long inputKafkaAppendEpochMicros,
                long flinkSourceReadEpochMicros) {
            this(
                    effectiveTimeMillis,
                    origin,
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
                    0L);
        }

        public static BoundarySignal from(BoundaryFields value, Origin origin) {
            return new BoundarySignal(
                    value.effectiveTimeMillis(),
                    origin,
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
                    value.boundarySignalArrivalEpochMicros());
        }

        @Override
        public boolean input() {
            return origin == Origin.INPUT;
        }

        @Override
        public boolean timer() {
            return origin != Origin.INPUT;
        }
    }

    public record SharedBoundary(
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
            implements BoundaryFields {}

}
