package dev.portfolio.taxi.boundary;

import java.io.Serializable;

public interface BoundaryFields extends Serializable {
    long effectiveTimeMillis();
    boolean input();
    boolean timer();
    String runId();
    String datasetId();
    long producerSendEpochMicros();
    long inputKafkaAppendEpochMicros();
    long flinkSourceReadEpochMicros();
    long boundaryFirstInputAppendEpochMicros();
    int triggerInputPartition();
    long triggerInputSourceLine();
    long triggerInputSourceReadEpochMicros();
    long partitionBoundaryEmittedEpochMicros();
    long boundarySignalArrivalEpochMicros();

    default long sharedBoundaryEmittedEpochMicros() {
        return 0L;
    }
}
