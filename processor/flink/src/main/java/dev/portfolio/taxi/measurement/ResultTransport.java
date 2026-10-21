package dev.portfolio.taxi.measurement;

import java.io.Serializable;

public record ResultTransport(
        Long boundaryFirstInputAppendEpochMicros,
        Long triggerInputAppendEpochMicros,
        Integer triggerInputPartition,
        Long triggerInputSourceLine,
        Long triggerInputSourceReadEpochMicros,
        Long maximumRequiredSourceReadEpochMicros,
        Long partitionBoundaryEmittedEpochMicros,
        Long firstInputBoundaryArrivalEpochMicros,
        Long lastInputBoundaryArrivalEpochMicros,
        Long lastRequiredGlobalMessageArrivalEpochMicros,
        Long watermarkClosedEpochMicros,
        Long globalBoundaryStartedEpochMicros,
        Long resultEmittedEpochMicros,
        Long maximumPartitionBoundaryDeliveryDurationMicros)
        implements Serializable {

    public static final int VERSION = 3;

    public int measurementVersion() {
        return VERSION;
    }

    public static long nowEpochMicros() {
        return System.currentTimeMillis() * 1000L;
    }
}
