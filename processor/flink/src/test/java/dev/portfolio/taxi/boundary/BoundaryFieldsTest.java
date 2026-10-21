package dev.portfolio.taxi.boundary;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.portfolio.taxi.boundary.SharedBoundaryModels.BoundarySignal;
import dev.portfolio.taxi.boundary.SharedBoundaryModels.Origin;
import dev.portfolio.taxi.boundary.SharedBoundaryModels.SharedBoundary;

import org.junit.jupiter.api.Test;

final class BoundaryFieldsTest {
    @Test
    void queryBoundaryConversionsPreserveEveryField() {
        SharedBoundary shared = new SharedBoundary(
                1L, true, true, "run", "dataset", 2L, 3L, 4L, 5L,
                2, 6L, 7L, 8L, 9L, 10L);

        assertFields(shared, dev.portfolio.taxi.query1.Query1Models.BoundaryMessage.from(shared));
        assertFields(shared, dev.portfolio.taxi.query2.Query2Models.BoundaryMessage.from(shared));
    }

    @Test
    void signalConversionPreservesTraceFieldsAndOrigin() {
        var query1 = new dev.portfolio.taxi.query1.Query1Models.BoundaryMessage(
                1L, false, true, "run", "dataset", 2L, 3L, 4L, 5L,
                2, 6L, 7L, 8L, 0L, 10L);

        BoundarySignal signal = BoundarySignal.from(query1, Origin.QUERY1_TIMER);

        assertFields(query1, signal);
        assertEquals(Origin.QUERY1_TIMER, signal.origin());
    }

    private static void assertFields(BoundaryFields expected, BoundaryFields actual) {
        assertEquals(expected.effectiveTimeMillis(), actual.effectiveTimeMillis());
        assertEquals(expected.input(), actual.input());
        assertEquals(expected.timer(), actual.timer());
        assertEquals(expected.runId(), actual.runId());
        assertEquals(expected.datasetId(), actual.datasetId());
        assertEquals(expected.producerSendEpochMicros(), actual.producerSendEpochMicros());
        assertEquals(expected.inputKafkaAppendEpochMicros(), actual.inputKafkaAppendEpochMicros());
        assertEquals(expected.flinkSourceReadEpochMicros(), actual.flinkSourceReadEpochMicros());
        assertEquals(expected.boundaryFirstInputAppendEpochMicros(), actual.boundaryFirstInputAppendEpochMicros());
        assertEquals(expected.triggerInputPartition(), actual.triggerInputPartition());
        assertEquals(expected.triggerInputSourceLine(), actual.triggerInputSourceLine());
        assertEquals(expected.triggerInputSourceReadEpochMicros(), actual.triggerInputSourceReadEpochMicros());
        assertEquals(expected.partitionBoundaryEmittedEpochMicros(), actual.partitionBoundaryEmittedEpochMicros());
        assertEquals(expected.sharedBoundaryEmittedEpochMicros(), actual.sharedBoundaryEmittedEpochMicros());
        assertEquals(expected.boundarySignalArrivalEpochMicros(), actual.boundarySignalArrivalEpochMicros());
    }
}
