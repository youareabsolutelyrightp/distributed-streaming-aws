package dev.portfolio.taxi.boundary;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;

import dev.portfolio.taxi.query1.Query1Models.BoundaryMessage;
import dev.portfolio.taxi.input.InputModels.EosEvent;
import dev.portfolio.taxi.input.InputModels.TripEvent;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

final class PartitionBoundaryPreAggregatorTest {
    @Test
    void equalTimestampInputsBecomeOneExactPartitionSummary() {
        PartitionBoundaryPreAggregator.Accumulator accumulator =
                new PartitionBoundaryPreAggregator.Accumulator(1_000L, "run", "dataset");
        accumulator.accept(trip(10L, 100L, 120L));
        accumulator.accept(trip(11L, 110L, 140L));
        BoundaryMessage boundary = accumulator.toBoundary(2);
        assertEquals(1_000L, boundary.effectiveTimeMillis());
        assertEquals(100L, boundary.boundaryFirstInputAppendEpochMicros());
        assertEquals(110L, boundary.inputKafkaAppendEpochMicros());
        assertEquals(11L, boundary.triggerInputSourceLine());
        assertEquals(140L, boundary.flinkSourceReadEpochMicros());
        assertEquals(2, boundary.triggerInputPartition());
    }

    @Test
    void emissionTimeUsesTheCausalSourceReadFloorAcrossTaskManagerClocks() {
        PartitionBoundaryPreAggregator.Accumulator accumulator =
                new PartitionBoundaryPreAggregator.Accumulator(1_000L, "run", "dataset");
        accumulator.accept(trip(10L, 100L, 140L));

        BoundaryMessage boundary = accumulator.toBoundary(2, 136L);

        assertEquals(140L, boundary.partitionBoundaryEmittedEpochMicros());
    }

    @Test
    void partitionIdentitySupportsManyPartitionsPerReader() {
        assertEquals(3, PartitionBoundaryInput.fromEnvelope(
                new EosEvent("run", "dataset", 3, 4, 2, 1_000L), 4).kafkaPartition());
        assertThrows(IllegalArgumentException.class, () ->
                PartitionBoundaryInput.fromEnvelope(
                        new EosEvent("run", "dataset", 4, 4, 2, 1_000L), 4));
    }

    @Test
    void compactProjectionExcludesQuerySemanticData() {
        TripEvent source = new TripEvent(
                "run", "dataset", "event", "joined", 9L, 1, "medallion", 2_000L,
                "-73.9", "40.7", "-73.8", "40.8", "12.34", "5.67",
                10L, 20L, 30L);
        PartitionBoundaryInput compact = PartitionBoundaryInput.fromEnvelope(source, 4);
        assertEquals(9L, compact.sourceLine());
        assertEquals("partition-1", compact.stablePartitionKey());
        Set<String> components = new HashSet<>();
        for (var component : PartitionBoundaryInput.class.getRecordComponents()) {
            components.add(component.getName());
        }
        for (String excluded : Set.of(
                "medallion", "eventId", "joinedRecordId", "pickupLongitude",
                "pickupLatitude", "dropoffLongitude", "dropoffLatitude",
                "fareAmount", "tipAmount", "rawJson")) {
            assertFalse(components.contains(excluded));
        }
    }

    private static PartitionBoundaryInput trip(long line, long appendMicros, long readMicros) {
        return new PartitionBoundaryInput(
                PartitionBoundaryInput.TRIP, 1_000L, 2, line,
                "run", "dataset", 90L, appendMicros, readMicros, 4);
    }
}
