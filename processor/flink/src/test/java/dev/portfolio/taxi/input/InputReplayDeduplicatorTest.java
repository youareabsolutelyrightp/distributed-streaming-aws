package dev.portfolio.taxi.input;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.portfolio.taxi.input.InputModels.TripEvent;

import java.nio.file.Path;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

final class InputReplayDeduplicatorTest {
    @Test
    void stablePartitionVectorsMatchTheProducerContract() throws Exception {
        JsonNode document = new ObjectMapper().readTree(
                Path.of("..", "..", "schemas", "partitioning-test-vectors.json").toFile());
        assertEquals(InputPartitioning.VERSION,
                document.path("partitioning_version").asText());
        assertEquals(InputPartitioning.HASH_VECTOR_VERSION,
                document.path("hash_test_vector_version").asText());
        for (JsonNode vector : document.path("vectors")) {
            assertEquals(
                    vector.path("expected_partition").asInt(),
                    InputPartitioning.partition(
                            vector.path("medallion").asText(),
                            vector.path("partition_count").asInt()));
        }
        assertThrows(IllegalArgumentException.class,
                () -> InputPartitioning.partition("taxi-a", 0));
    }

    @Test
    void highWaterStateDropsEqualAndOlderSourceLines() {
        InputReplayDeduplicator.PartitionState state =
                new InputReplayDeduplicator.PartitionState();
        assertEquals(InputReplayDeduplicator.Decision.ACCEPT, state.apply(trip(1L, "one")));
        assertEquals(InputReplayDeduplicator.Decision.DUPLICATE, state.apply(trip(1L, "one")));
        assertEquals(InputReplayDeduplicator.Decision.ACCEPT, state.apply(trip(8L, "eight")));
        assertEquals(InputReplayDeduplicator.Decision.DUPLICATE, state.apply(trip(3L, "three")));
        assertEquals(8L, state.highestAcceptedSourceLine);
    }

    @Test
    void equalSourceLineWithDifferentEventIdFails() {
        InputReplayDeduplicator.PartitionState state =
                new InputReplayDeduplicator.PartitionState();
        state.apply(trip(4L, "first"));
        TripEvent conflict = trip(4L, "second");
        assertThrows(IllegalArgumentException.class, () -> state.apply(conflict));
    }

    @Test
    void eventIdentityUsesDatasetAndJoinedRecord() {
        String expected = InputPartitioning.expectedEventId("dataset", "joined");
        assertEquals(64, expected.length());
        assertEquals(expected, InputPartitioning.expectedEventId("dataset", "joined"));
        assertEquals(true, InputPartitioning.eventIdMatches("dataset", "joined", expected));
        assertEquals(false, InputPartitioning.eventIdMatches("dataset", "other", expected));
        TripEvent valid = trip(1L, "joined");
        int partition = InputPartitioning.partition(valid.medallion(), 4);
        InputReplayDeduplicator.validateTrip(partition, 4, valid);
        TripEvent invalid = new TripEvent(
                valid.runId(), valid.datasetId(), "invalid", valid.joinedRecordId(),
                valid.sourceLine(), valid.medallion(), valid.eventTimeMillis(),
                valid.pickupLongitude(), valid.pickupLatitude(),
                valid.dropoffLongitude(), valid.dropoffLatitude(),
                valid.fareAmount(), valid.tipAmount(), valid.producerSendEpochMicros(),
                valid.inputKafkaAppendEpochMicros(), valid.flinkSourceReadEpochMicros());
        assertThrows(IllegalArgumentException.class,
                () -> InputReplayDeduplicator.validateTrip(partition, 4, invalid));
        assertThrows(IllegalArgumentException.class,
                () -> InputReplayDeduplicator.validateTrip((partition + 1) % 4, 4, valid));
    }

    private static TripEvent trip(long line, String joined) {
        String dataset = "dataset";
        return new TripEvent(
                "run", dataset, InputPartitioning.expectedEventId(dataset, joined), joined,
                line, "taxi-a", 1_000L,
                "-73.9", "40.7", "-73.8", "40.8",
                "1.00", "0.00", 1L, 2L, 3L);
    }
}
