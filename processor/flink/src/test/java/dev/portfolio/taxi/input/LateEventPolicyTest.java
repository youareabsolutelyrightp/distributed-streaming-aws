package dev.portfolio.taxi.input;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.portfolio.taxi.input.InputModels.TripEvent;

import org.junit.jupiter.api.Test;

final class LateEventPolicyTest {
    @Test
    void boundaryOrOlderIsLate() {
        assertTrue(InputValidator.isLate(100L, 100L));
        assertTrue(InputValidator.isLate(99L, 100L));
        assertFalse(InputValidator.isLate(101L, 100L));
    }

    @Test
    void benchmarkLateRecordFailsCorrectness() {
        TripEvent trip = new TripEvent(
                "run-1", "dataset-1", "event-1", "joined-1", 7L, "taxi-1", 100L,
                "-74.913585", "41.474937", "-74.913585", "41.474937",
                "1.00", "0.00", 1L, 0L, 0L);
        KafkaInputRecord record = new KafkaInputRecord(2, 9L, "{}", trip, null);

        String benchmark = InputValidator.lateEventEnvelope(record, trip, 100L);

        assertTrue(benchmark.contains("\"reason\":\"late_event\""));
        assertTrue(benchmark.contains("\"correctness_failure\":true"));
        assertTrue(benchmark.contains("\"max_out_of_orderness_ms\":0"));
    }
}
