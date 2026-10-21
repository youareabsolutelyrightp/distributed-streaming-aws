package dev.portfolio.taxi.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

final class JobSettingsTest {
    @Test
    void readsSharedRuntimeAndRecoverySettings() {
        Map<String, String> values = required();
        values.put("TAXI_SOURCE_PARALLELISM", "4");
        values.put("TAXI_BOUNDARY_PARALLELISM", "8");
        values.put("TAXI_RANKING_MAX_PARALLELISM", "128");
        values.put("TAXI_WATERMARK_ALIGNMENT_MAX_DRIFT_MS", "3600000");
        values.put("TAXI_WATERMARK_ALIGNMENT_UPDATE_MS", "50");
        values.put("TAXI_CHECKPOINT_INTERVAL_MS", "15000");
        values.put("TAXI_CHECKPOINT_TIMEOUT_MS", "60000");

        JobSettings settings = JobSettings.fromEnvironment(values);

        assertEquals(4, settings.sourceParallelism());
        assertEquals(8, settings.boundaryParallelism());
        assertEquals(128, settings.rankingMaxParallelism());
        assertEquals(3_600_000L, settings.watermarkAlignmentMaxDriftMillis());
        assertEquals(50L, settings.watermarkAlignmentUpdateMillis());
        assertEquals(15_000L, settings.checkpointIntervalMillis());
        assertEquals(60_000L, settings.checkpointTimeoutMillis());
        assertEquals("taxi.run-control.v1", settings.controlTopic());
    }

    @Test
    void rankingMaximumMustCoverBothQueries() {
        Map<String, String> values = required();
        values.put("TAXI_RANKING_MAX_PARALLELISM", "5");
        JobSettings settings = JobSettings.fromEnvironment(values);
        assertThrows(IllegalArgumentException.class,
                () -> settings.validateRankingParallelism(4, 6));
    }

    @Test
    void invalidCheckpointValuesFailEarly() {
        Map<String, String> values = required();
        values.put("TAXI_CHECKPOINT_INTERVAL_MS", "0");
        assertThrows(IllegalArgumentException.class,
                () -> JobSettings.fromEnvironment(values));
    }

    @Test
    void boundaryParallelismDefaultsToRouteParallelism() {
        Map<String, String> values = required();
        values.put("TAXI_ROUTE_PARALLELISM", "7");

        JobSettings settings = JobSettings.fromEnvironment(values);

        assertEquals(7, settings.boundaryParallelism());
    }

    private static Map<String, String> required() {
        Map<String, String> values = new HashMap<>();
        values.put("TAXI_BOOTSTRAP_SERVERS", "kafka:9092");
        values.put("TAXI_INPUT_TOPIC", "taxi.trips.v1");
        values.put("TAXI_DLQ_TOPIC", "taxi.dlq.v1");
        values.put("TAXI_GROUP_ID", "benchmark-test");
        values.put("TAXI_CHECKPOINT_DIRECTORY", "file:///tmp/checkpoints");
        return values;
    }
}
