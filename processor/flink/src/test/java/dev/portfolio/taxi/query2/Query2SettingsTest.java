package dev.portfolio.taxi.query2;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

final class Query2SettingsTest {
    @Test
    void containsOnlyQuerySpecificProcessingSettings() {
        Map<String, String> values = required();
        values.put("TAXI_QUERY2_RANKING_PARALLELISM", "6");
        values.put("TAXI_QUERY2_RANKING_SHARDS", "24");
        Query2Settings settings = Query2Settings.fromEnvironment(values);
        assertEquals(6, settings.rankingParallelism());
        assertEquals(24, settings.rankingShards());
    }

    private static Map<String, String> required() {
        Map<String, String> values = new HashMap<>();
        values.put("TAXI_QUERY2_OUTPUT_TOPIC", "taxi.profitable-areas.v1");
        return values;
    }
}
