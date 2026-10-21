package dev.portfolio.taxi.query1;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

final class Query1SettingsTest {
    @Test
    void containsOnlyQuerySpecificProcessingSettings() {
        Map<String, String> values = new HashMap<>();
        values.put("TAXI_OUTPUT_TOPIC", "taxi.top-routes.v1");
        values.put("TAXI_ROUTE_PARALLELISM", "7");
        values.put("TAXI_RANKING_PARALLELISM", "3");
        values.put("TAXI_RANKING_SHARDS", "32");

        Query1Settings settings = Query1Settings.fromEnvironment(values);

        assertEquals("taxi.top-routes.v1", settings.outputTopic());
        assertEquals(7, settings.routeParallelism());
        assertEquals(3, settings.rankingParallelism());
        assertEquals(32, settings.rankingShards());
    }
}
