package dev.portfolio.taxi.query1;

import java.io.Serializable;
import java.util.Map;

public record Query1Settings(
        String outputTopic,
        int routeParallelism,
        int rankingParallelism,
        int rankingShards)
        implements Serializable {

    public static Query1Settings fromEnvironment(Map<String, String> environment) {
        int route = positive(environment, "TAXI_ROUTE_PARALLELISM", 4);
        int ranking = positive(environment, "TAXI_RANKING_PARALLELISM", 4);
        int shards = positive(environment, "TAXI_RANKING_SHARDS", 64);
        return new Query1Settings(
                required(environment, "TAXI_OUTPUT_TOPIC"),
                route,
                ranking,
                shards);
    }

    private static int positive(Map<String, String> environment, String key, int fallback) {
        String value = environment.get(key);
        int parsed = value == null || value.isBlank() ? fallback : Integer.parseInt(value);
        if (parsed <= 0) {
            throw new IllegalArgumentException(key + " must be positive");
        }
        return parsed;
    }

    private static String required(Map<String, String> environment, String key) {
        String value = environment.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required environment variable: " + key);
        }
        return value;
    }
}
