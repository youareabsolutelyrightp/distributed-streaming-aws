package dev.portfolio.taxi.query2;

import java.io.Serializable;
import java.util.Map;

public record Query2Settings(
        String outputTopic,
        int taxiParallelism,
        int cellParallelism,
        int rankingParallelism,
        int rankingShards)
        implements Serializable {

    public static Query2Settings fromEnvironment(Map<String, String> environment) {
        int taxi = positive(environment, "TAXI_QUERY2_TAXI_PARALLELISM", 4);
        int cell = positive(environment, "TAXI_QUERY2_CELL_PARALLELISM", 4);
        int ranking = positive(environment, "TAXI_QUERY2_RANKING_PARALLELISM", 4);
        int shards = positive(environment, "TAXI_QUERY2_RANKING_SHARDS", 64);
        return new Query2Settings(
                required(environment, "TAXI_QUERY2_OUTPUT_TOPIC"),
                taxi,
                cell,
                ranking,
                shards);
    }

    private static int positive(Map<String, String> environment, String key, int fallback) {
        String value = environment.get(key);
        int parsed = value == null || value.isBlank() ? fallback : Integer.parseInt(value);
        if (parsed <= 0) throw new IllegalArgumentException(key + " must be positive");
        return parsed;
    }
    private static String required(Map<String, String> environment, String key) {
        String value = environment.get(key);
        if (value == null || value.isBlank()) throw new IllegalArgumentException("Missing required environment variable: " + key);
        return value;
    }
}
