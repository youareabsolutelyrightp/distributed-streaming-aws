package dev.portfolio.taxi.config;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

public record JobSettings(
        String bootstrapServers,
        String inputTopic,
        String dlqTopic,
        String controlTopic,
        String groupId,
        String checkpointDirectory,
        int inputPartitions,
        int sourceParallelism,
        int boundaryParallelism,
        int rankingMaxParallelism,
        long watermarkAlignmentMaxDriftMillis,
        long watermarkAlignmentUpdateMillis,
        long checkpointIntervalMillis,
        long checkpointTimeoutMillis,
        int tolerableCheckpointFailures)
        implements Serializable {

    public static JobSettings fromEnvironment(Map<String, String> environment) {
        Objects.requireNonNull(environment, "environment");
        int source = positive(environment, "TAXI_SOURCE_PARALLELISM", 4);
        int route = positive(environment, "TAXI_ROUTE_PARALLELISM", 4);
        int boundary = positive(environment, "TAXI_BOUNDARY_PARALLELISM", route);
        int rankingMax = positive(environment, "TAXI_RANKING_MAX_PARALLELISM", 128);
        if (rankingMax > 32_768) {
            throw new IllegalArgumentException(
                    "TAXI_RANKING_MAX_PARALLELISM must not exceed 32768");
        }
        return new JobSettings(
                required(environment, "TAXI_BOOTSTRAP_SERVERS"),
                required(environment, "TAXI_INPUT_TOPIC"),
                required(environment, "TAXI_DLQ_TOPIC"),
                environment.getOrDefault("TAXI_CONTROL_TOPIC", "taxi.run-control.v1"),
                required(environment, "TAXI_GROUP_ID"),
                required(environment, "TAXI_CHECKPOINT_DIRECTORY"),
                positive(environment, "TAXI_INPUT_PARTITIONS", 4),
                source,
                boundary,
                rankingMax,
                nonNegative(environment, "TAXI_WATERMARK_ALIGNMENT_MAX_DRIFT_MS", 0L),
                positiveLong(environment, "TAXI_WATERMARK_ALIGNMENT_UPDATE_MS", 200L),
                positiveLong(environment, "TAXI_CHECKPOINT_INTERVAL_MS", 5_000L),
                positiveLong(environment, "TAXI_CHECKPOINT_TIMEOUT_MS", 60_000L),
                nonNegativeInt(environment, "TAXI_TOLERABLE_CHECKPOINT_FAILURES", 0));
    }

    public void validateRankingParallelism(int query1Parallelism, int query2Parallelism) {
        int largest = Math.max(query1Parallelism, query2Parallelism);
        if (rankingMaxParallelism < largest) {
            throw new IllegalArgumentException(
                    "TAXI_RANKING_MAX_PARALLELISM must be at least each ranking parallelism");
        }
    }

    private static int positive(Map<String, String> environment, String key, int fallback) {
        String value = environment.get(key);
        int parsed = value == null || value.isBlank() ? fallback : Integer.parseInt(value);
        if (parsed <= 0) {
            throw new IllegalArgumentException(key + " must be positive");
        }
        return parsed;
    }

    private static long nonNegative(Map<String, String> environment, String key, long fallback) {
        String value = environment.get(key);
        long parsed = value == null || value.isBlank() ? fallback : Long.parseLong(value);
        if (parsed < 0L) {
            throw new IllegalArgumentException(key + " must be non-negative");
        }
        return parsed;
    }

    private static int nonNegativeInt(
            Map<String, String> environment, String key, int fallback) {
        String value = environment.get(key);
        int parsed = value == null || value.isBlank() ? fallback : Integer.parseInt(value);
        if (parsed < 0) {
            throw new IllegalArgumentException(key + " must be non-negative");
        }
        return parsed;
    }

    private static long positiveLong(Map<String, String> environment, String key, long fallback) {
        String value = environment.get(key);
        long parsed = value == null || value.isBlank() ? fallback : Long.parseLong(value);
        if (parsed <= 0L) {
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
