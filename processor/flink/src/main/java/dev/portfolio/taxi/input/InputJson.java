package dev.portfolio.taxi.input;

import static dev.portfolio.taxi.input.InputModels.EosEvent;
import static dev.portfolio.taxi.input.InputModels.InputEnvelope;
import static dev.portfolio.taxi.input.InputModels.TripEvent;

import java.io.IOException;
import java.time.Instant;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public final class InputJson {
    private static final ObjectMapper JSON = new ObjectMapper();

    private InputJson() {}

    public static InputEnvelope parse(String value) throws IOException {
        return parse(value, -1, 0L, 0L);
    }

    public static InputEnvelope parse(
            String value, long inputKafkaAppendEpochMicros, long flinkSourceReadEpochMicros)
            throws IOException {
        return parse(value, -1, inputKafkaAppendEpochMicros, flinkSourceReadEpochMicros);
    }

    public static InputEnvelope parse(
            String value,
            int kafkaPartition,
            long inputKafkaAppendEpochMicros,
            long flinkSourceReadEpochMicros)
            throws IOException {
        JsonNode root = JSON.readTree(value);
        if (root.path("schema_version").asInt(-1) != 1) {
            throw new IOException("unsupported schema_version");
        }
        String recordType = required(root, "record_type");
        return switch (recordType) {
            case "trip" -> new TripEvent(
                    required(root, "run_id"),
                    required(root, "dataset_id"),
                    required(root, "event_id"),
                    required(root, "joined_record_id"),
                    requiredLong(root, "source_line_number"),
                    kafkaPartition,
                    required(root, "medallion"),
                    parseTime(required(root, "dropoff_datetime")),
                    decimalText(root, "pickup_longitude"),
                    decimalText(root, "pickup_latitude"),
                    decimalText(root, "dropoff_longitude"),
                    decimalText(root, "dropoff_latitude"),
                    required(root, "fare_amount"),
                    required(root, "tip_amount"),
                    requiredLong(root, "producer_send_epoch_us"),
                    inputKafkaAppendEpochMicros,
                    flinkSourceReadEpochMicros);
            case "eos" -> new EosEvent(
                    required(root, "run_id"),
                    required(root, "dataset_id"),
                    Math.toIntExact(requiredLong(root, "partition")),
                    Math.toIntExact(requiredLong(root, "expected_partition_count")),
                    requiredLong(root, "expected_source_count"),
                    parseTime(required(root, "maximum_event_time")));
            default -> throw new IOException("unsupported record_type: " + recordType);
        };
    }

    static long parseTime(String value) throws IOException {
        try {
            return Instant.parse(value).toEpochMilli();
        } catch (RuntimeException error) {
            throw new IOException("invalid canonical UTC event time: " + value, error);
        }
    }

    private static String decimalText(JsonNode root, String field) throws IOException {
        JsonNode value = root.get(field);
        if (value == null || value.isNull()) {
            return "";
        }
        if (!value.isNumber() && !value.isTextual()) {
            throw new IOException(field + " is not numeric");
        }
        return value.asText();
    }

    private static String required(JsonNode root, String field) throws IOException {
        JsonNode value = root.get(field);
        if (value == null || value.isNull() || value.asText().isBlank()) {
            throw new IOException("missing required field: " + field);
        }
        return value.asText();
    }

    private static long requiredLong(JsonNode root, String field) throws IOException {
        JsonNode value = root.get(field);
        if (value == null || !value.canConvertToLong()) {
            throw new IOException("missing or invalid integer field: " + field);
        }
        return value.longValue();
    }
}
