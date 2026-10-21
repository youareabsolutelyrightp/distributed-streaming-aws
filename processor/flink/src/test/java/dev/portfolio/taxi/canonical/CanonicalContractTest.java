package dev.portfolio.taxi.canonical;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.portfolio.taxi.measurement.ResultTransport;
import dev.portfolio.taxi.model.Grid;
import dev.portfolio.taxi.query1.Query1Models.RouteRankEntry;
import dev.portfolio.taxi.query1.Query1Result;
import dev.portfolio.taxi.query2.Query2Models.ProfitRankEntry;
import dev.portfolio.taxi.query2.Query2Result;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

final class CanonicalContractTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void everyJavaResultMatchesTheSharedGoldenVectors() throws Exception {
        Path vectors = Path.of("..", "..", "schemas", "canonical-test-vectors.jsonl");
        for (String line : Files.readAllLines(vectors)) {
            if (line.isBlank()) continue;
            JsonNode vector = JSON.readTree(line);
            JsonNode record = vector.path("record");
            ResultValue result = result(record);
            assertEquals(
                    vector.path("expected_canonical_line").asText(),
                    result.canonicalLine(),
                    vector.path("name").asText());
            assertEquals(
                    vector.path("expected_output_id").asText(),
                    result.outputId(),
                    vector.path("name").asText());
        }
    }

    private static ResultValue result(JsonNode record) {
        long effectiveTime = Instant.parse(record.path("effective_time").asText()).toEpochMilli();
        String triggerType = record.path("trigger_type").asText();
        Long triggerLine = record.path("trigger_source_line").isNull()
                ? null : record.path("trigger_source_line").asLong();
        String triggerEvent = record.path("trigger_event_id").isNull()
                ? null : record.path("trigger_event_id").asText();
        boolean terminal = record.path("terminal").asBoolean();
        if (record.path("query_id").asText().equals("query1")) {
            List<RouteRankEntry> top = new ArrayList<>();
            for (JsonNode entry : record.path("top")) {
                int pickup = cell(entry.path("pickup_cell").asText());
                int dropoff = cell(entry.path("dropoff_cell").asText());
                top.add(new RouteRankEntry(
                        Grid.routeId(pickup, dropoff), pickup, dropoff,
                        entry.path("count").asLong(),
                        Instant.parse(entry.path("freshness_time").asText()).toEpochMilli(),
                        entry.path("freshness_source_line").asLong()));
            }
            Query1Result value = new Query1Result(
                    effectiveTime, triggerType, triggerLine, triggerEvent, terminal, top,
                    "contract-run", "contract-dataset", emptyTransport());
            return new ResultValue(value.canonicalBaseLine(), value.outputId());
        }
        List<ProfitRankEntry> top = new ArrayList<>();
        for (JsonNode entry : record.path("top")) {
            top.add(new ProfitRankEntry(
                    cell(entry.path("cell_id").asText()),
                    entry.path("median_fare_plus_tip_twice_cents").asLong(),
                    entry.path("empty_taxis").asLong(),
                    Instant.parse(entry.path("freshness_time").asText()).toEpochMilli(),
                    entry.path("freshness_source_line").asLong()));
        }
        Query2Result value = new Query2Result(
                effectiveTime, triggerType, triggerLine, triggerEvent, terminal, top,
                "contract-run", "contract-dataset", emptyTransport());
        return new ResultValue(value.canonicalBaseLine(), value.outputId());
    }

    private static int cell(String value) {
        String[] parts = value.split("\\.", -1);
        return (Integer.parseInt(parts[0]) << 10) | Integer.parseInt(parts[1]);
    }

    private static ResultTransport emptyTransport() {
        return new ResultTransport(
                null, null, null, null, null, null, null,
                null, null, null, null, null, null, null);
    }

    private record ResultValue(String canonicalLine, String outputId) {}
}
