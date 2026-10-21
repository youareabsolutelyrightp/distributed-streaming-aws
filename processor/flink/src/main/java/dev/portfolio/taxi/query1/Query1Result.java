package dev.portfolio.taxi.query1;

import static dev.portfolio.taxi.query1.Query1Models.RouteRankEntry;
import static dev.portfolio.taxi.canonical.CanonicalJson.quote;

import dev.portfolio.taxi.model.Grid;

import dev.portfolio.taxi.measurement.ResultTransport;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HexFormat;
import java.util.List;

public final class Query1Result implements Serializable {
    private static final DateTimeFormatter EVENT_TIME =
            DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC);
    private static final String GRADING_MODE = "event-time-state";

    private final long effectiveTimeMillis;
    private final String triggerType;
    private final Long triggerSourceLine;
    private final String triggerEventId;
    private final boolean terminal;
    private final List<RouteRankEntry> top;
    private final String runId;
    private final String datasetId;
    private final ResultTransport transport;
    private final String outputId;

    public Query1Result(
            long effectiveTimeMillis,
            String triggerType,
            Long triggerSourceLine,
            String triggerEventId,
            boolean terminal,
            List<RouteRankEntry> top,
            String runId,
            String datasetId,
            ResultTransport transport) {
        this.effectiveTimeMillis = effectiveTimeMillis;
        this.triggerType = triggerType;
        this.triggerSourceLine = triggerSourceLine;
        this.triggerEventId = triggerEventId;
        this.terminal = terminal;
        this.top = List.copyOf(top);
        this.runId = runId;
        this.datasetId = datasetId;
        this.transport = transport;
        this.outputId = sha256(canonicalBaseLine());
    }

    public String runId() {
        return runId;
    }

    public String outputId() {
        return outputId;
    }

    public boolean terminal() {
        return terminal;
    }

    public long effectiveTimeMillis() {
        return effectiveTimeMillis;
    }

    public String triggerType() {
        return triggerType;
    }

    public List<RouteRankEntry> top() {
        return top;
    }

    public ResultTransport transport() {
        return transport;
    }

    public String canonicalBaseLine() {
        StringBuilder result = new StringBuilder(1024);
        result.append("{\"canonical_format_version\":1");
        result.append(",\"effective_time\":").append(quote(formatTime(effectiveTimeMillis)));
        result.append(",\"grading_mode\":").append(quote(GRADING_MODE));
        result.append(",\"query_id\":\"query1\"");
        result.append(",\"terminal\":").append(terminal);
        result.append(",\"top\":").append(topJson());
        result.append(",\"trigger_event_id\":").append(nullableString(triggerEventId));
        result.append(",\"trigger_source_line\":").append(nullableLong(triggerSourceLine));
        result.append(",\"trigger_type\":").append(quote(triggerType));
        return result.append("}\n").toString();
    }

    public String canonicalFinalLine() {
        StringBuilder result = new StringBuilder(1088);
        result.append("{\"canonical_format_version\":1");
        result.append(",\"effective_time\":").append(quote(formatTime(effectiveTimeMillis)));
        result.append(",\"grading_mode\":").append(quote(GRADING_MODE));
        result.append(",\"output_id\":").append(quote(outputId));
        result.append(",\"query_id\":\"query1\"");
        result.append(",\"terminal\":").append(terminal);
        result.append(",\"top\":").append(topJson());
        result.append(",\"trigger_event_id\":").append(nullableString(triggerEventId));
        result.append(",\"trigger_source_line\":").append(nullableLong(triggerSourceLine));
        result.append(",\"trigger_type\":").append(quote(triggerType));
        return result.append("}\n").toString();
    }

    public String kafkaJson() {
        StringBuilder result = new StringBuilder(1120);
        result.append("{\"canonical_format_version\":1");
        result.append(",\"dataset_id\":").append(quote(datasetId));
        result.append(",\"effective_time\":").append(quote(formatTime(effectiveTimeMillis)));
        result.append(",\"grading_mode\":").append(quote(GRADING_MODE));
        result.append(",\"output_id\":").append(quote(outputId));
        result.append(",\"query_id\":\"query1\"");
        result.append(",\"run_id\":").append(quote(runId));
        result.append(",\"terminal\":").append(terminal);
        result.append(",\"top\":").append(topJson());
        result.append(",\"trigger_event_id\":").append(nullableString(triggerEventId));
        result.append(",\"trigger_source_line\":").append(nullableLong(triggerSourceLine));
        result.append(",\"trigger_type\":").append(quote(triggerType));
        result.append(",\"transport_measurement_version\":")
                .append(transport.measurementVersion());
        return result.append('}').toString();
    }

    private String topJson() {
        StringBuilder result = new StringBuilder(768).append('[');
        for (int index = 0; index < top.size(); index++) {
            if (index > 0) {
                result.append(',');
            }
            RouteRankEntry entry = top.get(index);
            String pickup = Grid.cellText(entry.pickupCell());
            String dropoff = Grid.cellText(entry.dropoffCell());
            result.append("{\"count\":").append(entry.count());
            result.append(",\"dropoff_cell\":").append(quote(dropoff));
            result.append(",\"freshness_source_line\":").append(entry.freshnessSourceLine());
            result.append(",\"freshness_time\":").append(quote(formatTime(entry.freshnessTimeMillis())));
            result.append(",\"pickup_cell\":").append(quote(pickup));
            result.append(",\"route_id\":").append(quote(pickup + ">" + dropoff));
            result.append('}');
        }
        return result.append(']').toString();
    }

    public static String formatTime(long epochMillis) {
        return EVENT_TIME.format(Instant.ofEpochMilli(epochMillis));
    }

    private static String nullableString(String value) {
        return value == null ? "null" : quote(value);
    }

    private static String nullableLong(Long value) {
        return value == null ? "null" : Long.toString(value);
    }

    private static String nullableInteger(Integer value) {
        return value == null ? "null" : Integer.toString(value);
    }

    private static String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }
}
