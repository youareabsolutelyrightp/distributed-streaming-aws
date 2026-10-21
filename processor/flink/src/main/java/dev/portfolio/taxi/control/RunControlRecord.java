package dev.portfolio.taxi.control;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

public final class RunControlRecord implements Serializable {
    private static final String GRADING_MODE = "event-time-state";
    private final String queryId;
    private final long maximumEventTimeMillis;
    private final String runId;
    private final String datasetId;
    private final String terminalOutputId;
    private final String controlId;

    private RunControlRecord(
            String queryId,
            long maximumEventTimeMillis,
            String runId,
            String datasetId,
            String terminalOutputId) {
        this.queryId = queryId;
        this.maximumEventTimeMillis = maximumEventTimeMillis;
        this.runId = runId;
        this.datasetId = datasetId;
        this.terminalOutputId = terminalOutputId;
        this.controlId = sha256(runId + "|query_complete|" + queryId + "|" + GRADING_MODE);
    }

    public static RunControlRecord queryComplete(
            String queryId,
            long maximumEventTimeMillis,
            String runId,
            String datasetId,
            String terminalOutputId) {
        if ((!queryId.equals("query1") && !queryId.equals("query2"))
                || runId == null
                || runId.isBlank()
                || datasetId == null
                || datasetId.isBlank()) {
            throw new IllegalArgumentException("invalid query completion identity");
        }
        return new RunControlRecord(
                queryId,
                maximumEventTimeMillis,
                runId,
                datasetId,
                terminalOutputId);
    }

    public String runId() { return runId; }
    public String queryId() { return queryId; }
    public String controlId() { return controlId; }
    public long maximumEventTimeMillis() { return maximumEventTimeMillis; }

    public String kafkaJson() {
        return "{\"completed_at_epoch_us\":" + (System.currentTimeMillis() * 1000L)
                + ",\"control_id\":\"" + controlId
                + "\",\"dataset_id\":\"" + escape(datasetId)
                + "\",\"grading_mode\":\"" + GRADING_MODE
                + "\",\"maximum_event_time_epoch_ms\":" + maximumEventTimeMillis
                + ",\"query_id\":\"" + queryId
                + "\",\"record_type\":\"query_complete\""
                + ",\"run_id\":\"" + escape(runId)
                + "\",\"schema_version\":1"
                + ",\"terminal_output_id\":" + nullable(terminalOutputId)
                + "}";
    }

    private static String nullable(String value) {
        return value == null ? "null" : "\"" + escape(value) + "\"";
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static String sha256(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
