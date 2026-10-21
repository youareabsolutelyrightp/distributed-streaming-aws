package dev.portfolio.taxi.query2;

import static dev.portfolio.taxi.query2.Query2Models.ProfitRankEntry;
import static dev.portfolio.taxi.canonical.CanonicalJson.quote;

import dev.portfolio.taxi.model.Grid;
import dev.portfolio.taxi.query1.Query1Result;
import dev.portfolio.taxi.measurement.ResultTransport;

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

public final class Query2Result implements Serializable {
    private static final String GRADING_MODE = "event-time-state";
    private final long effectiveTimeMillis;
    private final String triggerType;
    private final Long triggerSourceLine;
    private final String triggerEventId;
    private final boolean terminal;
    private final List<ProfitRankEntry> top;
    private final String runId;
    private final String datasetId;
    private final ResultTransport transport;
    private final String outputId;

    public Query2Result(
            long effectiveTimeMillis,
            String triggerType,
            Long triggerSourceLine,
            String triggerEventId,
            boolean terminal,
            List<ProfitRankEntry> top,
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

    public String runId() { return runId; }
    public String outputId() { return outputId; }
    public boolean terminal() { return terminal; }
    public long effectiveTimeMillis() { return effectiveTimeMillis; }
    public String triggerType() { return triggerType; }
    public List<ProfitRankEntry> top() { return top; }
    public ResultTransport transport() { return transport; }

    public String canonicalBaseLine() {
        StringBuilder result = new StringBuilder(1024);
        result.append("{\"canonical_format_version\":1");
        result.append(",\"effective_time\":").append(quote(Query1Result.formatTime(effectiveTimeMillis)));
        result.append(",\"grading_mode\":").append(quote(GRADING_MODE));
        result.append(",\"query_id\":\"query2\"");
        result.append(",\"terminal\":").append(terminal);
        result.append(",\"top\":").append(topJson());
        result.append(",\"trigger_event_id\":").append(nullableString(triggerEventId));
        result.append(",\"trigger_source_line\":").append(nullableLong(triggerSourceLine));
        result.append(",\"trigger_type\":").append(quote(triggerType));
        return result.append("}\n").toString();
    }

    public String canonicalFinalLine() {
        String base = canonicalBaseLine();
        int insert = base.indexOf(",\"query_id\"");
        return base.substring(0, insert)
                + ",\"output_id\":" + quote(outputId)
                + base.substring(insert);
    }

    public String kafkaJson() {
        StringBuilder result = new StringBuilder(1120);
        result.append("{\"canonical_format_version\":1");
        result.append(",\"dataset_id\":").append(quote(datasetId));
        result.append(",\"effective_time\":").append(quote(Query1Result.formatTime(effectiveTimeMillis)));
        result.append(",\"grading_mode\":").append(quote(GRADING_MODE));
        result.append(",\"output_id\":").append(quote(outputId));
        result.append(",\"query_id\":\"query2\"");
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
            if (index > 0) result.append(',');
            ProfitRankEntry entry = top.get(index);
            result.append("{\"cell_id\":").append(quote(Grid.cellText(entry.cellId())));
            result.append(",\"empty_taxis\":").append(entry.emptyTaxis());
            result.append(",\"freshness_source_line\":").append(entry.freshnessSourceLine());
            result.append(",\"freshness_time\":").append(quote(Query1Result.formatTime(entry.freshnessTimeMillis())));
            result.append(",\"median_fare_plus_tip_twice_cents\":").append(entry.medianTwiceCents());
            result.append(",\"profitability\":").append(quote(roundHalfEven(entry.medianTwiceCents(), entry.emptyTaxis() * 200L)));
            result.append('}');
        }
        return result.append(']').toString();
    }

    static String roundHalfEven(long numerator, long denominator) {
        if (numerator < 0L || denominator <= 0L) throw new IllegalArgumentException("invalid ratio");
        BigInteger scaled = BigInteger.valueOf(numerator).multiply(BigInteger.valueOf(1_000_000L));
        BigInteger[] parts = scaled.divideAndRemainder(BigInteger.valueOf(denominator));
        BigInteger quotient = parts[0];
        int half = parts[1].shiftLeft(1).compareTo(BigInteger.valueOf(denominator));
        if (half > 0 || (half == 0 && quotient.testBit(0))) quotient = quotient.add(BigInteger.ONE);
        BigInteger[] decimal = quotient.divideAndRemainder(BigInteger.valueOf(1_000_000L));
        return decimal[0] + "." + String.format("%06d", decimal[1].longValueExact());
    }

    private static String nullableString(String value) { return value == null ? "null" : quote(value); }
    private static String nullableLong(Long value) { return value == null ? "null" : value.toString(); }
    private static String nullableInteger(Integer value) { return value == null ? "null" : value.toString(); }
    private static String sha256(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
