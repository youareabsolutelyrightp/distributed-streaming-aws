package dev.portfolio.taxi.query1;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.portfolio.taxi.query1.Query1Models.RouteRankEntry;
import dev.portfolio.taxi.measurement.KafkaResultSchemas;
import dev.portfolio.taxi.measurement.ResultTransport;

import java.util.List;

import org.junit.jupiter.api.Test;

final class Query1ResultTest {
    @Test
    void outputIdIgnoresReplayAndTransportMetadata() {
        List<RouteRankEntry> top = List.of(new RouteRankEntry(1L, 1, 1, 3L, 100L, 7L));
        Query1Result first = new Query1Result(
                100L, "input", null, null, false,
                top, "run-a", "dataset", transport(1_000L));
        Query1Result replayed = new Query1Result(
                100L, "input", null, null, false,
                top, "run-b", "dataset", transport(2_000L));

        assertEquals(first.outputId(), replayed.outputId());
        assertEquals(first.canonicalFinalLine(), replayed.canonicalFinalLine());
        assertTrue(!first.kafkaJson().contains("input_kafka_append_epoch_us"));
        assertTrue(!first.kafkaJson().contains("flink_source_read_epoch_us"));
        assertTrue(first.kafkaJson().contains("\"transport_measurement_version\":3"));
    }

    @Test
    void kafkaResultUsesAppendTimeInsteadOfHistoricalEventTime() {
        long now = ResultTransport.nowEpochMicros();
        Query1Result result = new Query1Result(
                100L, "input", null, null, false,
                List.of(), "run-a", "dataset",
                new ResultTransport(
                        now - 100, now - 90, 0, 1L, now - 80, now - 70,
                        now - 60, now - 50, now - 45, now - 30,
                        now - 20, now - 10, now, 10L));

        var record = new KafkaResultSchemas.Query1("results")
                .serialize(result, null, 100L);

        assertNull(record.timestamp());
        assertEquals("run-a", new String(record.key(), java.nio.charset.StandardCharsets.UTF_8));
        byte[] trace = record.headers().lastHeader("transport_trace_v3").value();
        assertEquals(112, trace.length);
        assertEquals(3, trace[0]);
    }

    @Test
    void kafkaDlqUsesAppendTimeInsteadOfHistoricalEventTime() {
        var record = new KafkaResultSchemas.AppendTimeString("dlq")
                .serialize("rejected", null, 100L);

        assertNull(record.timestamp());
    }

    private static ResultTransport transport(long now) {
        return new ResultTransport(
                now - 100, now - 90, 0, 1L, now - 80, now - 70,
                now - 60, now - 50, now - 45, now - 30,
                now - 20, now - 10, now, 10L);
    }

}
