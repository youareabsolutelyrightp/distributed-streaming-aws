package dev.portfolio.taxi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.portfolio.taxi.query2.Query2Result;
import dev.portfolio.taxi.measurement.KafkaResultSchemas;
import dev.portfolio.taxi.measurement.ResultTransport;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.Test;

final class Query2KafkaRecordSchemaTest {
    @Test
    void resultUsesStableRunKeyAndBrokerAppendTime() {
        long now = ResultTransport.nowEpochMicros();
        Query2Result result = new Query2Result(
                100L,
                "input",
                1L,
                "event",
                false,
                List.of(),
                "run-a",
                "dataset",
                new ResultTransport(
                        now - 100, now - 90, 0, 1L, now - 80, now - 70,
                        now - 60, now - 50, now - 45, now - 30,
                        now - 20, now - 10, now, 10L));

        var record = new KafkaResultSchemas.Query2("results")
                .serialize(result, null, 100L);

        assertNull(record.timestamp());
        assertEquals("run-a", new String(record.key(), StandardCharsets.UTF_8));
        String value = new String(record.value(), StandardCharsets.UTF_8);
        assertTrue(value.contains("\"transport_measurement_version\":3"));
        assertTrue(!value.contains("result_emitted_epoch_us"));
        byte[] trace = record.headers().lastHeader("transport_trace_v3").value();
        assertEquals(112, trace.length);
        assertEquals(3, trace[0]);
    }
}
