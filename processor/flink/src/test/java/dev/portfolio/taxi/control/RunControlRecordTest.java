package dev.portfolio.taxi.control;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

final class RunControlRecordTest {
    @Test
    void identityIsStableButCompletionTimeIsTransportMetadata() {
        RunControlRecord first = RunControlRecord.queryComplete(
                "query1", 100L, "run", "dataset", "output");
        RunControlRecord second = RunControlRecord.queryComplete(
                "query1", 100L, "run", "dataset", "output");

        assertEquals(first.controlId(), second.controlId());
        assertEquals(64, first.controlId().length());
        assertTrue(first.kafkaJson().contains("\"record_type\":\"query_complete\""));
        assertTrue(first.kafkaJson().contains("\"maximum_event_time_epoch_ms\":100"));

        var kafka = new RunControlKafkaSchema("control").serialize(first, null, 100L);
        assertNull(kafka.timestamp());
        assertEquals("run", new String(kafka.key(), java.nio.charset.StandardCharsets.UTF_8));
    }
}
