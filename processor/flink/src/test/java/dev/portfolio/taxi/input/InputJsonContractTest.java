package dev.portfolio.taxi.input;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.portfolio.taxi.input.InputModels.TripEvent;

import java.io.IOException;

import org.junit.jupiter.api.Test;

final class InputJsonContractTest {
    @Test
    void preservesJoinedIdentityAndDecimalLexemes() throws Exception {
        String dataset = "dataset";
        String joined = "joined";
        String eventId = InputPartitioning.expectedEventId(dataset, joined);
        TripEvent event = (TripEvent) InputJson.parse("""
                {"schema_version":1,"record_type":"trip","run_id":"run",
                 "dataset_id":"dataset","joined_record_id":"joined",
                 "event_id":"%s","source_line_number":1,"medallion":"taxi-a",
                 "dropoff_datetime":"2013-01-01T00:00:00Z",
                 "pickup_longitude":-73.956528,"pickup_latitude":40.716976,
                 "dropoff_longitude":-73.962440,"dropoff_latitude":40.715008,
                 "fare_amount":"3.50","tip_amount":"0.00",
                 "producer_send_epoch_us":1}
                """.formatted(eventId));
        assertEquals(joined, event.joinedRecordId());
        assertEquals("-73.956528", event.pickupLongitude());
        assertEquals("40.716976", event.pickupLatitude());
        TripEvent partitioned = (TripEvent) InputJson.parse(
                """
                {"schema_version":1,"record_type":"trip","run_id":"run",
                 "dataset_id":"dataset","joined_record_id":"joined",
                 "event_id":"%s","source_line_number":1,"medallion":"taxi-a",
                 "dropoff_datetime":"2013-01-01T00:00:00Z",
                 "pickup_longitude":-73.956528,"pickup_latitude":40.716976,
                 "dropoff_longitude":-73.962440,"dropoff_latitude":40.715008,
                 "fare_amount":"3.50","tip_amount":"0.00",
                 "producer_send_epoch_us":1}
                """.formatted(eventId),
                3,
                10L,
                20L);
        assertEquals(3, partitioned.kafkaPartition());
    }

    @Test
    void requiresJoinedRecordIdentity() {
        assertThrows(IOException.class, () -> InputJson.parse("""
                {"schema_version":1,"record_type":"trip","run_id":"run",
                 "dataset_id":"dataset","event_id":"bad","source_line_number":1,
                 "medallion":"taxi-a","dropoff_datetime":"2013-01-01T00:00:00Z",
                 "fare_amount":"1.00","tip_amount":"0.00","producer_send_epoch_us":1}
                """));
    }
}
