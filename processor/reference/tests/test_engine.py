import unittest

from taxi_reference.engine import (
    Q1_EVENT_STATE,
    Q2_EVENT_STATE,
    ReferenceEngine,
)
from taxi_reference.model import Event

CELL_1 = (1 << 10) | 1
CELL_2 = (2 << 10) | 1


class CollectingSink:
    def __init__(self):
        self.records = {
            Q1_EVENT_STATE: [],
            Q2_EVENT_STATE: [],
        }

    def add(self, stream, record, anchor_line):
        self.records[stream].append((anchor_line, record))


def event(line, time, taxi, pickup=CELL_1, dropoff=CELL_1, money=100):
    return Event(line, f"event-{line}", time, taxi, pickup, dropoff, pickup, dropoff, money)


class EngineTests(unittest.TestCase):
    def test_expiration_precedes_input_at_same_boundary(self):
        sink = CollectingSink()
        engine = ReferenceEngine(sink)
        engine.run(
            [event(1, 0, "A"), event(2, 0, "B"), event(3, 1800, "C")],
            1800,
        )
        q1 = [record for _, record in sink.records[Q1_EVENT_STATE]]
        self.assertEqual(q1[0]["top"][0]["count"], 2)
        terminal = sink.records[Q1_EVENT_STATE][-1][1]
        self.assertTrue(terminal["terminal"])
        self.assertEqual(terminal["effective_time"], "1970-01-01T00:30:00Z")
        self.assertEqual(terminal["top"][0]["count"], 1)

    def test_taxi_transition_and_negative_money(self):
        sink = CollectingSink()
        engine = ReferenceEngine(sink)
        engine.run(
            [
                event(1, 0, "A", money=100),
                event(2, 10, "A", dropoff=CELL_2, money=300),
                event(3, 20, "B", money=None),
            ],
            20,
        )
        q2 = [record for _, record in sink.records[Q2_EVENT_STATE]]
        self.assertEqual(q2[0]["top"][0]["empty_taxis"], 1)
        self.assertEqual(q2[0]["top"][0]["median_fare_plus_tip_twice_cents"], 200)
        self.assertEqual(q2[1]["top"], [])
        self.assertEqual(q2[2]["top"][0]["empty_taxis"], 1)
        self.assertEqual(q2[2]["top"][0]["median_fare_plus_tip_twice_cents"], 400)
        self.assertTrue(sink.records[Q2_EVENT_STATE][-1][1]["terminal"])

    def test_equal_time_inputs_are_consolidated_for_event_state(self):
        sink = CollectingSink()
        engine = ReferenceEngine(sink)
        engine.run([event(1, 0, "A"), event(2, 0, "B", money=200)], 0)
        self.assertEqual(len(sink.records[Q1_EVENT_STATE]), 1)
        self.assertEqual(sink.records[Q1_EVENT_STATE][0][1]["top"][0]["count"], 2)


if __name__ == "__main__":
    unittest.main()
