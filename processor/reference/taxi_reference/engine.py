from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Iterable, Protocol

from .canonical import (
    profit_signature,
    profit_top,
    route_signature,
    route_top,
    with_output_id,
)
from .model import Event, event_time_text, route_id
from .ranking import ProfitEntry, RouteEntry, TopTen
from .windows import ExactMedianWindow

QUERY1_WINDOW_SECONDS = 30 * 60
QUERY2_FARE_WINDOW_SECONDS = 15 * 60
QUERY2_TAXI_WINDOW_SECONDS = 30 * 60

Q1_EVENT_STATE = "query1.event-time-state"
Q2_EVENT_STATE = "query2.event-time-state"
STREAMS = (Q1_EVENT_STATE, Q2_EVENT_STATE)


class ResultSink(Protocol):
    def add(self, stream: str, record: dict[str, object], anchor_line: int) -> None: ...


@dataclass(slots=True)
class EngineStats:
    input_events: int = 0
    timer_actions_query1: int = 0
    timer_actions_query2: int = 0
    terminal_event_time: int = 0


class ReferenceEngine:
    def __init__(self, sink: ResultSink) -> None:
        self.sink = sink
        self.stats = EngineStats()

        self.route_events: dict[int, deque[tuple[int, int]]] = {}
        self.route_expirations: deque[tuple[int, int, int]] = deque()
        self.route_ranking: TopTen[RouteEntry] = TopTen()

        self.fare_windows: dict[int, ExactMedianWindow] = {}
        self.fare_expirations: deque[tuple[int, int, int, int]] = deque()
        self.taxi_current: dict[str, tuple[int, int]] = {}
        self.taxi_generations: dict[str, int] = {}
        self.taxi_expirations: deque[tuple[int, int, str, int, int]] = deque()
        self.empty_taxis: dict[int, int] = {}
        self.profit_ranking: TopTen[ProfitEntry] = TopTen()

        self.event_state_q1: tuple[tuple[int, ...], ...] | None = None
        self.event_state_q2: tuple[tuple[int, ...], ...] | None = None

    def run(self, events: Iterable[Event], terminal_event_time: int) -> EngineStats:
        iterator = iter(events)
        pending = next(iterator, None)
        if pending is None:
            self._emit_event_boundary(
                terminal_event_time, "terminal", terminal=True
            )
            self.stats.terminal_event_time = terminal_event_time
            return self.stats

        while pending is not None:
            boundary = pending.event_time
            if boundary > terminal_event_time:
                raise ValueError("input event exceeds the declared terminal event time")
            self._process_timer_boundaries_before(boundary)
            had_timers = self._process_timers_at(boundary)

            while pending is not None and pending.event_time == boundary:
                self._process_input(pending)
                self.stats.input_events += 1
                pending = next(iterator, None)

            boundary_kind = "mixed" if had_timers else "input"
            self._emit_event_boundary(
                boundary,
                boundary_kind,
                terminal=pending is None and boundary == terminal_event_time,
            )

        self._process_remaining_timers(terminal_event_time)
        if self.stats.terminal_event_time != terminal_event_time:
            self._emit_event_boundary(
                terminal_event_time, "terminal", terminal=True
            )
        self.stats.terminal_event_time = terminal_event_time
        return self.stats

    def _next_timer_time(self) -> int | None:
        values = []
        if self.route_expirations:
            values.append(self.route_expirations[0][0])
        if self.fare_expirations:
            values.append(self.fare_expirations[0][0])
        if self.taxi_expirations:
            values.append(self.taxi_expirations[0][0])
        return min(values) if values else None

    def _process_timer_boundaries_before(self, target: int) -> None:
        while True:
            timer_time = self._next_timer_time()
            if timer_time is None or timer_time >= target:
                return
            self._process_timers_at(timer_time)
            self._emit_event_boundary(timer_time, "event_time_timer")

    def _process_remaining_timers(self, terminal: int) -> None:
        while True:
            timer_time = self._next_timer_time()
            if timer_time is None or timer_time > terminal:
                return
            self._process_timers_at(timer_time)
            self._emit_event_boundary(
                timer_time,
                "event_time_timer",
                terminal=timer_time == terminal,
            )

    def _process_timers_at(self, boundary: int) -> bool:
        had_timer = False
        while self.route_expirations and self.route_expirations[0][0] == boundary:
            _, source_line, route = self.route_expirations.popleft()
            route_values = self.route_events.get(route)
            if route_values is None:
                raise AssertionError("route expiration has no active route")
            expired_time, expired_line = route_values.popleft()
            if expired_line != source_line or expired_time + QUERY1_WINDOW_SECONDS != boundary:
                raise AssertionError("route expiration order changed")
            if route_values:
                freshness_time, freshness_line = route_values[-1]
                self.route_ranking.set(
                    RouteEntry(
                        key=route,
                        count=len(route_values),
                        freshness_time=freshness_time,
                        freshness_line=freshness_line,
                    )
                )
            else:
                del self.route_events[route]
                self.route_ranking.remove(route)
            self.stats.timer_actions_query1 += 1
            had_timer = True

        while True:
            fare = (
                self.fare_expirations[0]
                if self.fare_expirations and self.fare_expirations[0][0] == boundary
                else None
            )
            taxi = (
                self.taxi_expirations[0]
                if self.taxi_expirations and self.taxi_expirations[0][0] == boundary
                else None
            )
            if fare is None and taxi is None:
                break
            use_fare = taxi is None or (
                fare is not None and (fare[1], "fare") <= (taxi[1], "taxi")
            )
            if use_fare:
                _, source_line, cell, money = self.fare_expirations.popleft()
                window = self.fare_windows[cell]
                window.remove(money)
                if len(window) == 0:
                    del self.fare_windows[cell]
                self._touch_profit(cell, boundary, source_line)
            else:
                _, source_line, medallion, generation, cell = (
                    self.taxi_expirations.popleft()
                )
                if self.taxi_current.get(medallion) == (generation, cell):
                    del self.taxi_current[medallion]
                    self._change_empty(cell, -1)
                    self._touch_profit(cell, boundary, source_line)
                else:
                    continue
            self.stats.timer_actions_query2 += 1
            had_timer = True
        return had_timer

    def _process_input(self, event: Event) -> None:
        if event.pickup_q1 is not None and event.dropoff_q1 is not None:
            route = route_id(event.pickup_q1, event.dropoff_q1)
            values = self.route_events.setdefault(route, deque())
            values.append((event.event_time, event.source_line))
            self.route_expirations.append(
                (
                    event.event_time + QUERY1_WINDOW_SECONDS,
                    event.source_line,
                    route,
                )
            )
            self.route_ranking.set(
                RouteEntry(
                    key=route,
                    count=len(values),
                    freshness_time=event.event_time,
                    freshness_line=event.source_line,
                )
            )
        changed_cells: set[int] = set()
        previous = self.taxi_current.pop(event.medallion, None)
        if previous is not None:
            _, previous_cell = previous
            self._change_empty(previous_cell, -1)
            changed_cells.add(previous_cell)

        generation = self.taxi_generations.get(event.medallion, 0) + 1
        self.taxi_generations[event.medallion] = generation
        if event.dropoff_q2 is not None:
            cell = event.dropoff_q2
            self.taxi_current[event.medallion] = (generation, cell)
            self._change_empty(cell, 1)
            changed_cells.add(cell)
            self.taxi_expirations.append(
                (
                    event.event_time + QUERY2_TAXI_WINDOW_SECONDS,
                    event.source_line,
                    event.medallion,
                    generation,
                    cell,
                )
            )

        if event.pickup_q2 is not None and event.fare_plus_tip_cents is not None:
            cell = event.pickup_q2
            money = event.fare_plus_tip_cents
            window = self.fare_windows.setdefault(cell, ExactMedianWindow())
            window.add(money)
            changed_cells.add(cell)
            self.fare_expirations.append(
                (
                    event.event_time + QUERY2_FARE_WINDOW_SECONDS,
                    event.source_line,
                    cell,
                    money,
                )
            )

        for cell in sorted(changed_cells):
            self._touch_profit(cell, event.event_time, event.source_line)

    def _change_empty(self, cell: int, delta: int) -> None:
        updated = self.empty_taxis.get(cell, 0) + delta
        if updated < 0:
            raise AssertionError("empty-taxi count became negative")
        if updated == 0:
            self.empty_taxis.pop(cell, None)
        else:
            self.empty_taxis[cell] = updated

    def _touch_profit(self, cell: int, effective_time: int, source_line: int) -> None:
        window = self.fare_windows.get(cell)
        empty = self.empty_taxis.get(cell, 0)
        if window is None or empty == 0:
            self.profit_ranking.remove(cell)
            return
        self.profit_ranking.set(
            ProfitEntry(
                key=cell,
                median_twice_cents=window.median_twice_cents(),
                empty_taxis=empty,
                freshness_time=effective_time,
                freshness_line=source_line,
            )
        )

    def _base_record(
        self,
        query_id: str,
        grading_mode: str,
        effective_time: int,
        trigger_type: str,
        top: list[dict[str, object]],
        trigger_source_line: int | None = None,
        trigger_event_id: str | None = None,
        terminal: bool = False,
    ) -> dict[str, object]:
        return with_output_id(
            {
                "canonical_format_version": 1,
                "effective_time": event_time_text(effective_time),
                "grading_mode": grading_mode,
                "query_id": query_id,
                "terminal": terminal,
                "top": top,
                "trigger_event_id": trigger_event_id,
                "trigger_source_line": trigger_source_line,
                "trigger_type": trigger_type,
            }
        )

    def _emit_event_boundary(
        self,
        effective_time: int,
        trigger_type: str,
        *,
        terminal: bool = False,
    ) -> None:
        q1 = route_signature(self.route_ranking.top)
        if terminal or q1 != self.event_state_q1:
            self.event_state_q1 = q1
            self.sink.add(
                Q1_EVENT_STATE,
                self._base_record(
                    "query1",
                    "event-time-state",
                    effective_time,
                    trigger_type,
                    route_top(self.route_ranking.top),
                    terminal=terminal,
                ),
                self.stats.input_events,
            )

        q2 = profit_signature(self.profit_ranking.top)
        if terminal or q2 != self.event_state_q2:
            self.event_state_q2 = q2
            self.sink.add(
                Q2_EVENT_STATE,
                self._base_record(
                    "query2",
                    "event-time-state",
                    effective_time,
                    trigger_type,
                    profit_top(self.profit_ranking.top),
                    terminal=terminal,
                ),
                self.stats.input_events,
            )
        if terminal:
            self.stats.terminal_event_time = effective_time
