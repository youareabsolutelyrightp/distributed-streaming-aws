from __future__ import annotations

import heapq
from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar


class RankedEntry(Protocol):
    key: int

    def __lt__(self, other: object) -> bool: ...


T = TypeVar("T", bound=RankedEntry)


@dataclass(slots=True)
class RouteEntry:
    key: int
    count: int
    freshness_time: int
    freshness_line: int

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, RouteEntry):
            return NotImplemented
        if self.count != other.count:
            return self.count > other.count
        if self.freshness_time != other.freshness_time:
            return self.freshness_time > other.freshness_time
        if self.freshness_line != other.freshness_line:
            return self.freshness_line > other.freshness_line
        return self.key < other.key


@dataclass(slots=True)
class ProfitEntry:
    key: int
    median_twice_cents: int
    empty_taxis: int
    freshness_time: int
    freshness_line: int

    @property
    def numerator(self) -> int:
        return self.median_twice_cents

    @property
    def denominator(self) -> int:
        return self.empty_taxis * 2

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, ProfitEntry):
            return NotImplemented
        left = self.numerator * other.denominator
        right = other.numerator * self.denominator
        if left != right:
            return left > right
        if self.freshness_time != other.freshness_time:
            return self.freshness_time > other.freshness_time
        if self.freshness_line != other.freshness_line:
            return self.freshness_line > other.freshness_line
        return self.key < other.key


class TopTen(Generic[T]):
    def __init__(self) -> None:
        self.current: dict[int, T] = {}
        self.heap: list[T] = []
        self.top: tuple[T, ...] = ()
        self.top_keys: set[int] = set()

    def set(self, entry: T) -> None:
        was_top = entry.key in self.top_keys
        self.current[entry.key] = entry
        heapq.heappush(self.heap, entry)
        if (
            was_top
            or len(self.top) < 10
            or (self.top and entry < self.top[-1])
        ):
            self._rebuild_top()
        self._maybe_compact()

    def remove(self, key: int) -> None:
        was_top = key in self.top_keys
        self.current.pop(key, None)
        if was_top:
            self._rebuild_top()
        self._maybe_compact()

    def _rebuild_top(self) -> None:
        selected: list[T] = []
        while self.heap and len(selected) < 10:
            candidate = heapq.heappop(self.heap)
            if self.current.get(candidate.key) is candidate:
                selected.append(candidate)
        for candidate in selected:
            heapq.heappush(self.heap, candidate)
        self.top = tuple(selected)
        self.top_keys = {entry.key for entry in selected}

    def _maybe_compact(self) -> None:
        limit = max(4096, len(self.current) * 4)
        if len(self.heap) <= limit:
            return
        self.heap = list(self.current.values())
        heapq.heapify(self.heap)
        self._rebuild_top()
