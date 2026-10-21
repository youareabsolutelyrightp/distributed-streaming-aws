from __future__ import annotations

import heapq
from collections import Counter


class ExactMedianWindow:
    __slots__ = (
        "lower",
        "upper",
        "delayed",
        "lower_size",
        "upper_size",
    )

    def __init__(self) -> None:
        self.lower: list[int] = []
        self.upper: list[int] = []
        self.delayed: Counter[int] = Counter()
        self.lower_size = 0
        self.upper_size = 0

    def __len__(self) -> int:
        return self.lower_size + self.upper_size

    def add(self, value: int) -> None:
        self._prune_lower()
        self._prune_upper()
        if not self.lower or value <= -self.lower[0]:
            heapq.heappush(self.lower, -value)
            self.lower_size += 1
        else:
            heapq.heappush(self.upper, value)
            self.upper_size += 1
        self._rebalance()

    def remove(self, value: int) -> None:
        self._prune_lower()
        self._prune_upper()
        if self.lower_size == 0:
            raise ValueError("cannot remove from an empty median window")
        self.delayed[value] += 1
        if value <= -self.lower[0]:
            self.lower_size -= 1
            if value == -self.lower[0]:
                self._prune_lower()
        else:
            self.upper_size -= 1
            if self.upper and value == self.upper[0]:
                self._prune_upper()
        self._rebalance()

    def median_twice_cents(self) -> int:
        if len(self) == 0:
            raise ValueError("empty median window")
        self._prune_lower()
        self._prune_upper()
        if self.lower_size > self.upper_size:
            return -self.lower[0] * 2
        return -self.lower[0] + self.upper[0]

    def _rebalance(self) -> None:
        if self.lower_size > self.upper_size + 1:
            self._prune_lower()
            value = -heapq.heappop(self.lower)
            self.lower_size -= 1
            heapq.heappush(self.upper, value)
            self.upper_size += 1
            self._prune_lower()
        elif self.lower_size < self.upper_size:
            self._prune_upper()
            value = heapq.heappop(self.upper)
            self.upper_size -= 1
            heapq.heappush(self.lower, -value)
            self.lower_size += 1
            self._prune_upper()
        self._prune_lower()
        self._prune_upper()

    def _prune_lower(self) -> None:
        while self.lower:
            value = -self.lower[0]
            count = self.delayed.get(value, 0)
            if count == 0:
                return
            heapq.heappop(self.lower)
            if count == 1:
                del self.delayed[value]
            else:
                self.delayed[value] = count - 1

    def _prune_upper(self) -> None:
        while self.upper:
            value = self.upper[0]
            count = self.delayed.get(value, 0)
            if count == 0:
                return
            heapq.heappop(self.upper)
            if count == 1:
                del self.delayed[value]
            else:
                self.delayed[value] = count - 1
