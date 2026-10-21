from __future__ import annotations

import hashlib
import json
from typing import Any

from .model import cell_text, event_time_text, round_half_even_ratio, route_cells
from .ranking import ProfitEntry, RouteEntry


def canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
    ).encode("utf-8")


def with_output_id(record: dict[str, Any]) -> dict[str, Any]:
    digest = hashlib.sha256(canonical_bytes(record)).hexdigest()
    return {**record, "output_id": digest}


def route_signature(top: tuple[RouteEntry, ...]) -> tuple[tuple[int, ...], ...]:
    return tuple(
        (entry.key, entry.count, entry.freshness_time, entry.freshness_line)
        for entry in top
    )


def profit_signature(top: tuple[ProfitEntry, ...]) -> tuple[tuple[int, ...], ...]:
    return tuple(
        (
            entry.key,
            entry.median_twice_cents,
            entry.empty_taxis,
            entry.freshness_time,
            entry.freshness_line,
        )
        for entry in top
    )


def route_top(top: tuple[RouteEntry, ...]) -> list[dict[str, Any]]:
    values: list[dict[str, Any]] = []
    for entry in top:
        pickup, dropoff = route_cells(entry.key)
        pickup_text = cell_text(pickup)
        dropoff_text = cell_text(dropoff)
        values.append(
            {
                "count": entry.count,
                "dropoff_cell": dropoff_text,
                "freshness_source_line": entry.freshness_line,
                "freshness_time": event_time_text(entry.freshness_time),
                "pickup_cell": pickup_text,
                "route_id": f"{pickup_text}>{dropoff_text}",
            }
        )
    return values


def profit_top(top: tuple[ProfitEntry, ...]) -> list[dict[str, Any]]:
    return [
        {
            "cell_id": cell_text(entry.key),
            "empty_taxis": entry.empty_taxis,
            "freshness_source_line": entry.freshness_line,
            "freshness_time": event_time_text(entry.freshness_time),
            "median_fare_plus_tip_twice_cents": entry.median_twice_cents,
            "profitability": round_half_even_ratio(
                entry.median_twice_cents, entry.empty_taxis * 200
            ),
        }
        for entry in top
    ]
