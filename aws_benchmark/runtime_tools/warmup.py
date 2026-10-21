#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any


def read(path: Path, fallback: Any = None) -> Any:
    if not path.exists():
        return fallback
    return json.loads(path.read_text(encoding="utf-8"))


def rows(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def event_epoch_millis(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def validate(args: argparse.Namespace) -> dict[str, Any]:
    summary = read(args.producer_summary, {}) or {}
    samples = rows(args.samples)
    checkpoints = read(args.checkpoints, {}) or {}
    materializer_lag = int(args.materializer_lag.read_text(encoding="utf-8").strip())
    dlq_records = int(args.dlq_records.read_text(encoding="utf-8").strip())
    last_event_time = event_epoch_millis(summary.get("last_event_time", "1970-01-01T00:00:00Z"))
    buffers = [int(sample.get("buffered_events", -1)) for sample in samples]
    accepted = [int(sample.get("accepted_source_records", -1)) for sample in samples]
    watermarks = [int(sample.get("global_watermark_ms", -(2**63))) for sample in samples]
    oldest_pending = [
        int(sample.get("oldest_pending_boundary_event_time_ms", -(2**63)))
        for sample in samples
    ]
    late = [int(sample.get("late_events", -1)) for sample in samples]
    watermark_lag = [max(0, last_event_time - 1 - value) for value in watermarks]
    first_ack_ms = int(summary.get("first_acknowledged_epoch_us", 0)) // 1000
    last_ack_ms = int(summary.get("last_acknowledged_epoch_us", 0)) // 1000
    completed = [
        item
        for item in checkpoints.get("history", [])
        if item.get("status") == "COMPLETED"
    ]
    scheduled_during_warmup = [
        item
        for item in completed
        if first_ack_ms <= int(item.get("trigger_timestamp", -1))
        and int(item.get("latest_ack_timestamp", item.get("trigger_timestamp", 2**63))) <= last_ack_ms
    ]
    acknowledged_source_line = summary.get("last_contiguous_acknowledged_source_line")
    checks = {
        "producer_paused": summary.get("paused") is True
        and summary.get("progress_phase") == "warmup",
        "expected_warmup_records": acknowledged_source_line == args.expected_warmup_records,
        "exact_accepted_backlog_zero": len(accepted) == 3
        and all(value == args.expected_warmup_records for value in accepted),
        "materializer_lag_zero": materializer_lag == 0,
        "bounded_non_terminal_watermark_lag": len(watermark_lag) == 3
        and max(watermark_lag) <= args.maximum_watermark_lag_ms,
        "no_pending_boundary_at_or_before_watermark": len(oldest_pending) == 3
        and all(pending > watermark for pending, watermark in zip(oldest_pending, watermarks)),
        "buffers_bounded_and_stable": len(buffers) == 3
        and max(buffers) <= 1000
        and all(current <= previous for previous, current in zip(buffers, buffers[1:])),
        "no_late_or_dlq": len(late) == 3 and max(late) == 0 and dlq_records == 0,
        "scheduled_checkpoint_during_warmup": bool(scheduled_during_warmup),
    }
    if args.require_barrier_checkpoint:
        barrier = read(args.barrier_checkpoint, {}) or {}
        barrier_status = barrier.get("status", {})
        if isinstance(barrier_status, dict):
            barrier_status = barrier_status.get("id")
        checks["barrier_checkpoint_complete"] = (
            barrier_status == "COMPLETED" or barrier.get("id") == "COMPLETED"
        )
    failures = [name for name, passed in checks.items() if not passed]
    return {
        "version": 1,
        "operational_complete": not failures,
        "correctness_scope": "non-terminal warm-up barrier",
        "checks": checks,
        "failures": failures,
        "measurements": {
            "last_warmup_event_time": summary.get("last_event_time"),
            "last_acknowledged_source_line": acknowledged_source_line,
            "last_warmup_event_time_epoch_ms": last_event_time,
            "accepted_source_records": accepted,
            "global_watermark_ms": watermarks,
            "watermark_lag_ms": watermark_lag,
            "maximum_allowed_watermark_lag_ms": args.maximum_watermark_lag_ms,
            "oldest_pending_boundary_event_time_ms": oldest_pending,
            "buffered_events": buffers,
            "scheduled_checkpoint_ids": [item.get("id") for item in scheduled_during_warmup],
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--producer-summary", type=Path, required=True)
    parser.add_argument("--samples", type=Path, required=True)
    parser.add_argument("--checkpoints", type=Path, required=True)
    parser.add_argument("--materializer-lag", type=Path, required=True)
    parser.add_argument("--dlq-records", type=Path, required=True)
    parser.add_argument("--expected-warmup-records", type=int, required=True)
    parser.add_argument("--maximum-watermark-lag-ms", type=int, default=60_000)
    parser.add_argument("--require-barrier-checkpoint", action="store_true")
    parser.add_argument("--barrier-checkpoint", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.require_barrier_checkpoint and args.barrier_checkpoint is None:
        raise SystemExit("--barrier-checkpoint is required with --require-barrier-checkpoint")
    report = validate(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if not report["operational_complete"]:
        for failure in report["failures"]:
            print(f"Warm-up barrier failure: {failure}")
        return 1
    print(f"Warm-up barrier passed: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
