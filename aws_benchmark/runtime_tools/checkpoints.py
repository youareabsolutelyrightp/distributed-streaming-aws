#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CATEGORIES = (
    "pre_readiness_not_running",
    "measurement_unexpected",
    "post_measurement",
)


def read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}


def classify(checkpoints: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
    readiness_end = int(
        runtime.get("phases", {}).get("readiness", {}).get("end_epoch_seconds", 0) or 0
    ) * 1000
    measurement_end = int(
        runtime.get("phases", {}).get("measurement", {}).get("end_epoch_seconds", 0) or 0
    ) * 1000
    rows = []
    counts = {name: 0 for name in CATEGORIES}
    for item in checkpoints.get("history", []):
        if item.get("status") != "FAILED":
            continue
        triggered = int(item.get("trigger_timestamp", 0) or 0)
        if readiness_end and triggered < readiness_end:
            category = "pre_readiness_not_running"
        elif measurement_end and triggered > measurement_end:
            category = "post_measurement"
        else:
            category = "measurement_unexpected"
        counts[category] += 1
        rows.append({
            "checkpoint_id": item.get("id"),
            "trigger_timestamp_ms": triggered,
            "failure_timestamp_ms": item.get("failure_timestamp"),
            "reason": item.get("failure_message") or item.get("failure_reason") or "unknown",
            "category": category,
        })
    return {
        "version": 1,
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "total_failures": len(rows),
        "failures_by_category": counts,
        "measurement_failures": counts["measurement_unexpected"],
        "failures": rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--checkpoints", type=Path, required=True)
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = classify(read(args.checkpoints), read(args.runtime))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
