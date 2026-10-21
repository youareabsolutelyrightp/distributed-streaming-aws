#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}


def now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build(result: Path) -> dict[str, Any]:
    runtime = read(result / "runtime-manifest.json")
    measurements = read(result / "benchmark-operational-report.json")
    cost = read(result / "cost-report.json")
    cleanup = read(result / "cleanup-audit.json")
    failure = read(result / "run-failure.json")
    expected_evidence = (
        "runtime-manifest.json",
        "run-manifest.json",
        "benchmark-operational-report.json",
        "correctness-report.json",
        "latency-attribution.json",
        "raw-transport-trace-manifest.json",
    )
    missing = [name for name in expected_evidence if not (result / name).is_file()]
    failures = []
    if failure:
        failures.append({
            "phase": failure.get("phase", runtime.get("status", "unknown")),
            "category": failure.get("category", "runtime_exit"),
            "timestamp": failure.get("failed_at", now()),
            "affected_gate": failure.get("affected_gate", "operational"),
            "available_evidence": sorted(path.name for path in result.iterdir()),
            "cleanup_result": cleanup.get("status", "pending"),
        })
    return {
        "version": 1,
        "run_id": runtime.get("run_id", result.name),
        "created_at": now(),
        "runtime_complete": runtime.get("status") == "complete" and bool(measurements),
        "evidence_collected": not missing,
        "cleanup_recorded": bool(cleanup),
        "cost_recorded": bool(cost),
        "missing_evidence": missing,
        "failures": failures,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = build(args.result)
    output = args.output or args.result / "run-status.json"
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
