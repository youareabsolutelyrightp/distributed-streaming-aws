from __future__ import annotations

import json
from typing import Any

from .config import FULL, ROOT, SMALL, release


def repository() -> dict[str, Any]:
    selected = release()
    failures: list[str] = []
    for reference, count in ((FULL.reference, FULL.records), (SMALL.reference, SMALL.records)):
        value = json.loads(reference.read_text(encoding="utf-8"))
        if value.get("input_event_count") != count:
            failures.append(f"Reference count is invalid: {reference}")
    if selected["topology"]["total_vcpus"] != 28:
        failures.append("Frozen topology is not 28 vCPUs.")
    if selected["runtime"]["target_rate_per_second"] != 50_000:
        failures.append("Frozen target is not 50,000 records per second.")
    if selected["flink"] != {
        "execution.buffer-timeout.enabled": True,
        "execution.buffer-timeout.interval": "5 ms",
    }:
        failures.append("Frozen Flink buffer-timeout contract is invalid.")
    flink_source = ROOT / "processor" / "flink" / "src" / "main"
    overrides = [
        str(path.relative_to(ROOT))
        for path in flink_source.rglob("*.java")
        if ".setBufferTimeout(" in path.read_text(encoding="utf-8")
    ]
    if overrides:
        failures.append(
            "Operator-level buffer-timeout overrides are not allowed: "
            + ", ".join(overrides)
        )
    return {"version": 1, "passed": not failures, "failures": failures}
