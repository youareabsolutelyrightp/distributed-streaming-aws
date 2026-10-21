#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any


PAREN_ASSIGNMENT = re.compile(r"([A-Za-z0-9._-]+)\(([^)]*)\)")
KAFKA_ASSIGNMENT = re.compile(r"([A-Za-z0-9._-]+):([0-9]+(?:,[0-9]+)*)")


def read_json(path: Path, fallback: Any = None) -> Any:
    if not path.exists():
        return fallback
    return json.loads(path.read_text(encoding="utf-8"))


def assignment_map(text: str) -> dict[str, list[int]]:
    stripped = text.strip()
    if stripped.startswith("{"):
        value = json.loads(stripped)
        return {
            str(topic): sorted(int(partition) for partition in partitions)
            for topic, partitions in sorted(value.get("assignments", {}).items())
        }
    assignments: dict[str, set[int]] = {}
    matches = PAREN_ASSIGNMENT.findall(text) + KAFKA_ASSIGNMENT.findall(text)
    for topic, raw_partitions in matches:
        values = {
            int(value.strip())
            for value in raw_partitions.split(",")
            if value.strip().isdigit()
        }
        assignments.setdefault(topic, set()).update(values)
    return {topic: sorted(values) for topic, values in sorted(assignments.items())}


def stable_assignments(paths: list[Path]) -> tuple[bool, dict[str, list[int]]]:
    samples = [assignment_map(path.read_text(encoding="utf-8")) for path in paths]
    return bool(samples) and all(sample == samples[0] for sample in samples), samples[0] if samples else {}


def expected_partitions(values: list[str]) -> dict[str, list[int]]:
    result: dict[str, list[int]] = {}
    for value in values:
        topic, raw_count = value.rsplit("=", 1)
        result[topic] = list(range(int(raw_count)))
    return result


def topic_offsets(path: Path) -> dict[str, dict[int, int]]:
    result: dict[str, dict[int, int]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        parts = line.rsplit(":", 2)
        if len(parts) != 3:
            continue
        topic, partition, offset = parts
        if partition.isdigit() and offset.lstrip("-").isdigit():
            result.setdefault(topic, {})[int(partition)] = int(offset)
    return result


def validate(args: argparse.Namespace) -> dict[str, Any]:
    checks: dict[str, bool] = {}
    measurements: dict[str, Any] = {}

    jobs = [read_json(path, {}) or {} for path in args.job_snapshot]
    checks["flink_running_stable"] = len(jobs) == 2 and all(
        job.get("state") == "RUNNING"
        and all(vertex.get("status") == "RUNNING" for vertex in job.get("vertices", []))
        for job in jobs
    )
    measurements["flink_job_states"] = [job.get("state") for job in jobs]

    preflight = read_json(args.preflight, {}) or {}
    checks["flink_preflight"] = (
        preflight.get("passed") is True
        and preflight.get("benchmark_source_spread_passed") is True
        and len(preflight.get("source_taskmanagers", [])) == args.expected_source_readers
        and len(preflight.get("taskmanagers", [])) == args.expected_taskmanagers
    )
    measurements["source_taskmanagers"] = preflight.get("source_taskmanagers", [])

    exceptions = read_json(args.exceptions, {}) or {}
    checks["no_flink_restart_or_exception"] = not exceptions.get("all-exceptions", [])

    source_stable, source = stable_assignments(args.source_assignment)
    expected_source = expected_partitions(args.expected_source_topic)
    checks["source_assignments_stable"] = source_stable
    checks["source_assignments_complete"] = source == expected_source
    measurements["source_assignments"] = source

    materializer_stable, materializer = stable_assignments(args.materializer_assignment)
    expected_materializer = expected_partitions(args.expected_materializer_topic)
    checks["materializer_assignments_stable"] = materializer_stable
    checks["materializer_assignments_complete"] = materializer == expected_materializer
    measurements["materializer_assignments"] = materializer

    offsets = topic_offsets(args.topic_offsets)
    expected_empty = expected_partitions(args.expected_empty_topic)
    checks["run_topics_empty"] = all(
        sorted(offsets.get(topic, {})) == partitions
        and all(offsets[topic][partition] == 0 for partition in partitions)
        for topic, partitions in expected_empty.items()
    )
    measurements["topic_offsets"] = offsets

    topic_contract = read_json(args.topic_contract, {}) or {}
    contract_topics = {
        details.get("name"): list(range(int(details.get("partitions", 0))))
        for details in topic_contract.get("topics", {}).values()
        if isinstance(details, dict) and details.get("name")
    }
    contract_partitioning = topic_contract.get("input_partitioning", {})
    checks["kafka_topic_contract"] = (
        topic_contract.get("operational_complete") is True
        and contract_topics == expected_empty
        and contract_partitioning
        == {
            "version": "taxi-medallion-partition-v1",
            "hash_test_vector_version": "sha256-prefix64-be-v1",
            "partition_count": args.expected_source_readers,
            "verified": True,
        }
    )
    measurements["kafka_topic_contract"] = {
        "topics": topic_contract.get("topics", {}),
        "input_partitioning": contract_partitioning,
        "timestamp_canary": topic_contract.get("timestamp_canary", {}),
    }

    materializer_inspect = read_json(args.materializer_inspect, []) or []
    checks["materializer_running"] = bool(materializer_inspect) and bool(
        materializer_inspect[0].get("State", {}).get("Running")
    )
    checks["database_ready"] = args.database_ready.read_text(encoding="utf-8").strip() == "1"

    targets = read_json(args.prometheus_targets, {}) or {}
    active_targets = targets.get("data", {}).get("activeTargets", [])
    checks["prometheus_targets_up"] = (
        len(active_targets) == args.expected_prometheus_targets
        and all(target.get("health") == "up" for target in active_targets)
    )
    measurements["prometheus_targets"] = [
        {
            "scrape_pool": target.get("scrapePool"),
            "scrape_url": target.get("scrapeUrl"),
            "health": target.get("health"),
        }
        for target in active_targets
    ]

    up = read_json(args.prometheus_up, {}) or {}
    up_samples = up.get("data", {}).get("result", [])
    checks["prometheus_current_samples"] = (
        len(up_samples) == args.expected_prometheus_targets
        and all(float(sample.get("value", [0, "0"])[1]) == 1.0 for sample in up_samples)
    )

    failures = [name for name, passed in checks.items() if not passed]
    return {
        "version": 1,
        "operational_complete": not failures,
        "correctness_scope": "pre-input benchmark readiness",
        "checks": checks,
        "failures": failures,
        "measurements": measurements,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-snapshot", type=Path, action="append", required=True)
    parser.add_argument("--preflight", type=Path, required=True)
    parser.add_argument("--exceptions", type=Path, required=True)
    parser.add_argument("--source-assignment", type=Path, action="append", required=True)
    parser.add_argument("--materializer-assignment", type=Path, action="append", required=True)
    parser.add_argument("--topic-offsets", type=Path, required=True)
    parser.add_argument("--topic-contract", type=Path, required=True)
    parser.add_argument("--materializer-inspect", type=Path, required=True)
    parser.add_argument("--database-ready", type=Path, required=True)
    parser.add_argument("--prometheus-targets", type=Path, required=True)
    parser.add_argument("--prometheus-up", type=Path, required=True)
    parser.add_argument("--expected-source-topic", action="append", required=True)
    parser.add_argument("--expected-materializer-topic", action="append", required=True)
    parser.add_argument("--expected-empty-topic", action="append", required=True)
    parser.add_argument("--expected-source-readers", type=int, default=4)
    parser.add_argument("--expected-taskmanagers", type=int, default=4)
    parser.add_argument("--expected-prometheus-targets", type=int, default=6)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = validate(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if not report["operational_complete"]:
        for failure in report["failures"]:
            print(f"Readiness failure: {failure}")
        return 1
    print(f"Benchmark readiness passed: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
