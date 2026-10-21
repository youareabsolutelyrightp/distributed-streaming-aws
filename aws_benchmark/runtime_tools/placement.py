#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import sys
import urllib.parse
import urllib.request
from collections import Counter
from pathlib import Path
from typing import Any


QUERY1_GLOBAL_MARKERS = ("Query 1 exact global top ten",)
QUERY2_GLOBAL_MARKERS = ("Query 2 exact global top ten",)
SOURCE = "Shared taxi Kafka source"
PROCESSING_VERTICES = {
    "partition_boundary_preaggregate": "Pre-aggregate input boundaries by Kafka partition",
    "query1_shard_rank": "Query 1 shard-local top ten",
    "query2_shard_rank": "Query 2 shard-local top ten",
}
REQUIRED_PROCESSING_SPREAD = set(PROCESSING_VERTICES)


def fetch(base_url: str, path: str) -> Any:
    with urllib.request.urlopen(base_url.rstrip("/") + path, timeout=20) as response:
        return json.load(response)


def metric_values(base_url: str, taskmanager_id: str) -> dict[str, int]:
    names = [
        "Status.JVM.Memory.Heap.Max",
        "Status.JVM.Memory.Heap.Used",
        "Status.Flink.Memory.Managed.Total",
        "Status.Flink.Memory.Managed.Used",
    ]
    query = urllib.parse.urlencode({"get": ",".join(names)})
    values = fetch(base_url, f"/taskmanagers/{taskmanager_id}/metrics?{query}")
    return {item["id"]: int(float(item["value"])) for item in values}


def memory_value(configuration: dict[str, Any], *names: str) -> int | None:
    for name in names:
        value = configuration.get(name)
        if value is not None:
            return int(value)
    return None


def assigned_taskmanager(value: Any) -> str | None:
    if value is None:
        return None
    identifier = str(value).strip()
    if not identifier or identifier.lower() in {"(unassigned)", "unassigned"}:
        return None
    return identifier


def collect(args: argparse.Namespace) -> dict[str, Any]:
    failures: list[str] = []
    if args.source_parallelism != args.input_partitions:
        failures.append("source parallelism must equal the Kafka input partition count")

    taskmanager_list = fetch(args.rest_url, "/taskmanagers").get("taskmanagers", [])
    if len(taskmanager_list) != args.expected_taskmanagers:
        failures.append(
            f"expected {args.expected_taskmanagers} TaskManagers, found {len(taskmanager_list)}"
        )

    taskmanagers: list[dict[str, Any]] = []
    for summary in taskmanager_list:
        taskmanager_id = summary["id"]
        detail = fetch(args.rest_url, f"/taskmanagers/{taskmanager_id}")
        configuration = detail.get("memoryConfiguration", {})
        metrics = metric_values(args.rest_url, taskmanager_id)
        managed = memory_value(configuration, "managedMemory", "managed-memory")
        if managed is None:
            managed = metrics.get("Status.Flink.Memory.Managed.Total")
        if args.state_backend == "hashmap" and managed != 0:
            failures.append(
                f"TaskManager {taskmanager_id} reserves {managed} managed-memory bytes with HashMap"
            )
        slots = int(detail.get("slotsNumber", summary.get("slotsNumber", 0)))
        if slots != args.expected_slots:
            failures.append(
                f"TaskManager {taskmanager_id} has {slots} slots, expected {args.expected_slots}"
            )
        free_slots = int(detail.get("freeSlots", summary.get("freeSlots", 0)))
        allocated_slots = slots - free_slots
        if allocated_slots < 0 or allocated_slots > args.expected_slots:
            failures.append(
                f"TaskManager {taskmanager_id} has {allocated_slots} allocated slots, "
                f"expected no more than {args.expected_slots}"
            )
        taskmanagers.append(
            {
                "id": taskmanager_id,
                "path": detail.get("path"),
                "slots": slots,
                "free_slots": free_slots,
                "allocated_slots": allocated_slots,
                "memory_configuration": configuration,
                "memory_metrics": metrics,
            }
        )
    total_allocated_slots = sum(item["allocated_slots"] for item in taskmanagers)
    maximum_allocated_slots = args.expected_taskmanagers * args.expected_slots
    expected_allocated_slots = getattr(args, "expected_allocated_slots", None)
    if expected_allocated_slots is None:
        expected_allocated_slots = maximum_allocated_slots
    if total_allocated_slots > maximum_allocated_slots:
        failures.append(
            f"found {total_allocated_slots} allocated slots, maximum is {maximum_allocated_slots}"
        )
    require_processing_spread = getattr(args, "require_processing_spread", False)
    if require_processing_spread and total_allocated_slots != expected_allocated_slots:
        failures.append(
            f"found {total_allocated_slots} allocated slots, expected "
            f"{expected_allocated_slots} for the benchmark processing layout"
        )

    job = fetch(args.rest_url, f"/jobs/{args.job_id}")
    vertices: list[dict[str, Any]] = []
    query1_hosts: set[str] = set()
    query2_hosts: set[str] = set()
    source_hosts: set[str] = set()
    source_placements: list[dict[str, Any]] = []
    source_parallelism_seen: int | None = None
    processing_placements: dict[str, list[dict[str, Any]]] = {}
    for vertex_summary in job.get("vertices", []):
        vertex = fetch(args.rest_url, f"/jobs/{args.job_id}/vertices/{vertex_summary['id']}")
        name = vertex.get("name", vertex_summary.get("name", ""))
        placements = [
            {
                "subtask": item.get("subtask"),
                "status": item.get("status"),
                "taskmanager_id": item.get("taskmanager-id"),
                "host": item.get("host"),
            }
            for item in vertex.get("subtasks", [])
        ]
        vertices.append(
            {
                "id": vertex_summary["id"],
                "name": name,
                "parallelism": vertex.get("parallelism", vertex_summary.get("parallelism")),
                "placements": placements,
            }
        )
        assigned_hosts = [assigned_taskmanager(item["taskmanager_id"]) for item in placements]
        hosts = {host for host in assigned_hosts if host is not None}
        if len(placements) != len(assigned_hosts):
            raise ValueError("Operator placement and TaskManager counts differ.")
        incomplete = [
            item
            for item, host in zip(placements, assigned_hosts)
            if item["status"] != "RUNNING" or host is None
        ]
        if incomplete:
            failures.append(
                f"vertex {name} has {len(incomplete)} subtasks that are not running and assigned"
            )
        if any(marker in name for marker in QUERY1_GLOBAL_MARKERS):
            query1_hosts.update(hosts)
        if any(marker in name for marker in QUERY2_GLOBAL_MARKERS):
            query2_hosts.update(hosts)
        if SOURCE in name:
            source_parallelism_seen = int(
                vertex.get("parallelism", vertex_summary.get("parallelism", 0))
            )
            source_hosts.update(hosts)
            source_placements.extend(placements)
        for stage, marker in PROCESSING_VERTICES.items():
            if marker in name:
                processing_placements[stage] = placements
    boundary_mode = getattr(args, "shared_boundary_mode", "coalesced")

    if len(query1_hosts) != 1:
        failures.append(f"Query 1 global operator uses {len(query1_hosts)} TaskManagers")
    if len(query2_hosts) != 1:
        failures.append(f"Query 2 global operator uses {len(query2_hosts)} TaskManagers")
    if query1_hosts and query1_hosts == query2_hosts:
        failures.append("Query 1 and Query 2 global operators use the same TaskManager")
    if source_parallelism_seen != args.source_parallelism:
        failures.append(
            f"source vertex parallelism is {source_parallelism_seen}, expected {args.source_parallelism}"
        )
    if args.require_source_spread and len(source_hosts) != args.source_parallelism:
        failures.append(
            f"source readers use {len(source_hosts)} TaskManagers, "
            f"expected {args.source_parallelism} distinct TaskManagers"
        )
    one_partition_per_reader = bool(
        source_parallelism_seen == args.source_parallelism
        and args.source_parallelism == args.input_partitions
    )
    required_processing_stages = set(REQUIRED_PROCESSING_SPREAD)
    processing_spread: dict[str, Any] = {}
    for stage in sorted(required_processing_stages):
        placements = processing_placements.get(stage, [])
        hosts = [
            assigned_taskmanager(item.get("taskmanager_id")) for item in placements
        ]
        counts = Counter(host for host in hosts if host is not None)
        parallelism = len(placements)
        maximum_per_taskmanager = (
            math.ceil(parallelism / args.expected_taskmanagers) if parallelism else None
        )
        passed = bool(
            parallelism > 0
            and len(counts) == args.expected_taskmanagers
            and maximum_per_taskmanager is not None
            and max(counts.values(), default=0) <= maximum_per_taskmanager
        )
        processing_spread[stage] = {
            "passed": passed,
            "parallelism": parallelism,
            "taskmanager_counts": dict(sorted(counts.items())),
            "maximum_subtasks_per_taskmanager": maximum_per_taskmanager,
        }
        if require_processing_spread and stage in required_processing_stages and not passed:
            failures.append(
                f"{stage} does not use all {args.expected_taskmanagers} TaskManagers "
                "with balanced subtask counts"
            )

    return {
        "version": 1,
        "passed": not failures,
        "failures": failures,
        "job_id": args.job_id,
        "state_backend": args.state_backend,
        "source_parallelism": args.source_parallelism,
        "input_partitions": args.input_partitions,
        "partitioning_version": getattr(
            args, "partitioning_version", "taxi-medallion-partition-v1"
        ),
        "partition_hash_test_vector_version": getattr(
            args, "partition_hash_test_vector_version", "sha256-prefix64-be-v1"
        ),
        "benchmark_source_spread_required": args.require_source_spread,
        "benchmark_source_spread_passed": (
            not args.require_source_spread or len(source_hosts) == args.source_parallelism
        ),
        "benchmark_processing_spread_required": require_processing_spread,
        "benchmark_processing_spread_passed": (
            not require_processing_spread
            or all(
                processing_spread[stage]["passed"]
                for stage in required_processing_stages
            )
        ),
        "processing_spread": processing_spread,
        "shared_boundary_mode": boundary_mode,
        "one_partition_per_source_reader_structurally_possible": one_partition_per_reader,
        "one_partition_per_source_reader_required": True,
        "one_partition_per_source_reader_passed": one_partition_per_reader,
        "source_taskmanagers": sorted(source_hosts),
        "source_placements": source_placements,
        "taskmanagers": taskmanagers,
        "total_allocated_slots": total_allocated_slots,
        "maximum_allocated_slots": maximum_allocated_slots,
        "expected_allocated_slots": expected_allocated_slots,
        "operator_vertices": vertices,
        "query1_global_taskmanagers": sorted(query1_hosts),
        "query2_global_taskmanagers": sorted(query2_hosts),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rest-url", required=True)
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--expected-taskmanagers", type=int, required=True)
    parser.add_argument("--expected-slots", type=int, required=True)
    parser.add_argument("--expected-allocated-slots", type=int)
    parser.add_argument("--input-partitions", type=int, required=True)
    parser.add_argument(
        "--partitioning-version", default="taxi-medallion-partition-v1"
    )
    parser.add_argument(
        "--partition-hash-test-vector-version", default="sha256-prefix64-be-v1"
    )
    parser.add_argument("--source-parallelism", type=int, required=True)
    parser.add_argument("--require-source-spread", action="store_true")
    parser.add_argument("--require-processing-spread", action="store_true")
    parser.add_argument("--state-backend", choices=("hashmap",), required=True)
    parser.add_argument(
        "--shared-boundary-mode",
        choices=("coalesced",),
        default="coalesced",
    )
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        report = collect(args)
    except Exception as error:
        report = {
            "version": 1,
            "passed": False,
            "failures": [f"preflight collection failed: {error}"],
            "job_id": args.job_id,
        }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if not report["passed"]:
        for failure in report["failures"]:
            print(f"Preflight failure: {failure}", file=sys.stderr)
        return 1
    print(f"Flink preflight passed: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
