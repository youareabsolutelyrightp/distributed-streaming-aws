#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import re
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Any


def read(path: Path, fallback: Any = None) -> Any:
    if not path.exists():
        return fallback
    return json.loads(path.read_text(encoding="utf-8"))


def finite(raw: Any) -> float | None:
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def parse_time(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def node_metric_rows(result: Path, node: str) -> list[dict[str, Any]]:
    path = result / "metrics" / "node" / node / "node-metrics.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def delta_rate(rows: list[dict[str, Any]], key: str, scale: float = 1.0) -> float | None:
    values = [(parse_time(row["timestamp"]), finite(row.get(key))) for row in rows]
    values = [(timestamp, value) for timestamp, value in values if value is not None]
    if len(values) < 2 or values[-1][0] <= values[0][0]:
        return None
    return (values[-1][1] - values[0][1]) * scale / (values[-1][0] - values[0][0])


def cumulative_increase(rows: list[dict[str, Any]], key: str) -> float | None:
    values = [finite(row.get(key)) for row in rows]
    values = [value for value in values if value is not None]
    if len(values) < 2:
        return None
    return sum(
        current - previous if current >= previous else current
        for previous, current in zip(values, values[1:])
    )


def metric_series(result: Path) -> list[dict[str, Any]]:
    return read(result / "metrics" / "flink-range.json", {}).get("data", {}).get("result", [])


def metric_values(item: dict[str, Any]) -> list[float]:
    return [value for _, raw in item.get("values", []) if (value := finite(raw)) is not None]


def parse_byte_size(raw: str) -> int:
    match = re.fullmatch(r"(\d+)([kKmMgG]?)", raw)
    if match is None:
        raise ValueError(f"Invalid JVM byte size: {raw}")
    scale = {"": 1, "k": 1024, "m": 1024**2, "g": 1024**3}
    return int(match.group(1)) * scale[match.group(2).lower()]


def parse_jvm_size(text: str, short_name: str, long_name: str) -> int | None:
    short = re.search(rf"(?:^|\s)-X{short_name}(\d+[kKmMgG]?)\b", text)
    if short is not None:
        return parse_byte_size(short.group(1))
    long = re.search(rf"-XX:{long_name}=(\d+)", text)
    return int(long.group(1)) if long else None


def jvm_heap_evidence(jvm: dict[str, Any]) -> dict[str, Any]:
    flags = jvm.get("flags", "")
    command = jvm.get("command", "")
    xms_from_flags = parse_jvm_size(flags, "ms", "InitialHeapSize")
    xmx_from_flags = parse_jvm_size(flags, "mx", "MaxHeapSize")
    xms = xms_from_flags or parse_jvm_size(command, "ms", "InitialHeapSize")
    xmx = xmx_from_flags or parse_jvm_size(command, "mx", "MaxHeapSize")
    return {
        "xms_bytes": xms,
        "xmx_bytes": xmx,
        "measurement_source": (
            "jcmd_flags"
            if xms_from_flags is not None and xmx_from_flags is not None
            else "process_command_line"
            if xms is not None and xmx is not None
            else "unavailable"
        ),
        "jvm_command": command,
        "jvm_flags": flags,
    }


def ec2_report(result: Path) -> list[dict[str, Any]]:
    instances = read(result / "infrastructure" / "instances.json", {})
    instance_types = {
        item["InstanceType"]: item
        for item in read(result / "infrastructure" / "instance-types.json", {}).get(
            "InstanceTypes", []
        )
    }
    volumes = read(result / "infrastructure" / "volumes.json", {}).get("Volumes", [])
    volume_by_instance: dict[str, list[dict[str, Any]]] = {}
    for volume in volumes:
        for attachment in volume.get("Attachments", []):
            volume_by_instance.setdefault(attachment["InstanceId"], []).append(volume)
    rows = []
    for reservation in instances.get("Reservations", []):
        for instance in reservation.get("Instances", []):
            details = instance_types.get(instance["InstanceType"], {})
            tags = {item["Key"]: item["Value"] for item in instance.get("Tags", [])}
            rows.append(
                {
                    "node": tags.get("Name", instance["InstanceId"]),
                    "instance_id": instance["InstanceId"],
                    "instance_type": instance["InstanceType"],
                    "vcpu": details.get("VCpuInfo", {}).get("DefaultVCpus"),
                    "ram_bytes": details.get("MemoryInfo", {}).get("SizeInMiB", 0)
                    * 1024
                    * 1024,
                    "ebs": [
                        {
                            "volume_id": volume["VolumeId"],
                            "type": volume["VolumeType"],
                            "size_gib": volume["Size"],
                            "iops": volume.get("Iops"),
                            "throughput_mib_per_second": volume.get("Throughput"),
                        }
                        for volume in volume_by_instance.get(instance["InstanceId"], [])
                    ],
                }
            )
    return sorted(rows, key=lambda item: item["node"])


def flink_report(result: Path, series: list[dict[str, Any]]) -> dict[str, Any]:
    preflight = read(result / "preflight-report.json", {}) or {}
    raw_configuration = read(
        result / "readiness" / "flink-configuration.json", []
    ) or []
    configuration = {
        item.get("key"): item.get("value")
        for item in raw_configuration
        if item.get("key") in {
            "execution.buffer-timeout.enabled",
            "execution.buffer-timeout.interval",
        }
    }
    runtime_nodes = []
    for path in sorted((result / "metrics" / "node").glob("*/runtime-system.json")):
        item = read(path, {})
        if item.get("role") not in {"taskmanager", "smoke"}:
            continue
        heap_evidence = jvm_heap_evidence(item.get("jvm", {}))
        runtime_nodes.append(
            {
                "node": item.get("node"),
                **heap_evidence,
            }
        )
    tm_cpu: dict[str, list[float]] = {}
    tm_heap: dict[str, list[float]] = {}
    operator_busy = []
    for item in series:
        metric = item.get("metric", {})
        name = metric.get("__name__", "")
        values = metric_values(item)
        if not values:
            continue
        tm = str(metric.get("tm_id") or metric.get("instance") or "")
        if name.endswith("Status_JVM_CPU_Load") and "taskmanager" in name:
            tm_cpu.setdefault(tm, []).extend(value * 100.0 for value in values)
        if name.endswith("Status_JVM_Memory_Heap_Used") and "taskmanager" in name:
            tm_heap.setdefault(tm, []).extend(values)
        task_or_operator = metric.get("operator_name") or metric.get("task_name")
        if "busyTimeMsPerSecond" in name and task_or_operator is not None:
            operator_busy.append(
                {
                    "task_or_operator": task_or_operator,
                    "subtask": metric.get("subtask_index"),
                    "taskmanager": tm,
                    "busy_average_ms_per_second": mean(values),
                    "busy_maximum_ms_per_second": max(values),
                }
            )
    return {
        "effective_buffer_timeout": configuration,
        "task_slots_per_taskmanager": [
            item.get("slots") for item in preflight.get("taskmanagers", [])
        ],
        "memory_configuration_by_taskmanager": [
            {
                "taskmanager": item.get("id"),
                "process_memory": item.get("memory_configuration", {}).get("totalProcessMemory"),
                "task_heap": item.get("memory_configuration", {}).get("taskHeap"),
                "network_memory": item.get("memory_configuration", {}).get("networkMemory"),
                "managed_memory": item.get("memory_configuration", {}).get("managedMemory"),
                "jvm_metaspace": item.get("memory_configuration", {}).get("jvmMetaspace"),
            }
            for item in preflight.get("taskmanagers", [])
        ],
        "actual_jvm": runtime_nodes,
        "maximum_heap_used_bytes_by_taskmanager": {
            key: max(values) for key, values in tm_heap.items()
        },
        "cpu_percent_by_taskmanager": {
            key: {"average": mean(values), "maximum": max(values)}
            for key, values in tm_cpu.items()
        },
        "operator_subtask_cpu_evidence": {
            "measurement": "busyTimeMsPerSecond records chained task-thread occupancy. Flink does not isolate CPU for each chained operator.",
            "values": operator_busy,
        },
        "task_placement": preflight.get("operator_vertices", []),
    }


def kafka_report(result: Path) -> list[dict[str, Any]]:
    rows = []
    for path in sorted((result / "metrics" / "node").glob("*/runtime-system.json")):
        runtime = read(path, {})
        if runtime.get("role") != "kafka":
            continue
        node = runtime.get("node", path.parent.name)
        metrics = node_metric_rows(result, node)
        cpu = [finite(item.get("cpu_percent")) for item in metrics]
        cpu = [value for value in cpu if value is not None]
        heap = [finite(item.get("kafka_heap_used_kib")) for item in metrics]
        heap = [value for value in heap if value is not None]
        post_gc_heap = [
            finite(item.get("kafka_post_gc_heap_used_kib")) for item in metrics
        ]
        post_gc_heap = [value for value in post_gc_heap if value is not None]
        heap_capacity = [
            finite(item.get("kafka_heap_capacity_kib")) for item in metrics
        ]
        heap_capacity = [value for value in heap_capacity if value is not None]
        cache = [finite(item.get("page_cache_kib")) for item in metrics]
        cache = [value for value in cache if value is not None]
        operations = None
        if len(metrics) >= 2:
            operations = (
                float(metrics[-1].get("disk_reads", 0))
                + float(metrics[-1].get("disk_writes", 0))
                - float(metrics[0].get("disk_reads", 0))
                - float(metrics[0].get("disk_writes", 0))
            )
        io_delta = (
            float(metrics[-1].get("disk_io_time_ms", 0))
            - float(metrics[0].get("disk_io_time_ms", 0))
            if len(metrics) >= 2
            else None
        )
        heap_evidence = jvm_heap_evidence(runtime.get("jvm", {}))
        post_gc_growth = False
        if len(post_gc_heap) >= 5 and heap_capacity:
            tail = post_gc_heap[-5:]
            post_gc_growth = all(
                current > previous for previous, current in zip(tail, tail[1:])
            ) and tail[-1] - tail[0] > max(heap_capacity) * 0.05
        rows.append(
            {
                "node": node,
                "KAFKA_HEAP_OPTS": runtime.get("kafka_heap_opts"),
                "actual_jvm": heap_evidence,
                "actual_xms_bytes": heap_evidence["xms_bytes"],
                "actual_xmx_bytes": heap_evidence["xmx_bytes"],
                "maximum_heap_used_bytes": max(heap) * 1024 if heap else None,
                "heap_used_measurement": (
                    "maximum pre-collection heap from the Kafka GC log"
                    if heap
                    else "unavailable because this run did not collect Kafka GC-log metrics"
                ),
                "maximum_post_gc_heap_used_bytes": (
                    max(post_gc_heap) * 1024 if post_gc_heap else None
                ),
                "first_post_gc_heap_used_bytes": (
                    post_gc_heap[0] * 1024 if post_gc_heap else None
                ),
                "last_post_gc_heap_used_bytes": (
                    post_gc_heap[-1] * 1024 if post_gc_heap else None
                ),
                "continuing_post_gc_heap_growth": post_gc_growth,
                "heap_capacity_bytes": (
                    max(heap_capacity) * 1024 if heap_capacity else None
                ),
                "gc_pause_seconds": cumulative_increase(
                    metrics, "kafka_gc_pause_seconds"
                ),
                "gc_pause_measurement": (
                    "cumulative pause time from the Kafka GC log"
                    if any(
                        finite(item.get("kafka_gc_pause_seconds")) is not None
                        for item in metrics
                    )
                    else "unavailable because this run did not collect Kafka GC-log metrics"
                ),
                "maximum_page_cache_bytes": max(cache) * 1024 if cache else None,
                "broker_cpu_percent": {
                    "average": mean(cpu) if cpu else None,
                    "maximum": max(cpu) if cpu else None,
                },
                "network_receive_bytes_per_second": delta_rate(
                    metrics, "network_receive_bytes"
                ),
                "network_transmit_bytes_per_second": delta_rate(
                    metrics, "network_transmit_bytes"
                ),
                "disk_read_bytes_per_second": delta_rate(
                    metrics, "disk_sectors_read", 512.0
                ),
                "disk_write_bytes_per_second": delta_rate(
                    metrics, "disk_sectors_written", 512.0
                ),
                "disk_average_busy_latency_ms_per_operation": (
                    io_delta / operations if io_delta is not None and operations and operations > 0 else None
                ),
            }
        )
    return rows


def postgres_report(result: Path) -> dict[str, Any] | None:
    runtime = next(
        (
            read(path, {})
            for path in sorted((result / "metrics" / "node").glob("*/runtime-system.json"))
            if read(path, {}).get("role") == "database"
        ),
        None,
    )
    if runtime is None:
        return None
    node = runtime.get("node")
    metrics = node_metric_rows(result, node)
    memory = [finite(item.get("memory_total_kib")) for item in metrics]
    memory = [value for value in memory if value is not None]
    progress = read(result / "materializer-progress.json", {}) or {}
    postgres = runtime.get("postgres") or {}
    return {
        "node": node,
        "shared_buffers": postgres.get("shared_buffers"),
        "effective_cache_size": postgres.get("effective_cache_size"),
        "work_mem": postgres.get("work_mem"),
        "max_connections": postgres.get("max_connections"),
        "active_connections": postgres.get("active_connections"),
        "buffer_cache_hit_ratio": postgres.get("buffer_cache_hit_ratio"),
        "wal_bytes_per_second": delta_rate(metrics, "database_wal_bytes"),
        "materializer_transaction_commit_latency": progress.get(
            "database_transaction_commit_latency"
        ),
        "materializer_database_execution_latency": progress.get(
            "database_execution_latency"
        ),
        "materializer_kafka_offset_commit_latency": progress.get(
            "kafka_offset_commit_latency"
        ),
        "materializer_batch_size": progress.get("batch_size"),
        "materializer_database_retries": progress.get("database_retries"),
        "host_memory_bytes": memory[-1] * 1024 if memory else runtime.get("host", {}).get("ram_bytes"),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--result", type=Path, required=True)
    args = parser.parse_args()
    result = args.result
    manifest_path = result / "system-manifest.json"
    manifest: dict[str, Any] = {
        "version": 1,
        "run_id": result.name,
    }
    series = metric_series(result)
    manifest["system_configuration_and_runtime"] = {
        "version": 1,
        "ec2": ec2_report(result),
        "flink": flink_report(result, series),
        "kafka": kafka_report(result),
        "postgres_timescale": postgres_report(result),
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(f"AWS system manifest: {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
