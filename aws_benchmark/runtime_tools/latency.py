#!/usr/bin/env python3
from __future__ import annotations

import argparse
import bisect
import hashlib
import heapq
import json
import math
import random
import shutil
import statistics
import subprocess
import sys
from array import array
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


INPUT_FIELDS = (
    "trigger_input_append_epoch_us",
    "trigger_input_source_read_epoch_us",
    "maximum_required_source_read_epoch_us",
    "partition_boundary_emitted_epoch_us",
    "shared_boundary_emitted_epoch_us",
    "boundary_signal_arrival_epoch_us",
    "watermark_closed_epoch_us",
    "global_boundary_started_epoch_us",
    "result_emitted_epoch_us",
    "serialization_started_epoch_us",
    "serialization_completed_epoch_us",
    "result_kafka_append_epoch_us",
)
TIMER_FIELDS = (
    "watermark_closed_epoch_us",
    "global_boundary_started_epoch_us",
    "result_emitted_epoch_us",
    "serialization_started_epoch_us",
    "serialization_completed_epoch_us",
    "result_kafka_append_epoch_us",
)
TRANSPORT_TSV_FIELDS = (
    "query_id",
    "output_id",
    "effective_time",
    "trigger_type",
    "transport_measurement_version",
    "trigger_flags",
    "trigger_input_partition",
    "trigger_input_source_line",
    "boundary_first_input_append_epoch_us",
    "trigger_input_append_epoch_us",
    "trigger_input_source_read_epoch_us",
    "maximum_required_source_read_epoch_us",
    "partition_boundary_emitted_epoch_us",
    "shared_boundary_emitted_epoch_us",
    "boundary_signal_arrival_epoch_us",
    "watermark_closed_epoch_us",
    "global_boundary_started_epoch_us",
    "result_emitted_epoch_us",
    "serialization_started_epoch_us",
    "serialization_completed_epoch_us",
    "result_kafka_append_epoch_us",
)
TRANSPORT_TSV_TEXT_FIELDS = {
    "query_id",
    "output_id",
    "effective_time",
    "trigger_type",
}
GROUP_SAMPLE_CAPACITY = 50_000
BUCKET_SAMPLE_CAPACITY = 4_096


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def summarize(values: Iterable[float]) -> dict[str, Any]:
    observed = list(values)
    return {
        "count": len(observed),
        "p50_ms": percentile(observed, 0.50),
        "p95_ms": percentile(observed, 0.95),
        "p99_ms": percentile(observed, 0.99),
        "p999_ms": percentile(observed, 0.999),
        "maximum_ms": max(observed) if observed else None,
    }


class ReservoirGroup:
    def __init__(self, metric_names: Iterable[str], capacity: int, label: str) -> None:
        self.metric_names = tuple(sorted(set(metric_names)))
        self.capacity = max(1, capacity)
        seed = int.from_bytes(hashlib.sha256(label.encode("utf-8")).digest()[:8], "big")
        self.random = random.Random(seed)
        self.count = 0
        self.samples = {name: array("q") for name in self.metric_names}
        self.maximum = {name: -(2**63) for name in self.metric_names}

    def update(self, values: dict[str, int]) -> None:
        self.count += 1
        normalized = {name: int(values.get(name, 0)) for name in self.metric_names}
        for name, value in normalized.items():
            if value > self.maximum[name]:
                self.maximum[name] = value
        sample_count = len(self.samples[self.metric_names[0]])
        if sample_count < self.capacity:
            for name in self.metric_names:
                self.samples[name].append(normalized[name])
            return
        position = self.random.randrange(self.count)
        if position >= self.capacity:
            return
        for name in self.metric_names:
            self.samples[name][position] = normalized[name]

    def metric_report(self, name: str) -> dict[str, Any]:
        values = self.samples[name]
        observed = [value / 1000 for value in values]
        return {
            "count": self.count,
            "sample_count": len(values),
            "estimated": self.count > len(values),
            "p50_ms": percentile(observed, 0.50),
            "p95_ms": percentile(observed, 0.95),
            "p99_ms": percentile(observed, 0.99),
            "p999_ms": percentile(observed, 0.999),
            "maximum_ms": self.maximum[name] / 1000 if self.count else None,
        }

    def report(self) -> dict[str, Any]:
        return {
            "total": self.metric_report("total"),
            "segments": {
                name: self.metric_report(name)
                for name in self.metric_names
                if name != "total"
            },
        }


def tsv_record(line: str, line_number: int) -> dict[str, Any]:
    values = line.rstrip("\n").split("\t")
    if len(values) != len(TRANSPORT_TSV_FIELDS):
        raise ValueError(
            f"line {line_number} has {len(values)} TSV fields; "
            f"expected {len(TRANSPORT_TSV_FIELDS)}"
        )
    record: dict[str, Any] = {}
    for name, value in zip(TRANSPORT_TSV_FIELDS, values):
        if name in TRANSPORT_TSV_TEXT_FIELDS:
            record[name] = value
            continue
        try:
            record[name] = int(value)
        except ValueError as error:
            raise ValueError(f"line {line_number} field {name} is not an integer") from error
    return record


def required_integer(record: dict[str, Any], name: str) -> int:
    value = record.get(name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} is missing or is not an integer")
    return value


def classify(record: dict[str, Any]) -> str:
    flags = record.get("trigger_flags")
    if isinstance(flags, int) and not isinstance(flags, bool):
        if flags & 1:
            return "input"
        if flags & 2:
            return "timer"
        return "terminal"
    append_time = record.get("trigger_input_append_epoch_us")
    if isinstance(append_time, int) and not isinstance(append_time, bool) \
            and append_time > 0:
        return "input"
    if record.get("trigger_type") == "event_time_timer":
        return "timer"
    return "terminal"


def latency_segments(record: dict[str, Any], tolerance_us: int) -> dict[str, Any]:
    kind = classify(record)
    if kind == "terminal":
        return {"kind": kind, "segments_us": {}, "total_us": None}
    fields = INPUT_FIELDS if kind == "input" else TIMER_FIELDS
    values = {field: required_integer(record, field) for field in fields}
    if kind == "input":
        segments = {
            "trigger_input_queue": values["trigger_input_source_read_epoch_us"]
            - values["trigger_input_append_epoch_us"],
            "remaining_input_acquisition": values["maximum_required_source_read_epoch_us"]
            - values["trigger_input_source_read_epoch_us"],
            "watermark_wait": values["watermark_closed_epoch_us"]
            - values["boundary_signal_arrival_epoch_us"],
            "post_watermark_coordination": values["global_boundary_started_epoch_us"]
            - values["watermark_closed_epoch_us"],
            "global_computation_and_result_emission": values["result_emitted_epoch_us"]
            - values["global_boundary_started_epoch_us"],
            "result_handoff_to_serializer": values["serialization_started_epoch_us"]
            - values["result_emitted_epoch_us"],
            "serialization": values["serialization_completed_epoch_us"]
            - values["serialization_started_epoch_us"],
            "result_kafka_append": values["result_kafka_append_epoch_us"]
            - values["serialization_completed_epoch_us"],
        }
        coordination_details = {}
        segments.update({
            "partition_boundary_preaggregation":
                values["partition_boundary_emitted_epoch_us"]
                - values["maximum_required_source_read_epoch_us"],
            "shared_boundary_coalescing":
                values["shared_boundary_emitted_epoch_us"]
                - values["partition_boundary_emitted_epoch_us"],
            "shared_boundary_global_ingress":
                values["boundary_signal_arrival_epoch_us"]
                - values["shared_boundary_emitted_epoch_us"],
        })
        total = values["result_kafka_append_epoch_us"] - values["trigger_input_append_epoch_us"]
        first_append = required_integer(record, "boundary_first_input_append_epoch_us")
        ingress_span = values["trigger_input_append_epoch_us"] - first_append
    else:
        coordination_details = {}
        segments = {
            "post_watermark_coordination": values["global_boundary_started_epoch_us"]
            - values["watermark_closed_epoch_us"],
            "global_computation_and_result_emission": values["result_emitted_epoch_us"]
            - values["global_boundary_started_epoch_us"],
            "result_handoff_to_serializer": values["serialization_started_epoch_us"]
            - values["result_emitted_epoch_us"],
            "serialization": values["serialization_completed_epoch_us"]
            - values["serialization_started_epoch_us"],
            "result_kafka_append": values["result_kafka_append_epoch_us"]
            - values["serialization_completed_epoch_us"],
        }
        total = values["result_kafka_append_epoch_us"] - values["watermark_closed_epoch_us"]
        ingress_span = None
    negative = {name: value for name, value in segments.items() if value < 0}
    negative.update({
        name: value for name, value in coordination_details.items() if value < 0
    })
    if negative:
        raise ValueError(f"negative latency segment: {negative}")
    attributed = sum(segments.values())
    if abs(attributed - total) > tolerance_us:
        raise ValueError(
            f"segments do not reconstruct total latency: total={total} attributed={attributed}"
        )
    return {
        "kind": kind,
        "segments_us": segments,
        "total_us": total,
        "boundary_ingress_span_us": ingress_span,
        "coordination_details_us": coordination_details,
    }


def checkpoint_ranges(path: Path | None) -> list[tuple[int, int]]:
    if path is None or not path.exists():
        return []
    document = json.loads(path.read_text(encoding="utf-8"))
    ranges = []
    for checkpoint in document.get("history", []):
        start = checkpoint.get("trigger_timestamp")
        end = checkpoint.get("latest_ack_timestamp")
        if checkpoint.get("status") == "COMPLETED" and start is not None and end is not None:
            ranges.append((int(start) * 1000, int(end) * 1000))
    return sorted(ranges)


def checkpoint_activity(append_us: Any, ranges: list[tuple[int, int]]) -> str:
    if not isinstance(append_us, int) or not ranges:
        return "non_checkpoint"
    index = bisect.bisect_right(ranges, (append_us, 2**63 - 1)) - 1
    return (
        "checkpoint"
        if index >= 0 and ranges[index][0] <= append_us <= ranges[index][1]
        else "non_checkpoint"
    )


@contextmanager
def transport_stream(path: Path):
    if str(path) == "-":
        yield sys.stdin
        return
    if path.suffix != ".zst":
        with path.open(encoding="utf-8") as stream:
            yield stream
        return
    if shutil.which("zstd") is None:
        try:
            from compression import zstd
        except ImportError as error:
            raise ValueError("zstd decompression support is unavailable") from error
        with zstd.open(path, "rt", encoding="utf-8") as stream:
            yield stream
        return
    process = subprocess.Popen(
        ["zstd", "-dc", str(path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    )
    assert process.stdout is not None
    try:
        yield process.stdout
    finally:
        process.stdout.close()
        error = process.stderr.read() if process.stderr is not None else ""
        return_code = process.wait()
        if return_code:
            raise ValueError(f"zstd decompression failed: {error.strip()}")


def analyze_transport(
    path: Path,
    tolerance_us: int,
    checkpoints: Path | None = None,
    *,
    input_format: str = "jsonl",
    expected_records: int | None = None,
    raw_trace_evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    active_checkpoints = checkpoint_ranges(checkpoints)
    groups: dict[tuple[str, str], ReservoirGroup] = {}
    coordination_groups: dict[tuple[str, str], ReservoirGroup] = {}
    coordination_detail_groups: dict[tuple[str, str], ReservoirGroup] = {}
    partition_groups: dict[tuple[str, str, str], ReservoirGroup] = {}
    checkpoint_groups: dict[tuple[str, str, str], ReservoirGroup] = {}
    bucket_groups: dict[tuple[str, str, int], ReservoirGroup] = {}
    expected = max(1, int(expected_records or 1_000_000))
    worst_capacity = max(1, math.ceil(expected * 0.01))
    worst_heap: list[tuple[int, int, dict[str, int]]] = []
    diagnostic_heap: list[tuple[int, int, dict[str, Any]]] = []
    coordination_heap: list[tuple[int, int, dict[str, Any]]] = []
    record_count = 0
    attributed_record_count = 0
    observed_versions: set[int] = set()

    def group_for(
        collection: dict[Any, ReservoirGroup],
        key: Any,
        metrics: dict[str, int],
        capacity: int,
    ) -> ReservoirGroup:
        group = collection.get(key)
        if group is None:
            group = ReservoirGroup(metrics, capacity, repr(key))
            collection[key] = group
        return group

    with transport_stream(path) as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            record = (
                json.loads(line)
                if input_format == "jsonl"
                else tsv_record(line, line_number)
            )
            if record.get("transport_measurement_version") != 3:
                raise ValueError(f"line {line_number} is not Transport version 3")
            observed_versions.add(int(record["transport_measurement_version"]))
            try:
                attribution = latency_segments(record, tolerance_us)
            except ValueError as error:
                raise ValueError(f"line {line_number}: {error}") from error
            record_count += 1
            kind = attribution["kind"]
            if kind == "terminal":
                continue
            attributed_record_count += 1
            query = str(record["query_id"])
            append_us = record.get("result_kafka_append_epoch_us")
            activity = checkpoint_activity(append_us, active_checkpoints)
            metrics = {"total": int(attribution["total_us"])}
            metrics.update(
                {name: int(value) for name, value in attribution["segments_us"].items()}
            )
            group_for(
                groups, (query, kind), metrics, GROUP_SAMPLE_CAPACITY
            ).update(metrics)
            if kind == "input":
                coordination = (
                    metrics.get("source_and_shuffle_coordination", 0)
                    + metrics.get("partition_boundary_preaggregation", 0)
                    + metrics.get("shared_boundary_coalescing", 0)
                    + metrics.get("shared_boundary_global_ingress", 0)
                )
                group_for(
                    coordination_groups,
                    (query, kind),
                    {"total": coordination},
                    GROUP_SAMPLE_CAPACITY,
                ).update({"total": coordination})
                coordination_details = {
                    name: int(value)
                    for name, value in attribution["coordination_details_us"].items()
                }
                if coordination_details:
                    group_for(
                        coordination_detail_groups,
                        (query, kind),
                        coordination_details,
                        GROUP_SAMPLE_CAPACITY,
                    ).update(coordination_details)
            partition = str(record.get("trigger_input_partition", "timer"))
            group_for(
                partition_groups,
                (query, kind, partition),
                metrics,
                GROUP_SAMPLE_CAPACITY,
            ).update(metrics)
            group_for(
                checkpoint_groups,
                (query, kind, activity),
                metrics,
                GROUP_SAMPLE_CAPACITY,
            ).update(metrics)
            bucket = required_integer(record, "result_kafka_append_epoch_us") // 10_000_000 * 10
            group_for(
                bucket_groups,
                (query, kind, bucket),
                metrics,
                BUCKET_SAMPLE_CAPACITY,
            ).update(metrics)

            heap_item = (metrics["total"], attributed_record_count, metrics)
            if len(worst_heap) < worst_capacity:
                heapq.heappush(worst_heap, heap_item)
            elif heap_item[0] > worst_heap[0][0]:
                heapq.heapreplace(worst_heap, heap_item)
            diagnostic = {
                "query_id": query,
                "output_id": record.get("output_id", ""),
                "effective_time": record.get("effective_time", ""),
                "trigger_kind": kind,
                "trigger_input_partition": record.get("trigger_input_partition"),
                "trigger_input_source_line": record.get("trigger_input_source_line"),
                "result_kafka_append_epoch_us": append_us,
                "total_latency_us": metrics["total"],
                "segments_us": attribution["segments_us"],
                "coordination_details_us": attribution["coordination_details_us"],
            }
            diagnostic_item = (metrics["total"], attributed_record_count, diagnostic)
            if len(diagnostic_heap) < 100:
                heapq.heappush(diagnostic_heap, diagnostic_item)
            elif diagnostic_item[0] > diagnostic_heap[0][0]:
                heapq.heapreplace(diagnostic_heap, diagnostic_item)
            coordination = (
                metrics.get("source_and_shuffle_coordination", 0)
                + metrics.get("partition_boundary_preaggregation", 0)
                + metrics.get("shared_boundary_coalescing", 0)
                + metrics.get("shared_boundary_global_ingress", 0)
            )
            coordination_item = (coordination, attributed_record_count, diagnostic)
            if kind == "input":
                if len(coordination_heap) < 100:
                    heapq.heappush(coordination_heap, coordination_item)
                elif coordination_item[0] > coordination_heap[0][0]:
                    heapq.heapreplace(coordination_heap, coordination_item)
    worst_count = max(1, math.ceil(attributed_record_count * 0.01)) \
        if attributed_record_count else 0
    if worst_count > worst_capacity:
        raise ValueError("expected record count is below the observed attribution count")
    worst_items = heapq.nlargest(worst_count, worst_heap)
    worst_metric_names = {
        name for _, _, metrics in worst_items for name in metrics
    } or {"total"}
    worst_group = ReservoirGroup(
        worst_metric_names, max(1, worst_count), "worst-one-percent"
    )
    attributed_worst = 0
    total_worst = 0
    for total, _, metrics in worst_items:
        worst_group.update(metrics)
        total_worst += total
        attributed_worst += sum(
            value for name, value in metrics.items() if name != "total"
        )
    attributed_fraction = attributed_worst / total_worst if total_worst else 1.0
    if attributed_fraction < 0.90:
        raise ValueError("less than 90 percent of worst-one-percent latency is attributed")

    buckets = []
    for (query, kind, bucket), group in bucket_groups.items():
        item = group.report()
        item.update({"query_id": query, "trigger_kind": kind, "bucket_start_epoch_s": bucket})
        buckets.append(item)
    buckets.sort(key=lambda item: item["total"]["p99_ms"] or 0, reverse=True)

    diagnostic_sample = [item[2] for item in heapq.nlargest(100, diagnostic_heap)]
    coordination_sample = [
        item[2] for item in heapq.nlargest(100, coordination_heap)
    ]
    return {
        "schema_version": 3,
        "transport_measurement_versions": sorted(observed_versions),
        "analyzer": {
            "minimum_python_version": "3.9",
            "sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "analyzed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        },
        "raw_trace_evidence": raw_trace_evidence,
        "complete_attribution": True,
        "record_count": record_count,
        "negative_segment_count": 0,
        "reconstruction_tolerance_us": tolerance_us,
        "quantile_method": "deterministic bounded reservoir",
        "group_sample_capacity": GROUP_SAMPLE_CAPACITY,
        "bucket_sample_capacity": BUCKET_SAMPLE_CAPACITY,
        "worst_one_percent_attributed_fraction": attributed_fraction,
        "worst_one_percent": worst_group.report(),
        "timer_and_input_distributions_separate": True,
        "by_query_and_trigger": {
            f"{query}:{kind}": group.report()
            for (query, kind), group in sorted(groups.items())
        },
        "derived_by_query_and_trigger": {
            f"{query}:{kind}": {
                "source_and_shuffle_coordination": group.metric_report("total")
            }
            for (query, kind), group in sorted(coordination_groups.items())
        },
        "coordination_details_by_query_and_trigger": {
            f"{query}:{kind}": {
                name: group.metric_report(name)
                for name in group.metric_names
            }
            for (query, kind), group in sorted(coordination_detail_groups.items())
        },
        "by_query_trigger_and_partition": {
            f"{query}:{kind}:{partition}": group.report()
            for (query, kind, partition), group in sorted(partition_groups.items())
        },
        "by_query_trigger_and_checkpoint_activity": {
            f"{query}:{kind}:{activity}": group.report()
            for (query, kind, activity), group in sorted(checkpoint_groups.items())
        },
        "worst_100_ten_second_buckets": buckets[:100],
        "worst_100_results": diagnostic_sample,
        "worst_100_source_and_shuffle_results": coordination_sample,
    }


def write_json(path: Path | None, report: dict[str, Any]) -> None:
    encoded = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if path is None:
        print(encoded, end="")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(encoded, encoding="utf-8")
    temporary.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser()
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--transport-jsonl", type=Path)
    source.add_argument("--transport-tsv", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--checkpoints", type=Path)
    parser.add_argument("--clock-tolerance-us", type=int, default=2_000)
    parser.add_argument("--expected-records", type=int)
    parser.add_argument("--trace-manifest", type=Path)
    parser.add_argument("--diagnostic-sample", type=Path)
    arguments = parser.parse_args()
    try:
        trace_evidence = (
            json.loads(arguments.trace_manifest.read_text(encoding="utf-8"))
            if arguments.trace_manifest
            else None
        )
        report = analyze_transport(
            arguments.transport_jsonl or arguments.transport_tsv,
            arguments.clock_tolerance_us,
            arguments.checkpoints,
            input_format="tsv" if arguments.transport_tsv else "jsonl",
            expected_records=arguments.expected_records,
            raw_trace_evidence=trace_evidence,
        )
        write_json(arguments.output, report)
        if arguments.diagnostic_sample:
            arguments.diagnostic_sample.parent.mkdir(parents=True, exist_ok=True)
            with arguments.diagnostic_sample.open("w", encoding="utf-8") as stream:
                for item in report.get("worst_100_results", []):
                    stream.write(json.dumps(item, sort_keys=True) + "\n")
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        parser.error(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
