from __future__ import annotations

import bisect
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any

from .config import FULL_RECORDS
from .evidence import compact_evidence_valid


def _read(path: Path, default: Any = None) -> Any:
    if not path.is_file():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _metric_series(document: dict[str, Any], suffix: str) -> list[dict[str, Any]]:
    return [
        item for item in document.get("data", {}).get("result", [])
        if str(item.get("metric", {}).get("__name__", "")).endswith(suffix)
    ]


def _summed_history(items: list[dict[str, Any]]) -> tuple[list[float], list[float]]:
    totals: dict[float, float] = defaultdict(float)
    for item in items:
        for timestamp, raw in item.get("values", []):
            try:
                value = float(raw)
            except (TypeError, ValueError):
                continue
            if math.isfinite(value):
                totals[float(timestamp)] += value
    times = sorted(totals)
    return times, [totals[timestamp] for timestamp in times]


def _at_time(times: list[float], values: list[float], timestamp: float) -> float | None:
    right = bisect.bisect_left(times, timestamp)
    if right < len(times) and times[right] == timestamp:
        return values[right]
    if right == 0 or right >= len(times):
        return None
    left = right - 1
    width = times[right] - times[left]
    if width <= 0:
        return values[left]
    fraction = (timestamp - times[left]) / width
    return values[left] + (values[right] - values[left]) * fraction


def _linear(points: list[tuple[float, float]]) -> dict[str, float | int | None]:
    if len(points) < 3:
        return {"samples": len(points), "slope_per_second": None, "lower_95": None, "upper_95": None}
    origin = points[0][0]
    x = [timestamp - origin for timestamp, _ in points]
    y = [value for _, value in points]
    x_mean = statistics.fmean(x)
    y_mean = statistics.fmean(y)
    denominator = sum((value - x_mean) ** 2 for value in x)
    if denominator == 0:
        return {"samples": len(points), "slope_per_second": 0.0, "lower_95": 0.0, "upper_95": 0.0}
    slope = sum((x[index] - x_mean) * (y[index] - y_mean) for index in range(len(points))) / denominator
    residuals = [y[index] - (y_mean + slope * (x[index] - x_mean)) for index in range(len(points))]
    standard_error = math.sqrt(sum(value * value for value in residuals) / (len(points) - 2) / denominator)
    margin = 1.96 * standard_error
    return {
        "samples": len(points), "slope_per_second": slope,
        "lower_95": slope - margin, "upper_95": slope + margin,
    }


def _progress(path: Path, warmup_records: int) -> list[tuple[float, float]]:
    rows: list[tuple[float, float]] = []
    for item in _jsonl(path):
        acknowledged = item.get("phase_acknowledged_source_records")
        timestamp = item.get("timestamp_epoch_ms")
        if isinstance(acknowledged, int) and isinstance(timestamp, int):
            rows.append((timestamp / 1_000, warmup_records + acknowledged))
    return rows


def _backlog(result: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    metrics = _read(result / "metrics/flink-range.json", {"data": {"result": []}})
    accepted = _metric_series(metrics, "taxi_source_partition_partition_accepted_records_total")
    times, values = _summed_history(accepted)
    warmup = int(manifest.get("phases", {}).get("warmup", {}).get("records", 0))
    history: list[tuple[float, float, float]] = []
    for timestamp, acknowledged in _progress(result / "producer-progress.jsonl", warmup):
        observed = _at_time(times, values, timestamp)
        if observed is not None:
            history.append((timestamp, acknowledged, acknowledged - observed))
    if not history:
        return {"available": False}
    midpoint = history[0][0] + (history[-1][0] - history[0][0]) / 2
    final_half = [item for item in history if item[0] >= midpoint]
    backlog_trend = _linear([(timestamp, backlog) for timestamp, _, backlog in final_half])
    accepted_trend = _linear([
        (timestamp, acknowledged - backlog) for timestamp, acknowledged, backlog in final_half
    ])
    return {
        "available": True,
        "accepted_rate_per_second": accepted_trend["slope_per_second"],
        "final_half_backlog": backlog_trend,
        "final_accepted_backlog": max(0.0, history[-1][2]),
        "samples": len(history),
    }


def _latest_sum(items: list[dict[str, Any]]) -> float | None:
    latest: list[float] = []
    for item in items:
        valid = []
        for timestamp, raw in item.get("values", []):
            try:
                value = float(raw)
            except (TypeError, ValueError):
                continue
            if math.isfinite(value):
                valid.append((float(timestamp), value))
        if valid:
            latest.append(max(valid, key=lambda sample: sample[0])[1])
    return sum(latest) if latest else None


def _query_latency(attribution: dict[str, Any], query: str) -> dict[str, Any]:
    entry = attribution.get("by_query_and_trigger", {}).get(f"{query}:input", {})
    return entry.get("total", {}) if isinstance(entry, dict) else {}


def _checkpoint_summary(result: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    checkpoints = _read(result / "metrics/flink-checkpoints.json", {})
    classification = _read(result / "metrics/checkpoint-failure-classification.json", {})
    phase = manifest.get("phases", {}).get("measurement", {})
    start = float(phase.get("start_epoch_seconds", 0)) * 1_000
    end = float(phase.get("end_epoch_seconds", 0)) * 1_000
    retained_completed = sorted(
        (
            item for item in checkpoints.get("history", [])
            if item.get("status") == "COMPLETED"
            and start <= float(item.get("trigger_timestamp", -1)) <= end
        ),
        key=lambda item: int(item.get("id", -1)),
    )
    all_completed = sorted(
        (item for item in checkpoints.get("history", []) if item.get("status") == "COMPLETED"),
        key=lambda item: int(item.get("id", -1)),
    )
    metrics = _read(result / "metrics/flink-range.json", {"data": {"result": []}})
    completed_series = _metric_series(metrics, "numberOfCompletedCheckpoints")
    completed_count = len(retained_completed)
    if completed_series:
        times, values = _summed_history(completed_series)
        start_value = _at_time(times, values, start / 1_000)
        end_value = _at_time(times, values, end / 1_000)
        if start_value is not None and end_value is not None:
            completed_count = max(completed_count, int(end_value - start_value))
    sizes = [int(item.get("checkpointed_size", item.get("state_size", 0)) or 0) for item in all_completed[-5:]]
    positive_growth = [max(0, current - previous) for previous, current in zip(sizes, sizes[1:])]
    return {
        "measurement_completed": completed_count,
        "total_completed": int(checkpoints.get("counts", {}).get("completed", len(all_completed))),
        "failed": int(classification.get("measurement_failures", checkpoints.get("counts", {}).get("failed", 0))),
        "restored": int(checkpoints.get("counts", {}).get("restored", 0)),
        "last_five_state_sizes_bytes": sizes,
        "maximum_state_size_bytes": max(sizes, default=0),
        "maximum_positive_growth_bytes": max(positive_growth, default=0),
    }


def _query_correctness(correctness: dict[str, Any], query: str) -> dict[str, Any]:
    value = correctness.get(query, {})
    return {
        name: value.get(name)
        for name in (
            "expected_records", "observed_records", "raw_output_records",
            "duplicate_outputs", "extra_unique_outputs", "expected_digest",
            "observed_digest", "canonical_match", "output_ids_verified",
            "terminal_result_seen", "producer_complete_seen", "late_events",
            "late_correctness_failures", "post_terminal_outputs",
        )
        if name in value
    }


def summarize(result: Path, cleanup: dict[str, Any] | None = None) -> dict[str, Any]:
    status = _read(result / "run-status.json", {})
    profile = _read(result / "run-profile.json", {})
    manifest = _read(result / "run-manifest.json", {})
    correctness = _read(result / "correctness-report.json", {})
    latency = _read(result / "latency-histogram.json", {})
    attribution = _read(result / "latency-attribution.json", {})
    cost = _read(result / "cost-report.json", {})
    producer = _read(result / "producer-summary.json", {})
    warmup = _read(result / "producer-summary-warmup.json", {})
    sink = _read(result / "sink-summary.json", {})
    job = _read(result / "metrics/flink-job.json", {})
    full = bool(
        profile.get("run_size") == "full"
        or manifest.get("run_kind") == "full"
        or int(manifest.get("verified_event_count_N", 0)) == FULL_RECORDS
    )
    backlog = _backlog(result, manifest)
    checkpoints = _checkpoint_summary(result, manifest)
    metrics = _read(result / "metrics/flink-range.json", {"data": {"result": []}})
    final_buffers = _latest_sum(_metric_series(metrics, "taxi_state_stage_buffered_event_count"))
    q1 = _query_latency(attribution, "query1")
    q2 = _query_latency(attribution, "query2")
    q1_segments = attribution.get("by_query_and_trigger", {}).get("query1:input", {}).get("segments", {})
    q2_segments = attribution.get("by_query_and_trigger", {}).get("query2:input", {}).get("segments", {})
    database = latency.get("timescaledb_materialization_latency_ms", {})
    accepted = backlog.get("accepted_rate_per_second")
    final_backlog = backlog.get("final_accepted_backlog")
    backlog_slope = backlog.get("final_half_backlog", {}).get("slope_per_second")
    total_cost = cost.get("total_usd", cost.get("estimated_runtime_cost_usd"))
    verified_records = int(manifest.get("verified_event_count_N", profile.get("dataset", {}).get("records", 0)) or 0)
    completion = correctness.get("completion", {})
    cleanup_value = cleanup or {}
    remaining_resources = {
        name: cleanup_value.get(name, [])
        for name in (
            "remaining_instances", "remaining_volumes", "remaining_addresses",
            "remaining_nat_gateways", "remaining_load_balancers",
        )
    }
    return {
        "version": 2,
        "run_id": status.get("run_id", manifest.get("run_id", result.name)),
        "run_size": "full" if full else "small",
        "workflow": {
            "runtime_complete": status.get("runtime_complete", status.get("operational_complete")) is True,
            "evidence_bundle_complete": compact_evidence_valid(result),
            "cost_recorded": total_cost is not None,
            "cleanup_complete": cleanup_value.get("passed") is True,
            "remaining_resources": remaining_resources,
        },
        "workload": {
            "verified_records": verified_records,
            "warmup_records": manifest.get("phases", {}).get("warmup", {}).get("records"),
            "measurement_records": manifest.get("phases", {}).get("measurement", {}).get("records"),
            "requested_measurement_rate_per_second": producer.get("requested_rate_per_second"),
            "producer_acknowledged_rate_per_second": producer.get("source_records_per_second"),
            "accepted_rate_per_second": accepted,
            "final_half_backlog_slope_per_second": backlog_slope,
            "final_accepted_backlog": final_backlog,
            "warmup_acknowledged_records": warmup.get("data_acknowledged"),
            "measurement_acknowledged_records": producer.get("data_acknowledged"),
        },
        "latency_ms": {
            "query1_input_triggered": q1,
            "query2_input_triggered": q2,
            "query1_input_queue": q1_segments.get("trigger_input_queue", {}),
            "query2_input_queue": q2_segments.get("trigger_input_queue", {}),
            "timescaledb_materialization": database,
        },
        "correctness": {
            "query1": _query_correctness(correctness, "query1"),
            "query2": _query_correctness(correctness, "query2"),
            "completion": {
                name: completion.get(name)
                for name in (
                    "producer_complete_seen", "producer_eos_acknowledged",
                    "query_complete_seen", "duplicate_completion_controls",
                    "query1_terminal_result_seen", "query2_terminal_result_seen",
                    "late_correctness_failures", "zero_result_and_control_lag",
                    "quiet_period_observed", "errors",
                )
                if name in completion
            },
            "materialized_rows": sink,
        },
        "runtime": {
            "job_state": job.get("state"),
            "final_buffers": final_buffers,
            "checkpoints": checkpoints,
        },
        "cost": {
            "currency": cost.get("currency", "USD"),
            "total": total_cost,
            "per_million_verified_records": (
                float(total_cost) * 1_000_000 / verified_records
                if total_cost is not None and verified_records > 0 else None
            ),
            "measurement": cost.get("measurement"),
        },
    }
