#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def read(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected one JSON object: {path}")
    return value


def stage(profile: dict[str, Any], role: str) -> dict[str, Any]:
    matches = [item for item in profile["stages"] if item.get("role") == role]
    if len(matches) != 1:
        raise ValueError(f"run profile must contain one {role} stage")
    return matches[0]


def phase_epoch(runtime: dict[str, Any], phase: str, field: str) -> int:
    value = runtime.get("phases", {}).get(phase, {}).get(field)
    if not isinstance(value, int) or value <= 0:
        raise ValueError(f"runtime manifest has no valid {phase}.{field}")
    return value


def build(
    result: Path,
    release_path: Path,
    data_manifest_path: Path,
    deployment_profile: str,
    artifact_bucket: str,
    completion_epoch: int,
) -> dict[str, Any]:
    profile = read(result / "run-profile.json")
    runtime = read(result / "runtime-manifest.json")
    release = read(release_path)
    data_manifest = read(data_manifest_path)
    warmup_summary = read(result / "producer-summary-warmup.json")
    measurement_summary = read(result / "producer-summary.json")
    warmup = stage(profile, "warmup")
    measurement = stage(profile, "measurement")
    run_id = profile["run_id"]
    topic_prefix = f"taxi.{run_id}"
    topology = release["topology"]
    stream = release["stream"]
    flink = release["flink"]
    recovery = release["recovery"]
    materializer = release["materializer"]
    processing = topology["processing_parallelism"]
    source = topology["source_parallelism"]
    warmup_phase = _producer_phase(warmup, warmup_summary)
    warmup_phase["start_epoch_seconds"] = phase_epoch(
        runtime, "warmup", "start_epoch_seconds"
    )
    warmup_phase["end_epoch_seconds"] = phase_epoch(
        runtime, "warmup", "end_epoch_seconds"
    )

    return {
        "version": 3,
        "run_id": run_id,
        "run_kind": profile["run_size"],
        "environment": "aws",
        "deployment_profile": deployment_profile,
        "dataset_id": data_manifest["dataset_id"],
        "verified_event_count_N": profile["dataset"]["records"],
        "replay_source_records": profile["dataset"]["records"],
        "requested_rate_per_second": measurement["rate_per_second"],
        "start_epoch_seconds": phase_epoch(runtime, "warmup", "start_epoch_seconds"),
        "producer_end_epoch_seconds": int(
            measurement_summary["last_acknowledged_epoch_us"] // 1_000_000
        ),
        "completion_epoch_seconds": completion_epoch,
        "topics": {
            "input": f"{topic_prefix}.trips",
            "control": f"{topic_prefix}.control",
            "dlq": f"{topic_prefix}.dlq",
            "query1": f"{topic_prefix}.top-routes",
            "query2": f"{topic_prefix}.profitable-areas",
        },
        "input_partitioning": {
            "version": "taxi-medallion-partition-v1",
            "hash_test_vector_version": "sha256-prefix64-be-v1",
            "partition_count": topology["input_partitions"],
        },
        "consumer_groups": {
            "flink": f"{run_id}-flink",
            "materializer": f"{run_id}-materializer",
        },
        "checkpoint_uri": f"s3://{artifact_bucket}/checkpoints/{run_id}",
        "parallelism": {
            "source": source,
            "route": processing,
            "ranking": processing,
            "taxi": processing,
            "cell": processing,
            "boundary": processing,
        },
        "late_event_mode": "benchmark",
        "max_out_of_orderness_ms": stream["maximum_out_of_orderness_ms"],
        "checkpoint_interval_ms": recovery["checkpoint_interval_ms"],
        "checkpoint_timeout_ms": profile["checkpoint"]["timeout_ms"],
        "phases": {
            "deployment": {
                "end_epoch_seconds": phase_epoch(
                    runtime, "deployment", "end_epoch_seconds"
                )
            },
            "readiness": {
                "end_epoch_seconds": phase_epoch(
                    runtime, "readiness", "end_epoch_seconds"
                )
            },
            "warmup": warmup_phase,
            "warmup_barrier": {
                "end_epoch_seconds": phase_epoch(
                    runtime, "warmup_barrier", "end_epoch_seconds"
                )
            },
            "measurement": _producer_phase(measurement, measurement_summary),
            "drain": {"end_epoch_seconds": completion_epoch},
            "evidence_export": {"end_epoch_seconds": completion_epoch},
            "cleanup": {"completed": False},
        },
        "result_definitions": {
            "cold_all_run": "startup, warm-up, measurement, drain, and cost",
            "steady_state": "measurement after the drained warm-up barrier",
        },
        "tolerable_checkpoint_failures": profile["checkpoint"]["tolerable_failures"],
        "state_backend": release["runtime"]["state_backend"],
        "flink_configuration": {
            "execution.buffer-timeout.enabled": flink[
                "execution.buffer-timeout.enabled"
            ],
            "execution.buffer-timeout.interval": flink[
                "execution.buffer-timeout.interval"
            ],
        },
        "taskmanager_process_memory": release["runtime"]["taskmanager_process_memory"],
        "task_slots_per_taskmanager": topology["task_slots_per_taskmanager"],
        "query1_ranking_shards": topology["ranking_virtual_shards"],
        "query2_ranking_shards": topology["ranking_virtual_shards"],
        "ranking_max_parallelism": topology["ranking_max_parallelism"],
        "boundary_mode": stream["shared_boundary_mode"],
        "watermark_alignment_max_drift_ms": stream[
            "watermark_alignment_max_drift_ms"
        ],
        "watermark_alignment_update_ms": stream["watermark_alignment_update_ms"],
        "source_idle_timeout_ms": stream["source_idle_timeout_ms"],
        "materializer": {
            "batch_size": materializer["batch_size"],
            "flush_interval": materializer["flush_interval"],
            "retry_attempts": materializer["retry_attempts"],
            "retry_backoff": materializer["retry_backoff"],
        },
        "dashboard_active": False,
        "grafana_active": False,
        "airflow_active": False,
        "prometheus_active": True,
    }


def _producer_phase(stage_value: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "records": stage_value["records"],
        "requested_rate_per_second": stage_value["rate_per_second"],
        "start_epoch_seconds": int(summary["first_acknowledged_epoch_us"] // 1_000_000),
        "end_epoch_seconds": int(summary["last_acknowledged_epoch_us"] // 1_000_000),
        "first_acknowledged_epoch_us": summary["first_acknowledged_epoch_us"],
        "last_acknowledged_epoch_us": summary["last_acknowledged_epoch_us"],
        "first_source_line": summary["first_source_line"],
        "last_source_line": summary["last_source_line"],
        "first_event_time": summary["first_event_time"],
        "last_event_time": summary["last_event_time"],
        "partition_totals": summary["topic_partitions"],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--result-dir", type=Path, required=True)
    parser.add_argument("--release-config", type=Path, required=True)
    parser.add_argument("--data-manifest", type=Path, required=True)
    parser.add_argument("--deployment-profile", required=True)
    parser.add_argument("--artifact-bucket", required=True)
    parser.add_argument("--completion-epoch", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    value = build(
        arguments.result_dir,
        arguments.release_config,
        arguments.data_manifest,
        arguments.deployment_profile,
        arguments.artifact_bucket,
        arguments.completion_epoch,
    )
    arguments.output.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
