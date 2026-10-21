from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
RELEASE_PATH = ROOT / "config" / "release.json"
FULL_REFERENCE = ROOT / "config" / "reference-full.json"
SMALL_REFERENCE = ROOT / "config" / "reference-small.json"

FULL_RECORDS = 173_176_321
FULL_WARMUP = 3_200_000
SMALL_RECORDS = 10_000_000
SMALL_WARMUP = 1_600_000
WARMUP_RATE = 20_000
FULL_SAFEGUARD_HOURS = 6
SMALL_SAFEGUARD_HOURS = 2

RUN_ID = re.compile(r"^[a-z0-9][a-z0-9-]{2,31}$")


@dataclass(frozen=True)
class RunSize:
    name: str
    records: int
    warmup_records: int
    reference: Path
    safeguard_hours: int

    @property
    def measurement_records(self) -> int:
        return self.records - self.warmup_records


FULL = RunSize("full", FULL_RECORDS, FULL_WARMUP, FULL_REFERENCE, FULL_SAFEGUARD_HOURS)
SMALL = RunSize("small", SMALL_RECORDS, SMALL_WARMUP, SMALL_REFERENCE, SMALL_SAFEGUARD_HOURS)


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected one JSON object: {path}")
    return value


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def release() -> dict[str, Any]:
    value = read_json(RELEASE_PATH)
    required_sections = (
        "stream", "flink", "recovery", "topology", "runtime", "kafka",
        "materializer", "database", "delivery",
    )
    missing = [name for name in required_sections if not isinstance(value.get(name), dict)]
    if missing:
        raise ValueError(f"Release configuration is missing sections: {', '.join(missing)}")
    stream = value["stream"]
    runtime = value["runtime"]
    if stream.get("shared_boundary_mode") != "coalesced" or stream.get("transport_version") != 3:
        raise ValueError("This repository supports only compact-coalesced Transport v3.")
    if runtime.get("transport_mode") != "full":
        raise ValueError("The AWS benchmark requires full Transport measurement.")
    if value["flink"].get("execution.buffer-timeout.enabled") is not True:
        raise ValueError("The frozen release requires Flink output-buffer timeouts.")
    if value["flink"].get("execution.buffer-timeout.interval") != "5 ms":
        raise ValueError("The frozen release requires a 5 ms Flink output-buffer timeout.")
    if value.get("delivery", {}).get("canonical_results") != "at-least-once-kafka":
        raise ValueError("The result-delivery contract changed.")
    if value.get("delivery", {}).get("serving_copy") != "timescaledb":
        raise ValueError("The serving-copy contract changed.")
    return value


def versions() -> dict[str, str]:
    values: dict[str, str] = {}
    for line in (ROOT / "versions.env").read_text(encoding="utf-8").splitlines():
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key] = value
    return values


def _profile_sections(selected: dict[str, Any], size: RunSize) -> dict[str, Any]:
    stream = dict(selected["stream"])
    recovery = selected["recovery"]
    topology = selected["topology"]
    runtime = selected["runtime"]
    stream["warmup_maximum_watermark_lag_ms"] = stream["watermark_alignment_max_drift_ms"]
    return {
        "region": selected["region"],
        "flink": dict(selected["flink"]),
        "stages": [
            {
                "name": "warmup", "role": "warmup", "records": size.warmup_records,
                "rate_per_second": WARMUP_RATE,
            },
            {
                "name": "measurement", "role": "measurement", "records": size.measurement_records,
                "rate_per_second": runtime["target_rate_per_second"],
            },
        ],
        "stream": stream,
        "checkpoint": {
            "interval_ms": recovery["checkpoint_interval_ms"],
            "timeout_ms": 300_000,
            "max_concurrent": 1,
            "tolerable_failures": 0,
        },
        "restart": {
            "delay_ms": recovery["restart_delay_ms"],
            "heartbeat_interval_ms": recovery["heartbeat_interval_ms"],
            "heartbeat_timeout_ms": recovery["heartbeat_timeout_ms"],
            "rpc_failure_threshold": recovery["rpc_failure_threshold"],
            "attempts": recovery["restart_attempts"],
        },
        "infrastructure": {
            "expected_taskmanagers": topology["taskmanagers"],
            "taskmanager_instance_type": topology["taskmanager_instance_type"],
            "total_vcpus": topology["total_vcpus"],
            "task_slots_per_taskmanager": topology["task_slots_per_taskmanager"],
            "expected_allocated_slots": topology["expected_allocated_slots"],
            "input_partitions": topology["input_partitions"],
            "source_parallelism": topology["source_parallelism"],
            "processing_parallelism": topology["processing_parallelism"],
            "ranking_virtual_shards": topology["ranking_virtual_shards"],
            "ranking_max_parallelism": topology["ranking_max_parallelism"],
        },
        "timeouts": {"ssm_execution_timeout_seconds": size.safeguard_hours * 3_600},
        "safeguard_hours": size.safeguard_hours,
        "actions": {
            "measurement_outcome": "complete_export_cleanup",
            "operational_or_safety_failure": "stop_export_cleanup",
        },
    }


def validate_dataset(manifest_path: Path, dataset_path: Path, size: RunSize) -> dict[str, Any]:
    manifest = read_json(manifest_path)
    if manifest.get("prepared_event_count") != FULL_RECORDS:
        raise ValueError("The prepared manifest must describe exactly 173,176,321 records.")
    expected = manifest.get("prepared_checksum")
    if not isinstance(expected, str) or not re.fullmatch(r"[0-9a-f]{64}", expected):
        raise ValueError("The prepared manifest has no valid SHA-256 checksum.")
    if not dataset_path.is_file():
        raise ValueError(f"Prepared dataset is missing: {dataset_path}")
    if sha256_file(dataset_path) != expected:
        raise ValueError("The complete prepared dataset checksum does not match its manifest.")
    reference = read_json(size.reference)
    if reference.get("input_event_count") != size.records:
        raise ValueError(f"Frozen {size.name} reference has the wrong event count.")
    if reference.get("dataset_id") != manifest.get("dataset_id"):
        raise ValueError("Dataset and reference identities do not match.")
    return manifest


def build_profile(
    *, run_id: str, size: RunSize, dataset_path: Path, manifest_path: Path,
    dataset_sha256: str | None = None,
) -> dict[str, Any]:
    if not RUN_ID.fullmatch(run_id):
        raise ValueError("Run ID must contain 3 to 32 lowercase letters, numbers, or hyphens.")
    selected = release()
    frozen_sections = _profile_sections(selected, size)
    manifest = read_json(manifest_path)
    reference = read_json(size.reference)
    checksum = dataset_sha256 or sha256_file(dataset_path)
    if size is FULL and checksum != manifest.get("prepared_checksum"):
        raise ValueError("The complete prepared dataset checksum does not match its manifest.")
    profile: dict[str, Any] = {
        "version": 1,
        "run_id": run_id,
        "run_size": size.name,
        "region": frozen_sections["region"],
        "dataset": {
            "path": str(dataset_path.resolve()),
            "manifest_path": str(manifest_path.resolve()),
            "dataset_id": manifest["dataset_id"],
            "sha256": checksum,
            "records": size.records,
            "maximum_event_time": reference["maximum_event_time"],
            "reference_path": str(size.reference.resolve()),
            "reference_sha256": sha256_file(size.reference),
        },
        "stages": frozen_sections["stages"],
        "stream": frozen_sections["stream"],
        "flink": frozen_sections["flink"],
        "checkpoint": frozen_sections["checkpoint"],
        "restart": frozen_sections["restart"],
        "infrastructure": frozen_sections["infrastructure"],
        "timeouts": frozen_sections["timeouts"],
        "safeguard_hours": frozen_sections["safeguard_hours"],
        "release_configuration_sha256": sha256_file(RELEASE_PATH),
        "actions": frozen_sections["actions"],
    }
    unsigned = json.dumps(profile, sort_keys=True, separators=(",", ":")).encode()
    profile["profile_sha256"] = hashlib.sha256(unsigned).hexdigest()
    return profile


def validate_profile(profile: dict[str, Any]) -> RunSize:
    size = FULL if profile.get("run_size") == "full" else SMALL if profile.get("run_size") == "small" else None
    if size is None:
        raise ValueError("Run profile must select full or small.")
    unsigned = dict(profile)
    observed_digest = unsigned.pop("profile_sha256", None)
    expected_digest = hashlib.sha256(
        json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    if observed_digest != expected_digest:
        raise ValueError("Run profile checksum does not match its content.")
    selected = release()
    if profile.get("release_configuration_sha256") != sha256_file(RELEASE_PATH):
        raise ValueError("Run profile does not identify the frozen release configuration.")
    if profile.get("dataset", {}).get("records") != size.records:
        raise ValueError("Run profile has an invalid dataset record count.")
    for name, expected in _profile_sections(selected, size).items():
        if profile.get(name) != expected:
            raise ValueError(f"Run {name} differs from the frozen benchmark definition.")
    return size
