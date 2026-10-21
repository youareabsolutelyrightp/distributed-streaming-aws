from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .config import sha256_file
from .lifecycle import AwsContext


REQUIRED = (
    "runtime-manifest.json", "system-manifest.json", "run-status.json",
    "correctness-report.json", "latency-histogram.json", "latency-attribution.json",
    "raw-transport-trace-manifest.json", "remote-evidence-manifest.json",
    "compact-evidence-manifest.json", "cost-report.json",
)


def download(context: AwsContext, bucket: str, run_id: str) -> Path:
    target = context.run_root / "results"
    target.mkdir(parents=True, exist_ok=True)
    context.commands.run(
        context.aws("s3", "sync", f"s3://{bucket}/results/{run_id}/", str(target), "--only-show-errors"),
        cwd=context.run_root.parent.parent,
    )
    missing = [name for name in REQUIRED if not (target / name).is_file()]
    if missing:
        protect_raw_trace(context, bucket, target)
        raise ValueError(f"Evidence bundle is incomplete: {', '.join(missing)}")
    validate_raw_trace_manifest(target / "raw-transport-trace-manifest.json")
    return target


def validate_raw_trace_manifest(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    for name in ("compressed_sha256", "uncompressed_sha256"):
        observed = value.get(name)
        if not isinstance(observed, str) or len(observed) != 64:
            raise ValueError(f"Raw trace manifest has no valid {name}.")
    if int(value.get("observed_row_count", value.get("observed_rows", value.get("row_count", 0)))) <= 0:
        raise ValueError("Raw trace manifest has no positive row count.")
    return value


def protect_raw_trace(context: AwsContext, bucket: str, result: Path) -> None:
    manifest_path = result / "raw-transport-trace-manifest.json"
    if not manifest_path.is_file():
        return
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    key = manifest.get("s3_key")
    checksum = manifest.get("compressed_sha256")
    if not key or not checksum:
        return
    target = result / "temporary" / Path(key).name
    target.parent.mkdir(parents=True, exist_ok=True)
    context.commands.run(
        context.aws("s3", "cp", f"s3://{bucket}/{key}", str(target), "--only-show-errors"),
        cwd=context.run_root.parent.parent,
    )
    if sha256_file(target) != checksum:
        raise ValueError("Downloaded raw Transport trace checksum does not match.")


def compact_evidence_valid(result: Path) -> bool:
    if any(not (result / name).is_file() for name in REQUIRED):
        return False
    trace = validate_raw_trace_manifest(result / "raw-transport-trace-manifest.json")
    remote = json.loads((result / "remote-evidence-manifest.json").read_text(encoding="utf-8"))
    compact = json.loads((result / "compact-evidence-manifest.json").read_text(encoding="utf-8"))
    expected = remote.get("expected_nodes", [])
    nodes = result / "metrics" / "node"
    node_evidence = len(expected) == 9 and all(
        (nodes / name / "node-metrics.jsonl").is_file() for name in expected
    )
    return bool(
        trace.get("analysis_passed")
        and remote.get("evidence_complete") is True
        and compact.get("validated") is True
        and node_evidence
        and (result / "transport-diagnostic-sample.jsonl.zst").is_file()
    )
