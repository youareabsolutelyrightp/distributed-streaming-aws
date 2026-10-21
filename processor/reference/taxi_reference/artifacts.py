from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import CANONICAL_FORMAT_VERSION, REFERENCE_ENGINE_VERSION
from .canonical import canonical_bytes
from .engine import STREAMS, EngineStats


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(4 * 1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def tree_checksum(root: Path) -> str:
    digest = hashlib.sha256()
    paths = (
        path
        for path in root.rglob("*")
        if path.is_file()
        and path.name != ".DS_Store"
        and "__pycache__" not in path.parts
        and path.suffix not in {".pyc", ".pyo"}
    )
    for path in sorted(paths):
        digest.update(str(path.relative_to(root)).encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def git_commit() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return "uncommitted"
    return result.stdout.strip() if result.returncode == 0 else "uncommitted"


@dataclass(slots=True)
class ChunkState:
    digest: Any = field(default_factory=hashlib.sha256)
    records: int = 0
    last_record: dict[str, object] | None = None


class DigestSink:
    def __init__(self, chunk_size: int) -> None:
        if chunk_size <= 0:
            raise ValueError("chunk size must be positive")
        self.chunk_size = chunk_size
        self.digests = {stream: hashlib.sha256() for stream in STREAMS}
        self.record_counts = {stream: 0 for stream in STREAMS}
        self.chunks: dict[str, dict[int, ChunkState]] = {
            stream: {} for stream in STREAMS
        }

    def add(self, stream: str, record: dict[str, object], anchor_line: int) -> None:
        if stream not in self.digests:
            raise ValueError(f"unknown reference stream: {stream}")
        encoded = canonical_bytes(record)
        self.digests[stream].update(encoded)
        self.record_counts[stream] += 1
        chunk_index = max(anchor_line - 1, 0) // self.chunk_size
        chunk = self.chunks[stream].setdefault(chunk_index, ChunkState())
        chunk.digest.update(encoded)
        chunk.records += 1
        chunk.last_record = record

    def digest_hex(self, stream: str) -> str:
        return self.digests[stream].hexdigest()


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def _write_zstd_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    import zstandard

    with path.open("wb") as raw:
        with zstandard.ZstdCompressor(level=9, threads=1).stream_writer(raw) as writer:
            for row in rows:
                writer.write(canonical_bytes(row))


def bundle_identity(source_root: Path) -> tuple[str, str, str]:
    code_sha = tree_checksum(source_root)
    commit = git_commit()
    identity = commit if commit != "uncommitted" else f"uncommitted-{code_sha[:12]}"
    return identity, commit, code_sha


def write_bundle(
    output_directory: Path,
    sink: DigestSink,
    stats: EngineStats,
    *,
    dataset_manifest: dict[str, Any],
    dataset_manifest_path: Path,
    source_root: Path,
    started_monotonic: float,
) -> dict[str, Any]:
    output_directory.mkdir(parents=True, exist_ok=False)
    for stream in STREAMS:
        (output_directory / f"{stream}.digest").write_text(
            sink.digest_hex(stream) + "\n"
        )

    total_chunks = max(
        1,
        (int(dataset_manifest["prepared_event_count"]) + sink.chunk_size - 1)
        // sink.chunk_size,
    )
    empty_digest = hashlib.sha256().hexdigest()
    chunk_rows: list[dict[str, Any]] = []
    sample_rows: list[dict[str, Any]] = []
    for chunk_index in range(total_chunks):
        first = chunk_index * sink.chunk_size + 1
        last = min(
            (chunk_index + 1) * sink.chunk_size,
            int(dataset_manifest["prepared_event_count"]),
        )
        streams: dict[str, Any] = {}
        samples: dict[str, Any] = {}
        for stream in STREAMS:
            state = sink.chunks[stream].get(chunk_index)
            streams[stream] = {
                "digest": state.digest.hexdigest() if state else empty_digest,
                "records": state.records if state else 0,
            }
            samples[stream] = state.last_record if state else None
        chunk_rows.append(
            {
                "chunk_index": chunk_index,
                "first_source_line": first,
                "last_source_line": last,
                "streams": streams,
            }
        )
        sample_rows.append(
            {
                "chunk_index": chunk_index,
                "last_record_by_stream": samples,
            }
        )

    chunk_path = output_directory / "chunk-digests.jsonl.zst"
    sample_path = output_directory / "sampled-boundaries.jsonl.zst"
    _write_zstd_jsonl(chunk_path, chunk_rows)
    _write_zstd_jsonl(sample_path, sample_rows)

    reference_id, commit, code_sha = bundle_identity(source_root)
    artifact_names = [f"{stream}.digest" for stream in STREAMS] + [
        chunk_path.name,
        sample_path.name,
    ]
    artifacts = {
        name: {"bytes": (output_directory / name).stat().st_size, "sha256": sha256_file(output_directory / name)}
        for name in artifact_names
    }
    manifest = {
        "artifacts": artifacts,
        "canonical_format_version": CANONICAL_FORMAT_VERSION,
        "chunk_size_source_lines": sink.chunk_size,
        "dataset_id": dataset_manifest["dataset_id"],
        "dataset_manifest_path": str(dataset_manifest_path),
        "dataset_manifest_sha256": sha256_file(dataset_manifest_path),
        "generated_at_epoch_seconds": int(time.time()),
        "input_event_count": stats.input_events,
        "maximum_event_time": dataset_manifest["maximum_event_time"],
        "minimum_event_time": dataset_manifest["minimum_event_time"],
        "prepared_checksum": dataset_manifest["prepared_checksum"],
        "prepared_event_count": dataset_manifest["prepared_event_count"],
        "record_counts": sink.record_counts,
        "reference_code_sha256": code_sha,
        "reference_commit": commit,
        "reference_engine_version": REFERENCE_ENGINE_VERSION,
        "reference_id": reference_id,
        "run_duration_seconds": round(time.monotonic() - started_monotonic, 3),
        "terminal_event_time": dataset_manifest["maximum_event_time"],
        "timer_actions": {
            "query1": stats.timer_actions_query1,
            "query2": stats.timer_actions_query2,
        },
    }
    _write_json(output_directory / "manifest.json", manifest)
    return manifest


def verify_bundle(directory: Path) -> dict[str, Any]:
    manifest_path = directory / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    for name, expected in manifest["artifacts"].items():
        path = directory / name
        if not path.is_file():
            raise ValueError(f"reference artifact is missing: {name}")
        if path.stat().st_size != expected["bytes"]:
            raise ValueError(f"reference artifact size differs: {name}")
        if sha256_file(path) != expected["sha256"]:
            raise ValueError(f"reference artifact checksum differs: {name}")
    for stream in STREAMS:
        digest = (directory / f"{stream}.digest").read_text().strip()
        if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise ValueError(f"invalid digest file for {stream}")
    return manifest


def locate_bundle(root: Path) -> Path:
    candidates = sorted(root.glob("**/manifest.json"))
    candidates = [path.parent for path in candidates if "input" not in path.parts]
    if len(candidates) != 1:
        raise ValueError(
            f"expected one reference bundle below {root}, found {len(candidates)}"
        )
    return candidates[0]
