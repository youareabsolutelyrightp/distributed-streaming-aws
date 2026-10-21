#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def line_count(path: Path) -> int:
    count = 0
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            count += chunk.count(b"\n")
    return count


def metadata_value(head: dict[str, Any], name: str) -> str | None:
    metadata = head.get("Metadata") or {}
    return metadata.get(name) or metadata.get(name.lower())


def build(arguments: argparse.Namespace) -> dict[str, Any]:
    raw = arguments.raw.resolve()
    compressed = arguments.compressed.resolve()
    if not raw.is_file() or not compressed.is_file():
        raise ValueError("Raw and compressed Transport trace files are required.")
    observed = line_count(raw)
    if observed != arguments.expected_rows:
        raise ValueError(
            f"Transport trace has {observed} rows, expected {arguments.expected_rows}."
        )
    raw_digest = sha256(raw)
    compressed_digest = sha256(compressed)
    compressed_size = compressed.stat().st_size
    head = json.loads(arguments.s3_head.read_text(encoding="utf-8"))
    if int(head.get("ContentLength", -1)) != compressed_size:
        raise ValueError("S3 Transport trace size does not match the local file.")
    if metadata_value(head, "sha256") != compressed_digest:
        raise ValueError("S3 Transport trace SHA-256 metadata does not match.")
    if metadata_value(head, "uncompressed-sha256") != raw_digest:
        raise ValueError("S3 raw Transport trace SHA-256 metadata does not match.")
    if metadata_value(head, "row-count") != str(observed):
        raise ValueError("S3 Transport trace row-count metadata does not match.")
    return {
        "schema_version": 1,
        "run_id": arguments.run_id,
        "expected_row_count": arguments.expected_rows,
        "observed_row_count": observed,
        "uncompressed_sha256": raw_digest,
        "compressed_sha256": compressed_digest,
        "compressed_size_bytes": compressed_size,
        "analyzer_sha256": sha256(arguments.analyzer.resolve()),
        "analyzer_source_revision": arguments.source_revision,
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "s3_key": arguments.s3_key,
        "s3_verified": True,
        "analysis_passed": False,
        "diagnostic_sample_rows": 0,
    }


def write(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--compressed", type=Path, required=True)
    parser.add_argument("--expected-rows", type=int, required=True)
    parser.add_argument("--analyzer", type=Path, required=True)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--s3-key", required=True)
    parser.add_argument("--s3-head", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    arguments = parser.parse_args()
    report = build(arguments)
    write(arguments.output, report)
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
