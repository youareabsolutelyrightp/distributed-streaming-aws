from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import argparse
from pathlib import Path
from typing import Any


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def extract_prefix(source: Path, target: Path, records: int) -> dict[str, Any]:
    try:
        import zstandard
    except ImportError as error:
        raise RuntimeError("Prefix extraction must run in the pinned data-tools container.") from error
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".part")
    count = 0
    maximum_event_time = ""
    with source.open("rb") as raw_source, temporary.open("wb") as raw_target:
        with zstandard.ZstdDecompressor().stream_reader(raw_source) as decoded:
            with zstandard.ZstdCompressor(level=3).stream_writer(raw_target, closefd=False) as encoded:
                source_text = io.TextIOWrapper(decoded, encoding="utf-8", newline="")
                target_text = io.TextIOWrapper(encoded, encoding="utf-8", newline="")
                reader = csv.DictReader(source_text)
                if reader.fieldnames is None:
                    raise ValueError("Prepared input has no header.")
                writer = csv.DictWriter(target_text, fieldnames=reader.fieldnames, lineterminator="\n")
                writer.writeheader()
                for row in reader:
                    writer.writerow(row)
                    count += 1
                    maximum_event_time = row["dropoff_datetime"]
                    if count == records:
                        break
                target_text.flush()
                target_text.detach()
    if count != records:
        temporary.unlink(missing_ok=True)
        raise ValueError(f"Prepared input contains {count} rows, expected {records}.")
    os.replace(temporary, target)
    return {
        "version": 1, "records": count, "maximum_event_time": maximum_event_time,
        "sha256": sha256(target),
    }


def write_report(report: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--target", type=Path, required=True)
    parser.add_argument("--records", type=int, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    report = extract_prefix(args.source, args.target, args.records)
    write_report(report, args.report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
