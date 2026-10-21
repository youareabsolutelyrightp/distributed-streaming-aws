from __future__ import annotations

import argparse
import itertools
import json
import os
import sys
import time
from pathlib import Path
from typing import Iterable

from .artifacts import bundle_identity, locate_bundle, verify_bundle, write_bundle
from .engine import ReferenceEngine
from .model import Event, PreparedEventReader, parse_event_time
from .artifacts import DigestSink


def _source_root() -> Path:
    return Path(__file__).resolve().parent


def _progress(events: Iterable[Event], interval: int) -> Iterable[Event]:
    started = time.monotonic()
    for count, event in enumerate(events, 1):
        if count % interval == 0:
            elapsed = max(time.monotonic() - started, 0.001)
            print(
                f"reference progress: {count:,} events read "
                f"({count / elapsed:,.0f} events/s)",
                flush=True,
            )
        yield event


def run_reference(args: argparse.Namespace) -> None:
    manifest_path = Path(args.manifest)
    dataset_manifest = json.loads(manifest_path.read_text())
    prepared_path = (
        Path(args.input)
        if args.input
        else manifest_path.parent / dataset_manifest["prepared_filename"]
    )
    if not prepared_path.is_file():
        raise ValueError(f"prepared input does not exist: {prepared_path}")
    if int(dataset_manifest["prepared_event_count"]) < 0:
        raise ValueError("prepared event count is invalid")

    source_root = _source_root()
    reference_id, _, _ = bundle_identity(source_root)
    output_directory = (
        Path(args.output_root) / dataset_manifest["dataset_id"] / reference_id
    )
    if output_directory.exists():
        raise ValueError(f"reference bundle already exists: {output_directory}")

    chunk_size = int(args.chunk_size)
    sink = DigestSink(chunk_size)
    engine = ReferenceEngine(sink)
    terminal = parse_event_time(dataset_manifest["maximum_event_time"])
    started = time.monotonic()
    reader = PreparedEventReader(prepared_path, dataset_manifest["dataset_id"])
    expected = int(dataset_manifest["prepared_event_count"])
    stats = engine.run(
        _progress(itertools.islice(reader, expected), int(args.progress_interval)),
        terminal,
    )
    if stats.input_events != expected:
        raise ValueError(
            f"reference read {stats.input_events} events; manifest declares {expected}"
        )
    manifest = write_bundle(
        output_directory,
        sink,
        stats,
        dataset_manifest=dataset_manifest,
        dataset_manifest_path=manifest_path,
        source_root=source_root,
        started_monotonic=started,
    )
    print(
        json.dumps(
            {
                "bundle": str(output_directory),
                "dataset_id": manifest["dataset_id"],
                "digests": {
                    stream: sink.digest_hex(stream) for stream in sink.digests
                },
                "prepared_event_count": manifest["prepared_event_count"],
                "record_counts": manifest["record_counts"],
            },
            indent=2,
            sort_keys=True,
        )
    )


def verify(args: argparse.Namespace) -> None:
    directory = locate_bundle(Path(args.root)) if args.root else Path(args.bundle)
    manifest = verify_bundle(directory)
    print(
        json.dumps(
            {
                "bundle": str(directory),
                "dataset_id": manifest["dataset_id"],
                "prepared_event_count": manifest["prepared_event_count"],
                "verified": True,
            },
            indent=2,
            sort_keys=True,
        )
    )


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description="Deterministic taxi reference engine")
    commands = root.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run", help="generate a reference bundle")
    run.add_argument("--manifest", required=True)
    run.add_argument("--input")
    run.add_argument("--output-root", required=True)
    run.add_argument(
        "--chunk-size", default=os.environ.get("REFERENCE_CHUNK_SIZE", "1000000")
    )
    run.add_argument("--progress-interval", default="1000000")
    run.set_defaults(handler=run_reference)

    verify_command = commands.add_parser("verify", help="verify a reference bundle")
    location = verify_command.add_mutually_exclusive_group(required=True)
    location.add_argument("--bundle")
    location.add_argument("--root")
    verify_command.set_defaults(handler=verify)

    return root


def main() -> None:
    args = parser().parse_args()
    try:
        args.handler(args)
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as error:
        print(f"taxi-reference: {error}", file=sys.stderr)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
