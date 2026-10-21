from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import os
import platform
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


MONTHS = tuple(range(1, 13))
PROFILE = "reconstructed-foil"
JOIN_POLICY = (
    "trimmed medallion,hack_license,vendor_id,pickup_datetime plus independent "
    "one-based occurrence order within each repeated key"
)
SORT_POLICY = (
    "dropoff_datetime,month_id,trip_original_row_number,fare_original_row_number"
)
JOINED_ID_FORMAT = (
    "month_id={month:02d}|trip_sha256={trip_sha}|trip_row={trip_row}|"
    "fare_sha256={fare_sha}|fare_row={fare_row}"
)
MONTHLY_OUTPUT_VERSION = 3


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def sha256_file(path: Path, block_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(block_size):
            digest.update(block)
    return digest.hexdigest()


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def download_file(url: str, destination: Path) -> dict[str, Any]:
    destination.parent.mkdir(parents=True, exist_ok=True)
    manifest_path = destination.with_suffix(destination.suffix + ".manifest.json")
    if destination.is_file():
        if manifest_path.is_file():
            existing = load_json(manifest_path)
            if (
                existing.get("url") == url
                and int(existing.get("bytes", -1)) == destination.stat().st_size
                and existing.get("sha256")
            ):
                print(
                    f"Reusing {destination.name}: {existing['bytes']:,} bytes, "
                    f"sha256={existing['sha256']}"
                )
                return existing
        existing = {
            "url": url,
            "filename": destination.name,
            "bytes": destination.stat().st_size,
            "sha256": sha256_file(destination),
        }
        write_json(manifest_path, existing)
        print(
            f"Recorded existing {destination.name}: {existing['bytes']:,} bytes, "
            f"sha256={existing['sha256']}"
        )
        return existing
    partial = destination.with_suffix(destination.suffix + ".part")
    offset = partial.stat().st_size if partial.exists() else 0
    headers = {"User-Agent": "reconstructed-foil/1"}
    if offset:
        headers["Range"] = f"bytes={offset}-"
    print(f"Downloading {url} -> {destination} (resume offset {offset:,})", flush=True)
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=120) as response:
        status = getattr(response, "status", 200)
        if offset and status != 206:
            print("Server did not honor Range; restarting this archive download", flush=True)
            offset = 0
        mode = "ab" if offset else "wb"
        content_range = response.headers.get("Content-Range", "")
        if "/" in content_range:
            expected_bytes = int(content_range.rsplit("/", 1)[1])
        else:
            expected_bytes = offset + int(response.headers.get("Content-Length", "0"))
        if expected_bytes:
            print(f"  expected size: {expected_bytes / (1024**3):.2f} GiB", flush=True)
        written = offset
        last_notice = written
        with partial.open(mode) as output:
            while chunk := response.read(8 * 1024 * 1024):
                output.write(chunk)
                written += len(chunk)
                if written - last_notice >= 256 * 1024 * 1024:
                    print(f"  {destination.name}: {written / (1024**3):.2f} GiB", flush=True)
                    last_notice = written
    partial.replace(destination)
    result = {
        "url": url,
        "filename": destination.name,
        "bytes": destination.stat().st_size,
        "sha256": sha256_file(destination),
    }
    write_json(manifest_path, result)
    print(f"Downloaded {destination.name}: {result['bytes']:,} bytes, sha256={result['sha256']}")
    return result


def command_download(args: argparse.Namespace) -> None:
    raw_dir = Path(args.raw_dir)
    downloads = (
        (args.trip_url, raw_dir / "trip_data.7z"),
        (args.fare_url, raw_dir / "trip_fare.7z"),
    )
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(download_file, *download) for download in downloads]
        for future in concurrent.futures.as_completed(futures):
            future.result()


def run_checked(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=True, text=True, **kwargs)


def archive_members(archive: Path) -> dict[str, str]:
    result = run_checked(["7z", "l", "-slt", str(archive)], capture_output=True)
    members: dict[str, str] = {}
    for line in result.stdout.splitlines():
        if not line.startswith("Path = "):
            continue
        member = line.removeprefix("Path = ").strip()
        base = Path(member).name.lower()
        if base.endswith(".csv"):
            members[base] = member
    return members


def ensure_archive_integrity(archive: Path) -> None:
    sidecar = archive.with_suffix(archive.suffix + ".manifest.json")
    metadata = load_json(sidecar) if sidecar.is_file() else {}
    if (
        metadata.get("integrity_test") == "7z_test_passed"
        and int(metadata.get("bytes", -1)) == archive.stat().st_size
    ):
        print(f"Reusing passed 7z integrity test for {archive.name}", flush=True)
        return
    print(f"Testing every compressed block in {archive.name}", flush=True)
    run_checked(["7z", "t", "-bd", str(archive)], stdout=subprocess.DEVNULL)
    if not metadata:
        metadata = {
            "filename": archive.name,
            "bytes": archive.stat().st_size,
            "sha256": sha256_file(archive),
        }
    metadata["integrity_test"] = "7z_test_passed"
    write_json(sidecar, metadata)


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def normalize_archive_member(
    kind: str,
    archive: Path,
    member: str,
    output: Path,
    rejects: Path,
    stats_path: Path,
    reconstruction_version: str,
) -> dict[str, Any]:
    extractor = subprocess.Popen(
        ["7z", "x", "-so", str(archive), member], stdout=subprocess.PIPE
    )
    assert extractor.stdout is not None
    normalizer = subprocess.Popen(
        [
            "taxi-data", "normalize", "--kind", kind, "--input", "-",
            "--source-name", Path(member).name, "--output", str(output),
            "--rejects", str(rejects), "--stats", str(stats_path),
        ],
        stdin=extractor.stdout,
    )
    extractor.stdout.close()
    normalizer_status = normalizer.wait()
    extractor_status = extractor.wait()
    if extractor_status != 0 or normalizer_status != 0:
        raise RuntimeError(
            f"archive normalization failed: 7z={extractor_status}, "
            f"taxi-data={normalizer_status}, member={member}"
        )
    stats = load_json(stats_path)
    stats["reconstruction_version"] = reconstruction_version
    write_json(stats_path, stats)
    return stats


def stage_source(
    *,
    kind: str,
    month: int,
    work_dir: Path,
    quarantine_dir: Path,
    reconstruction_version: str,
    archive: Path,
    member: str,
    force: bool = False,
) -> dict[str, Any]:
    stage_dir = work_dir / "numbered"
    output = stage_dir / f"{kind}-{month:02d}.numbered.csv.zst"
    stats_path = stage_dir / f"{kind}-{month:02d}.stats.json"
    rejects = quarantine_dir / f"{kind}-{month:02d}.malformed.jsonl.zst"
    if not force and output.exists() and rejects.exists() and stats_path.exists():
        cached = load_json(stats_path)
        if cached.get("reconstruction_version") == reconstruction_version:
            print(f"Reusing numbered {kind} month {month:02d}", flush=True)
            return cached
    output.parent.mkdir(parents=True, exist_ok=True)
    rejects.parent.mkdir(parents=True, exist_ok=True)
    stats = normalize_archive_member(
        kind, archive, member, output, rejects, stats_path, reconstruction_version
    )
    stats["numbered_path"] = str(output)
    stats["malformed_report"] = str(rejects)
    write_json(stats_path, stats)
    return stats


def sql_string(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def joined_condition(left: str, right: str, include_occurrence: bool = True) -> str:
    fields = ["medallion", "hack_license", "vendor_id", "pickup_datetime"]
    clauses = [f"{left}.{field} = {right}.{field}" for field in fields]
    if include_occurrence:
        clauses.append(f"{left}.occurrence_index = {right}.occurrence_index")
    return " AND ".join(clauses)


def configure_duckdb(connection: duckdb.DuckDBPyConnection, temp_dir: Path) -> None:
    memory_limit = os.environ.get("DATA_MEMORY_LIMIT", "20GB")
    threads = int(os.environ.get("DATA_THREADS", "12"))
    temp_dir.mkdir(parents=True, exist_ok=True)
    connection.execute(f"SET memory_limit={sql_string(memory_limit)}")
    connection.execute(f"SET threads={threads}")
    connection.execute(f"SET temp_directory={sql_string(temp_dir)}")
    connection.execute("SET preserve_insertion_order=true")


def monthly_join(
    month: int,
    trip_stats: dict[str, Any],
    fare_stats: dict[str, Any],
    work_dir: Path,
    quarantine_dir: Path,
    force: bool,
) -> dict[str, Any]:
    monthly_dir = work_dir / "monthly"
    monthly_dir.mkdir(parents=True, exist_ok=True)
    output = monthly_dir / f"month-{month:02d}.sorted.csv.zst"
    manifest_path = monthly_dir / f"month-{month:02d}.manifest.json"
    unmatched_path = quarantine_dir / f"month-{month:02d}.unmatched.csv.zst"
    if not force and output.exists() and manifest_path.exists() and unmatched_path.exists():
        cached = load_json(manifest_path)
        if (
            cached.get("trip_source_checksum") == trip_stats["source_sha256"]
            and cached.get("fare_source_checksum") == fare_stats["source_sha256"]
            and cached.get("monthly_output_version") == MONTHLY_OUTPUT_VERSION
        ):
            print(f"Reusing joined month {month:02d}", flush=True)
            return cached

    temp_dir = work_dir / "duckdb-spill" / f"month-{month:02d}"
    connection = duckdb.connect()
    configure_duckdb(connection, temp_dir)
    trip_path = Path(trip_stats["numbered_path"])
    fare_path = Path(fare_stats["numbered_path"])
    try:
        connection.execute(
            f"""
            CREATE TEMP TABLE trips AS
            SELECT *, row_number() OVER (
                PARTITION BY medallion, hack_license, vendor_id, pickup_datetime
                ORDER BY CAST(source_row_number AS UBIGINT)
            )::UBIGINT AS occurrence_index
            FROM read_csv({sql_string(trip_path)}, header=true, all_varchar=true)
            """
        )
        connection.execute(
            f"""
            CREATE TEMP TABLE fares AS
            SELECT *, row_number() OVER (
                PARTITION BY medallion, hack_license, vendor_id, pickup_datetime
                ORDER BY CAST(source_row_number AS UBIGINT)
            )::UBIGINT AS occurrence_index
            FROM read_csv({sql_string(fare_path)}, header=true, all_varchar=true)
            """
        )
        connection.execute(
            "CREATE TEMP TABLE trip_keys AS SELECT medallion, hack_license, vendor_id, "
            "pickup_datetime, count(*) AS key_count FROM trips GROUP BY ALL"
        )
        connection.execute(
            "CREATE TEMP TABLE fare_keys AS SELECT medallion, hack_license, vendor_id, "
            "pickup_datetime, count(*) AS key_count FROM fares GROUP BY ALL"
        )

        trip_counts = connection.execute(
            f"""
            SELECT
                count(*) FILTER (WHERE f.source_row_number IS NOT NULL) AS joined,
                count(*) FILTER (WHERE f.source_row_number IS NULL AND fk.key_count IS NULL) AS unmatched,
                count(*) FILTER (WHERE f.source_row_number IS NULL AND fk.key_count IS NOT NULL) AS duplicate
            FROM trips t
            LEFT JOIN fares f ON {joined_condition('t', 'f')}
            LEFT JOIN fare_keys fk ON {joined_condition('t', 'fk', False)}
            """
        ).fetchone()
        fare_counts = connection.execute(
            f"""
            SELECT
                count(*) FILTER (WHERE t.source_row_number IS NOT NULL) AS joined,
                count(*) FILTER (WHERE t.source_row_number IS NULL AND tk.key_count IS NULL) AS unmatched,
                count(*) FILTER (WHERE t.source_row_number IS NULL AND tk.key_count IS NOT NULL) AS duplicate
            FROM fares f
            LEFT JOIN trips t ON {joined_condition('f', 't')}
            LEFT JOIN trip_keys tk ON {joined_condition('f', 'tk', False)}
            """
        ).fetchone()
        assert trip_counts is not None and fare_counts is not None
        joined_count = int(trip_counts[0])
        if joined_count != int(fare_counts[0]):
            raise RuntimeError("trip and fare joined counts differ")

        connection.execute("SET threads=1")
        unmatched_tmp = unmatched_path.with_suffix(unmatched_path.suffix + ".tmp")
        unmatched_tmp.unlink(missing_ok=True)
        connection.execute(
            f"""
            COPY (
                SELECT 'trip' AS source_side,
                       CASE WHEN fk.key_count IS NULL THEN 'unmatched'
                            ELSE 'unmatched_duplicate_occurrence' END AS disposition,
                       CAST(t.source_row_number AS UBIGINT) AS source_row_number,
                       t.medallion, t.hack_license, t.vendor_id,
                       t.pickup_datetime, t.occurrence_index
                FROM trips t
                LEFT JOIN fares f ON {joined_condition('t', 'f')}
                LEFT JOIN fare_keys fk ON {joined_condition('t', 'fk', False)}
                WHERE f.source_row_number IS NULL
                UNION ALL
                SELECT 'fare' AS source_side,
                       CASE WHEN tk.key_count IS NULL THEN 'unmatched'
                            ELSE 'unmatched_duplicate_occurrence' END AS disposition,
                       CAST(f.source_row_number AS UBIGINT) AS source_row_number,
                       f.medallion, f.hack_license, f.vendor_id,
                       f.pickup_datetime, f.occurrence_index
                FROM fares f
                LEFT JOIN trips t ON {joined_condition('f', 't')}
                LEFT JOIN trip_keys tk ON {joined_condition('f', 'tk', False)}
                WHERE t.source_row_number IS NULL
                ORDER BY source_side, source_row_number
            ) TO {sql_string(unmatched_tmp)} (FORMAT CSV, HEADER true, COMPRESSION zstd)
            """
        )
        unmatched_tmp.replace(unmatched_path)

        output_tmp = output.with_suffix(output.suffix + ".tmp")
        output_tmp.unlink(missing_ok=True)
        trip_sha = trip_stats["source_sha256"]
        fare_sha = fare_stats["source_sha256"]
        prefix = f"month_id={month:02d}|trip_sha256={trip_sha}|trip_row="
        middle = f"|fare_sha256={fare_sha}|fare_row="
        connection.execute(
            f"""
            COPY (
                SELECT
                    {month}::UTINYINT AS month_id,
                    {sql_string(trip_sha)} AS trip_source_checksum,
                    CAST(t.source_row_number AS UBIGINT) AS trip_original_row_number,
                    {sql_string(fare_sha)} AS fare_source_checksum,
                    CAST(f.source_row_number AS UBIGINT) AS fare_original_row_number,
                    {sql_string(prefix)} || t.source_row_number || {sql_string(middle)} || f.source_row_number
                        AS joined_record_id,
                    t.medallion, t.hack_license, t.vendor_id, t.pickup_datetime,
                    t.dropoff_datetime, t.rate_code, t.store_and_fwd_flag,
                    t.passenger_count, t.trip_time_seconds, t.trip_distance_miles,
                    t.pickup_longitude, t.pickup_latitude, t.dropoff_longitude,
                    t.dropoff_latitude, f.payment_type, f.fare_amount, f.surcharge,
                    f.mta_tax, f.tip_amount, f.tolls_amount, f.total_amount
                FROM trips t
                INNER JOIN fares f ON {joined_condition('t', 'f')}
                ORDER BY t.dropoff_datetime,
                         CAST(t.source_row_number AS UBIGINT),
                         CAST(f.source_row_number AS UBIGINT)
            ) TO {sql_string(output_tmp)} (FORMAT CSV, HEADER true, COMPRESSION zstd)
            """
        )
        output_tmp.replace(output)
        time_bounds = connection.execute(
            f"""
            SELECT min(t.dropoff_datetime), max(t.dropoff_datetime)
            FROM trips t INNER JOIN fares f ON {joined_condition('t', 'f')}
            """
        ).fetchone()
    finally:
        connection.close()

    trip_accounting = {
        "source_rows": int(trip_stats["source_rows"]),
        "joined": joined_count,
        "malformed": int(trip_stats["malformed_rows"]),
        "unmatched": int(trip_counts[1]),
        "unmatched_duplicate_occurrence": int(trip_counts[2]),
    }
    fare_accounting = {
        "source_rows": int(fare_stats["source_rows"]),
        "joined": joined_count,
        "malformed": int(fare_stats["malformed_rows"]),
        "unmatched": int(fare_counts[1]),
        "unmatched_duplicate_occurrence": int(fare_counts[2]),
    }
    for side, accounting in (("trip", trip_accounting), ("fare", fare_accounting)):
        classified = sum(value for key, value in accounting.items() if key != "source_rows")
        if classified != accounting["source_rows"]:
            raise RuntimeError(
                f"month {month:02d} {side} accounting failed: "
                f"classified={classified}, source={accounting['source_rows']}"
            )
    manifest = {
        "month_id": month,
        "monthly_output_version": MONTHLY_OUTPUT_VERSION,
        "trip_source_name": trip_stats["source_name"],
        "trip_source_checksum": trip_stats["source_sha256"],
        "fare_source_name": fare_stats["source_name"],
        "fare_source_checksum": fare_stats["source_sha256"],
        "joined_event_count": joined_count,
        "trip_accounting": trip_accounting,
        "fare_accounting": fare_accounting,
        "minimum_event_time": time_bounds[0] if time_bounds else None,
        "maximum_event_time": time_bounds[1] if time_bounds else None,
        "sort_key": SORT_POLICY,
        "monthly_export_threads": 1,
        "sort_verified": False,
        "monthly_filename": output.name,
        "monthly_checksum": sha256_file(output),
        "unmatched_report": str(unmatched_path),
        "unmatched_report_checksum": sha256_file(unmatched_path),
    }
    write_json(manifest_path, manifest)
    print(
        f"Month {month:02d}: {joined_count:,} joined; "
        f"{trip_accounting['malformed'] + fare_accounting['malformed']:,} malformed; "
        f"{trip_accounting['unmatched'] + fare_accounting['unmatched']:,} unmatched; "
        f"{trip_accounting['unmatched_duplicate_occurrence'] + fare_accounting['unmatched_duplicate_occurrence']:,} extra duplicate occurrences",
        flush=True,
    )
    return manifest


def code_tree_checksum() -> str:
    root = Path("data/tools")
    digest = hashlib.sha256()
    ignored_names = {".DS_Store"}
    ignored_suffixes = {".pyc", ".pyo"}
    paths = (
        path
        for path in root.rglob("*")
        if path.is_file()
        and path.name not in ignored_names
        and "__pycache__" not in path.parts
        and path.suffix not in ignored_suffixes
    )
    for path in sorted(paths):
        digest.update(str(path.relative_to(root)).encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def git_commit() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"], text=True, capture_output=True, check=False
        )
    except FileNotFoundError:
        return "uncommitted"
    return result.stdout.strip() if result.returncode == 0 else "uncommitted"


def archive_descriptor(path: Path, kind: str, url: str | None) -> dict[str, Any]:
    sidecar = path.with_suffix(path.suffix + ".manifest.json")
    if sidecar.is_file():
        cached = load_json(sidecar)
        if int(cached.get("bytes", -1)) == path.stat().st_size and cached.get("sha256"):
            return {
                "kind": kind,
                "url": url,
                "filename": path.name,
                "bytes": path.stat().st_size,
                "sha256": cached["sha256"],
            }
    return {
        "kind": kind,
        "url": url,
        "filename": path.name,
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def command_prepare(args: argparse.Namespace) -> None:
    started_at = time.monotonic()
    reconstruction_version = os.environ.get("RECONSTRUCTION_VERSION", "1")
    schema_version = int(os.environ.get("EVENT_SCHEMA_VERSION", "1"))
    work_dir = Path(args.work_dir)
    output_dir = Path(args.output_dir)
    quarantine_dir = Path(args.quarantine_dir)
    for directory in (work_dir, output_dir, quarantine_dir):
        directory.mkdir(parents=True, exist_ok=True)

    trip_archive = Path(args.trip_archive)
    fare_archive = Path(args.fare_archive)
    for archive in (trip_archive, fare_archive):
        if not archive.is_file():
            raise FileNotFoundError(f"required archive is missing: {archive}")
        ensure_archive_integrity(archive)
    trip_members = archive_members(trip_archive)
    fare_members = archive_members(fare_archive)

    trip_stats: list[dict[str, Any]] = []
    fare_stats: list[dict[str, Any]] = []
    for month in MONTHS:
        trip_name = f"trip_data_{month}.csv"
        fare_name = f"trip_fare_{month}.csv"
        if trip_name not in trip_members or fare_name not in fare_members:
            raise RuntimeError(f"archive is missing the source pair for month {month:02d}")
        trip = stage_source(
            kind="trip", month=month, archive=trip_archive,
            member=trip_members[trip_name], work_dir=work_dir,
            quarantine_dir=quarantine_dir,
            reconstruction_version=reconstruction_version, force=args.force,
        )
        fare = stage_source(
            kind="fare", month=month, archive=fare_archive,
            member=fare_members[fare_name], work_dir=work_dir,
            quarantine_dir=quarantine_dir,
            reconstruction_version=reconstruction_version, force=args.force,
        )
        trip_stats.append(trip)
        fare_stats.append(fare)

    archives = [
        archive_descriptor(trip_archive, "trip", os.environ.get("TRIP_ARCHIVE_URL")),
        archive_descriptor(fare_archive, "fare", os.environ.get("FARE_ARCHIVE_URL")),
    ]

    source_descriptor = []
    for month, trip, fare in zip(MONTHS, trip_stats, fare_stats):
        source_descriptor.append(
            {
                "month_id": month,
                "trip_source_name": trip["source_name"],
                "trip_source_sha256": trip["source_sha256"],
                "fare_source_name": fare["source_name"],
                "fare_source_sha256": fare["source_sha256"],
            }
        )
    identity_descriptor = {
        "dataset_profile": PROFILE,
        "input_profile": "full",
        "archives": archives,
        "monthly_sources": source_descriptor,
        "reconstruction_version": reconstruction_version,
        "join_policy": JOIN_POLICY,
        "joined_record_id_format": JOINED_ID_FORMAT,
        "sort_policy": SORT_POLICY,
        "event_schema_version": schema_version,
    }
    dataset_digest = hashlib.sha256(canonical_json(identity_descriptor)).hexdigest()
    dataset_id = f"{PROFILE}:{dataset_digest}"

    monthly_manifests = []
    for month, trip, fare in zip(MONTHS, trip_stats, fare_stats):
        monthly_manifests.append(
            monthly_join(
                month, trip, fare, work_dir, quarantine_dir, force=args.force
            )
        )

    prepared = output_dir / f"{PROFILE}.csv.zst"
    merge_report = work_dir / "merge-report.json"
    merge_command = [
        "taxi-data", "merge", "--dataset-id", dataset_id,
        "--schema-version", str(schema_version), "--output", str(prepared),
        "--report", str(merge_report),
    ]
    for month in MONTHS:
        merge_command.extend(
            ["--input", str(work_dir / "monthly" / f"month-{month:02d}.sorted.csv.zst")]
        )
    run_checked(merge_command)
    merge = load_json(merge_report)
    for monthly in monthly_manifests:
        monthly["sort_verified"] = True
        write_json(
            work_dir / "monthly" / f"month-{monthly['month_id']:02d}.manifest.json",
            monthly,
        )

    trip_totals = aggregate_accounting(monthly_manifests, "trip_accounting")
    fare_totals = aggregate_accounting(monthly_manifests, "fare_accounting")
    manifest = {
        "dataset_id": dataset_id,
        "dataset_profile": PROFILE,
        "input_profile": "full",
        "reconstruction_version": reconstruction_version,
        "event_schema_version": schema_version,
        "identity_descriptor": identity_descriptor,
        "sources": archives,
        "months": monthly_manifests,
        "source_accounting": {"trip": trip_totals, "fare": fare_totals},
        "join_policy": JOIN_POLICY,
        "joined_record_id_format": JOINED_ID_FORMAT,
        "event_id_policy": "lowercase SHA-256(dataset_id + joined_record_id)",
        "sort_key": SORT_POLICY,
        "prepared_filename": prepared.name,
        "prepared_compression": "zstd",
        "prepared_checksum": merge["prepared_checksum"],
        "prepared_compressed_bytes": merge["prepared_compressed_bytes"],
        "prepared_uncompressed_bytes": merge["prepared_uncompressed_bytes"],
        "prepared_event_count": merge["prepared_event_count"],
        "minimum_event_time": merge["minimum_event_time"],
        "maximum_event_time": merge["maximum_event_time"],
        "global_sort_verified": merge["sort_verified"],
        "tool_configuration": {
            "python": platform.python_version(),
            "duckdb": duckdb.__version__,
            "go": os.environ.get("GO_VERSION"),
            "zstandard": os.environ.get("ZSTANDARD_VERSION"),
            "memory_limit": os.environ.get("DATA_MEMORY_LIMIT", "20GB"),
            "threads": int(os.environ.get("DATA_THREADS", "12")),
            "machine": platform.machine(),
        },
        "code_commit": git_commit(),
        "code_tree_sha256": code_tree_checksum(),
        "preparation_command": "python3 -m reconstruction.cli prepare",
        "preparation_duration_seconds": round(time.monotonic() - started_at, 3),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    write_json(output_dir / "data-manifest.json", manifest)
    run_checked(
        ["taxi-data", "verify", "--input", str(prepared), "--manifest", str(output_dir / "data-manifest.json")]
    )
    print(
        f"Prepared {manifest['prepared_event_count']:,} verified events; "
        f"dataset_id={dataset_id}; sha256={manifest['prepared_checksum']}",
        flush=True,
    )


def aggregate_accounting(months: list[dict[str, Any]], key: str) -> dict[str, int]:
    fields = (
        "source_rows", "joined", "malformed", "unmatched",
        "unmatched_duplicate_occurrence",
    )
    result = {field: sum(int(month[key][field]) for month in months) for field in fields}
    classified = sum(result[field] for field in fields if field != "source_rows")
    if classified != result["source_rows"]:
        raise RuntimeError(f"aggregate accounting failed for {key}")
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    download = subparsers.add_parser("download", help="resume both archive downloads")
    download.add_argument("--trip-url", required=True)
    download.add_argument("--fare-url", required=True)
    download.add_argument("--raw-dir", required=True)
    download.set_defaults(handler=command_download)

    prepare = subparsers.add_parser("prepare", help="reconstruct the complete dataset")
    prepare.add_argument("--trip-archive", required=True)
    prepare.add_argument("--fare-archive", required=True)
    prepare.add_argument("--work-dir", required=True)
    prepare.add_argument("--output-dir", required=True)
    prepare.add_argument("--quarantine-dir", required=True)
    prepare.add_argument("--force", action="store_true")
    prepare.set_defaults(handler=command_prepare)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    try:
        args.handler(args)
    except (OSError, RuntimeError, ValueError, subprocess.CalledProcessError) as error:
        print(f"reconstruction: {error}", file=sys.stderr)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
