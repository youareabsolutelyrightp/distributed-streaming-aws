from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from .config import release, validate_profile


RELEASE_ROOT = Path("/opt/taxi/release")
DATA_ROOT = Path("/opt/taxi/data")


def run(profile_path: Path) -> int:
    profile = json.loads(profile_path.read_text(encoding="utf-8"))
    validate_profile(profile)
    dataset = profile["dataset"]
    DATA_ROOT.mkdir(parents=True, exist_ok=True)
    (DATA_ROOT / "price-check.json").write_text(
        json.dumps({"estimated_hourly_usd": release()["estimated_hourly_usd"]}) + "\n",
        encoding="utf-8",
    )
    object_prefix = f"datasets/{dataset['sha256']}"
    reference_key = f"references/{dataset['reference_sha256']}.json"
    bucket = os.environ.get("ARTIFACT_BUCKET")
    if not bucket:
        raise RuntimeError("ARTIFACT_BUCKET is required on the control node.")
    commands = (
        ["aws", "s3", "cp", f"s3://{bucket}/{object_prefix}/prepared.csv.zst", str(DATA_ROOT / "prepared.csv.zst")],
        ["aws", "s3", "cp", f"s3://{bucket}/{object_prefix}/manifest.json", str(DATA_ROOT / "manifest.json")],
        ["aws", "s3", "cp", f"s3://{bucket}/{reference_key}", str(DATA_ROOT / "reference.json")],
    )
    for command in commands:
        result = subprocess.run(command, check=False)
        if result.returncode:
            return result.returncode
    checksum = subprocess.run(
        ["sha256sum", "-c", "-"], input=f"{dataset['sha256']}  {DATA_ROOT / 'prepared.csv.zst'}\n",
        text=True, check=False,
    )
    if checksum.returncode:
        return checksum.returncode
    return subprocess.run(
        [str(RELEASE_ROOT / "infra/runtime/benchmark-node.sh"), str(profile_path)],
        check=False,
    ).returncode


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: python3 -m aws_benchmark.remote PROFILE", file=sys.stderr)
        raise SystemExit(2)
    raise SystemExit(run(Path(sys.argv[1])))
