from __future__ import annotations

import hashlib
import json
import shutil
import tarfile
import tempfile
import zipfile
from pathlib import Path
from typing import Any

from .commands import Commands
from .config import ROOT, RELEASE_PATH, sha256_file, versions


def _copy(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    if source.is_dir():
        shutil.copytree(source, target, dirs_exist_ok=True, ignore=shutil.ignore_patterns("target", "__pycache__", "*.pyc"))
    else:
        shutil.copy2(source, target)


def build(commands: Commands, output_root: Path) -> dict[str, Any]:
    commands.run(["make", "build"], cwd=ROOT)
    pins = versions()
    commands.run(
        ["docker", "build", "--platform", "linux/arm64", "--build-arg", f"GO_IMAGE={pins['GO_IMAGE']}",
         "-t", pins["PRODUCER_IMAGE"], "producer"], cwd=ROOT,
    )
    commands.run(
        ["docker", "build", "--platform", "linux/arm64", "--build-arg", f"GO_IMAGE={pins['GO_IMAGE']}",
         "-t", pins["MATERIALIZER_IMAGE"], "materializer"], cwd=ROOT,
    )
    output_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="aws-release-", dir=output_root) as directory:
        stage = Path(directory)
        _copy(ROOT / "processor/flink/target/taxi-flink-benchmark-0.1.0.jar", stage / "app/taxi-flink.jar")
        _copy(ROOT / "aws_benchmark/__init__.py", stage / "aws_benchmark/__init__.py")
        _copy(ROOT / "aws_benchmark/config.py", stage / "aws_benchmark/config.py")
        _copy(ROOT / "aws_benchmark/remote.py", stage / "aws_benchmark/remote.py")
        _copy(ROOT / "aws_benchmark/runtime_tools", stage / "aws_benchmark/runtime_tools")
        _copy(ROOT / "infra/runtime", stage / "infra/runtime")
        _copy(RELEASE_PATH, stage / "config/release.json")
        _copy(ROOT / "schemas/evidence", stage / "schemas/evidence")
        _copy(ROOT / "database/migrations/0001_canonical_results.sql", stage / "database/migrations/0001_canonical_results.sql")
        _copy(ROOT / "versions.env", stage / "versions.env")
        image_archive = stage / "images/application-images.tar"
        image_archive.parent.mkdir(parents=True, exist_ok=True)
        commands.run(
            ["docker", "save", "-o", str(image_archive), pins["PRODUCER_IMAGE"],
             pins["MATERIALIZER_IMAGE"]],
            cwd=ROOT,
        )
        image_tags = image_archive_tags(image_archive)
        required_tags = {pins["PRODUCER_IMAGE"], pins["MATERIALIZER_IMAGE"]}
        missing_tags = sorted(required_tags - image_tags)
        if missing_tags:
            raise RuntimeError(
                "Application image archive is missing required tags: "
                + ", ".join(missing_tags)
            )
        jar_content_sha256 = archive_content_sha256(stage / "app/taxi-flink.jar")
        release_configuration = json.loads(RELEASE_PATH.read_text(encoding="utf-8"))
        expected_jar = release_configuration["build_identity"]["flink_jar_content_sha256"]
        if jar_content_sha256 != expected_jar:
            raise RuntimeError(
                "Built Flink content differs from the frozen lean source identity: "
                f"{jar_content_sha256} != {expected_jar}"
            )
        manifest = {
            "version": 1,
            "release_configuration_sha256": sha256_file(RELEASE_PATH),
            "flink_jar_sha256": sha256_file(stage / "app/taxi-flink.jar"),
            "flink_jar_content_sha256": jar_content_sha256,
            "application_images_sha256": sha256_file(image_archive),
            "application_image_tags": sorted(image_tags),
            "source_tree_sha256": source_tree_sha256(),
        }
        (stage / "release-manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8",
        )
        archive = output_root / "aws-benchmark-release.tar.gz"
        with tarfile.open(archive, "w:gz") as bundle:
            for path in sorted(stage.rglob("*")):
                bundle.add(path, arcname=path.relative_to(stage), recursive=False)
    result = {
        "version": 1,
        "path": str(archive),
        "sha256": sha256_file(archive),
        "object_key": f"releases/{sha256_file(archive)}.tar.gz",
        "manifest": manifest,
    }
    record = output_root / "release.json"
    record.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


def image_archive_tags(path: Path) -> set[str]:
    with tarfile.open(path, "r") as archive:
        manifest_file = archive.extractfile("manifest.json")
        if manifest_file is None:
            raise RuntimeError("Application image archive has no manifest.json.")
        manifest = json.load(manifest_file)
    return {
        tag
        for image in manifest
        for tag in image.get("RepoTags", [])
        if isinstance(tag, str)
    }


def archive_content_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with zipfile.ZipFile(path) as archive:
        for name in sorted(item for item in archive.namelist() if not item.endswith("/")):
            content_digest = hashlib.sha256(archive.read(name)).hexdigest()
            digest.update(name.encode("utf-8"))
            digest.update(b"\0")
            digest.update(content_digest.encode("ascii"))
            digest.update(b"\n")
    return digest.hexdigest()


def source_tree_sha256() -> str:
    digest = hashlib.sha256()
    roots = (
        "processor/flink/src", "producer", "materializer", "database", "schemas",
        "aws_benchmark", "infra", "config", "versions.env",
    )
    paths: list[Path] = []
    for name in roots:
        path = ROOT / name
        paths.extend([path] if path.is_file() else [item for item in path.rglob("*") if item.is_file()])
    for path in sorted(paths):
        if any(part in {"target", "__pycache__", ".terraform"} for part in path.parts):
            continue
        digest.update(str(path.relative_to(ROOT)).encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()
