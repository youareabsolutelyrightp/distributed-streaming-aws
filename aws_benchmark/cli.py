from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import analysis, checks, evidence, lifecycle, release as release_builder
from .commands import Commands
from .config import (
    FULL,
    ROOT,
    SMALL,
    build_profile,
    read_json,
    release,
    sha256_file,
    validate_dataset,
    validate_profile,
    versions,
)
from .state import RunState


def _write(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _run_id(small: bool) -> str:
    label = "small" if small else "full"
    return f"aws-{label}-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"


def _latest_run() -> str:
    candidates = sorted((ROOT / ".runs").glob("*/state.json"), key=lambda item: item.stat().st_mtime)
    if not candidates:
        raise ValueError("No local benchmark run state exists.")
    return candidates[-1].parent.name


def _prepare_small(commands: Commands, source: Path, run_root: Path) -> tuple[Path, str]:
    pins = versions()
    commands.run(["docker", "build", "-t", pins["DATA_TOOLS_IMAGE"], "data/tools"], cwd=ROOT)
    target = run_root / "input" / "reconstructed-foil-prefix-10000000.csv.zst"
    report = run_root / "input" / "prefix-manifest.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    commands.run(
        [
            "docker", "run", "--rm", "--user", f"{os.getuid()}:{os.getgid()}",
            "-v", f"{ROOT}:/workspace", "-v", f"{source.resolve()}:/input/full.csv.zst:ro",
            "-w", "/workspace", pins["DATA_TOOLS_IMAGE"], "python3", "-m", "aws_benchmark.dataset",
            "--source", "/input/full.csv.zst", "--target", f"/workspace/{target.relative_to(ROOT)}",
            "--records", str(SMALL.records), "--report", f"/workspace/{report.relative_to(ROOT)}",
        ], cwd=ROOT,
    )
    value = read_json(report)
    if value["records"] != SMALL.records:
        raise ValueError("Small-run prefix contains the wrong number of records.")
    return target, value["sha256"]


def _bucket(output: dict[str, Any]) -> str:
    return output["artifact_bucket"]["value"]


def _control(output: dict[str, Any]) -> str:
    return output["control_instance_id"]["value"]


def _wait_nodes(context: lifecycle.AwsContext, output: dict[str, Any]) -> None:
    ids = list(output["instance_ids"]["value"].values())
    context.commands.run(context.aws("ec2", "wait", "instance-running", "--instance-ids", *ids), cwd=ROOT)
    for instance_id in ids:
        lifecycle.wait_ssm_online(context, instance_id)
        ready = lifecycle.ssm_run(
            context, instance_id,
            "cloud-init status --wait && systemctl is-active taxi-node.service && systemctl is-active taxi-node-metrics.service",
            1_200,
        )
        if ready.get("Status") != "Success":
            raise RuntimeError(f"Node readiness failed: {instance_id}")
    lifecycle.wait_ssm_online(context, _control(output), stable_checks=3)


def _upload_inputs(
    context: lifecycle.AwsContext, bucket: str, profile: dict[str, Any], built: dict[str, Any]
) -> str:
    lifecycle.upload(context, bucket, Path(built["path"]), built["object_key"], built["sha256"])
    dataset_info = profile["dataset"]
    data_key = f"datasets/{dataset_info['sha256']}/prepared.csv.zst"
    lifecycle.upload(context, bucket, Path(dataset_info["path"]), data_key, dataset_info["sha256"])
    manifest_source = Path(dataset_info["manifest_path"])
    lifecycle.upload(
        context, bucket, manifest_source, f"datasets/{dataset_info['sha256']}/manifest.json",
        sha256_file(manifest_source),
    )
    reference_source = Path(dataset_info["reference_path"])
    lifecycle.upload(
        context, bucket, reference_source, f"references/{dataset_info['reference_sha256']}.json",
        dataset_info["reference_sha256"],
    )
    return data_key


def execute(args: argparse.Namespace) -> int:
    commands = Commands()
    if args.resume:
        run_id = args.resume
        state = RunState.load(ROOT, run_id)
        if state.status in {"completed", "cleanup_incomplete"}:
            raise ValueError(
                f"Run {run_id} already finished with status {state.status}; "
                "use status or cleanup instead of resume."
            )
        state.reset_after_cleanup()
        profile_path = state.path.parent / "profile.json"
        profile = read_json(profile_path)
        size = validate_profile(profile)
    else:
        size = SMALL if args.small_run else FULL
        run_id = args.run_id or _run_id(args.small_run)
        state = RunState.create(ROOT, run_id)
        profile_path = state.path.parent / "profile.json"
        manifest_path = args.manifest.resolve()
        source = args.dataset.resolve()
        state.begin("preflight")
        validate_dataset(manifest_path, source, size)
        dataset_path, checksum = source, read_json(manifest_path)["prepared_checksum"]
        if size is SMALL:
            dataset_path, checksum = _prepare_small(commands, source, state.path.parent)
        profile = build_profile(
            run_id=run_id, size=size, dataset_path=dataset_path,
            manifest_path=manifest_path, dataset_sha256=checksum,
        )
        _write(profile_path, profile)
        state.complete("preflight", profile_sha256=profile["profile_sha256"])

    context = lifecycle.AwsContext(run_id, args.aws_profile, size, state.path.parent, commands)
    budget = float(release()["budgets_usd"][size.name])
    result_path: Path | None = None
    cleanup_report: dict[str, Any] = {"passed": False}
    run_error: BaseException | None = None
    failure_phase: str | None = None
    try:
        if "release" not in state.completed:
            state.begin("release")
            gate = lifecycle.preflight(context, budget)
            _write(state.path.parent / "preflight.json", gate)
            built = release_builder.build(commands, state.path.parent / "release")
            state.complete("release", release_sha256=built["sha256"])
        else:
            built = read_json(state.path.parent / "release/release.json")

        if "foundation" not in state.completed:
            state.begin("foundation")
            lifecycle.apply(context, False)
            foundation = lifecycle.outputs(context)
            _write(state.path.parent / "terraform-outputs-foundation.json", foundation)
            state.complete("foundation", artifact_bucket=_bucket(foundation))
        foundation = read_json(state.path.parent / "terraform-outputs-foundation.json")
        bucket = _bucket(foundation)

        if "dataset" not in state.completed:
            state.begin("dataset")
            data_key = _upload_inputs(context, bucket, profile, built)
            state.complete("dataset", dataset_key=data_key)

        if "deploy" not in state.completed:
            state.begin("deploy")
            lifecycle.apply(context, True, built)
            deployed = lifecycle.outputs(context)
            _write(state.path.parent / "terraform-outputs.json", deployed)
            state.complete("deploy")
        deployed = read_json(state.path.parent / "terraform-outputs.json")

        if "readiness" not in state.completed:
            state.begin("readiness")
            _wait_nodes(context, deployed)
            state.complete("readiness")

        if "benchmark" not in state.completed:
            state.begin("benchmark")
            remote_result = lifecycle.ssm_run(
                context, _control(deployed), lifecycle.remote_command(profile),
                size.safeguard_hours * 3_600, state=state, purpose="benchmark",
            )
            _write(state.path.parent / "ssm-benchmark.json", remote_result)
            state.complete("benchmark", remote_status=remote_result.get("Status"))
            if lifecycle.undeliverable(remote_result):
                raise RuntimeError(
                    "The control-node benchmark command remained undeliverable after retries."
                )

        state.begin("evidence")
        last_error: Exception | None = None
        evidence_attempts = 30 if state.details.get("remote_status") == "Success" else 3
        for _ in range(evidence_attempts):
            try:
                result_path = evidence.download(context, bucket, run_id)
                break
            except (ValueError, RuntimeError) as error:
                last_error = error
                time.sleep(10)
        if result_path is None:
            raise RuntimeError(f"Durable evidence did not become complete: {last_error}")
        state.complete("evidence")
        if state.details.get("remote_status") != "Success":
            raise RuntimeError(
                "The control-node benchmark ended with status "
                f"{state.details.get('remote_status', 'unknown')}; durable evidence was downloaded."
            )
    except BaseException as error:
        run_error = error
        failure_phase = state.phase
    finally:
        try:
            state.begin("cleanup")
            lifecycle.cleanup(context)
            state.complete("cleanup")
        except BaseException as error:
            if run_error is None:
                run_error = error
                failure_phase = "cleanup"
            state.details["cleanup_error"] = f"{type(error).__name__}: {error}"
            state.save()
        try:
            state.begin("audit")
            cleanup_report = lifecycle.audit(context)
            _write(state.path.parent / "account-audit.json", cleanup_report)
            state.complete("audit", cleanup_passed=cleanup_report["passed"])
        except BaseException as error:
            if run_error is None:
                run_error = error
                failure_phase = "audit"
            state.details["audit_error"] = f"{type(error).__name__}: {error}"
            state.save()

    if run_error is not None:
        state.finish(
            "failed", failure_phase=failure_phase,
            failure_type=type(run_error).__name__, failure_message=str(run_error),
        )
        evidence_path = str(result_path) if result_path is not None else "unavailable"
        cleanup_state = "complete" if "cleanup" in state.completed else "incomplete"
        audit_state = "clean" if cleanup_report.get("passed") is True else "resources remain"
        print(
            f"ERROR run={run_id} phase={failure_phase}: {run_error}\n"
            f"Evidence: {evidence_path}\n"
            f"Cleanup: {cleanup_state}\n"
            f"Account audit: {audit_state}",
            file=sys.stderr,
        )
        return 1

    if result_path is None:
        state.finish("failed")
        return 1
    try:
        state.begin("analysis")
        final = analysis.summarize(result_path, cleanup_report)
        _write(result_path / "final-summary.json", final)
        state.complete("analysis")
        state.complete("summary", final_summary=str(result_path / "final-summary.json"))
    except BaseException as error:
        state.finish(
            "failed", failure_phase="analysis",
            failure_type=type(error).__name__, failure_message=str(error),
        )
        print(
            f"ERROR run={run_id} phase=analysis: {error}\n"
            f"Evidence: {result_path}\n"
            "Cleanup: complete\n"
            f"Account audit: {'clean' if cleanup_report.get('passed') else 'resources remain'}",
            file=sys.stderr,
        )
        return 1
    cleanup_complete = cleanup_report.get("passed") is True
    state.finish("completed" if cleanup_complete else "cleanup_incomplete")
    if cleanup_complete:
        for generated in ("input", "release", "terraform-data"):
            shutil.rmtree(state.path.parent / generated, ignore_errors=True)
    print(json.dumps(final, indent=2, sort_keys=True))
    return 0 if cleanup_complete else 2


def check_command(args: argparse.Namespace) -> int:
    report = checks.repository()
    if args.dataset and args.manifest:
        validate_dataset(args.manifest.resolve(), args.dataset.resolve(), SMALL if args.small_run else FULL)
        report["dataset_valid"] = True
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["passed"] else 1


def status_command(args: argparse.Namespace) -> int:
    run_id = args.run_id or _latest_run()
    state = RunState.load(ROOT, run_id)
    print(state.path.read_text(encoding="utf-8"), end="")
    return 0


def cleanup_command(args: argparse.Namespace) -> int:
    run_id = args.run_id or _latest_run()
    state = RunState.load(ROOT, run_id)
    profile = read_json(state.path.parent / "profile.json")
    size = validate_profile(profile)
    context = lifecycle.AwsContext(run_id, args.aws_profile, size, state.path.parent, Commands())
    lifecycle.cleanup(context)
    report = lifecycle.audit(context)
    _write(state.path.parent / "account-audit.json", report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["passed"] else 1


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="benchmark")
    sub = root.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run")
    run.add_argument("--aws-profile", default="dev-user")
    run.add_argument("--dataset", type=Path, default=Path("data/prepared/full/reconstructed-foil.csv.zst"))
    run.add_argument("--manifest", type=Path, default=Path("data/prepared/full/data-manifest.json"))
    run.add_argument("--run-id")
    run.add_argument("--small-run", action="store_true")
    run.add_argument("--resume", metavar="RUN_ID")
    check = sub.add_parser("check")
    check.add_argument("--dataset", type=Path)
    check.add_argument("--manifest", type=Path)
    check.add_argument("--small-run", action="store_true")
    status = sub.add_parser("status")
    status.add_argument("--run-id")
    cleanup = sub.add_parser("cleanup")
    cleanup.add_argument("--run-id")
    cleanup.add_argument("--aws-profile", default="dev-user")
    return root


def main() -> int:
    args = parser().parse_args()
    if args.command == "run":
        if args.resume and (args.run_id or args.small_run):
            raise ValueError("--resume cannot be combined with --run-id or --small-run.")
        return execute(args)
    if args.command == "check":
        if bool(args.dataset) != bool(args.manifest):
            raise ValueError("--dataset and --manifest must be supplied together.")
        return check_command(args)
    if args.command == "status":
        return status_command(args)
    if args.command == "cleanup":
        return cleanup_command(args)
    raise AssertionError(args.command)


if __name__ == "__main__":
    raise SystemExit(main())
