from __future__ import annotations

import argparse
import io
import json
import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from aws_benchmark import analysis, checks, config, lifecycle, release
from aws_benchmark.cli import parser
from aws_benchmark.commands import Commands
from aws_benchmark.runtime_tools import contracts, latency, readiness, run_manifest
from aws_benchmark.state import RunState


class FakeCommands(Commands):
    def __init__(self, quota: float = 32, offered: int = 1) -> None:
        self.quota = quota
        self.offered = offered

    def output(self, arguments, *, cwd, env=None):
        text = " ".join(arguments)
        if "get-caller-identity" in text:
            return json.dumps({"Account": "000000000000", "Arn": "test", "UserId": "test"})
        if "describe-instance-type-offerings" in text:
            return str(self.offered)
        if "get-service-quota" in text:
            return str(self.quota)
        raise AssertionError(text)


class FrozenConfigurationTest(unittest.TestCase):
    def test_full_and_small_shapes_are_exact(self) -> None:
        self.assertEqual(config.FULL.records, 173_176_321)
        self.assertEqual(config.FULL.warmup_records, 3_200_000)
        self.assertEqual(config.SMALL.records, 10_000_000)
        self.assertEqual(config.SMALL.warmup_records, 1_600_000)
        selected = config.release()
        self.assertEqual(selected["topology"]["taskmanager_instance_type"], "c9g.xlarge")
        self.assertEqual(selected["topology"]["total_vcpus"], 28)
        self.assertEqual(selected["flink"], {
            "execution.buffer-timeout.enabled": True,
            "execution.buffer-timeout.interval": "5 ms",
        })

    def test_profile_rejects_a_frozen_value_change(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({
                "dataset_id": json.loads(config.SMALL_REFERENCE.read_text())["dataset_id"],
                "prepared_checksum": "a" * 64,
            }))
            source = root / "input.zst"
            source.write_bytes(b"prefix")
            profile = config.build_profile(
                run_id="test-small", size=config.SMALL, dataset_path=source,
                manifest_path=manifest, dataset_sha256="b" * 64,
            )
            self.assertIs(config.validate_profile(profile), config.SMALL)
            profile["stream"]["watermark_alignment_max_drift_ms"] = 1
            unsigned = dict(profile)
            unsigned.pop("profile_sha256")
            import hashlib
            profile["profile_sha256"] = hashlib.sha256(
                json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
            with self.assertRaisesRegex(ValueError, "frozen"):
                config.validate_profile(profile)

    def test_public_cli_has_no_frozen_overrides(self) -> None:
        run = next(action for action in parser()._actions if isinstance(action, argparse._SubParsersAction)).choices["run"]
        options = {option for action in run._actions for option in action.option_strings}
        for forbidden in ("--rate", "--taskmanagers", "--alignment", "--transport", "--shards"):
            self.assertNotIn(forbidden, options)

    def test_run_id_matches_terraform_limit(self) -> None:
        self.assertIsNotNone(config.RUN_ID.fullmatch("r" * 32))
        self.assertIsNone(config.RUN_ID.fullmatch("r" * 33))

    def test_dataset_checksum_rejection_precedes_aws(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dataset = root / "prepared.csv.zst"
            dataset.write_bytes(b"not-the-accepted-dataset")
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps({
                "prepared_event_count": config.FULL_RECORDS,
                "prepared_checksum": "0" * 64,
                "dataset_id": json.loads(config.SMALL_REFERENCE.read_text())["dataset_id"],
            }))
            with self.assertRaisesRegex(ValueError, "checksum"):
                config.validate_dataset(manifest, dataset, config.SMALL)

    def test_interrupted_state_loads_for_resume(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = RunState.create(root, "resume-test")
            state.complete("preflight", profile_sha256="abc")
            state.begin("deploy")
            restored = RunState.load(root, "resume-test")
            self.assertEqual(restored.phase, "deploy")
            self.assertEqual(restored.completed, ["preflight"])
            self.assertEqual(restored.details["profile_sha256"], "abc")

    def test_cleaned_failed_run_recreates_remote_lifecycle_on_resume(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = RunState.create(root, "resume-cleaned")
            state.complete("preflight", profile_sha256="profile")
            state.complete("release", release_sha256="release")
            state.complete("foundation", artifact_bucket="deleted-bucket")
            state.complete("dataset", dataset_key="deleted-object")
            state.complete("cleanup")
            state.complete("audit", cleanup_passed=True)
            state.reset_after_cleanup()
            self.assertEqual(state.completed, ["preflight", "release"])
            self.assertEqual(state.phase, "release")
            self.assertEqual(state.details["profile_sha256"], "profile")
            self.assertEqual(state.details["release_sha256"], "release")
            self.assertNotIn("artifact_bucket", state.details)
            self.assertNotIn("cleanup_passed", state.details)


class AwsPreflightTest(unittest.TestCase):
    def context(self, commands: Commands) -> lifecycle.AwsContext:
        return lifecycle.AwsContext("test-run", "test", config.SMALL, config.ROOT / ".runs/test-run", commands)

    @mock.patch("aws_benchmark.lifecycle.shutil.which", return_value="/usr/bin/tool")
    def test_offering_and_quota_gates(self, _which) -> None:
        with self.assertRaisesRegex(RuntimeError, "not offered"):
            lifecycle.preflight(self.context(FakeCommands(offered=0)), 10)
        with self.assertRaisesRegex(RuntimeError, "28 are required"):
            lifecycle.preflight(self.context(FakeCommands(quota=27)), 10)

    @mock.patch("aws_benchmark.lifecycle.shutil.which", return_value="/usr/bin/tool")
    def test_budget_gate(self, _which) -> None:
        with self.assertRaisesRegex(RuntimeError, "exceeds budget"):
            lifecycle.preflight(self.context(FakeCommands()), 2)

    def test_account_audit_detects_retained_compute(self) -> None:
        context = self.context(FakeCommands())
        responses = {
            "describe-instances": {"Reservations": [{"Instances": [{"InstanceId": "i-1"}]}]},
            "describe-volumes": {"Volumes": []},
            "describe-addresses": {"Addresses": []},
            "describe-nat-gateways": {"NatGateways": []},
            "describe-load-balancers": {"LoadBalancers": []},
        }

        def output(*arguments):
            command = next(name for name in responses if name in arguments)
            return json.dumps(responses[command])

        with mock.patch.object(context, "output", side_effect=output):
            report = lifecycle.audit(context)
        self.assertFalse(report["passed"])
        self.assertEqual(report["remaining_instances"], ["i-1"])

    @mock.patch("aws_benchmark.lifecycle.time.sleep")
    def test_ssm_retries_only_an_undeliverable_command(self, _sleep) -> None:
        context = self.context(FakeCommands())
        invocations = iter((
            {"Status": "Failed", "StatusDetails": "Undeliverable", "ResponseCode": -1},
            {"Status": "Success", "StatusDetails": "Success", "ResponseCode": 0},
        ))
        command_ids = iter(("command-1", "command-2"))
        with mock.patch("aws_benchmark.lifecycle.wait_ssm_online") as online, \
                mock.patch("aws_benchmark.lifecycle._send_ssm_command", side_effect=command_ids), \
                mock.patch.object(context, "output", side_effect=lambda *args: (
                    json.dumps(next(invocations)) if "get-command-invocation" in args else "Online"
                )):
            result = lifecycle.ssm_run(context, "i-control", "true", 60)
        self.assertEqual(result["Status"], "Success")
        self.assertEqual(online.call_count, 2)

    @mock.patch("aws_benchmark.lifecycle.time.sleep")
    def test_ssm_does_not_retry_an_executed_failure(self, _sleep) -> None:
        context = self.context(FakeCommands())
        failed = {"Status": "Failed", "StatusDetails": "Failed", "ResponseCode": 1}
        with mock.patch("aws_benchmark.lifecycle.wait_ssm_online"), \
                mock.patch("aws_benchmark.lifecycle._send_ssm_command", return_value="command-1"), \
                mock.patch.object(context, "output", return_value=json.dumps(failed)):
            result = lifecycle.ssm_run(context, "i-control", "false", 60)
        self.assertEqual(result, failed)

    @mock.patch("aws_benchmark.lifecycle.time.sleep")
    def test_ssm_records_the_long_command_purpose(self, _sleep) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            context = lifecycle.AwsContext(
                "test-run", "test", config.SMALL, root / ".runs/test-run", FakeCommands(),
            )
            complete = {"Status": "Success", "StatusDetails": "Success", "ResponseCode": 0}
            state = RunState.create(root, context.run_id)
            with mock.patch("aws_benchmark.lifecycle.wait_ssm_online"), \
                    mock.patch("aws_benchmark.lifecycle._send_ssm_command", return_value="command-1"), \
                    mock.patch.object(context, "output", return_value=json.dumps(complete)):
                lifecycle.ssm_run(
                    context, "i-control", "true", 60, state=state, purpose="benchmark",
                )
            saved = RunState.load(root, context.run_id)
            self.assertEqual(saved.details["ssm_command_id"], "command-1")
            self.assertEqual(saved.details["ssm_command_purpose"], "benchmark")
            self.assertIn("ssm_command_started_at", saved.details)


class RepositoryTest(unittest.TestCase):
    def test_repository_contract(self) -> None:
        report = checks.repository()
        self.assertEqual(report["failures"], [])

    def test_runtime_contract_schemas_resolve_from_release_root(self) -> None:
        self.assertEqual(contracts.ROOT, config.ROOT)
        self.assertEqual(
            [path for path in contracts.SCHEMAS.values() if not path.is_file()],
            [],
        )

    def test_transport_v3_export_row_maps_all_coalescer_timestamps(self) -> None:
        values = {
            "query_id": "query1",
            "output_id": "a" * 64,
            "effective_time": "2013-01-01T00:00:01.000000Z",
            "trigger_type": "input",
        }
        numeric = 100
        row = []
        for name in latency.TRANSPORT_TSV_FIELDS:
            if name in latency.TRANSPORT_TSV_TEXT_FIELDS:
                row.append(values[name])
            elif name == "transport_measurement_version":
                row.append("3")
            else:
                row.append(str(numeric))
                numeric += 10
        parsed = latency.tsv_record("\t".join(row), 1)
        self.assertEqual(parsed["transport_measurement_version"], 3)
        self.assertGreater(parsed["shared_boundary_emitted_epoch_us"], 0)
        self.assertGreater(parsed["boundary_signal_arrival_epoch_us"], 0)

    def test_transport_v3_analyzer_cli_reconstructs_the_export_shape(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            row = [
                "query1", "a" * 64, "2013-01-01T00:00:01.000000Z", "input",
                "3", "1", "0", "1", "1000000", "1000000", "1001000",
                "1002000", "1003000", "1004000", "1005000", "1006000",
                "1007000", "1008000", "1009000", "1010000", "1011000",
            ]
            trace = root / "trace.tsv"
            output = root / "latency-attribution.json"
            sample = root / "sample.jsonl"
            trace.write_text("\t".join(row) + "\n", encoding="utf-8")
            subprocess.run(
                [
                    sys.executable, str(Path(latency.__file__)), "--transport-tsv", str(trace),
                    "--expected-records", "1", "--diagnostic-sample", str(sample),
                    "--output", str(output),
                ],
                check=True,
            )
            report = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(report["schema_version"], 3)
            self.assertTrue(report["complete_attribution"])
            self.assertEqual(report["negative_segment_count"], 0)
            self.assertTrue(sample.is_file())

    def test_release_archive_tag_validation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "images.tar"
            payload = json.dumps([
                {"RepoTags": ["taxi-replay:test"]},
                {"RepoTags": ["taxi-materializer:test"]},
            ]).encode()
            info = tarfile.TarInfo("manifest.json")
            info.size = len(payload)
            with tarfile.open(path, "w") as archive:
                archive.addfile(info, io.BytesIO(payload))
            self.assertEqual(
                release.image_archive_tags(path),
                {"taxi-replay:test", "taxi-materializer:test"},
            )


class ReadinessTest(unittest.TestCase):
    def test_missing_readiness_evidence_fails_before_input(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = {}
            for name in (
                "job-a", "job-b", "preflight", "exceptions", "source-a", "source-b",
                "materializer-a", "materializer-b", "offsets", "topic-contract",
                "materializer", "database", "targets", "up",
            ):
                path = root / name
                path.write_text("{}" if name not in {"database", "offsets"} else "")
                paths[name] = path
            args = argparse.Namespace(
                job_snapshot=[paths["job-a"], paths["job-b"]],
                preflight=paths["preflight"], exceptions=paths["exceptions"],
                source_assignment=[paths["source-a"], paths["source-b"]],
                materializer_assignment=[paths["materializer-a"], paths["materializer-b"]],
                topic_offsets=paths["offsets"], topic_contract=paths["topic-contract"],
                materializer_inspect=paths["materializer"], database_ready=paths["database"],
                prometheus_targets=paths["targets"], prometheus_up=paths["up"],
                expected_source_topic=["input=4"],
                expected_materializer_topic=["query1=1", "query2=1"],
                expected_empty_topic=["input=4", "query1=1", "query2=1"],
                expected_source_readers=4, expected_taskmanagers=4,
                expected_prometheus_targets=9,
            )
            report = readiness.validate(args)
            self.assertFalse(report["operational_complete"])
            self.assertIn("flink_running_stable", report["failures"])


class AnalysisTest(unittest.TestCase):
    def test_summary_reports_measurements(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "metrics").mkdir()
            documents = {
                "run-status.json": {"version": 1, "run_id": "neutral", "runtime_complete": True},
                "run-profile.json": {"run_size": "small"},
                "run-manifest.json": {
                    "verified_event_count_N": 10_000_000,
                    "phases": {
                        "warmup": {"records": 1_600_000},
                        "measurement": {
                            "records": 8_400_000,
                            "start_epoch_seconds": 1,
                            "end_epoch_seconds": 10,
                        },
                    },
                },
                "correctness-report.json": {
                    "query1": {
                        "expected_records": 2,
                        "observed_records": 2,
                        "expected_digest": "a" * 64,
                        "observed_digest": "a" * 64,
                        "canonical_match": True,
                    },
                    "query2": {
                        "expected_records": 3,
                        "observed_records": 3,
                        "expected_digest": "b" * 64,
                        "observed_digest": "b" * 64,
                        "canonical_match": True,
                    },
                    "completion": {"producer_complete_seen": True},
                },
                "latency-histogram.json": {
                    "timescaledb_materialization_latency_ms": {
                        "count": 5, "p50": 100, "p95": 300, "p99": 450,
                    },
                },
                "latency-attribution.json": {
                    "by_query_and_trigger": {
                        "query1:input": {
                            "total": {"p50_ms": 500, "p95_ms": 700, "p99_ms": 900},
                            "segments": {"trigger_input_queue": {"p99_ms": 20}},
                        },
                        "query2:input": {
                            "total": {"p50_ms": 600, "p95_ms": 800, "p99_ms": 1_000},
                            "segments": {"trigger_input_queue": {"p99_ms": 30}},
                        },
                    },
                },
                "cost-report.json": {"currency": "USD", "total_usd": 0.25},
                "producer-summary.json": {
                    "requested_rate_per_second": 50_000,
                    "source_records_per_second": 49_900,
                    "data_acknowledged": 8_400_000,
                },
                "producer-summary-warmup.json": {"data_acknowledged": 1_600_000},
                "sink-summary.json": {"rows": 5, "duplicate_rows": 0},
                "metrics/flink-job.json": {"state": "RUNNING"},
                "metrics/flink-range.json": {"data": {"result": []}},
                "metrics/flink-checkpoints.json": {
                    "counts": {"completed": 3, "failed": 0, "restored": 0},
                    "history": [
                        {
                            "id": index,
                            "status": "COMPLETED",
                            "trigger_timestamp": index * 1_000,
                            "checkpointed_size": size,
                        }
                        for index, size in enumerate((10_000_000, 7_000_000, 4_000_000), start=1)
                    ],
                },
            }
            for name, value in documents.items():
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(value))
            (root / "metrics/checkpoint-failure-classification.json").write_text(
                json.dumps({"measurement_failures": 0})
            )
            with mock.patch("aws_benchmark.analysis.compact_evidence_valid", return_value=False):
                report = analysis.summarize(root, {"passed": True})
            self.assertEqual(report["latency_ms"]["query1_input_triggered"]["p99_ms"], 900)
            self.assertEqual(report["latency_ms"]["timescaledb_materialization"]["p99"], 450)
            self.assertEqual(report["runtime"]["checkpoints"]["last_five_state_sizes_bytes"], [
                10_000_000, 7_000_000, 4_000_000,
            ])


def producer_phase(start: int, end: int, first_line: int, last_line: int) -> dict[str, object]:
    return {
        "first_acknowledged_epoch_us": start * 1_000_000,
        "last_acknowledged_epoch_us": end * 1_000_000,
        "first_source_line": first_line,
        "last_source_line": last_line,
        "first_event_time": "2013-01-01T00:00:00Z",
        "last_event_time": "2013-01-01T00:01:00Z",
        "topic_partitions": {"taxi.sample-run.trips/0": {"acknowledged": 1}},
    }


class RunManifestTest(unittest.TestCase):
    def test_final_manifest_is_built_from_frozen_and_incremental_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            documents = {
                "run-profile.json": {
                    "run_id": "sample-run",
                    "run_size": "small",
                    "dataset": {"records": 10_000_000},
                    "stages": [
                        {"role": "warmup", "records": 1_600_000, "rate_per_second": 20_000},
                        {"role": "measurement", "records": 8_400_000, "rate_per_second": 50_000},
                    ],
                    "checkpoint": {"timeout_ms": 300_000, "tolerable_failures": 0},
                },
                "runtime-manifest.json": {
                    "phases": {
                        "deployment": {"end_epoch_seconds": 10},
                        "readiness": {"end_epoch_seconds": 20},
                        "warmup": {
                            "start_epoch_seconds": 30,
                            "end_epoch_seconds": 36,
                        },
                        "warmup_barrier": {"end_epoch_seconds": 40},
                    }
                },
                "producer-summary-warmup.json": producer_phase(30, 35, 1, 1_600_000),
                "producer-summary.json": producer_phase(50, 60, 1_600_001, 10_000_000),
            }
            for name, value in documents.items():
                (root / name).write_text(json.dumps(value), encoding="utf-8")
            data_manifest = root / "data-manifest.json"
            data_manifest.write_text(json.dumps({"dataset_id": "dataset"}), encoding="utf-8")

            first = run_manifest.build(
                root, config.RELEASE_PATH, data_manifest, "frozen", "bucket", 70
            )
            second = run_manifest.build(
                root, config.RELEASE_PATH, data_manifest, "frozen", "bucket", 70
            )

            self.assertEqual(first, second)
            self.assertEqual(first["parallelism"]["source"], 4)
            self.assertEqual(first["parallelism"]["route"], 8)
            self.assertEqual(first["boundary_mode"], "coalesced")
            self.assertEqual(first["checkpoint_uri"], "s3://bucket/checkpoints/sample-run")


if __name__ == "__main__":
    unittest.main()
