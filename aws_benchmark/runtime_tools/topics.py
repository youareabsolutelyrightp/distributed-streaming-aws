#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


PARTITION_COUNT = re.compile(r"PartitionCount:\s*(\d+)")
REPLICATION_FACTOR = re.compile(r"ReplicationFactor:\s*(\d+)")


@dataclass(frozen=True)
class ExpectedTopic:
    role: str
    name: str
    partitions: int
    replication_factor: int
    minimum_isr: int


def parse_topic(value: str) -> ExpectedTopic:
    parts = value.split("=")
    if len(parts) != 5:
        raise argparse.ArgumentTypeError(
            "topic must be ROLE=NAME=PARTITIONS=REPLICATION_FACTOR=MINIMUM_ISR"
        )
    role, name, partitions, replication_factor, minimum_isr = parts
    return ExpectedTopic(
        role=role,
        name=name,
        partitions=int(partitions),
        replication_factor=int(replication_factor),
        minimum_isr=int(minimum_isr),
    )


def read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def offset_map(text: str) -> dict[int, int]:
    offsets: dict[int, int] = {}
    for line in text.splitlines():
        parts = line.rsplit(":", 2)
        if len(parts) != 3:
            continue
        _, partition, offset = parts
        if partition.isdigit() and offset.lstrip("-").isdigit():
            offsets[int(partition)] = int(offset)
    return offsets


def validate(args: argparse.Namespace) -> dict[str, Any]:
    topic_reports: dict[str, Any] = {}
    failures: list[str] = []
    expected_roles = {"input", "control", "dlq", "query1", "query2"}
    observed_roles = {topic.role for topic in args.topic}
    if observed_roles != expected_roles:
        failures.append("topic_roles")
    input_topic = next((topic for topic in args.topic if topic.role == "input"), None)
    partitioning_valid = (
        input_topic is not None
        and input_topic.partitions == args.input_partition_count
        and args.partitioning_version == "taxi-medallion-partition-v1"
        and args.hash_test_vector_version == "sha256-prefix64-be-v1"
    )
    if not partitioning_valid:
        failures.append("input_partitioning")

    for topic in args.topic:
        description = read(args.capture_dir / f"{topic.role}.describe.txt")
        configs = read(args.capture_dir / f"{topic.role}.configs.txt")
        offsets = offset_map(read(args.capture_dir / f"{topic.role}.offsets.txt"))
        count_match = PARTITION_COUNT.search(description)
        replication_match = REPLICATION_FACTOR.search(description)
        checks = {
            "partition_count": bool(count_match)
            and int(count_match.group(1)) == topic.partitions,
            "replication_factor": bool(replication_match)
            and int(replication_match.group(1)) == topic.replication_factor,
            "minimum_isr": f"min.insync.replicas={topic.minimum_isr}" in configs,
            "cleanup_policy": "cleanup.policy=delete" in configs,
            "retention": f"retention.ms={args.retention_ms}" in configs,
            "timestamp_type": "message.timestamp.type=LogAppendTime" in configs,
            "empty": offsets == {partition: 0 for partition in range(topic.partitions)},
        }
        if not all(checks.values()):
            failures.append(f"topic:{topic.role}")
        topic_reports[topic.role] = {
            "name": topic.name,
            "partitions": topic.partitions,
            "replication_factor": topic.replication_factor,
            "minimum_isr": topic.minimum_isr,
            "cleanup_policy": "delete",
            "retention_ms": args.retention_ms,
            "message_timestamp_type": "LogAppendTime",
            "checks": checks,
        }

    canary_text = read(args.canary_output)
    topic_list = read(args.topic_list_after_canary).splitlines()
    canary_checks = {
        "consumer_reported_log_append_time": "LogAppendTime:" in canary_text,
        "topic_deleted_before_input": args.canary_topic not in topic_list,
    }
    if not all(canary_checks.values()):
        failures.append("timestamp_canary")

    return {
        "version": 1,
        "operational_complete": not failures,
        "correctness_scope": "Kafka run-topic and broker append-time preflight",
        "input_partitioning": {
            "version": args.partitioning_version,
            "hash_test_vector_version": args.hash_test_vector_version,
            "partition_count": args.input_partition_count,
            "verified": partitioning_valid,
        },
        "topics": topic_reports,
        "timestamp_canary": {
            "topic": args.canary_topic,
            "observed": canary_text.strip(),
            "checks": canary_checks,
        },
        "failures": failures,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--capture-dir", type=Path, required=True)
    parser.add_argument("--topic", action="append", type=parse_topic, required=True)
    parser.add_argument("--retention-ms", type=int, default=86_400_000)
    parser.add_argument("--partitioning-version", required=True)
    parser.add_argument("--hash-test-vector-version", required=True)
    parser.add_argument("--input-partition-count", type=int, required=True)
    parser.add_argument("--canary-topic", required=True)
    parser.add_argument("--canary-output", type=Path, required=True)
    parser.add_argument("--topic-list-after-canary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = validate(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if not report["operational_complete"]:
        for failure in report["failures"]:
            print(f"Kafka topic contract failure: {failure}")
        return 1
    print(f"Kafka topic contract passed: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
