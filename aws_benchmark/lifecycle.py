from __future__ import annotations

import base64
import json
import os
import shutil
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .commands import Commands
from .config import ROOT, RunSize, release, sha256_file, versions
from .state import RunState


PROJECT_TAG = "reconstructed-foil-benchmark"


@dataclass
class AwsContext:
    run_id: str
    profile: str
    size: RunSize
    run_root: Path
    commands: Commands

    @property
    def region(self) -> str:
        return str(release()["region"])

    def aws(self, *arguments: str) -> list[str]:
        return ["aws", "--profile", self.profile, "--region", self.region, *arguments]

    def output(self, *arguments: str) -> str:
        return self.commands.output(self.aws(*arguments), cwd=ROOT)

    def terraform(self, *arguments: str) -> None:
        pins = versions()
        credentials = json.loads(self.commands.output(
            ["aws", "configure", "export-credentials", "--profile", self.profile, "--format", "process"],
            cwd=ROOT,
        ))
        environment = {
            "AWS_REGION": self.region,
            "AWS_DEFAULT_REGION": self.region,
            "AWS_ACCESS_KEY_ID": credentials["AccessKeyId"],
            "AWS_SECRET_ACCESS_KEY": credentials["SecretAccessKey"],
            "AWS_SESSION_TOKEN": credentials.get("SessionToken", ""),
            "TF_DATA_DIR": f"/workspace/.runs/{self.run_id}/terraform-data",
        }
        command = [
            "docker", "run", "--rm",
            "-e", "AWS_REGION", "-e", "AWS_DEFAULT_REGION", "-e", "AWS_ACCESS_KEY_ID",
            "-e", "AWS_SECRET_ACCESS_KEY", "-e", "AWS_SESSION_TOKEN", "-e", "TF_DATA_DIR",
            "-v", f"{ROOT}:/workspace", "-w", "/workspace/infra/terraform",
            pins["TERRAFORM_IMAGE"], *arguments,
        ]
        self.commands.run(command, cwd=ROOT, env=environment)

    def variables(self, create_compute: bool, release: dict[str, Any] | None = None) -> list[str]:
        expires = datetime.now(timezone.utc) + timedelta(hours=self.size.safeguard_hours)
        values = {
            "aws_region": self.region,
            "deployment_id": self.run_id,
            "owner_tag": os.environ.get("USER", "benchmark-user"),
            "create_compute": str(create_compute).lower(),
            "release_object_key": release["object_key"] if release else "",
            "release_sha256": release["sha256"] if release else "",
            "deployment_expires_at": expires.replace(microsecond=0).isoformat().replace("+00:00", "Z") if create_compute else "",
            "ttl_hours": str(self.size.safeguard_hours),
        }
        return [f"-var={key}={value}" for key, value in values.items()]


def preflight(context: AwsContext, budget_usd: float) -> dict[str, Any]:
    missing = [name for name in ("aws", "docker", "git") if shutil.which(name) is None]
    if missing:
        raise RuntimeError(f"Required commands are missing: {', '.join(missing)}")
    identity = json.loads(context.output("sts", "get-caller-identity", "--output", "json"))
    selected = release()
    topology = selected["topology"]
    taskmanager_type = str(topology["taskmanager_instance_type"])
    required_vcpus = int(topology["total_vcpus"])
    offering = int(context.output(
        "ec2", "describe-instance-type-offerings", "--location-type", "region",
        "--filters", f"Name=location,Values={context.region}",
        f"Name=instance-type,Values={taskmanager_type}",
        "--query", "length(InstanceTypeOfferings)", "--output", "text",
    ))
    if offering < 1:
        raise RuntimeError(f"{taskmanager_type} is not offered in {context.region}.")
    quota = float(context.output(
        "service-quotas", "get-service-quota", "--service-code", "ec2",
        "--quota-code", "L-1216C47A", "--query", "Quota.Value", "--output", "text",
    ))
    if quota < required_vcpus:
        raise RuntimeError(
            f"EC2 standard-instance quota is {quota:g} vCPUs; {required_vcpus} are required."
        )
    estimated_hours = context.size.safeguard_hours
    hourly = float(release().get("estimated_hourly_usd", 1.35))
    estimate = hourly * estimated_hours
    if estimate > budget_usd:
        raise RuntimeError(f"Worst-case estimate ${estimate:.2f} exceeds budget ${budget_usd:.2f}.")
    return {
        "version": 1, "identity": identity,
        "taskmanager_instance_type": taskmanager_type,
        "taskmanager_instance_type_offered": True,
        "standard_vcpu_quota": quota,
        "required_vcpus": required_vcpus, "estimated_hourly_usd": hourly,
        "worst_case_estimate_usd": estimate, "budget_usd": budget_usd,
    }


def terraform_init(context: AwsContext) -> None:
    state = f"/workspace/.runs/{context.run_id}/terraform.tfstate"
    context.terraform("init", "-input=false", "-reconfigure", f"-backend-config=path={state}")


def apply(context: AwsContext, create_compute: bool, release: dict[str, Any] | None = None) -> None:
    terraform_init(context)
    context.terraform("apply", "-input=false", "-auto-approve", *context.variables(create_compute, release))


def outputs(context: AwsContext) -> dict[str, Any]:
    terraform_data = f"/workspace/.runs/{context.run_id}/terraform-data"
    return json.loads(context.commands.output(
        [
            "docker", "run", "--rm", "-e", "TF_DATA_DIR", "-v", f"{ROOT}:/workspace",
            "-w", "/workspace/infra/terraform", versions()["TERRAFORM_IMAGE"],
            "output", f"-state=/workspace/.runs/{context.run_id}/terraform.tfstate", "-json",
        ], cwd=ROOT, env={"TF_DATA_DIR": terraform_data},
    ))


def upload(context: AwsContext, bucket: str, source: Path, key: str, checksum: str) -> None:
    if sha256_file(source) != checksum:
        raise ValueError(f"Upload source checksum changed: {source}")
    context.commands.run(
        context.aws("s3", "cp", str(source), f"s3://{bucket}/{key}", "--only-show-errors", "--metadata", f"sha256={checksum}"),
        cwd=ROOT,
    )
    head = json.loads(context.output("s3api", "head-object", "--bucket", bucket, "--key", key, "--output", "json"))
    if head.get("Metadata", {}).get("sha256") != checksum:
        raise RuntimeError(f"S3 checksum metadata does not match for {key}.")


def wait_ssm_online(
    context: AwsContext, instance_id: str, *, timeout_seconds: int = 900,
    stable_checks: int = 2,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    consecutive = 0
    while time.monotonic() < deadline:
        response = context.output(
            "ssm", "describe-instance-information", "--filters",
            f"Key=InstanceIds,Values={instance_id}",
            "--query", "InstanceInformationList[0].PingStatus", "--output", "text",
        )
        consecutive = consecutive + 1 if response == "Online" else 0
        if consecutive >= stable_checks:
            return
        time.sleep(5)
    raise TimeoutError(f"Instance did not remain available through SSM: {instance_id}")


def _send_ssm_command(
    context: AwsContext, instance_id: str, command: str, timeout_seconds: int,
) -> str:
    parameters = json.dumps(
        {"commands": [command], "executionTimeout": [str(timeout_seconds)]},
        separators=(",", ":"),
    )
    return context.output(
        "ssm", "send-command", "--instance-ids", instance_id,
        "--document-name", "AWS-RunShellScript", "--parameters", parameters,
        "--timeout-seconds", str(timeout_seconds), "--query", "Command.CommandId",
        "--output", "text",
    )


def undeliverable(result: dict[str, Any]) -> bool:
    return bool(
        result.get("Status") == "Failed"
        and result.get("StatusDetails") == "Undeliverable"
        and int(result.get("ResponseCode", -1)) == -1
    )


def ssm_run(
    context: AwsContext, instance_id: str, command: str, timeout_seconds: int,
    *, state: RunState | None = None, delivery_attempts: int = 3,
    purpose: str = "command",
) -> dict[str, Any]:
    if delivery_attempts < 1:
        raise ValueError("delivery_attempts must be positive.")
    command_id = str(state.details.get("ssm_command_id", "")) if state else ""
    if command_id and state and state.details.get("ssm_instance_id") != instance_id:
        raise RuntimeError("Saved SSM command belongs to a different control instance.")
    attempt_ids = list(state.details.get("ssm_command_attempt_ids", [])) if state else []
    for attempt in range(delivery_attempts):
        if not command_id:
            wait_ssm_online(context, instance_id, timeout_seconds=300, stable_checks=2)
            command_id = _send_ssm_command(context, instance_id, command, timeout_seconds)
            attempt_ids.append(command_id)
            if state:
                state.details["ssm_command_id"] = command_id
                state.details["ssm_command_attempt_ids"] = attempt_ids
                state.details["ssm_instance_id"] = instance_id
                state.details["ssm_command_purpose"] = purpose
                state.details["ssm_command_started_at"] = datetime.now(timezone.utc).isoformat()
                state.save()
                print(
                    f"SSM run={context.run_id} purpose={purpose} "
                    f"command_id={command_id}",
                    flush=True,
                )
        deadline = time.monotonic() + timeout_seconds + 300
        while time.monotonic() < deadline:
            try:
                result = json.loads(context.output(
                    "ssm", "get-command-invocation", "--command-id", command_id,
                    "--instance-id", instance_id, "--output", "json",
                ))
            except subprocess.CalledProcessError:
                time.sleep(2)
                continue
            if result.get("Status") in {"Success", "Failed", "TimedOut", "Cancelled"}:
                if undeliverable(result) and attempt + 1 < delivery_attempts:
                    command_id = ""
                    time.sleep(5)
                    break
                return result
            time.sleep(5)
        else:
            raise TimeoutError(f"SSM command did not finish: {command_id}")
    raise RuntimeError("SSM command remained undeliverable after all delivery attempts.")


def remote_command(profile: dict[str, Any]) -> str:
    encoded = base64.b64encode((json.dumps(profile, sort_keys=True) + "\n").encode()).decode()
    return (
        "set -a && . /etc/taxi/node.env && set +a && "
        "install -d -m 0755 /opt/taxi/run && "
        f"printf '%s' '{encoded}' | base64 -d > /opt/taxi/run/profile.json && "
        "PYTHONPATH=/opt/taxi/release python3 -m aws_benchmark.remote /opt/taxi/run/profile.json"
    )


def cleanup(context: AwsContext) -> None:
    state = context.run_root / "terraform.tfstate"
    if state.exists():
        terraform_init(context)
        context.terraform("destroy", "-input=false", "-auto-approve", *context.variables(False))


def audit(context: AwsContext) -> dict[str, Any]:
    filters = [
        f"Name=tag:Project,Values={PROJECT_TAG}",
        f"Name=tag:Deployment,Values={context.run_id}",
    ]
    instances = json.loads(context.output(
        "ec2", "describe-instances", "--filters", *filters,
        "Name=instance-state-name,Values=pending,running,stopping,stopped", "--output", "json",
    ))
    volumes = json.loads(context.output("ec2", "describe-volumes", "--filters", *filters, "--output", "json"))
    addresses = json.loads(context.output("ec2", "describe-addresses", "--filters", *filters, "--output", "json"))
    nat = json.loads(context.output("ec2", "describe-nat-gateways", "--filter", *filters, "--output", "json"))
    load_balancers = json.loads(context.output("elbv2", "describe-load-balancers", "--output", "json"))
    tagged_load_balancers: list[str] = []
    arns = [item["LoadBalancerArn"] for item in load_balancers.get("LoadBalancers", [])]
    if arns:
        tags = json.loads(context.output(
            "elbv2", "describe-tags", "--resource-arns", *arns, "--output", "json",
        ))
        for item in tags.get("TagDescriptions", []):
            values = {tag["Key"]: tag["Value"] for tag in item.get("Tags", [])}
            if values.get("Project") == PROJECT_TAG and values.get("Deployment") == context.run_id:
                tagged_load_balancers.append(item["ResourceArn"])
    report = {
        "version": 1,
        "remaining_instances": [item["InstanceId"] for reservation in instances.get("Reservations", []) for item in reservation.get("Instances", [])],
        "remaining_volumes": [item["VolumeId"] for item in volumes.get("Volumes", [])],
        "remaining_addresses": [item.get("AllocationId", item.get("PublicIp")) for item in addresses.get("Addresses", [])],
        "remaining_nat_gateways": [item["NatGatewayId"] for item in nat.get("NatGateways", []) if item.get("State") != "deleted"],
        "remaining_load_balancers": tagged_load_balancers,
    }
    report["passed"] = not any(report[key] for key in report if key.startswith("remaining_"))
    return report
