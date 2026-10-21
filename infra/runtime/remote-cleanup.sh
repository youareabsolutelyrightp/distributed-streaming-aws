#!/usr/bin/env bash
set -uo pipefail

source /etc/taxi/node.env
run_id=${1:?Run ID is required.}
result_dir=${2:?Result directory is required.}
deployment_id=${DEPLOYMENT_ID:?DEPLOYMENT_ID is required.}
region=${AWS_REGION:?AWS_REGION is required.}
bucket=${ARTIFACT_BUCKET:?ARTIFACT_BUCKET is required.}

mapfile -t instance_rows < <(aws ec2 describe-instances --region "$region" \
  --filters "Name=tag:Deployment,Values=$deployment_id" \
    "Name=instance-state-name,Values=pending,running,stopping,stopped" \
  --query 'Reservations[].Instances[].[InstanceId,Tags[?Key==`NodeName`]|[0].Value,Tags[?Key==`Deployment`]|[0].Value]' \
  --output text)
if (( ${#instance_rows[@]} == 0 )); then
  echo "Remote cleanup found no deployment instances." >&2
  exit 1
fi
expected_node_count=$((5 + EXPECTED_TASKMANAGERS))
if (( ${#instance_rows[@]} != expected_node_count )); then
  echo "Remote cleanup expected $expected_node_count instances but found ${#instance_rows[@]}." >&2
  exit 1
fi

install -d -m 0755 "$result_dir/metrics/node" "$result_dir/infrastructure"
instance_ids=()
non_control_instance_ids=()
control_instance_id=""
expected_nodes=()
collection_failures=()
for row in "${instance_rows[@]}"; do
  read -r instance_id node_name observed_deployment <<<"$row"
  if [[ $observed_deployment != "$deployment_id" ]]; then
    echo "Instance $instance_id has the wrong deployment tag." >&2
    exit 1
  fi
  if [[ ! $node_name =~ ^(control|database|kafka-[1-3]|taskmanager-[1-4])$ ]]; then
    echo "Instance $instance_id has an invalid node name." >&2
    exit 1
  fi
  instance_ids+=("$instance_id")
  expected_nodes+=("$node_name")
  if [[ $node_name == control ]]; then
    control_instance_id=$instance_id
    /opt/taxi/release/infra/runtime/capture-system-evidence.sh || true
    install -d -m 0755 "$result_dir/metrics/node/$node_name"
    cp -R /opt/taxi/evidence/. "$result_dir/metrics/node/$node_name/" || true
    continue
  fi
  non_control_instance_ids+=("$instance_id")
  command="/opt/taxi/release/infra/runtime/capture-system-evidence.sh; if [[ '$node_name' == taskmanager-* ]]; then docker logs taxi-taskmanager >/opt/taxi/evidence/taskmanager.log 2>&1 || true; docker inspect taxi-taskmanager >/opt/taxi/evidence/taskmanager-inspect.json 2>/dev/null || true; fi; if [[ '$node_name' == kafka-* ]]; then docker logs taxi-kafka >/opt/taxi/evidence/kafka.log 2>&1 || true; fi; aws s3 cp /opt/taxi/evidence s3://$bucket/node-evidence/$deployment_id/$run_id/$node_name/ --recursive --only-show-errors"
  parameters=$(jq -nc --arg command "$command" '{commands:[$command]}')
  command_id=$(aws ssm send-command --region "$region" --instance-ids "$instance_id" \
    --document-name AWS-RunShellScript --parameters "$parameters" \
    --query Command.CommandId --output text 2>/dev/null || true)
  if [[ -z $command_id || $command_id == None ]]; then
    collection_failures+=("$node_name:ssm_send")
    continue
  fi
  deadline=$((SECONDS + 300))
  while (( SECONDS < deadline )); do
    status=$(aws ssm get-command-invocation --region "$region" \
      --command-id "$command_id" --instance-id "$instance_id" \
      --query Status --output text 2>/dev/null || true)
    [[ $status == Success ]] && break
    [[ $status =~ ^(Failed|Cancelled|TimedOut)$ ]] && break
    sleep 2
  done
  if [[ ${status:-} != Success ]]; then
    collection_failures+=("$node_name:${status:-unknown}")
  fi
done

aws s3 sync "s3://$bucket/node-evidence/$deployment_id/$run_id/" \
  "$result_dir/metrics/node/" --only-show-errors
missing=()
stale=()
for node in "${expected_nodes[@]}"; do
  metrics="$result_dir/metrics/node/$node/node-metrics.jsonl"
  if [[ ! -s $metrics ]]; then
    missing+=("$node")
    continue
  fi
  sample_time=$(tail -1 "$metrics" | jq -r '.timestamp // empty')
  if [[ -z $sample_time ]]; then
    stale+=("$node:missing_timestamp")
    continue
  fi
  sample_epoch=$(date -d "$sample_time" +%s 2>/dev/null || true)
  now_epoch=$(date +%s)
  if [[ -z $sample_epoch || $((now_epoch - sample_epoch)) -gt 180 ]]; then
    stale+=("$node:$sample_time")
  fi
done

aws ec2 describe-instances --region "$region" --instance-ids "${instance_ids[@]}" \
  >"$result_dir/infrastructure/instances.json"
mapfile -t instance_types < <(jq -r '.Reservations[].Instances[].InstanceType' \
  "$result_dir/infrastructure/instances.json" | sort -u)
aws ec2 describe-instance-types --region "$region" --instance-types "${instance_types[@]}" \
  >"$result_dir/infrastructure/instance-types.json"
aws ec2 describe-volumes --region "$region" \
  --filters "Name=attachment.instance-id,Values=$(IFS=,; echo "${instance_ids[*]}")" \
  >"$result_dir/infrastructure/volumes.json"
python3 /opt/taxi/release/aws_benchmark/runtime_tools/system_manifest.py --result "$result_dir" || \
  collection_failures+=("control:system_manifest")
if [[ -s /opt/taxi/data/price-check.json && -s $result_dir/runtime-manifest.json ]]; then
  jq -n --slurpfile price /opt/taxi/data/price-check.json \
    --slurpfile runtime "$result_dir/runtime-manifest.json" \
    --argjson end "$(date +%s)" \
    '{version:1,currency:"USD",measurement:"estimated before local billing reconciliation",
      start_epoch_seconds:$runtime[0].created_epoch_seconds,end_epoch_seconds:$end,
      duration_hours:(($end-$runtime[0].created_epoch_seconds)/3600),
      hourly_usd:$price[0].estimated_hourly_usd,
      estimated_runtime_usd:((($end-$runtime[0].created_epoch_seconds)/3600)*$price[0].estimated_hourly_usd),
      total_usd:((($end-$runtime[0].created_epoch_seconds)/3600)*$price[0].estimated_hourly_usd
        + ($price[0].estimated_s3_storage_usd // 0)),complete:true,
      estimated_s3_storage_usd:($price[0].estimated_s3_storage_usd // 0)}' \
    >"$result_dir/cost-report.json"
else
  collection_failures+=("control:cost_estimate")
fi

jq -n --arg run_id "$run_id" --arg deployment_id "$deployment_id" \
  --argjson instance_ids "$(printf '%s\n' "${instance_ids[@]}" | jq -R . | jq -s .)" \
  '{version:1,run_id:$run_id,deployment_id:$deployment_id,
    instance_ids:$instance_ids,requested_at:(now|todateiso8601),
    tag_validation_passed:true,status:"requested"}' \
  >"$result_dir/cleanup-intent.json"

required_evidence=(
  runtime-manifest.json
  run-manifest.json
  run-status.json
  system-manifest.json
  preflight-report.json
  metrics/flink-range.json
  metrics/flink-checkpoints.json
  metrics/checkpoint-failure-classification.json
  correctness-report.json
  latency-histogram.json
  latency-attribution.json
  raw-transport-trace-manifest.json
  cost-report.json
  cleanup-intent.json
)
missing_files=()
for evidence_path in "${required_evidence[@]}"; do
  [[ -s $result_dir/$evidence_path ]] || missing_files+=("$evidence_path")
done

jq -n --arg run_id "$run_id" --arg deployment_id "$deployment_id" \
  --argjson expected "$(printf '%s\n' "${expected_nodes[@]}" | jq -R . | jq -s .)" \
  --argjson missing "$(printf '%s\n' "${missing[@]:-}" | sed '/^$/d' | jq -R . | jq -s .)" \
  --argjson stale "$(printf '%s\n' "${stale[@]:-}" | sed '/^$/d' | jq -R . | jq -s .)" \
  --argjson required_files "$(printf '%s\n' "${required_evidence[@]}" | jq -R . | jq -s .)" \
  --argjson missing_files "$(printf '%s\n' "${missing_files[@]:-}" | sed '/^$/d' | jq -R . | jq -s .)" \
  --argjson failures "$(printf '%s\n' "${collection_failures[@]:-}" | sed '/^$/d' | jq -R . | jq -s .)" \
  '{version:1,run_id:$run_id,deployment_id:$deployment_id,
    expected_nodes:$expected,missing_nodes:$missing,stale_nodes:$stale,
    required_files:$required_files,missing_files:$missing_files,
    collection_failures:$failures,
    evidence_complete:(($missing|length==0) and ($stale|length==0)
      and ($missing_files|length==0) and ($failures|length==0))}' \
  >"$result_dir/remote-evidence-manifest.json"

compact_files=(
  runtime-manifest.json
  run-manifest.json
  run-status.json
  system-manifest.json
  remote-evidence-manifest.json
  correctness-report.json
  latency-histogram.json
  latency-attribution.json
  latency-10-second-buckets.json
  raw-transport-trace-manifest.json
  trace-analysis-validation.json
  transport-diagnostic-sample.jsonl.zst
  cost-report.json
  metrics/flink-range.json
  metrics/flink-checkpoints.json
  metrics/checkpoint-failure-classification.json
)
compact_manifest="$result_dir/compact-evidence-manifest.json"
compact_rows="$result_dir/compact-evidence-manifest.rows"
: >"$compact_rows"
compact_missing=()
for compact_path in "${compact_files[@]}"; do
  if [[ ! -s $result_dir/$compact_path ]]; then
    compact_missing+=("$compact_path")
    continue
  fi
  compact_size=$(stat -c %s "$result_dir/$compact_path")
  compact_sha=$(sha256sum "$result_dir/$compact_path" | awk '{print $1}')
  jq -nc --arg path "$compact_path" --arg sha256 "$compact_sha" \
    --argjson size_bytes "$compact_size" \
    '{path:$path,sha256:$sha256,size_bytes:$size_bytes}' >>"$compact_rows"
done
jq -n --arg run_id "$run_id" \
  --slurpfile files "$compact_rows" \
  --argjson missing "$(printf '%s\n' "${compact_missing[@]:-}" | sed '/^$/d' | jq -R . | jq -s .)" \
  '{version:1,run_id:$run_id,files:$files,missing:$missing,
    validated:($missing|length==0),created_at:(now|todateiso8601)}' \
  >"$compact_manifest"
rm -f "$compact_rows"

bundle_uploaded=0
if aws s3 cp "$result_dir" "s3://$bucket/results/$run_id/" \
    --recursive --only-show-errors; then
  compact_manifest_sha=$(sha256sum "$compact_manifest" | awk '{print $1}')
  if aws s3 cp "$compact_manifest" \
      "s3://$bucket/results/$run_id/compact-evidence-manifest.json" \
      --only-show-errors --metadata "sha256=$compact_manifest_sha"; then
    uploaded_manifest_sha=$(aws s3api head-object --bucket "$bucket" \
      --key "results/$run_id/compact-evidence-manifest.json" \
      --query 'Metadata.sha256' --output text 2>/dev/null || true)
    [[ $uploaded_manifest_sha == "$compact_manifest_sha" ]] && bundle_uploaded=1
  fi
fi

trace_key=$(jq -r '.s3_key // empty' \
  "$result_dir/raw-transport-trace-manifest.json" 2>/dev/null || true)
trace_delete_ready=$(jq -r '.raw_trace_delete_ready // false' \
  "$result_dir/trace-analysis-validation.json" 2>/dev/null || echo false)
evidence_complete=$(jq -r '.evidence_complete // false' \
  "$result_dir/remote-evidence-manifest.json" 2>/dev/null || echo false)
compact_valid=$(jq -r '.validated // false' "$compact_manifest")
trace_action=retained
if (( bundle_uploaded == 1 )) && [[ $evidence_complete == true \
    && $compact_valid == true && $trace_delete_ready == true \
    && -n $trace_key ]]; then
  if aws s3 rm "s3://$bucket/$trace_key" --only-show-errors; then
    trace_action=deleted_after_validation
  fi
fi
jq -n --arg run_id "$run_id" --arg action "$trace_action" \
  --arg s3_key "$trace_key" --argjson bundle_uploaded "$bundle_uploaded" \
  --argjson evidence_complete "$([[ $evidence_complete == true ]] && echo true || echo false)" \
  --argjson compact_valid "$([[ $compact_valid == true ]] && echo true || echo false)" \
  '{version:1,run_id:$run_id,action:$action,s3_key:$s3_key,
    bundle_uploaded_and_verified:($bundle_uploaded==1),
    evidence_complete:$evidence_complete,compact_bundle_valid:$compact_valid}' \
  >"$result_dir/raw-trace-retention-report.json"
aws s3 cp "$result_dir/raw-trace-retention-report.json" \
  "s3://$bucket/results/$run_id/raw-trace-retention-report.json" \
  --only-show-errors || true

if (( ${#non_control_instance_ids[@]} > 0 )); then
  aws ec2 terminate-instances --region "$region" \
    --instance-ids "${non_control_instance_ids[@]}" \
    >"$result_dir/cleanup-worker-termination-response.json"
fi

control_termination_delay_seconds=60
self_termination_unit="taxi-control-terminate-${run_id//[^a-zA-Z0-9_.-]/-}"
aws_path=$(command -v aws)
if ! systemd-run --quiet --unit "$self_termination_unit" \
    --on-active="${control_termination_delay_seconds}s" \
    "$aws_path" ec2 terminate-instances --region "$region" \
    --instance-ids "$control_instance_id"; then
  aws ec2 terminate-instances --region "$region" \
    --instance-ids "$control_instance_id" >/dev/null 2>&1 || true
  exit 1
fi
jq -n --slurpfile worker "$result_dir/cleanup-worker-termination-response.json" \
  --arg control_instance_id "$control_instance_id" \
  --arg unit "$self_termination_unit" \
  --argjson delay_seconds "$control_termination_delay_seconds" \
  '{worker_response:$worker[0],control_response:{scheduled:true,
    instance_id:$control_instance_id,systemd_unit:$unit,
    delay_seconds:$delay_seconds}}' \
  >"$result_dir/cleanup-termination-response.json"
aws s3 cp "$result_dir/cleanup-termination-response.json" \
  "s3://$bucket/results/$run_id/cleanup-termination-response.json" --only-show-errors || true
