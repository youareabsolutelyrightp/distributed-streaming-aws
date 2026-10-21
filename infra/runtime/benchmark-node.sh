#!/usr/bin/env bash
set -euo pipefail

source /etc/taxi/node.env
source /opt/taxi/release/versions.env

current_phase=profile
fail() {
  printf 'ERROR phase=%s: %s\n' "$current_phase" "$1" >&2
  return 1
}
require() {
  local message=$1
  shift
  "$@" || fail "$message"
}

run_profile_path=${1:?The frozen run profile is required.}
python3 - "$run_profile_path" <<'PY'
import json, sys
from pathlib import Path
sys.path.insert(0, "/opt/taxi/release")
from aws_benchmark.config import validate_profile
validate_profile(json.loads(Path(sys.argv[1]).read_text()))
PY
benchmark_profile_json=$(cat "$run_profile_path")
kind=$(jq -r '.run_size' <<<"$benchmark_profile_json")
run_id=$(jq -r '.run_id' <<<"$benchmark_profile_json")
rate=$(jq -r '[.stages[] | select(.role == "measurement") | .rate_per_second][0]' \
  <<<"$benchmark_profile_json")
record_limit=$(jq -r '.dataset.records' <<<"$benchmark_profile_json")
expected_buffer_timeout_enabled=$(jq -er \
  '.flink["execution.buffer-timeout.enabled"] | select(type == "boolean")' \
  <<<"$benchmark_profile_json")
expected_buffer_timeout_ms=$(jq -er \
  '.flink["execution.buffer-timeout.interval"]
   | capture("^(?<milliseconds>[0-9]+) ms$").milliseconds | tonumber' \
  <<<"$benchmark_profile_json")
if [[ ! $run_id =~ ^[a-z0-9][a-z0-9-]{2,31}$ ]]; then
  echo "Run ID must contain 3 to 32 lowercase letters, numbers, or hyphens." >&2
  exit 2
fi
require "run profile identity changed while loading" \
  test "$(jq -r '.run_id' <<<"$benchmark_profile_json")" = "$run_id"

result_dir="/opt/taxi/results/$run_id"
if [[ -e $result_dir ]]; then
  echo "The run result directory already exists: $result_dir" >&2
  exit 1
fi
mkdir -p "$result_dir/metrics" "$result_dir/provenance"
if [[ -n $run_profile_path ]]; then
  cp "$run_profile_path" "$result_dir/run-profile.json"
fi
exec > >(tee "$result_dir/run-console.log") 2>&1

topic_prefix="taxi.$run_id"
input_topic="$topic_prefix.trips"
control_topic="$topic_prefix.control"
dlq_topic="$topic_prefix.dlq"
query1_topic="$topic_prefix.top-routes"
query2_topic="$topic_prefix.profitable-areas"
timestamp_canary_topic="$topic_prefix.timestamp-canary"
result_partitions=1
release_config=/opt/taxi/release/config/release.json
topic_retention_ms=$(jq -r '.kafka.topic_retention_ms' "$release_config")
flink_group="$run_id-flink"
materializer_group="$run_id-materializer"
checkpoint_uri="s3://$ARTIFACT_BUCKET/checkpoints/$run_id"
job_id=
materializer_started=0
run_completed=0
start_epoch=0
infrastructure_ready_epoch=0
readiness_epoch=0
warmup_start_epoch=0
warmup_end_epoch=0
warmup_barrier_epoch=0
measurement_start_epoch=0
measurement_end_epoch=0
drain_end_epoch=0
evidence_end_epoch=0
flink_monitor_pid=
flink_monitor_stop="$result_dir/flink-monitor.stop"
flink_failure_signal="$result_dir/flink-failure.json"
producer_container="taxi-producer-$run_id"

runtime_manifest="$result_dir/runtime-manifest.json"
jq -n --arg run_id "$run_id" --arg run_kind "$kind" \
  '{version:1,run_id:$run_id,run_kind:$run_kind,environment:"aws",
    status:"initializing",created_epoch_seconds:now,
    phases:{deployment:{status:"started",start_epoch_seconds:now}}}' \
  >"$runtime_manifest"

update_runtime_phase() {
  local phase=$1 status=$2 epoch=${3:-$(date +%s)}
  current_phase=$phase
  local temporary="$runtime_manifest.tmp"
  jq --arg phase "$phase" --arg status "$status" --argjson epoch "$epoch" \
    '.status=$status
     | .updated_epoch_seconds=$epoch
     | .phases[$phase] = ((.phases[$phase] // {})
       + {status:$status}
       + (if $status == "started" then {start_epoch_seconds:$epoch}
          else {end_epoch_seconds:$epoch} end))' \
    "$runtime_manifest" >"$temporary"
  mv "$temporary" "$runtime_manifest"
}

stop_flink_monitor() {
  touch "$flink_monitor_stop"
  if [[ -n $flink_monitor_pid ]]; then
    wait "$flink_monitor_pid" >/dev/null 2>&1 || true
    flink_monitor_pid=
  fi
}

capture_runtime_evidence() {
  local end_epoch=${1:-$(date +%s)}
  install -d -m 0755 "$result_dir/metrics"
  if [[ -n $job_id ]]; then
    curl --silent "http://$CONTROL_ADDRESS:8081/jobs/$job_id" \
      >"$result_dir/metrics/flink-job.json" || true
    curl --silent "http://$CONTROL_ADDRESS:8081/jobs/$job_id/exceptions" \
      >"$result_dir/metrics/flink-exceptions.json" || true
    curl --silent "http://$CONTROL_ADDRESS:8081/jobs/$job_id/checkpoints" \
      >"$result_dir/metrics/flink-checkpoints.json" || true
    if [[ -s $result_dir/metrics/flink-checkpoints.json ]]; then
      mkdir -p "$result_dir/metrics/checkpoint-details" \
        "$result_dir/metrics/checkpoint-subtasks" "$result_dir/metrics/vertex-details"
      mapfile -t vertex_ids < <(jq -r '.vertices[]?.id' "$result_dir/metrics/flink-job.json")
      while IFS= read -r checkpoint_id; do
        curl --silent \
          "http://$CONTROL_ADDRESS:8081/jobs/$job_id/checkpoints/details/$checkpoint_id" \
          >"$result_dir/metrics/checkpoint-details/$checkpoint_id.json" || true
        mkdir -p "$result_dir/metrics/checkpoint-subtasks/$checkpoint_id"
        for vertex_id in "${vertex_ids[@]}"; do
          curl --silent \
            "http://$CONTROL_ADDRESS:8081/jobs/$job_id/checkpoints/details/$checkpoint_id/subtasks/$vertex_id" \
            >"$result_dir/metrics/checkpoint-subtasks/$checkpoint_id/$vertex_id.json" || true
        done
      done < <(jq -r '.history[]? | select(.status == "COMPLETED") | .id' \
        "$result_dir/metrics/flink-checkpoints.json")
      for vertex_id in "${vertex_ids[@]}"; do
        curl --silent "http://$CONTROL_ADDRESS:8081/jobs/$job_id/vertices/$vertex_id" \
          >"$result_dir/metrics/vertex-details/$vertex_id.json" || true
      done
    fi
  fi
  curl --silent "http://$CONTROL_ADDRESS:8081/taskmanagers" \
    >"$result_dir/metrics/flink-taskmanagers.json" || true
  mkdir -p "$result_dir/metrics/taskmanager-details"
  while IFS= read -r taskmanager_id; do
    curl --silent "http://$CONTROL_ADDRESS:8081/taskmanagers/$taskmanager_id" \
      >"$result_dir/metrics/taskmanager-details/$taskmanager_id.json" || true
    curl --silent --get --data-urlencode \
      'get=Status.JVM.Memory.Heap.Max,Status.JVM.Memory.Heap.Used,Status.JVM.Memory.Metaspace.Max,Status.JVM.Memory.Metaspace.Used,Status.Flink.Memory.Managed.Total,Status.Flink.Memory.Managed.Used,Status.JVM.GarbageCollector.All.TimeMsPerSecond,Status.JVM.CPU.Load,Status.JVM.CPU.Time' \
      "http://$CONTROL_ADDRESS:8081/taskmanagers/$taskmanager_id/metrics" \
      >"$result_dir/metrics/taskmanager-details/$taskmanager_id-metrics.json" || true
  done < <(jq -r '.taskmanagers[]?.id' "$result_dir/metrics/flink-taskmanagers.json" 2>/dev/null)
  docker logs taxi-jobmanager >"$result_dir/flink-jobmanager.log" 2>&1 || true
  if (( start_epoch > 0 )); then
    curl --silent --get --data-urlencode \
      'query={__name__=~"flink_.*(watermark|Watermark|buffer|checkpoint|Checkpoint|backPressured|busyTime|idleTime|numRecords|taxi_state|taxi_amplification|taxi_source_partition|taxi_performance|taxi_ranking|Heap|Metaspace|Managed|GarbageCollector|JVM_CPU|mailbox|late_events).*"}' \
      --data-urlencode "start=$start_epoch" --data-urlencode "end=$end_epoch" \
      --data-urlencode 'step=1s' "http://$CONTROL_ADDRESS:9090/api/v1/query_range" \
      >"$result_dir/metrics/flink-range.json" || true
    curl --silent --get \
      --data-urlencode "query=sum(kafka_consumergroup_lag{consumergroup=~\"$run_id.*\"})" \
      --data-urlencode "start=$start_epoch" --data-urlencode "end=$end_epoch" \
      --data-urlencode 'step=1s' "http://$CONTROL_ADDRESS:9090/api/v1/query_range" \
      >"$result_dir/metrics/kafka-lag-range.json" || true
  fi
  curl --silent "http://$CONTROL_ADDRESS:9090/api/v1/targets" \
    >"$result_dir/metrics/prometheus-targets.json" || true
  docker run --rm --network host "$KAFKA_IMAGE" \
    /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "$KAFKA_BROKERS" \
    --group "$flink_group" --describe \
    >"$result_dir/metrics/flink-consumer-group.txt" 2>&1 || true
  docker run --rm --network host "$KAFKA_IMAGE" \
    /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "$KAFKA_BROKERS" \
    --group "$materializer_group" --describe \
    >"$result_dir/metrics/materializer-consumer-group.txt" 2>&1 || true
  docker run --rm --network host "$KAFKA_IMAGE" \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$KAFKA_BROKERS" \
    --describe --under-replicated-partitions \
    >"$result_dir/metrics/kafka-under-replicated-partitions.txt" 2>&1 || true
  docker run --rm --network host "$KAFKA_IMAGE" \
    /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$KAFKA_BROKERS" \
    >"$result_dir/metrics/kafka-broker-request-health.txt" 2>&1 || true
  checkpoint_args=(--checkpoints "$result_dir/metrics/flink-checkpoints.json"
    --runtime "$runtime_manifest"
    --output "$result_dir/metrics/checkpoint-failure-classification.json")
  python3 /opt/taxi/release/aws_benchmark/runtime_tools/checkpoints.py \
    "${checkpoint_args[@]}" || true
}

upload_results() {
  local status=$?
  set +e
  stop_flink_monitor
  if (( status != 0 )); then
    update_runtime_phase evidence_export failed "$(date +%s)" || true
    docker stop "$producer_container" >/dev/null 2>&1 || true
    if docker inspect "$producer_container" >/dev/null 2>&1; then
      docker logs "$producer_container" >"$result_dir/producer.log" 2>&1 || true
    fi
    capture_runtime_evidence "$(date +%s)"
  fi
  if [[ -n $job_id && $run_completed != 1 ]]; then
    docker exec taxi-jobmanager flink cancel "$job_id" >/dev/null 2>&1 || true
  fi
  if (( materializer_started == 1 )); then
    docker logs taxi-materializer >"$result_dir/materializer.log" 2>&1 || true
    docker rm -f taxi-materializer >/dev/null 2>&1 || true
  fi
  if (( status != 0 )); then
    failure_phase=$(jq -r '.status // "unknown"' "$runtime_manifest" 2>/dev/null || echo unknown)
    jq -n --arg run_id "$run_id" --argjson exit_code "$status" \
      --arg phase "$failure_phase" \
      '{version:2,run_id:$run_id,exit_code:$exit_code,
        phase:$phase,category:"runtime_exit",
        evidence_export_attempted:true,cleanup_result:"pending",
        failed_at:(now|todateiso8601)}' >"$result_dir/run-failure.json"
  fi
  python3 /opt/taxi/release/aws_benchmark/runtime_tools/status.py \
    --result "$result_dir" --output "$result_dir/run-status.json" || true
  aws s3 cp "$result_dir" "s3://$ARTIFACT_BUCKET/results/$run_id/" \
    --recursive --only-show-errors || true
  bash /opt/taxi/release/infra/runtime/remote-cleanup.sh "$run_id" "$result_dir" || true
  trap - EXIT
  exit "$status"
}
trap upload_results EXIT

wait_until() {
  local description=$1
  local timeout_seconds=$2
  shift 2
  local deadline=$((SECONDS + timeout_seconds))
  while (( SECONDS <= deadline )); do
    if "$@"; then return 0; fi
    sleep 2
  done
  echo "Timed out while waiting for $description." >&2
  return 1
}

docker pull "$KAFKA_IMAGE"
docker pull "$TIMESCALEDB_IMAGE"

kafka_topics() {
  docker run --rm --network host "$KAFKA_IMAGE" \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$KAFKA_BROKERS" --list \
    >/dev/null 2>&1
}
wait_until "Kafka" 180 kafka_topics
flink_ready() {
  curl --fail --silent "http://$CONTROL_ADDRESS:8081/overview" >/dev/null
}
wait_until "the Flink JobManager" 180 flink_ready
expected_taskmanagers=$(jq -r '.infrastructure.expected_taskmanagers' \
  <<<"$benchmark_profile_json")
taskmanagers_ready() {
  local observed
  observed=$(curl --fail --silent "http://$CONTROL_ADDRESS:8081/taskmanagers" \
    | jq -r '.taskmanagers | length') || return 1
  (( observed >= expected_taskmanagers ))
}
wait_until "$expected_taskmanagers Flink TaskManagers" 180 taskmanagers_ready
infrastructure_ready_epoch=$(date +%s)
update_runtime_phase deployment complete "$infrastructure_ready_epoch"

existing=$(docker run --rm --network host "$KAFKA_IMAGE" \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$KAFKA_BROKERS" --list)
if grep -Fxq "$input_topic" <<<"$existing"; then
  echo "Run-specific topic already exists: $input_topic" >&2
  exit 1
fi
if aws s3 ls "$checkpoint_uri/" | grep -q .; then
  echo "Run-specific checkpoint prefix already exists: $checkpoint_uri" >&2
  exit 1
fi

create_topic() {
  local topic=$1 partitions=$2
  docker run --rm --network host "$KAFKA_IMAGE" \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$KAFKA_BROKERS" \
    --create --topic "$topic" --partitions "$partitions" \
    --replication-factor "$KAFKA_REPLICATION_FACTOR" \
    --config "min.insync.replicas=$KAFKA_MINIMUM_ISR" \
    --config cleanup.policy=delete \
    --config "retention.ms=$topic_retention_ms" \
    --config message.timestamp.type=LogAppendTime
}
create_topic "$input_topic" "$INPUT_PARTITIONS"
create_topic "$control_topic" 1
create_topic "$dlq_topic" "$INPUT_PARTITIONS"
create_topic "$query1_topic" "$result_partitions"
create_topic "$query2_topic" "$result_partitions"

topic_contract_dir="$result_dir/kafka-topic-contract"
mkdir -p "$topic_contract_dir"

capture_topic_contract() {
  local role=$1 topic=$2
  docker run --rm --network host "$KAFKA_IMAGE" \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$KAFKA_BROKERS" \
    --describe --topic "$topic" >"$topic_contract_dir/$role.describe.txt"
  docker run --rm --network host "$KAFKA_IMAGE" \
    /opt/kafka/bin/kafka-configs.sh --bootstrap-server "$KAFKA_BROKERS" \
    --entity-type topics --entity-name "$topic" --describe \
    >"$topic_contract_dir/$role.configs.txt"
  docker run --rm --network host "$KAFKA_IMAGE" \
    /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server "$KAFKA_BROKERS" \
    --topic "$topic" >"$topic_contract_dir/$role.offsets.txt"
}

create_topic "$timestamp_canary_topic" 1
printf '{"probe":"log-append-time"}\n' | docker run --rm -i --network host "$KAFKA_IMAGE" \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server "$KAFKA_BROKERS" \
  --topic "$timestamp_canary_topic" --producer-property acks=all >/dev/null
docker run --rm --network host "$KAFKA_IMAGE" \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server "$KAFKA_BROKERS" \
  --topic "$timestamp_canary_topic" --from-beginning --max-messages 1 \
  --timeout-ms 10000 --formatter-property print.timestamp=true \
  >"$topic_contract_dir/timestamp-canary.txt"
grep -Fq 'LogAppendTime:' "$topic_contract_dir/timestamp-canary.txt"
docker run --rm --network host "$KAFKA_IMAGE" \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$KAFKA_BROKERS" \
  --delete --topic "$timestamp_canary_topic"
timestamp_canary_deleted() {
  ! docker run --rm --network host "$KAFKA_IMAGE" \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$KAFKA_BROKERS" --list \
    | grep -Fxq "$timestamp_canary_topic"
}
wait_until "timestamp canary topic deletion" 60 timestamp_canary_deleted
docker run --rm --network host "$KAFKA_IMAGE" \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$KAFKA_BROKERS" --list \
  >"$topic_contract_dir/topic-list-after-canary.txt"

capture_topic_contract input "$input_topic"
capture_topic_contract control "$control_topic"
capture_topic_contract dlq "$dlq_topic"
capture_topic_contract query1 "$query1_topic"
capture_topic_contract query2 "$query2_topic"
python3 /opt/taxi/release/aws_benchmark/runtime_tools/topics.py \
  --capture-dir "$topic_contract_dir" \
  --topic "input=$input_topic=$INPUT_PARTITIONS=$KAFKA_REPLICATION_FACTOR=$KAFKA_MINIMUM_ISR" \
  --topic "control=$control_topic=1=$KAFKA_REPLICATION_FACTOR=$KAFKA_MINIMUM_ISR" \
  --topic "dlq=$dlq_topic=$INPUT_PARTITIONS=$KAFKA_REPLICATION_FACTOR=$KAFKA_MINIMUM_ISR" \
  --topic "query1=$query1_topic=$result_partitions=$KAFKA_REPLICATION_FACTOR=$KAFKA_MINIMUM_ISR" \
  --topic "query2=$query2_topic=$result_partitions=$KAFKA_REPLICATION_FACTOR=$KAFKA_MINIMUM_ISR" \
  --retention-ms "$topic_retention_ms" \
  --partitioning-version taxi-medallion-partition-v1 \
  --hash-test-vector-version sha256-prefix64-be-v1 \
  --input-partition-count "$INPUT_PARTITIONS" \
  --canary-topic "$timestamp_canary_topic" \
  --canary-output "$topic_contract_dir/timestamp-canary.txt" \
  --topic-list-after-canary "$topic_contract_dir/topic-list-after-canary.txt" \
  --output "$result_dir/kafka-topic-contract-report.json"

attach_kafka_topic_contract() {
  local temporary="$result_dir/run-manifest.json.tmp"
  if [[ -s $result_dir/run-profile.json ]]; then
    jq --slurpfile contract "$result_dir/kafka-topic-contract-report.json" \
      --slurpfile profile "$result_dir/run-profile.json" \
      '.kafka_topic_contract = $contract[0]
       | .run_profile = $profile[0]
       | .profile_sha256 = $profile[0].profile_sha256' \
      "$result_dir/run-manifest.json" >"$temporary"
  else
    jq --slurpfile contract "$result_dir/kafka-topic-contract-report.json" \
      '.kafka_topic_contract = $contract[0]' "$result_dir/run-manifest.json" \
      >"$temporary"
  fi
  mv "$temporary" "$result_dir/run-manifest.json"
}

database_url="postgres://taxi:$DATABASE_PASSWORD@$DATABASE_ADDRESS:5432/taxi?sslmode=disable"
materializer_batch_size=$(jq -r '.materializer.batch_size' "$release_config")
materializer_flush_interval=$(jq -r '.materializer.flush_interval' "$release_config")
materializer_retry_attempts=$(jq -r '.materializer.retry_attempts' "$release_config")
materializer_retry_backoff=$(jq -r '.materializer.retry_backoff' "$release_config")
docker rm -f taxi-materializer >/dev/null 2>&1 || true
docker run -d --name taxi-materializer --network host \
  -e MATERIALIZER_BROKERS="$KAFKA_BROKERS" \
  -e MATERIALIZER_DATABASE_URL="$database_url" \
  -e MATERIALIZER_GROUP_ID="$materializer_group" \
  -e MATERIALIZER_QUERY1_TOPIC="$query1_topic" \
  -e MATERIALIZER_QUERY2_TOPIC="$query2_topic" \
  -e MATERIALIZER_BATCH_SIZE="$materializer_batch_size" \
  -e MATERIALIZER_FLUSH_INTERVAL="$materializer_flush_interval" \
  -e MATERIALIZER_RETRY_ATTEMPTS="$materializer_retry_attempts" \
  -e MATERIALIZER_RETRY_BACKOFF="$materializer_retry_backoff" \
  -e MATERIALIZER_PROGRESS_PATH=/results/materializer-progress.json \
  -v "$result_dir:/results" \
  "$MATERIALIZER_IMAGE"
materializer_started=1

write_latency_evidence() {
  local scope=${1:-measurement}
  local first_ack_us=${2:-0}
  local last_ack_us=${3:-0}
  local latency_file="$result_dir/latency-histogram.json"
  local bucket_file="$result_dir/latency-10-second-buckets.json"
  local phase_filter=
  if [[ $scope == all-run ]]; then
    latency_file="$result_dir/latency-all-run.json"
    bucket_file="$result_dir/latency-all-run-10-second-buckets.json"
  elif [[ $scope != measurement ]]; then
    echo "Unknown latency evidence scope: $scope" >&2
    return 2
  fi
  if (( first_ack_us > 0 && last_ack_us >= first_ack_us )); then
    phase_filter="AND COALESCE(transport_trace->>'trigger_input_append_epoch_us',
        result->>'input_kafka_append_epoch_us')::bigint >= $first_ack_us
      AND COALESCE(transport_trace->>'trigger_input_append_epoch_us',
        result->>'input_kafka_append_epoch_us')::bigint <= $last_ack_us"
  fi
  docker run --rm --network host -e PGPASSWORD="$DATABASE_PASSWORD" \
    "$TIMESCALEDB_IMAGE" psql -h "$DATABASE_ADDRESS" -U taxi -d taxi -Atc "
WITH samples AS (
  SELECT query_id,
    CASE WHEN COALESCE(transport_trace->>'trigger_input_append_epoch_us',
                       result->>'input_kafka_append_epoch_us') IS NOT NULL
      AND result->>'trigger_type' IN ('input','mixed')
      THEN GREATEST(0,extract(epoch FROM kafka_append_time)*1000
        -COALESCE(transport_trace->>'trigger_input_append_epoch_us',
                  result->>'input_kafka_append_epoch_us')::double precision/1000) END AS flink_ms,
    CASE WHEN result->>'input_kafka_append_epoch_us' IS NOT NULL
      AND result->>'input_producer_send_epoch_us' IS NOT NULL
      AND result->>'trigger_type' IN ('input','mixed')
      THEN GREATEST(0,((result->>'input_kafka_append_epoch_us')::double precision
        -(result->>'input_producer_send_epoch_us')::double precision)/1000) END AS kafka_ms,
    GREATEST(0,extract(epoch FROM (materialized_at-kafka_append_time))*1000) AS database_ms
  FROM canonical_results WHERE run_id='$run_id' $phase_filter
), by_query AS (
  SELECT query_id,json_build_object(
    'count',count(flink_ms),'p50',percentile_cont(0.50) WITHIN GROUP (ORDER BY flink_ms),
    'p95',percentile_cont(0.95) WITHIN GROUP (ORDER BY flink_ms),
    'p99',percentile_cont(0.99) WITHIN GROUP (ORDER BY flink_ms)) AS values
  FROM samples GROUP BY query_id
)
SELECT json_build_object(
  'flink_result_latency_ms',json_build_object(
    'count',count(flink_ms),'p50',percentile_cont(0.50) WITHIN GROUP (ORDER BY flink_ms),
    'p95',percentile_cont(0.95) WITHIN GROUP (ORDER BY flink_ms),
    'p99',percentile_cont(0.99) WITHIN GROUP (ORDER BY flink_ms)),
  'flink_result_latency_by_query_ms',
    (SELECT json_object_agg(query_id,values) FROM by_query),
  'kafka_queue_latency_ms',json_build_object(
    'count',count(kafka_ms),'p50',percentile_cont(0.50) WITHIN GROUP (ORDER BY kafka_ms),
    'p95',percentile_cont(0.95) WITHIN GROUP (ORDER BY kafka_ms),
    'p99',percentile_cont(0.99) WITHIN GROUP (ORDER BY kafka_ms)),
  'timescaledb_materialization_latency_ms',json_build_object(
    'count',count(database_ms),'p50',percentile_cont(0.50) WITHIN GROUP (ORDER BY database_ms),
    'p95',percentile_cont(0.95) WITHIN GROUP (ORDER BY database_ms),
    'p99',percentile_cont(0.99) WITHIN GROUP (ORDER BY database_ms))) FROM samples;" \
    >"$latency_file"

  docker run --rm --network host -e PGPASSWORD="$DATABASE_PASSWORD" \
    "$TIMESCALEDB_IMAGE" psql -h "$DATABASE_ADDRESS" -U taxi -d taxi -Atc "
WITH samples AS (
  SELECT query_id,
    to_timestamp(floor(COALESCE(transport_trace->>'trigger_input_append_epoch_us',
                                result->>'input_kafka_append_epoch_us')::numeric
      / 10000000) * 10) AS bucket_start,
    GREATEST(0,extract(epoch FROM kafka_append_time)*1000
      -COALESCE(transport_trace->>'trigger_input_append_epoch_us',
                result->>'input_kafka_append_epoch_us')::double precision/1000) AS flink_ms
  FROM canonical_results WHERE run_id='$run_id'
    AND COALESCE(transport_trace->>'trigger_input_append_epoch_us',
                 result->>'input_kafka_append_epoch_us') IS NOT NULL
    AND result->>'trigger_type' IN ('input','mixed')
    $phase_filter
), summaries AS (
  SELECT bucket_start,coalesce(query_id,'combined') AS query_id,count(*) AS count,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY flink_ms) AS p50,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY flink_ms) AS p95,
    percentile_cont(0.99) WITHIN GROUP (ORDER BY flink_ms) AS p99
  FROM samples GROUP BY GROUPING SETS ((bucket_start,query_id),(bucket_start))
)
SELECT coalesce(json_agg(row_to_json(summaries)
  ORDER BY bucket_start,query_id),'[]'::json) FROM summaries;" \
    >"$bucket_file"
}

write_latency_attribution_evidence() {
  local expected_count=${1:-}
  local trace_root="/opt/taxi/temporary/$run_id"
  local raw_trace="$trace_root/transport-trace-v3.tsv"
  local compressed_trace="$trace_root/transport-trace-v3.tsv.zst"
  local trace_key="temporary/$run_id/transport-trace-v3.tsv.zst"
  local trace_manifest="$result_dir/raw-transport-trace-manifest.json"
  local trace_head="$result_dir/raw-transport-trace-s3-head.json"
  local sample="$result_dir/transport-diagnostic-sample.jsonl"
  local sample_compressed="$result_dir/transport-diagnostic-sample.jsonl.zst"
  local raw_sha compressed_sha observed_count source_revision sample_rows temporary latency_schema
  install -d -m 0755 "$trace_root"
  if [[ -z $expected_count ]]; then
    expected_count=$(docker run --rm --network host -e PGPASSWORD="$DATABASE_PASSWORD" \
      "$TIMESCALEDB_IMAGE" psql -h "$DATABASE_ADDRESS" -U taxi -d taxi -Atc \
      "SELECT count(*) FROM canonical_results WHERE run_id='$run_id';")
  fi
  docker run --rm --network host -e PGPASSWORD="$DATABASE_PASSWORD" \
    "$TIMESCALEDB_IMAGE" psql -q -h "$DATABASE_ADDRESS" -U taxi -d taxi -c "
COPY (
  SELECT query_id,
    output_id,
    to_char(effective_time AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),
    COALESCE(result->>'trigger_type',''),
    COALESCE(transport_trace->>'transport_measurement_version','0'),
    COALESCE(transport_trace->>'trigger_flags','0'),
    COALESCE(transport_trace->>'trigger_input_partition','-1'),
    COALESCE(transport_trace->>'trigger_input_source_line','0'),
    COALESCE(transport_trace->>'boundary_first_input_append_epoch_us','0'),
    COALESCE(transport_trace->>'trigger_input_append_epoch_us','0'),
    COALESCE(transport_trace->>'trigger_input_source_read_epoch_us','0'),
    COALESCE(transport_trace->>'maximum_required_source_read_epoch_us','0'),
    COALESCE(transport_trace->>'partition_boundary_emitted_epoch_us','0'),
    COALESCE(transport_trace->>'shared_boundary_emitted_epoch_us','0'),
    COALESCE(transport_trace->>'boundary_signal_arrival_epoch_us','0'),
    COALESCE(transport_trace->>'watermark_closed_epoch_us','0'),
    COALESCE(transport_trace->>'global_boundary_started_epoch_us','0'),
    COALESCE(transport_trace->>'result_emitted_epoch_us','0'),
    COALESCE(serialization_started_epoch_us,0),
    COALESCE(serialization_completed_epoch_us,0),
    floor(extract(epoch FROM kafka_append_time)*1000000)::bigint
  FROM canonical_results
  WHERE run_id='$run_id'
) TO STDOUT WITH (FORMAT text, DELIMITER E'\\t', NULL '');" >"$raw_trace"
  observed_count=$(wc -l <"$raw_trace" | tr -d ' ')
  require "raw Transport trace row count does not match canonical results" \
    test "$observed_count" -eq "$expected_count"
  raw_sha=$(sha256sum "$raw_trace" | awk '{print $1}')
  zstd -q -T0 -f "$raw_trace" -o "$compressed_trace"
  compressed_sha=$(sha256sum "$compressed_trace" | awk '{print $1}')
  aws s3 cp "$compressed_trace" "s3://$ARTIFACT_BUCKET/$trace_key" \
    --only-show-errors \
    --metadata "sha256=$compressed_sha,uncompressed-sha256=$raw_sha,row-count=$observed_count"
  aws s3api head-object --bucket "$ARTIFACT_BUCKET" --key "$trace_key" \
    >"$trace_head"
  source_revision=$(jq -r '.source_revision // "unknown"' \
    /opt/taxi/release/release-manifest.json)
  python3 /opt/taxi/release/aws_benchmark/runtime_tools/trace_manifest.py \
    --run-id "$run_id" --raw "$raw_trace" --compressed "$compressed_trace" \
    --expected-rows "$expected_count" \
    --analyzer /opt/taxi/release/aws_benchmark/runtime_tools/latency.py \
    --source-revision "$source_revision" --s3-key "$trace_key" \
    --s3-head "$trace_head" --output "$trace_manifest" >/dev/null
  python3 /opt/taxi/release/aws_benchmark/runtime_tools/contracts.py \
    --schema raw-transport-trace-v1 --input "$trace_manifest" >/dev/null
  rm -f "$raw_trace"
  zstd -q -dc "$compressed_trace" \
    | python3 /opt/taxi/release/aws_benchmark/runtime_tools/latency.py \
      --transport-tsv - --expected-records "$expected_count" \
      --trace-manifest "$trace_manifest" --diagnostic-sample "$sample" \
      --output "$result_dir/latency-attribution.json" \
      --checkpoints "$result_dir/metrics/flink-checkpoints.json"
  latency_schema="latency-attribution-v$(jq -r '.schema_version' \
    "$result_dir/latency-attribution.json")"
  python3 /opt/taxi/release/aws_benchmark/runtime_tools/contracts.py \
    --schema "$latency_schema" \
    --input "$result_dir/latency-attribution.json" >/dev/null
  sample_rows=$(wc -l <"$sample" | tr -d ' ')
  require "Transport diagnostic sample is empty" test "$sample_rows" -gt 0
  require "Transport diagnostic sample exceeds 100 rows" test "$sample_rows" -le 100
  zstd -q -T0 -f "$sample" -o "$sample_compressed"
  rm -f "$sample"
  temporary="$trace_manifest.tmp"
  jq --argjson rows "$sample_rows" \
    '.analysis_passed=true | .diagnostic_sample_rows=$rows' \
    "$trace_manifest" >"$temporary"
  mv "$temporary" "$trace_manifest"
  temporary="$result_dir/latency-attribution.json.tmp"
  jq '.raw_trace_evidence.analysis_passed=true' \
    "$result_dir/latency-attribution.json" >"$temporary"
  mv "$temporary" "$result_dir/latency-attribution.json"
  python3 /opt/taxi/release/aws_benchmark/runtime_tools/contracts.py \
    --schema raw-transport-trace-v1 --input "$trace_manifest" >/dev/null
  python3 /opt/taxi/release/aws_benchmark/runtime_tools/contracts.py \
    --schema "$latency_schema" \
    --input "$result_dir/latency-attribution.json" >/dev/null
  jq -n --arg run_id "$run_id" --arg s3_key "$trace_key" \
    --argjson rows "$observed_count" --argjson sample_rows "$sample_rows" \
    '{version:1,run_id:$run_id,analysis_passed:true,schemas_passed:true,
      raw_trace_rows:$rows,diagnostic_sample_rows:$sample_rows,s3_key:$s3_key,
      raw_trace_delete_ready:true}' \
    >"$result_dir/trace-analysis-validation.json"
}

source_parallelism=$(jq -r '.infrastructure.source_parallelism' \
  <<<"$benchmark_profile_json")
route_parallelism=$(jq -r '.infrastructure.processing_parallelism' \
  <<<"$benchmark_profile_json")
ranking_parallelism=$route_parallelism
ranking_shards=$(jq -r '.infrastructure.ranking_virtual_shards' \
  <<<"$benchmark_profile_json")
ranking_max_parallelism=$(jq -r '.infrastructure.ranking_max_parallelism' \
  <<<"$benchmark_profile_json")
taxi_parallelism=$route_parallelism
cell_parallelism=$route_parallelism
task_slots=$(jq -r '.infrastructure.task_slots_per_taskmanager' \
  <<<"$benchmark_profile_json")
expected_allocated_slots=$(jq -r '.infrastructure.expected_allocated_slots' \
  <<<"$benchmark_profile_json")
profile_input_partitions=$(jq -r '.infrastructure.input_partitions' \
  <<<"$benchmark_profile_json")
require "run profile input partitions differ from the frozen node configuration" \
  test "$profile_input_partitions" -eq "$INPUT_PARTITIONS"
checkpoint_interval=$(jq -r '.checkpoint.interval_ms' <<<"$benchmark_profile_json")
profile_warmup_records=$(jq -r \
  '[.stages[] | select(.role == "warmup") | .records][0] // 0' \
  <<<"$benchmark_profile_json")
profile_warmup_rate=$(jq -r \
  '[.stages[] | select(.role == "warmup") | .rate_per_second][0] // 0' \
  <<<"$benchmark_profile_json")
warmup_max_watermark_lag=$(jq -r \
  '.stream.warmup_maximum_watermark_lag_ms' <<<"$benchmark_profile_json")
alignment_drift=$(jq -r '.stream.watermark_alignment_max_drift_ms' \
  <<<"$benchmark_profile_json")
alignment_update=$(jq -r '.stream.watermark_alignment_update_ms' \
  <<<"$benchmark_profile_json")
boundary_mode=$(jq -r '.stream.shared_boundary_mode' <<<"$benchmark_profile_json")
require "run profile does not select Transport v3" \
  test "$(jq -r '.stream.transport_version' <<<"$benchmark_profile_json")" -eq 3
checkpoint_timeout=$(jq -r '.checkpoint.timeout_ms' <<<"$benchmark_profile_json")
tolerable_checkpoint_failures=$(jq -r '.checkpoint.tolerable_failures // 0' \
  <<<"$benchmark_profile_json")

submission=$(docker exec \
  -e TAXI_BOOTSTRAP_SERVERS="$KAFKA_BROKERS" \
  -e TAXI_INPUT_TOPIC="$input_topic" -e TAXI_OUTPUT_TOPIC="$query1_topic" \
  -e TAXI_QUERY2_OUTPUT_TOPIC="$query2_topic" -e TAXI_DLQ_TOPIC="$dlq_topic" \
  -e TAXI_CONTROL_TOPIC="$control_topic" -e TAXI_GROUP_ID="$flink_group" \
  -e TAXI_CHECKPOINT_DIRECTORY="$checkpoint_uri" \
  -e TAXI_CHECKPOINT_INTERVAL_MS="$checkpoint_interval" \
  -e TAXI_CHECKPOINT_TIMEOUT_MS="$checkpoint_timeout" \
  -e TAXI_TOLERABLE_CHECKPOINT_FAILURES="$tolerable_checkpoint_failures" \
  -e TAXI_SOURCE_PARALLELISM="$source_parallelism" \
  -e TAXI_ROUTE_PARALLELISM="$route_parallelism" \
  -e TAXI_RANKING_PARALLELISM="$ranking_parallelism" \
  -e TAXI_RANKING_SHARDS="$ranking_shards" \
  -e TAXI_RANKING_MAX_PARALLELISM="$ranking_max_parallelism" \
  -e TAXI_QUERY2_TAXI_PARALLELISM="$taxi_parallelism" \
  -e TAXI_QUERY2_CELL_PARALLELISM="$cell_parallelism" \
  -e TAXI_QUERY2_RANKING_PARALLELISM="$ranking_parallelism" \
  -e TAXI_QUERY2_RANKING_SHARDS="$ranking_shards" \
  -e TAXI_BOUNDARY_PARALLELISM="$ranking_parallelism" \
  -e TAXI_WATERMARK_ALIGNMENT_MAX_DRIFT_MS="$alignment_drift" \
  -e TAXI_WATERMARK_ALIGNMENT_UPDATE_MS="$alignment_update" \
  taxi-jobmanager flink run --detached -c dev.portfolio.taxi.TaxiQueriesJob \
  /opt/flink/usrlib/taxi-flink.jar)
job_id=$(grep -Eo '[[:xdigit:]]{32}' <<<"$submission" | tail -n 1)
require "Flink submission did not return a valid job ID" \
  grep -Eq '^[[:xdigit:]]{32}$' <<<"$job_id"
printf '%s\n' "$job_id" >"$result_dir/flink-job-id.txt"

job_running() {
  curl --fail --silent "http://$CONTROL_ADDRESS:8081/jobs/$job_id" | grep -q '"state":"RUNNING"'
}
wait_until "the Flink job" 120 job_running

preflight_command=(python3 /opt/taxi/release/aws_benchmark/runtime_tools/placement.py \
    --rest-url "http://$CONTROL_ADDRESS:8081" --job-id "$job_id" \
    --expected-taskmanagers "$expected_taskmanagers" --expected-slots "$task_slots" \
    --expected-allocated-slots "$expected_allocated_slots" \
    --require-source-spread \
    --require-processing-spread \
    --input-partitions "$INPUT_PARTITIONS" --source-parallelism "$source_parallelism" \
    --shared-boundary-mode "$boundary_mode" \
    --state-backend hashmap --output "$result_dir/preflight-report.json")
  preflight_ready() {
    "${preflight_command[@]}" >"$result_dir/preflight-attempt.txt" 2>&1
  }
  wait_until "the Flink memory and placement preflight" 60 preflight_ready
"${preflight_command[@]}"

start_flink_monitor() {
  rm -f "$flink_monitor_stop" "$flink_failure_signal"
  (
    unavailable_count=0
    nonrunning_since=0
    while [[ ! -e $flink_monitor_stop ]]; do
      local_response=$(curl --silent "http://$CONTROL_ADDRESS:8081/jobs/$job_id" || true)
      local_state=$(jq -r '.state // "UNAVAILABLE"' <<<"$local_response" 2>/dev/null || echo UNAVAILABLE)
      jq -n --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg state "$local_state" \
        '{observed_at:$at,state:$state}' >>"$result_dir/metrics/flink-state-monitor.jsonl"
      if [[ $local_state == UNAVAILABLE ]]; then
        unavailable_count=$((unavailable_count + 1))
        if (( unavailable_count < 3 )); then
          sleep 5
          continue
        fi
      else
        unavailable_count=0
      fi
      if [[ $local_state != RUNNING ]]; then
        jq -n --arg at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg state "$local_state" \
          '{failed_at:$at,state:$state}' >"$flink_failure_signal"
        docker stop "$producer_container" >/dev/null 2>&1 || true
        exit 1
      fi
      sleep 5
    done
  ) &
  flink_monitor_pid=$!
}

capture_consumer_assignment() {
  local group=$1 output=$2
  docker run --rm --network host "$KAFKA_IMAGE" \
    /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "$KAFKA_BROKERS" \
    --group "$group" --describe --members --verbose >"$output" 2>&1
}

capture_flink_source_assignment() {
  local output=$1
  local source_vertex
  source_vertex=$(curl --fail --silent "http://$CONTROL_ADDRESS:8081/jobs/$job_id" \
    | jq -er '[.vertices[]
      | select(.name | startswith("Source: Shared taxi Kafka source"))]
      | select(length == 1) | .[0].id') || return 1
  curl --fail --silent \
    "http://$CONTROL_ADDRESS:8081/jobs/$job_id/vertices/$source_vertex" \
    | jq -e --arg topic "$input_topic" --argjson expected "$source_parallelism" '
      select(.parallelism == $expected)
      | select(all(.subtasks[]; .status == "RUNNING"))
      | {
          assignment_source: "flink_source_reader_subtasks",
          assignments: {($topic): [.subtasks[].subtask] | sort},
          readers: [.subtasks[] | {
            subtask,
            taskmanager_id: ."taskmanager-id",
            status
          }]
        }
    ' >"$output"
}

capture_readiness() {
  local readiness_dir="$result_dir/readiness"
  mkdir -p "$readiness_dir"
  curl --fail --silent "http://$CONTROL_ADDRESS:8081/jobmanager/config" \
    >"$readiness_dir/flink-configuration.json" || return 1
  jq -e --arg expected "$expected_buffer_timeout_enabled" \
    'any(.[]; .key == "execution.buffer-timeout.enabled"
      and (.value | ascii_downcase) == $expected)' \
    "$readiness_dir/flink-configuration.json" >/dev/null || return 1
  jq -e --argjson expected "$expected_buffer_timeout_ms" \
    'any(.[]; .key == "execution.buffer-timeout.interval"
      and ((.value | capture("^\\s*(?<milliseconds>[0-9]+)\\s*ms\\s*$")
        | .milliseconds | tonumber) == $expected))' \
    "$readiness_dir/flink-configuration.json" >/dev/null || return 1
  curl --fail --silent "http://$CONTROL_ADDRESS:8081/jobs/$job_id" \
    >"$readiness_dir/flink-job-1.json" || return 1
  sleep 5
  curl --fail --silent "http://$CONTROL_ADDRESS:8081/jobs/$job_id" \
    >"$readiness_dir/flink-job-2.json" || return 1
  curl --fail --silent "http://$CONTROL_ADDRESS:8081/jobs/$job_id/exceptions" \
    >"$readiness_dir/flink-exceptions.json" || return 1
  for sample in 1 2 3; do
    capture_flink_source_assignment \
      "$readiness_dir/source-assignment-$sample.json" || return 1
    capture_consumer_assignment "$materializer_group" \
      "$readiness_dir/materializer-assignment-$sample.txt" || return 1
    if (( sample < 3 )); then sleep 2; fi
  done
  : >"$readiness_dir/topic-offsets.txt"
  for topic in "$input_topic" "$control_topic" "$dlq_topic" \
    "$query1_topic" "$query2_topic"; do
    docker run --rm --network host "$KAFKA_IMAGE" \
      /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server "$KAFKA_BROKERS" \
      --topic "$topic" >>"$readiness_dir/topic-offsets.txt" || return 1
  done
  docker inspect taxi-materializer >"$readiness_dir/materializer-inspect.json" || return 1
  docker run --rm --network host -e PGPASSWORD="$DATABASE_PASSWORD" \
    "$TIMESCALEDB_IMAGE" psql -h "$DATABASE_ADDRESS" -U taxi -d taxi -Atc \
    'SELECT 1;' >"$readiness_dir/database-ready.txt" || return 1
  curl --fail --silent "http://$CONTROL_ADDRESS:9090/api/v1/targets" \
    >"$readiness_dir/prometheus-targets.json" || return 1
  curl --fail --silent --get --data-urlencode 'query=up' \
    "http://$CONTROL_ADDRESS:9090/api/v1/query" \
    >"$readiness_dir/prometheus-up.json" || return 1
  python3 /opt/taxi/release/aws_benchmark/runtime_tools/readiness.py \
    --job-snapshot "$readiness_dir/flink-job-1.json" \
    --job-snapshot "$readiness_dir/flink-job-2.json" \
    --preflight "$result_dir/preflight-report.json" \
    --exceptions "$readiness_dir/flink-exceptions.json" \
    --source-assignment "$readiness_dir/source-assignment-1.json" \
    --source-assignment "$readiness_dir/source-assignment-2.json" \
    --source-assignment "$readiness_dir/source-assignment-3.json" \
    --materializer-assignment "$readiness_dir/materializer-assignment-1.txt" \
    --materializer-assignment "$readiness_dir/materializer-assignment-2.txt" \
    --materializer-assignment "$readiness_dir/materializer-assignment-3.txt" \
    --topic-offsets "$readiness_dir/topic-offsets.txt" \
    --materializer-inspect "$readiness_dir/materializer-inspect.json" \
    --database-ready "$readiness_dir/database-ready.txt" \
    --prometheus-targets "$readiness_dir/prometheus-targets.json" \
    --prometheus-up "$readiness_dir/prometheus-up.json" \
    --topic-contract "$result_dir/kafka-topic-contract-report.json" \
    --expected-source-topic "$input_topic=$INPUT_PARTITIONS" \
    --expected-materializer-topic "$query1_topic=$result_partitions" \
    --expected-materializer-topic "$query2_topic=$result_partitions" \
    --expected-empty-topic "$input_topic=$INPUT_PARTITIONS" \
    --expected-empty-topic "$control_topic=1" \
    --expected-empty-topic "$dlq_topic=$INPUT_PARTITIONS" \
    --expected-empty-topic "$query1_topic=$result_partitions" \
    --expected-empty-topic "$query2_topic=$result_partitions" \
    --expected-source-readers "$source_parallelism" \
    --expected-taskmanagers "$expected_taskmanagers" \
    --expected-prometheus-targets "$((expected_taskmanagers + 2))" \
    --output "$result_dir/readiness-report.json"
}

start_flink_monitor
readiness_ready() {
  capture_readiness >"$result_dir/readiness-attempt.txt" 2>&1
}
wait_until "stable benchmark readiness" 30 readiness_ready
capture_readiness
readiness_epoch=$(date +%s)
update_runtime_phase readiness complete "$readiness_epoch"

input=/opt/taxi/data/prepared.csv.zst
data_manifest=/opt/taxi/data/manifest.json
reference=/opt/taxi/data/reference.json
limit=$record_limit
producer_rate=$rate
require "prepared benchmark input is missing" test -s "$input"
require "prepared data manifest is missing" test -s "$data_manifest"
require "frozen reference is missing" test -s "$reference"
maximum_event_time=$(jq -r '.dataset.maximum_event_time' <<<"$benchmark_profile_json")
dataset_id=$(jq -r '.dataset_id' "$data_manifest")
verified_n=$record_limit

warmup_records=$profile_warmup_records
warmup_rate=$profile_warmup_rate
measurement_records=$((limit - warmup_records))

consumer_group_lag_total() {
  local group=$1 output lag_values
  output=$(docker run --rm --network host "$KAFKA_IMAGE" \
    /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "$KAFKA_BROKERS" \
    --group "$group" --describe 2>/dev/null) || return 1
  lag_values=$(awk 'NR > 2 && $1 != "" && $6 != "-" {print $6}' <<<"$output")
  [[ -n $lag_values ]] || return 1
  awk '{sum += $1} END {print sum + 0}' <<<"$lag_values"
}

consumer_group_lag_zero() {
  local total
  total=$(consumer_group_lag_total "$1") || return 1
  [[ $total == 0 ]]
}

checkpoint_request_complete() {
  curl --fail --silent \
    "http://$CONTROL_ADDRESS:8081/jobs/$job_id/checkpoints/$1" \
    | grep -q '"id":"COMPLETED"'
}

prometheus_scalar() {
  local query=$1 value
  value=$(curl --fail --silent --get --data-urlencode "query=$query" \
    "http://$CONTROL_ADDRESS:9090/api/v1/query" \
    | jq -er '.data.result[0].value[1]') || return 1
  printf '%s\n' "$value"
}

capture_warmup_barrier_inputs() {
  local samples="$result_dir/warmup-barrier-samples.jsonl"
  : >"$samples"
  for sample in 1 2 3; do
    local accepted watermark buffered oldest late
    accepted=$(prometheus_scalar \
      'sum(flink_taskmanager_job_task_operator_taxi_amplification_stage_records_total{stage="A_source_trips"})') || return 1
    watermark=$(prometheus_scalar \
      'min(flink_taskmanager_job_task_operator_query1_global_watermark)') || return 1
    buffered=$(prometheus_scalar \
      'sum(flink_taskmanager_job_task_operator_taxi_state_stage_buffered_event_count)') || return 1
    oldest=$(prometheus_scalar \
      'min(flink_taskmanager_job_task_operator_taxi_state_stage_oldest_pending_boundary_event_time_ms)') || return 1
    late=$(prometheus_scalar \
      'sum(flink_taskmanager_job_task_operator_query1_late_events)') || return 1
    jq -cn --argjson accepted "$accepted" --argjson watermark "$watermark" \
      --argjson buffered "$buffered" --argjson oldest "$oldest" \
      --argjson late "$late" \
      '{accepted_source_records:$accepted,global_watermark_ms:$watermark,
        buffered_events:$buffered,oldest_pending_boundary_event_time_ms:$oldest,
        late_events:$late}' >>"$samples"
    if (( sample < 3 )); then sleep 2; fi
  done
  consumer_group_lag_total "$materializer_group" \
    >"$result_dir/warmup-materializer-lag.txt" || return 1
  docker run --rm --network host "$KAFKA_IMAGE" \
    /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server "$KAFKA_BROKERS" \
    --topic "$dlq_topic" | awk -F: '{sum += $3} END {print sum + 0}' \
    >"$result_dir/warmup-dlq-records.txt" || return 1
  curl --fail --silent "http://$CONTROL_ADDRESS:8081/jobs/$job_id/checkpoints" \
    >"$result_dir/warmup-checkpoints.json" || return 1
}

validate_warmup_barrier() {
  capture_warmup_barrier_inputs || return 1
  python3 /opt/taxi/release/aws_benchmark/runtime_tools/warmup.py \
    --producer-summary "$result_dir/producer-summary-warmup.json" \
    --samples "$result_dir/warmup-barrier-samples.jsonl" \
    --checkpoints "$result_dir/warmup-checkpoints.json" \
    --materializer-lag "$result_dir/warmup-materializer-lag.txt" \
    --dlq-records "$result_dir/warmup-dlq-records.txt" \
    --expected-warmup-records "$warmup_records" \
    --maximum-watermark-lag-ms "$warmup_max_watermark_lag" \
    --output "$result_dir/warmup-barrier-report.json" >/dev/null 2>&1
}

start_epoch=$(date +%s)
warmup_start_epoch=$(date +%s)
update_runtime_phase warmup started "$warmup_start_epoch"
docker rm -f "$producer_container" >/dev/null 2>&1 || true
docker run -d --name "$producer_container" --network host \
  -v "$input:/data/input.csv.zst:ro" \
  -v "$data_manifest:/data/data-manifest.json:ro" \
  -v "$result_dir:/results" "$PRODUCER_IMAGE" replay \
  --brokers "$KAFKA_BROKERS" --manifest /data/data-manifest.json \
  --input /data/input.csv.zst --run-id "$run_id" --input-topic "$input_topic" \
  --input-partitions "$INPUT_PARTITIONS" --control-topic "$control_topic" \
  --dlq-topic "$dlq_topic" --limit "$limit" \
  --checkpoint /results/producer-checkpoint.json \
  --summary /results/producer-summary-warmup.json \
  --progress /results/producer-progress-warmup.jsonl \
  --progress-phase warmup --rate "$warmup_rate" \
  --pause-after-record "$warmup_records" >/dev/null
producer_exit=$(docker wait "$producer_container")
docker logs "$producer_container" >"$result_dir/producer-warmup.log" 2>&1 || true
docker rm "$producer_container" >/dev/null 2>&1 || true
require "warm-up producer did not stop at the planned pause" \
  test "$producer_exit" -eq 75
require "Flink failed during warm-up" test ! -s "$flink_failure_signal"
warmup_end_epoch=$(date +%s)
update_runtime_phase warmup input_complete "$warmup_end_epoch"
wait_until "the drained non-terminal warm-up barrier" 150 validate_warmup_barrier
validate_warmup_barrier
curl --fail --silent -X POST -H 'Content-Type: application/json' \
  -d '{"checkpointType":"FULL"}' \
  "http://$CONTROL_ADDRESS:8081/jobs/$job_id/checkpoints" \
  >"$result_dir/warmup-barrier-checkpoint-trigger.json"
barrier_trigger_id=$(jq -r '."request-id"' \
  "$result_dir/warmup-barrier-checkpoint-trigger.json")
wait_until "the warm-up barrier checkpoint" 180 \
  checkpoint_request_complete "$barrier_trigger_id"
curl --fail --silent \
  "http://$CONTROL_ADDRESS:8081/jobs/$job_id/checkpoints/$barrier_trigger_id" \
  >"$result_dir/warmup-barrier-checkpoint.json"
capture_warmup_barrier_inputs
python3 /opt/taxi/release/aws_benchmark/runtime_tools/warmup.py \
  --producer-summary "$result_dir/producer-summary-warmup.json" \
  --samples "$result_dir/warmup-barrier-samples.jsonl" \
  --checkpoints "$result_dir/warmup-checkpoints.json" \
  --materializer-lag "$result_dir/warmup-materializer-lag.txt" \
  --dlq-records "$result_dir/warmup-dlq-records.txt" \
  --expected-warmup-records "$warmup_records" \
  --maximum-watermark-lag-ms "$warmup_max_watermark_lag" \
  --require-barrier-checkpoint \
  --barrier-checkpoint "$result_dir/warmup-barrier-checkpoint.json" \
  --output "$result_dir/warmup-barrier-report.json"
warmup_barrier_epoch=$(date +%s)
update_runtime_phase warmup_barrier complete "$warmup_barrier_epoch"

update_runtime_phase measurement started "$(date +%s)"
docker run -d --name "$producer_container" --network host \
  -v "$input:/data/input.csv.zst:ro" \
  -v "$data_manifest:/data/data-manifest.json:ro" \
  -v "$result_dir:/results" "$PRODUCER_IMAGE" replay \
  --brokers "$KAFKA_BROKERS" --manifest /data/data-manifest.json \
  --input /data/input.csv.zst --run-id "$run_id" --input-topic "$input_topic" \
  --input-partitions "$INPUT_PARTITIONS" --control-topic "$control_topic" \
  --dlq-topic "$dlq_topic" --limit "$limit" --resume \
  --checkpoint /results/producer-checkpoint.json \
  --summary /results/producer-summary.json \
  --progress /results/producer-progress.jsonl \
  --progress-phase measurement --rate "$producer_rate" >/dev/null
producer_exit=$(docker wait "$producer_container")
docker logs "$producer_container" >"$result_dir/producer.log" 2>&1 || true
docker rm "$producer_container" >/dev/null 2>&1 || true
require "measurement producer did not complete successfully" \
  test "$producer_exit" -eq 0
require "Flink failed during measurement" test ! -s "$flink_failure_signal"
measurement_start_epoch=$(jq -r '.first_acknowledged_epoch_us / 1000000 | floor' \
  "$result_dir/producer-summary.json")
measurement_end_epoch=$(jq -r '.last_acknowledged_epoch_us / 1000000 | floor' \
  "$result_dir/producer-summary.json")
update_runtime_phase measurement input_complete "$measurement_end_epoch"
producer_end_epoch=$(date +%s)

docker run --rm --memory 2g --memory-swap 2g --network host \
  -v "$result_dir:/results" \
  -v "$reference:/reference.json:ro" "$VERIFIER_IMAGE" verify-run \
  --brokers "$KAFKA_BROKERS" --run-id "$run_id" \
  --control-topic "$control_topic" --query1-topic "$query1_topic" \
  --query2-topic "$query2_topic" --dlq-topic "$dlq_topic" \
  --input-partitions "$INPUT_PARTITIONS" --result-partitions "$result_partitions" \
  --reference /reference.json --maximum-event-time "$maximum_event_time" \
  --output /results --quiet-period 2s --timeout 30m

wait_until "zero Flink input lag" 180 consumer_group_lag_zero "$flink_group"
wait_until "zero materializer lag" 180 consumer_group_lag_zero "$materializer_group"
docker run --rm --network host "$KAFKA_IMAGE" \
  /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "$KAFKA_BROKERS" \
  --group "$flink_group" --describe >"$result_dir/metrics/flink-consumer-group.txt"
docker run --rm --network host "$KAFKA_IMAGE" \
  /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "$KAFKA_BROKERS" \
  --group "$materializer_group" --describe >"$result_dir/metrics/materializer-consumer-group.txt"

expected_query1=$(jq -r '.streams["query1.event-time-state"].records' "$reference")
expected_query2=$(jq -r '.streams["query2.event-time-state"].records' "$reference")
expected_rows=$((expected_query1 + expected_query2))
database_complete() {
  rows=$(docker run --rm --network host -e PGPASSWORD="$DATABASE_PASSWORD" \
    "$TIMESCALEDB_IMAGE" psql -h "$DATABASE_ADDRESS" -U taxi -d taxi -Atc \
    "SELECT count(*) FROM canonical_results WHERE run_id='$run_id';" 2>/dev/null) || return 1
  require "TimescaleDB row count does not match canonical output" \
    test "$rows" -eq "$expected_rows"
}
wait_until "TimescaleDB materialization" 300 database_complete

curl --fail --silent -X POST -H 'Content-Type: application/json' \
  -d '{"checkpointType":"FULL"}' \
  "http://$CONTROL_ADDRESS:8081/jobs/$job_id/checkpoints" \
  >"$result_dir/final-checkpoint-trigger.json"
trigger_id=$(jq -r '."request-id"' "$result_dir/final-checkpoint-trigger.json")
wait_until "the final checkpoint" 180 checkpoint_request_complete "$trigger_id"
end_epoch=$(date +%s)
capture_runtime_evidence "$end_epoch"

write_latency_evidence all-run
measurement_first_ack_us=$(jq -r '.first_acknowledged_epoch_us' \
  "$result_dir/producer-summary.json")
measurement_last_ack_us=$(jq -r '.last_acknowledged_epoch_us' \
  "$result_dir/producer-summary.json")
write_latency_evidence measurement "$measurement_first_ack_us" "$measurement_last_ack_us"

docker run --rm --network host -e PGPASSWORD="$DATABASE_PASSWORD" \
  "$TIMESCALEDB_IMAGE" psql -h "$DATABASE_ADDRESS" -U taxi -d taxi -Atc "
SELECT json_build_object('rows',count(*),'unique_output_ids',count(DISTINCT output_id),
  'duplicate_rows',count(*)-count(DISTINCT output_id),
  'query1_rows',count(*) FILTER (WHERE query_id='query1'),
  'query2_rows',count(*) FILTER (WHERE query_id='query2'))
FROM canonical_results WHERE run_id='$run_id';" >"$result_dir/sink-summary.json"

cp /opt/taxi/release/release-manifest.json "$result_dir/provenance/"
docker inspect taxi-jobmanager taxi-materializer >"$result_dir/provenance/container-inspect.json"
timedatectl show >"$result_dir/provenance/time-sync.txt"
chronyc tracking >>"$result_dir/provenance/time-sync.txt" 2>&1 || true
python3 /opt/taxi/release/aws_benchmark/runtime_tools/run_manifest.py \
  --result-dir "$result_dir" \
  --release-config "$release_config" \
  --data-manifest "$data_manifest" \
  --deployment-profile "$DEPLOYMENT_PROFILE" \
  --artifact-bucket "$ARTIFACT_BUCKET" \
  --completion-epoch "$end_epoch" \
  --output "$result_dir/run-manifest.json"
attach_kafka_topic_contract
jq -n --slurpfile query1 "$result_dir/query1-verification.json" \
  --slurpfile query2 "$result_dir/query2-verification.json" \
  --slurpfile completion "$result_dir/completion-report.json" \
  '{version:1,query1:$query1[0],query2:$query2[0],completion:$completion[0],
    passed:($query1[0].passed and $query2[0].passed and $completion[0].passed)}' \
  >"$result_dir/correctness-report.json"

jq -n --slurpfile producer "$result_dir/producer-summary.json" \
  --slurpfile warmup "$result_dir/producer-summary-warmup.json" \
  --slurpfile checkpoints "$result_dir/metrics/flink-checkpoints.json" \
  --slurpfile checkpoint_failures "$result_dir/metrics/checkpoint-failure-classification.json" \
  --slurpfile job "$result_dir/metrics/flink-job.json" \
  --slurpfile correctness "$result_dir/correctness-report.json" \
  --slurpfile sink "$result_dir/sink-summary.json" \
  --arg run_kind "$kind" --argjson expected_records "$limit" \
  --argjson tolerable_failures "$tolerable_checkpoint_failures" \
  '{version:2,run_kind:$run_kind,expected_records:$expected_records,
    producer:$producer[0],checkpoint_counts:$checkpoints[0].counts,
    job_state:$job[0].state,dlq_records:0,sink:$sink[0],
    correctness_scope:"complete frozen reference digest and materialized output",
    operational_complete:($warmup[0].paused == true
      and $warmup[0].data_acknowledged + $producer[0].data_acknowledged
        == $expected_records
      and $producer[0].completed == true
      and $producer[0].last_contiguous_acknowledged_source_line == $expected_records
      and $checkpoint_failures[0].measurement_failures <= $tolerable_failures
      and $job[0].state == "RUNNING" and $correctness[0].passed == true
      and $sink[0].duplicate_rows == 0)}' \
  >"$result_dir/benchmark-operational-report.json"

require "canonical verification did not complete successfully" \
  jq -e '.passed == true' "$result_dir/correctness-report.json" >/dev/null
require "benchmark accounting is incomplete" \
  jq -e '.operational_complete == true' \
    "$result_dir/benchmark-operational-report.json" >/dev/null
require "TimescaleDB contains duplicate canonical rows" \
  jq -e '.duplicate_rows == 0' "$result_dir/sink-summary.json" >/dev/null

write_latency_attribution_evidence "$expected_rows"

stop_flink_monitor
docker exec taxi-jobmanager flink cancel "$job_id" >"$result_dir/cancel-command.txt"
update_runtime_phase evidence_export complete "$(date +%s)"
run_completed=1
printf 'AWS %s run completed: %s\n' "$kind" "$run_id"
