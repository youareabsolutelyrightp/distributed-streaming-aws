#!/usr/bin/env bash
set -euo pipefail

source /etc/taxi/node.env
source /opt/taxi/release/versions.env
release_config=/opt/taxi/release/config/release.json

action=${1:-}
if [[ $action == stop ]]; then
  docker rm -f taxi-materializer taxi-prometheus taxi-kafka-exporter \
    taxi-jobmanager taxi-taskmanager taxi-kafka taxi-timescaledb >/dev/null 2>&1 || true
  exit 0
fi
if [[ $action != start ]]; then
  echo "usage: node-service.sh start|stop" >&2
  exit 2
fi

docker load -i /opt/taxi/release/images/application-images.tar
docker image inspect "$PRODUCER_IMAGE" >/dev/null
docker image inspect "$MATERIALIZER_IMAGE" >/dev/null
docker tag "$PRODUCER_IMAGE" "$VERIFIER_IMAGE"

pull_image() {
  docker pull "$1"
}

start_kafka() {
  pull_image "$KAFKA_IMAGE"
  docker volume create taxi-kafka-data >/dev/null
  docker rm -f taxi-kafka >/dev/null 2>&1 || true
  docker run -d --name taxi-kafka --restart unless-stopped --network host \
    -v taxi-kafka-data:/var/lib/kafka/data \
    -e CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk \
    -e KAFKA_NODE_ID="$KAFKA_NODE_ID" \
    -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
    -e KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://$PRIVATE_IP:9092" \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS="$KAFKA_CONTROLLER_VOTERS" \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR="$KAFKA_REPLICATION_FACTOR" \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR="$KAFKA_REPLICATION_FACTOR" \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR="$KAFKA_MINIMUM_ISR" \
    -e KAFKA_DEFAULT_REPLICATION_FACTOR="$KAFKA_REPLICATION_FACTOR" \
    -e KAFKA_MIN_INSYNC_REPLICAS="$KAFKA_MINIMUM_ISR" \
    -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
    -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false \
    -e KAFKA_HEAP_OPTS="-Xms$(jq -r '.kafka.heap' "$release_config") -Xmx$(jq -r '.kafka.heap' "$release_config")" \
    "$KAFKA_IMAGE"
}

start_database() {
  pull_image "$TIMESCALEDB_IMAGE"
  docker volume create taxi-timescaledb-data >/dev/null
  docker rm -f taxi-timescaledb >/dev/null 2>&1 || true
  docker run -d --name taxi-timescaledb --restart unless-stopped --network host \
    -v taxi-timescaledb-data:/var/lib/postgresql/data \
    -e POSTGRES_USER=taxi -e POSTGRES_DB=taxi \
    -e POSTGRES_PASSWORD="$DATABASE_PASSWORD" \
    "$TIMESCALEDB_IMAGE" postgres \
    -c "shared_buffers=$(jq -r '.database.shared_buffers' "$release_config")" \
    -c "effective_cache_size=$(jq -r '.database.effective_cache_size' "$release_config")" \
    -c "work_mem=$(jq -r '.database.work_mem' "$release_config")" \
    -c "max_connections=$(jq -r '.database.max_connections' "$release_config")"
  for attempt in $(seq 1 120); do
    if docker exec taxi-timescaledb pg_isready -U taxi -d taxi >/dev/null 2>&1; then
      docker exec -i taxi-timescaledb psql -U taxi -d taxi \
        </opt/taxi/release/database/migrations/0001_canonical_results.sql
      return
    fi
    sleep 2
  done
  echo "TimescaleDB did not become ready." >&2
  exit 1
}

flink_common_properties() {
  local taskmanager_memory managed_memory task_slots processing_parallelism
  local heartbeat_interval heartbeat_timeout rpc_failure_threshold restart_attempts restart_delay
  local buffer_timeout_enabled buffer_timeout_interval
  taskmanager_memory=$(jq -r '.runtime.taskmanager_process_memory' "$release_config")
  managed_memory=$(jq -r '.runtime.managed_memory' "$release_config")
  task_slots=$(jq -r '.topology.task_slots_per_taskmanager' "$release_config")
  processing_parallelism=$(jq -r '.topology.processing_parallelism' "$release_config")
  heartbeat_interval=$(jq -r '.recovery.heartbeat_interval_ms' "$release_config")
  heartbeat_timeout=$(jq -r '.recovery.heartbeat_timeout_ms' "$release_config")
  rpc_failure_threshold=$(jq -r '.recovery.rpc_failure_threshold' "$release_config")
  restart_attempts=$(jq -r '.recovery.restart_attempts' "$release_config")
  restart_delay=$(jq -r '.recovery.restart_delay_ms' "$release_config")
  buffer_timeout_enabled=$(jq -er '.flink["execution.buffer-timeout.enabled"]
    | select(type == "boolean")' "$release_config")
  buffer_timeout_interval=$(jq -er '.flink["execution.buffer-timeout.interval"]
    | select(type == "string" and test("^[0-9]+ ms$"))' "$release_config")
  cat <<EOF
jobmanager.rpc.address: $CONTROL_ADDRESS
jobmanager.memory.process.size: 2048m
taskmanager.memory.process.size: $taskmanager_memory
taskmanager.memory.managed.size: $managed_memory
taskmanager.numberOfTaskSlots: $task_slots
parallelism.default: $processing_parallelism
taskmanager.load-balance.mode: slots
slot.request.max-interval: 1000 ms
external-resources: benchmark-placement
external-resource.benchmark-placement.amount: 100
heartbeat.interval: $heartbeat_interval ms
heartbeat.timeout: $heartbeat_timeout ms
heartbeat.rpc-failure-threshold: $rpc_failure_threshold
execution.checkpointing.max-concurrent-checkpoints: 1
restart-strategy.type: fixed-delay
restart-strategy.fixed-delay.attempts: $restart_attempts
restart-strategy.fixed-delay.delay: $restart_delay ms
execution.buffer-timeout.enabled: $buffer_timeout_enabled
execution.buffer-timeout.interval: $buffer_timeout_interval
metrics.reporters: prom
metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
EOF
}

start_jobmanager() {
  pull_image "$FLINK_IMAGE"
  local properties
  properties="$(flink_common_properties)
jobmanager.bind-host: 0.0.0.0
rest.address: $CONTROL_ADDRESS
rest.bind-address: 0.0.0.0
metrics.reporter.prom.port: 9249"
  docker rm -f taxi-jobmanager >/dev/null 2>&1 || true
  docker run -d --name taxi-jobmanager --restart unless-stopped --network host \
    -e AWS_REGION="$AWS_REGION" -e AWS_DEFAULT_REGION="$AWS_REGION" \
    -e ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-presto-2.2.1.jar \
    -e FLINK_PROPERTIES="$properties" \
    -v /opt/taxi/release/app/taxi-flink.jar:/opt/flink/usrlib/taxi-flink.jar:ro \
    "$FLINK_IMAGE" jobmanager
}

start_taskmanager() {
  pull_image "$FLINK_IMAGE"
  local metric_port=9249
  local properties
  properties="$(flink_common_properties)
taskmanager.host: $PRIVATE_IP
taskmanager.bind-host: 0.0.0.0
metrics.reporter.prom.port: $metric_port"
  docker rm -f taxi-taskmanager >/dev/null 2>&1 || true
  docker run -d --name taxi-taskmanager --restart unless-stopped --network host \
    -e AWS_REGION="$AWS_REGION" -e AWS_DEFAULT_REGION="$AWS_REGION" \
    -e ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-presto-2.2.1.jar \
    -e FLINK_PROPERTIES="$properties" \
    -v /opt/taxi/release/app/taxi-flink.jar:/opt/flink/usrlib/taxi-flink.jar:ro \
    "$FLINK_IMAGE" taskmanager
}

start_observability() {
  pull_image "$PROMETHEUS_IMAGE"
  pull_image "$KAFKA_EXPORTER_IMAGE"
  install -d -m 0755 /opt/taxi/prometheus
  docker volume create taxi-prometheus-data >/dev/null
  local flink_targets="'$CONTROL_ADDRESS:9249'"
  for taskmanager in $(seq 1 "$EXPECTED_TASKMANAGERS"); do
    flink_targets+=",'10.42.10.$((30 + taskmanager)):9249'"
  done
  cat >/opt/taxi/prometheus/prometheus.yml <<EOF
global:
  scrape_interval: 1s
  evaluation_interval: 1s

scrape_configs:
  - job_name: flink
    static_configs:
      - targets: [$flink_targets]
  - job_name: kafka-exporter
    static_configs:
      - targets: ['$CONTROL_ADDRESS:9308']
EOF
  docker rm -f taxi-kafka-exporter taxi-prometheus >/dev/null 2>&1 || true
  local exporter_arguments=()
  IFS=',' read -r -a broker_list <<<"$KAFKA_BROKERS"
  for broker in "${broker_list[@]}"; do
    exporter_arguments+=(--kafka.server="$broker")
  done
  docker run -d --name taxi-kafka-exporter --restart unless-stopped --network host \
    "$KAFKA_EXPORTER_IMAGE" "${exporter_arguments[@]}" --kafka.version=4.3.1 \
    '--topic.filter=taxi\..*'
  docker run -d --name taxi-prometheus --restart unless-stopped --network host \
    -v /opt/taxi/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro \
    -v taxi-prometheus-data:/prometheus \
    "$PROMETHEUS_IMAGE" \
    --config.file=/etc/prometheus/prometheus.yml \
    --storage.tsdb.path=/prometheus --storage.tsdb.retention.time=8h
}

case "$NODE_ROLE" in
  control)
    start_jobmanager
    start_observability
    ;;
  kafka)
    start_kafka
    ;;
  taskmanager)
    start_taskmanager
    ;;
  database)
    start_database
    ;;
  *)
    echo "Unknown node role: $NODE_ROLE" >&2
    exit 2
    ;;
esac

image_ids=()
while IFS= read -r value; do image_ids+=("$value"); done \
  < <(docker image ls --quiet --no-trunc | sort -u)
docker image inspect \
  --format '{{json .RepoDigests}}' "${image_ids[@]}" \
  | jq -s --arg node "$NODE_NAME" --arg role "$NODE_ROLE" \
    '{version:1,node:$node,role:$role,started_at:(now|todateiso8601),image_repo_digests:.}' \
  >/opt/taxi/evidence/node-start.json
