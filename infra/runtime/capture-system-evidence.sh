#!/usr/bin/env bash
set -euo pipefail

source /etc/taxi/node.env
output=/opt/taxi/evidence/runtime-system.json
temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' EXIT

jq -n --arg node "$NODE_NAME" --arg role "$NODE_ROLE" \
  --arg kernel "$(uname -r)" --arg architecture "$(uname -m)" \
  --argjson vcpus "$(getconf _NPROCESSORS_ONLN)" \
  --argjson ram_bytes "$(awk '$1=="MemTotal:" {print $2*1024}' /proc/meminfo)" \
  '{version:1,captured_at:(now|todateiso8601),node:$node,role:$role,
    host:{kernel:$kernel,architecture:$architecture,vcpus:$vcpus,ram_bytes:$ram_bytes}}' \
  >"$temporary/base.json"

case "$NODE_ROLE" in
  taskmanager|smoke)
    if docker inspect taxi-taskmanager >/dev/null 2>&1; then
      docker inspect taxi-taskmanager >"$temporary/container.json"
      docker exec taxi-taskmanager sh -c 'tr "\000" " " </proc/1/cmdline' \
        >"$temporary/jvm-command.txt" 2>&1 || true
    fi
    ;;
  kafka)
    docker inspect taxi-kafka >"$temporary/container.json"
    docker exec taxi-kafka sh -c 'printf "%s\n" "$KAFKA_HEAP_OPTS"' \
      >"$temporary/kafka-heap-opts.txt" 2>&1 || true
    docker exec taxi-kafka sh -c 'tr "\000" " " </proc/1/cmdline' \
      >"$temporary/jvm-command.txt" 2>&1 || true
    ;;
  database)
    docker exec taxi-timescaledb psql -U taxi -d taxi -Atc "
SELECT json_build_object(
  'shared_buffers',current_setting('shared_buffers'),
  'effective_cache_size',current_setting('effective_cache_size'),
  'work_mem',current_setting('work_mem'),
  'max_connections',current_setting('max_connections'),
  'active_connections',(SELECT count(*) FROM pg_stat_activity),
  'buffer_cache_hit_ratio',(SELECT CASE WHEN sum(blks_hit+blks_read)=0 THEN NULL
    ELSE sum(blks_hit)::double precision/sum(blks_hit+blks_read) END FROM pg_stat_database),
  'wal_bytes',pg_wal_lsn_diff(pg_current_wal_lsn(),'0/0'),
  'committed_transactions',(SELECT sum(xact_commit) FROM pg_stat_database),
  'rolled_back_transactions',(SELECT sum(xact_rollback) FROM pg_stat_database));" \
      >"$temporary/postgres.json"
    ;;
esac

jq -n --slurpfile base "$temporary/base.json" \
  --argjson container "$(cat "$temporary/container.json" 2>/dev/null || printf 'null')" \
  --arg jvm_command "$(cat "$temporary/jvm-command.txt" 2>/dev/null || true)" \
  --arg jvm_flags "$(cat "$temporary/jvm-flags.txt" 2>/dev/null || true)" \
  --arg jvm_heap "$(cat "$temporary/jvm-heap.txt" 2>/dev/null || true)" \
  --arg kafka_heap_opts "$(cat "$temporary/kafka-heap-opts.txt" 2>/dev/null || true)" \
  --arg jvm_gc "$(cat "$temporary/jvm-gc.txt" 2>/dev/null || true)" \
  --argjson postgres "$(cat "$temporary/postgres.json" 2>/dev/null || printf 'null')" \
  '$base[0] + {container_inspect:$container,jvm:{command:$jvm_command,flags:$jvm_flags,
    heap:$jvm_heap},kafka_heap_opts:$kafka_heap_opts,kafka_gc:$jvm_gc,
    postgres:$postgres}' >"$output"
