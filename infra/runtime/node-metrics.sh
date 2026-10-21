#!/usr/bin/env bash
set -euo pipefail

source /etc/taxi/node.env
output=/opt/taxi/evidence/node-metrics.jsonl
previous_total=0
previous_idle=0
root_source=$(findmnt -n -o SOURCE /)
root_device=$(lsblk -n -o PKNAME "$root_source" 2>/dev/null | head -n 1)
if [[ -z $root_device ]]; then root_device=$(basename "$root_source"); fi

while true; do
  read -r _ user nice system idle iowait irq softirq steal _ </proc/stat
  total=$((user + nice + system + idle + iowait + irq + softirq + steal))
  idle_total=$((idle + iowait))
  delta_total=$((total - previous_total))
  delta_idle=$((idle_total - previous_idle))
  cpu_percent=0
  if (( previous_total > 0 && delta_total > 0 )); then
    cpu_percent=$((10000 * (delta_total - delta_idle) / delta_total))
  fi
  previous_total=$total
  previous_idle=$idle_total
  memory_total_kib=$(awk '$1=="MemTotal:" {print $2}' /proc/meminfo)
  memory_available_kib=$(awk '$1=="MemAvailable:" {print $2}' /proc/meminfo)
  page_cache_kib=$(awk '
    $1=="Cached:" {cached=$2}
    $1=="SReclaimable:" {reclaim=$2}
    $1=="Shmem:" {shmem=$2}
    END {print cached+reclaim-shmem}' /proc/meminfo)
  root_used_bytes=$(df -B1 / | awk 'NR==2 {print $3}')
  read -r network_receive_bytes network_transmit_bytes < <(awk -F'[: ]+' '
    $2 != "lo" && NF > 10 {receive += $3; transmit += $11}
    END {print receive+0, transmit+0}' /proc/net/dev)
  read -r disk_reads disk_sectors_read disk_writes disk_sectors_written disk_io_time_ms \
    < <(awk -v device="$root_device" '$3==device {
      reads=$4; sectors_read=$6; writes=$8; sectors_written=$10; io_time=$13}
      END {print reads+0,sectors_read+0,writes+0,sectors_written+0,io_time+0}' \
      /proc/diskstats)
  kafka_heap_used_kib=null
  kafka_heap_capacity_kib=null
  kafka_gc_pause_seconds=null
  kafka_post_gc_heap_used_kib=null
  if [[ $NODE_ROLE == kafka ]] && docker inspect taxi-kafka >/dev/null 2>&1; then
    read -r kafka_heap_used_kib kafka_heap_capacity_kib kafka_gc_pause_seconds \
      kafka_post_gc_heap_used_kib \
      < <(docker exec taxi-kafka sh -c \
        'cat /opt/kafka/logs/kafkaServer-gc.log* 2>/dev/null' \
        | awk -f /opt/taxi/release/infra/runtime/kafka-gc-summary.awk)
  fi
  database_wal_bytes=null
  database_committed_transactions=null
  database_buffer_cache_hit_ratio=null
  database_connections=null
  if [[ $NODE_ROLE == database ]] && docker inspect taxi-timescaledb >/dev/null 2>&1; then
    read -r database_wal_bytes database_committed_transactions \
      database_buffer_cache_hit_ratio database_connections \
      < <(docker exec taxi-timescaledb psql -U taxi -d taxi -AtF' ' -c "
        SELECT pg_wal_lsn_diff(pg_current_wal_lsn(),'0/0'),
          (SELECT sum(xact_commit) FROM pg_stat_database),
          (SELECT CASE WHEN sum(blks_hit+blks_read)=0 THEN 0
            ELSE sum(blks_hit)::double precision/sum(blks_hit+blks_read) END
            FROM pg_stat_database),
          (SELECT count(*) FROM pg_stat_activity);" 2>/dev/null \
        || printf 'null null null null\n')
  fi
  read -r load1 load5 load15 _ </proc/loadavg
  jq -nc --arg node "$NODE_NAME" --arg role "$NODE_ROLE" \
    --argjson cpu_basis_points "$cpu_percent" \
    --argjson memory_total_kib "$memory_total_kib" \
    --argjson memory_available_kib "$memory_available_kib" \
    --argjson page_cache_kib "$page_cache_kib" \
    --argjson root_used_bytes "$root_used_bytes" \
    --arg root_device "$root_device" \
    --argjson network_receive_bytes "$network_receive_bytes" \
    --argjson network_transmit_bytes "$network_transmit_bytes" \
    --argjson disk_reads "${disk_reads:-0}" \
    --argjson disk_sectors_read "${disk_sectors_read:-0}" \
    --argjson disk_writes "${disk_writes:-0}" \
    --argjson disk_sectors_written "${disk_sectors_written:-0}" \
    --argjson disk_io_time_ms "${disk_io_time_ms:-0}" \
    --argjson kafka_heap_used_kib "$kafka_heap_used_kib" \
    --argjson kafka_heap_capacity_kib "$kafka_heap_capacity_kib" \
    --argjson kafka_gc_pause_seconds "$kafka_gc_pause_seconds" \
    --argjson kafka_post_gc_heap_used_kib "$kafka_post_gc_heap_used_kib" \
    --argjson database_wal_bytes "$database_wal_bytes" \
    --argjson database_committed_transactions "$database_committed_transactions" \
    --argjson database_buffer_cache_hit_ratio "$database_buffer_cache_hit_ratio" \
    --argjson database_connections "$database_connections" \
    --arg load1 "$load1" --arg load5 "$load5" --arg load15 "$load15" \
    '{timestamp:(now|todateiso8601),node:$node,role:$role,
      cpu_percent:($cpu_basis_points/100),memory_total_kib:$memory_total_kib,
      memory_available_kib:$memory_available_kib,page_cache_kib:$page_cache_kib,
      root_used_bytes:$root_used_bytes,root_device:$root_device,
      network_receive_bytes:$network_receive_bytes,
      network_transmit_bytes:$network_transmit_bytes,
      disk_reads:$disk_reads,disk_sectors_read:$disk_sectors_read,
      disk_writes:$disk_writes,disk_sectors_written:$disk_sectors_written,
      disk_io_time_ms:$disk_io_time_ms,kafka_heap_used_kib:$kafka_heap_used_kib,
      kafka_heap_capacity_kib:$kafka_heap_capacity_kib,
      kafka_gc_pause_seconds:$kafka_gc_pause_seconds,
      kafka_post_gc_heap_used_kib:$kafka_post_gc_heap_used_kib,
      database_wal_bytes:$database_wal_bytes,
      database_committed_transactions:$database_committed_transactions,
      database_buffer_cache_hit_ratio:$database_buffer_cache_hit_ratio,
      database_connections:$database_connections,
      load_1m:($load1|tonumber),load_5m:($load5|tonumber),load_15m:($load15|tonumber)}' \
    >>"$output"
  sleep 5
done
