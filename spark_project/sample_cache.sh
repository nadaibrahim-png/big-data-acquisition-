#!/usr/bin/env bash
# Samples the running Spark app's cache + executor stats every 15 s into results/cache_stats.csv
# columns: time,app_name,cached_partitions,total_partitions,cache_mem_MB,cache_disk_MB,exec_gc_s,exec_task_s
out="$(dirname "$0")/results/cache_stats.csv"
[ -f "$out" ] || echo "time,app,cached_parts,total_parts,mem_MB,disk_MB,gc_s,task_s,n_exec" > "$out"
while true; do
  apps=$(curl -s --max-time 3 localhost:4040/api/v1/applications 2>/dev/null)
  id=$(echo "$apps" | grep -o '"id" : "[^"]*' | head -1 | cut -d'"' -f4)
  name=$(echo "$apps" | grep -o '"name" : "[^"]*' | head -1 | cut -d'"' -f4)
  if [ -n "$id" ]; then
    rdd=$(curl -s --max-time 3 "localhost:4040/api/v1/applications/$id/storage/rdd")
    ex=$(curl -s --max-time 3 "localhost:4040/api/v1/applications/$id/executors")
    sum() { echo "$1" | grep -o "\"$2\" : [0-9]*" | awk '{s+=$3} END {print s+0}'; }
    nexec=$(echo "$ex" | grep -c '"hostPort"'); nexec=$((nexec-1))
    echo "$(date +%T),$name,$(sum "$rdd" numCachedPartitions),$(sum "$rdd" numPartitions),$(( $(sum "$rdd" memoryUsed)/1048576 )),$(( $(sum "$rdd" diskUsed)/1048576 )),$(( $(sum "$ex" totalGCTime)/1000 )),$(( $(sum "$ex" totalDuration)/1000 )),$nexec" >> "$out"
  fi
  sleep 15
done
