# Runs the combined pipeline end to end: Kafka -> Spark Streaming -> HDFS -> MapReduce on YARN -> reconciliation
# Usage: powershell -ExecutionPolicy Bypass -File .\run_pipeline.ps1 [-Limit 200000] [-Rate 0]
#   -Limit 0 replays the whole of January 2024 (about 2.96 M trips)
param([int]$Limit = 0, [int]$Rate = 0, [switch]$NoShots)
Set-Location $PSScriptRoot
New-Item -ItemType Directory -Force results\logs, screenshots | Out-Null
$prog = "results\progress.log"; if (Test-Path $prog) { Remove-Item $prog }
function Say($m) { $l = "$(Get-Date -Format 'HH:mm:ss') $m"; Write-Host $l; Add-Content $prog $l }
function Wait-Http($url, $what) {
  for ($i = 0; $i -lt 60; $i++) {
    try { Invoke-WebRequest -UseBasicParsing $url -TimeoutSec 3 | Out-Null; Say "$what is up"; return } catch { Start-Sleep 3 }
  }
  throw "$what did not start"
}
$edge = "C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"
function Shot($url, $name) {   # saves a screenshot of a local web UI with headless Edge
  if ($NoShots -or !(Test-Path $edge)) { return }
  $out = Join-Path $PSScriptRoot "screenshots\$name.png"
  $prof = Join-Path $env:TEMP "edge-shot-$name"     # own profile, or Edge hands off to the last process and saves nothing
  $e = Start-Process $edge -ArgumentList "--headless=new --disable-gpu --hide-scrollbars --user-data-dir=`"$prof`" --window-size=1600,1000 --virtual-time-budget=6000 --screenshot=`"$out`" $url" -PassThru
  if (!$e.WaitForExit(45000)) { Stop-Process -Id $e.Id -Force -ErrorAction SilentlyContinue }
  Remove-Item -Recurse -Force $prof -ErrorAction SilentlyContinue
  Say "screenshot $name saved=$(Test-Path $out)"
}
function KTopics { param([Parameter(ValueFromRemainingArguments)]$rest)
  docker exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 @rest 2>$null }

# ---------- 1. Start storage, ingest and streaming ----------
docker compose --profile batch down -v 2>$null | Out-Null
docker compose build namenode spark-master 2>$null | Out-Null    # one service per image, to avoid a tagging race
docker compose up -d zookeeper kafka namenode datanode-1 datanode-2 spark-master spark-worker 2>$null | Out-Null
Wait-Http "http://localhost:9870" "HDFS NameNode"
Wait-Http "http://localhost:8080" "Spark master"
docker exec namenode hdfs dfsadmin -safemode wait 2>$null | Out-Null
for ($i = 0; $i -lt 40; $i++) { $live = (docker exec namenode hdfs dfsadmin -report 2>$null | Select-String 'Live datanodes') ; if ("$live" -match '\(2\)') { break }; Start-Sleep 3 }
Say "HDFS: $live"
for ($i = 0; $i -lt 40; $i++) { KTopics --create --if-not-exists --topic taxi-trips --partitions 3 --replication-factor 1 | Out-Null; if ($LASTEXITCODE -eq 0) { break }; Start-Sleep 3 }
KTopics --create --if-not-exists --topic taxi-hourly-stats --partitions 1 --replication-factor 1 | Out-Null
KTopics --describe | Out-File results\logs\kafka_topics.txt -Encoding utf8
Say "Kafka topics created"

$expected = $Limit
if ($Limit -eq 0) {
  $expected = [int](docker exec cb-spark-master python3 -c "import pyarrow.parquet as p; print(p.ParquetFile('/opt/data/taxi/yellow_tripdata_2024-01.parquet').metadata.num_rows)")
}
Say "expecting $expected trips"

# ---------- 2. Stream: producer and Spark run at the same time ----------
$submit = "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --driver-memory 512m --executor-memory 1g --conf spark.hadoop.dfs.replication=2 /opt/app/stream_to_hdfs.py expected=$expected idle=240"
$spark = Start-Process docker -ArgumentList "exec cb-spark-master $submit" -NoNewWindow -PassThru -RedirectStandardOutput results\logs\stream.log -RedirectStandardError results\logs\stream.err
Start-Sleep 30
$pArgs = "exec cb-spark-master python3 -u /opt/producer/producer.py --limit $Limit --rate $Rate"
$producer = Start-Process docker -ArgumentList $pArgs -NoNewWindow -PassThru -RedirectStandardOutput results\logs\producer.log -RedirectStandardError results\logs\producer.err
Say "producer and streaming job started"
Start-Sleep 40
Shot "http://localhost:8080" "01_spark_master"
Shot "http://localhost:4040/StreamingQuery/" "02_spark_streaming_queries"
Shot "http://localhost:4040/executors/" "03_spark_executors"
$producer.WaitForExit(); Say "producer finished: $(Get-Content results\logs\producer.log -Tail 1)"
docker exec kafka /opt/bitnami/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list kafka:9092 --topic taxi-trips 2>$null | Out-File results\logs\kafka_offsets.txt -Encoding utf8
Shot "http://localhost:4040/StreamingQuery/" "04_spark_streaming_queries_late"
$spark.WaitForExit(); Say "streaming job finished: $(Get-Content results\logs\stream.log -Tail 1)"
docker exec kafka /opt/bitnami/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic taxi-hourly-stats --from-beginning --timeout-ms 60000 2>$null | Out-File results\logs\hourly_stats_topic.txt -Encoding utf8
Say "hourly stats topic messages: $((Get-Content results\logs\hourly_stats_topic.txt | Measure-Object).Count)"
docker exec namenode hdfs dfs -du -s -h /taxi/clean_trips 2>$null | Out-File results\logs\hdfs_du.txt -Encoding utf8
docker exec namenode hdfs fsck /taxi/clean_trips -files 2>$null | Select-String -Pattern 'Total size|Total files|Total blocks|Average block replication|Under-replicated|Status' | ForEach-Object { $_.Line } | Out-File results\logs\hdfs_fsck.txt -Encoding utf8
Shot "http://localhost:9870/dfshealth.html#tab-overview" "05_hdfs_overview"
Shot "http://localhost:9870/dfshealth.html#tab-datanode" "06_hdfs_datanodes"
Shot "http://localhost:9870/explorer.html#/taxi/clean_trips" "07_hdfs_clean_trips"

# ---------- 3. Batch: MapReduce on YARN, with Spark and Kafka stopped to free memory ----------
docker compose stop spark-worker spark-master kafka zookeeper 2>$null | Out-Null
docker compose --profile batch up -d resourcemanager nodemanager 2>$null | Out-Null
Wait-Http "http://localhost:8088" "YARN ResourceManager"
Start-Sleep 15
foreach ($j in 'zone', 'hour') {
  $t = [Diagnostics.Stopwatch]::StartNew()
  $job = Start-Process docker -ArgumentList "exec resourcemanager yarn jar /opt/jobs/taxi-mr.jar TaxiJobs $j /taxi/clean_trips /taxi/mr_$j" -NoNewWindow -PassThru -RedirectStandardOutput "results\logs\mr_$j.log" -RedirectStandardError "results\logs\mr_$j.err"
  $null = $job.Handle    # keeps ExitCode readable after the process ends
  Start-Sleep 25
  if ($j -eq 'zone') { Shot "http://localhost:8088/cluster/apps/RUNNING" "08_yarn_running" }
  $job.WaitForExit(); $t.Stop()
  Say ("MapReduce {0} exit={1} seconds={2:N0}" -f $j, $job.ExitCode, $t.Elapsed.TotalSeconds)
}
Shot "http://localhost:8088/cluster/apps" "09_yarn_finished"
Shot "http://localhost:9870/explorer.html#/taxi" "10_hdfs_outputs"
docker exec namenode hdfs dfs -cat /taxi/mr_zone/part-r-00000 2>$null | Select-Object -First 8 | Out-File results\logs\mr_zone_sample.txt -Encoding utf8

# ---------- 4. Reconcile ----------
docker compose --profile batch stop resourcemanager nodemanager 2>$null | Out-Null
docker compose up -d spark-master spark-worker 2>$null | Out-Null
Wait-Http "http://localhost:8080" "Spark master"
Start-Sleep 15
docker exec cb-spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --driver-memory 512m --executor-memory 1g /opt/app/reconcile.py limit=$Limit 2> results\logs\reconcile.err | Out-File results\logs\reconcile.log -Encoding utf8
Say "reconcile finished"
Say "=== pipeline finished ==="
