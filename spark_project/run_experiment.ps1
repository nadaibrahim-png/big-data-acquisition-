param(
  [string]$Tag, [int]$Workers,
  [string]$SparkConf = "",
  [string[]]$JobArgs = @(),
  [string]$Script = "/opt/app/app/taxi_pipeline.py"
)
New-Item -ItemType Directory -Force results\logs | Out-Null
if (!(Test-Path results\timings.csv)) { "tag,workers,stage,seconds" | Out-File results\timings.csv -Encoding ascii }
$log = "results\logs\$Tag.log"
$conf = if ($SparkConf) { $SparkConf -split ' ' } else { @() }
docker exec -e PYTHONUNBUFFERED=1 spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 `
  --executor-memory 768m --driver-memory 512m `
  --conf spark.sql.files.maxPartitionBytes=33554432 `
  @conf $Script @JobArgs 2> "$log.err" | Out-File $log -Encoding utf8
Select-String -Path $log -Pattern '^RESULT' | ForEach-Object { "$Tag,$Workers," + ($_.Line -replace '^RESULT,','') } | Add-Content results\timings.csv
Select-String -Path $log -Pattern '^METRICS' | ForEach-Object { $_.Line } | Set-Content "results\logs\${Tag}_metrics.txt"
