# Runs the full benchmark from scratch: blocks A-D (3 repetitions each) + one full run with GBT.
#   A = baseline: 2 workers, cache on, broadcast join
#   B = scaling:  1 worker  (spark-worker-2 stopped), otherwise as A
#   C = no cache: 2 workers, cache off
#   D = join:     2 workers, sort-merge join (broadcast disabled)
#   F = full run with GBT (2 workers, cache, broadcast) for model metrics
# Usage:  powershell -ExecutionPolicy Bypass -File .\run_all.ps1 [-Reps 3]
param([int]$Reps = 3, [string[]]$Blocks = @('A_w2','B_w1','C_nocache','D_smj','F'), [int]$StartRep = 1)
# heartbeat/network timeouts raised: the Docker VM clock can jump ~480 s, which otherwise
# makes the driver declare healthy executors dead.
$Conf = "--conf spark.network.timeout=1200s --conf spark.executor.heartbeatInterval=60s"
Set-Location $PSScriptRoot
$progress = "results\run_all_progress.log"
function Say($m) { $l = "$(Get-Date -Format 'HH:mm:ss') $m"; Write-Host $l; Add-Content $progress $l }

function Wait-Workers([int]$n) {
  for ($i = 0; $i -lt 40; $i++) {
    $json = try { (Invoke-WebRequest -UseBasicParsing http://localhost:8080/json/ -TimeoutSec 5).Content | ConvertFrom-Json } catch { $null }
    if ($json) {
      $alive = @($json.workers | Where-Object { $_.state -eq 'ALIVE' }).Count
      if ($alive -eq $n) { Say "cluster ready: $alive alive worker(s)"; return }
    }
    Start-Sleep 3
  }
  throw "cluster did not reach $n alive workers"
}

function Restart-Cluster([int]$n) {
  docker compose down | Out-Null
  if ($n -eq 1) { docker compose up -d spark-master spark-worker-1 | Out-Null }
  else          { docker compose up -d | Out-Null }
  Wait-Workers $n
}

function Run([string]$tag, [int]$workers, [string[]]$jobArgs) {
  Say "start $tag ($($jobArgs -join ' '))"
  & .\run_experiment.ps1 -Tag $tag -Workers $workers -SparkConf $Conf -JobArgs $jobArgs
  $lost = Select-String -Path "results\logs\$tag.log.err" -Pattern 'Lost executor|ExecutorLostFailure' -Quiet
  $ok   = Select-String -Path "results\logs\$tag.log" -Pattern '^METRICS' -Quiet
  Say "done  $tag  metrics=$ok  executor_lost=$lost"
}

Say "=== run_all begin (reps=$Reps) ==="
$defs = @(
  @{ name = 'A_w2';     workers = 2; args = @('cache=1', 'join=broadcast', 'gbt=0') },
  @{ name = 'B_w1';     workers = 1; args = @('cache=1', 'join=broadcast', 'gbt=0') },
  @{ name = 'C_nocache'; workers = 2; args = @('cache=0', 'join=broadcast', 'gbt=0') },
  @{ name = 'D_smj';    workers = 2; args = @('cache=1', 'join=smj',       'gbt=0') }
)
foreach ($b in $defs | Where-Object { $Blocks -contains $_.name }) {
  Restart-Cluster $b.workers          # fresh JVMs per block
  for ($r = $StartRep; $r -lt $StartRep + $Reps; $r++) { Run "$($b.name)_r$r" $b.workers $b.args }
}
if ($Blocks -contains 'F') {
  Restart-Cluster 2
  Run 'F_full_gbt' 2 @('cache=1', 'join=broadcast', 'gbt=1')
}
Say "=== run_all finished ==="
