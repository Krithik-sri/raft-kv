param(
    [int]$Iterations = 20,
    [int]$WritesPerBatch = 5,
    [string]$WorkDir = (Join-Path $env:TEMP "raft-kv-crash-test"),
    [switch]$KeepLogs
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot

$nodes = @(
    @{ Id = "node1"; Port = 7301 },
    @{ Id = "node2"; Port = 7302 },
    @{ Id = "node3"; Port = 7303 }
)

$allAddresses = ($nodes | ForEach-Object { "localhost:$($_.Port)" }) -join ","

if (Test-Path $WorkDir) { Remove-Item -Recurse -Force $WorkDir }
$dataDir = Join-Path $WorkDir "data"
$logDir = Join-Path $WorkDir "logs"
$binDir = Join-Path $WorkDir "bin"
New-Item -ItemType Directory -Force -Path $dataDir, $logDir, $binDir | Out-Null

$serverExe = Join-Path $binDir "raftnode.exe"
$kvctlExe = Join-Path $binDir "kvctl.exe"

Write-Host "building..."
Push-Location $repo
try {
    & go build -o $serverExe ./cmd/server
    if ($LASTEXITCODE -ne 0) { throw "failed to build server" }
    & go build -o $kvctlExe ./cmd/kvctl
    if ($LASTEXITCODE -ne 0) { throw "failed to build kvctl" }
}
finally {
    Pop-Location
}

$processes = @{}

function Start-Node($node, $tag) {
    $peers = ($nodes | Where-Object { $_.Id -ne $node.Id } |
        ForEach-Object { "$($_.Id)=localhost:$($_.Port)" }) -join ","

    $logPath = Join-Path $logDir "$($node.Id)-$tag.log"

    $processes[$node.Id] = Start-Process -FilePath $serverExe -ArgumentList @(
        "--id", $node.Id,
        "--addr", "localhost:$($node.Port)",
        "--peers", $peers,
        "--data-dir", $dataDir
    ) -RedirectStandardOutput $logPath -NoNewWindow -PassThru
}

function Stop-Node($node) {
    $proc = $processes[$node.Id]
    if ($null -ne $proc) {
        try { Stop-Process -Id $proc.Id -Force -ErrorAction Stop } catch {}
        $processes.Remove($node.Id)
    }
}

function Stop-All {
    foreach ($node in $nodes) { Stop-Node $node }
}

function Invoke-Put($key, $value) {
    & $kvctlExe --peers $allAddresses --timeout 15s put $key $value | Out-Null
    return ($LASTEXITCODE -eq 0)
}

function Invoke-Get($key) {
    $output = & $kvctlExe --peers $allAddresses --timeout 15s get $key 2>&1
    if ($LASTEXITCODE -ne 0) { return $null }
    return ($output | Select-Object -First 1).ToString().Trim()
}

trap { Stop-All; break }

Write-Host "starting cluster..."
foreach ($node in $nodes) { Start-Node $node "initial" }
Start-Sleep -Seconds 2

$acked = @{}
$writeCount = 0
$failures = @()

for ($i = 1; $i -le $Iterations; $i++) {
    # Batch A: healthy cluster.
    for ($w = 0; $w -lt $WritesPerBatch; $w++) {
        $writeCount++
        $key = "k$writeCount"
        $value = "v$writeCount"
        if (Invoke-Put $key $value) { $acked[$key] = $value }
    }

    # Kill one node hard, mid-workload.
    $victim = $nodes | Get-Random
    Stop-Node $victim

    # Batch B: degraded cluster, must still accept writes on the majority.
    for ($w = 0; $w -lt $WritesPerBatch; $w++) {
        $writeCount++
        $key = "k$writeCount"
        $value = "v$writeCount"
        if (Invoke-Put $key $value) { $acked[$key] = $value }
    }

    # Restart the victim from its on-disk state.
    Start-Node $victim "iter$i"
    Start-Sleep -Milliseconds 1500

    # Every acknowledged write must still be readable.
    $lost = 0
    foreach ($key in $acked.Keys) {
        $got = Invoke-Get $key
        if ($got -ne $acked[$key]) {
            $lost++
            $failures += "iteration ${i}: key $key = '$got', expected '$($acked[$key])'"
        }
    }

    $status = if ($lost -eq 0) { "ok" } else { "LOST $lost" }
    Write-Host ("iteration {0,3}/{1}  killed {2}  acked {3,4}  verified {4}" -f `
        $i, $Iterations, $victim.Id, $acked.Count, $status)

    if ($lost -gt 0) { break }
}

Stop-All

Write-Host ""
if ($failures.Count -eq 0) {
    Write-Host "PASS: $($acked.Count) acknowledged writes survived $Iterations crash/restart cycles"
    $exit = 0
}
else {
    Write-Host "FAIL: committed writes were lost"
    $failures | Select-Object -First 20 | ForEach-Object { Write-Host "  $_" }
    $exit = 1
}

if (-not $KeepLogs) { Remove-Item -Recurse -Force $WorkDir -ErrorAction SilentlyContinue }
else { Write-Host "artifacts kept in $WorkDir" }

exit $exit
