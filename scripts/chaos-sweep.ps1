param(
    [int]$Seeds = 20,
    [string]$Duration = "3s",
    [int]$StartSeed = 1
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot

Push-Location $repo
try {
    $passed = 0
    $failed = @()

    for ($i = 0; $i -lt $Seeds; $i++) {
        $seed = $StartSeed + ($i * 13)

        $output = & go test ./raft/ -run TestChaos "-chaos.seed=$seed" "-chaos.duration=$Duration" -count 1 -v 2>&1
        $ok = ($LASTEXITCODE -eq 0)

        $safety = @($output | Select-String -Pattern "SAFETY").Count
        $summary = ($output | Select-String -Pattern "invariant checks=") -replace '.*invariant', 'invariant'

        if ($ok -and $safety -eq 0) {
            $passed++
            Write-Host ("seed {0,-8} ok    {1}" -f $seed, $summary)
        }
        else {
            $failed += $seed
            Write-Host ("seed {0,-8} FAIL  {1}" -f $seed, $summary) -ForegroundColor Red

            $output |
                Select-String -Pattern "invariant violation|missing after|SAFETY" |
                Select-Object -First 5 |
                ForEach-Object { Write-Host "    $_" -ForegroundColor Red }
        }
    }

    Write-Host ""
    if ($failed.Count -eq 0) {
        Write-Host "PASS: $passed/$Seeds seeds clean at $Duration each"
        exit 0
    }

    Write-Host "FAIL: $($failed.Count)/$Seeds seeds violated an invariant"
    Write-Host "failing seeds: $($failed -join ', ')"
    Write-Host "reproduce with: go test ./raft/ -run TestChaos -chaos.seed=<seed> -v"
    exit 1
}
finally {
    Pop-Location
}
