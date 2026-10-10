# Run from the repository root after building bin/check.exe.
# Pair members use independent server/load processes and UDP sockets.
$ErrorActionPreference = 'Stop'
$taskOutput = (Resolve-Path reports/development-v0.3.0).Path
$taskBinary = (Resolve-Path bin/check.exe).Path
$taskCommon = @('-isolate', '-clients', '16', '-entities', '256', '-state-bytes', '32')

function Invoke-Check($taskName, $taskArguments) {
    Write-Output "START $taskName"
    & $taskBinary @taskCommon @taskArguments -report "reports/development-v0.3.0/$taskName.json" 2>&1 |
        Tee-Object -FilePath "$taskOutput/$taskName.txt"
    Write-Output "END $taskName exit=$LASTEXITCODE"
}

foreach ($taskEncoding in @('delta', 'full')) {
    Invoke-Check "normal-$taskEncoding-60s" @('-duration', '60s', '-workload', 'motion', '-encoding', $taskEncoding)
}

foreach ($taskRTT in @('150ms', '300ms')) {
    foreach ($taskSeed in @(7, 701)) {
        $taskProcesses = @()
        try {
            foreach ($taskEncoding in @('delta', 'full')) {
                $taskName = "weak-$taskRTT-seed$taskSeed-$taskEncoding-5m"
                $taskArguments = $taskCommon + @('-duration', '5m', '-workload', 'motion', '-encoding', $taskEncoding,
                    '-rtt', $taskRTT, '-jitter', '30ms', '-loss', '.05', '-duplicate', '.02', '-reorder', '.02',
                    '-seed', "$taskSeed", '-report', "reports/development-v0.3.0/$taskName.json")
                Write-Output "START $taskName"
                $taskProcesses += Start-Process -FilePath $taskBinary -ArgumentList $taskArguments -WindowStyle Hidden -PassThru `
                    -RedirectStandardOutput "$taskOutput/$taskName.txt" -RedirectStandardError "$taskOutput/$taskName.stderr.txt"
            }
            do {
                Start-Sleep -Seconds 30
                $taskRunning = @($taskProcesses | Where-Object { -not $_.HasExited })
                Write-Output "PROGRESS weak-$taskRTT-seed$taskSeed running=$($taskRunning.Count)"
            } while ($taskRunning.Count -gt 0)
            foreach ($taskProcess in $taskProcesses) {
                $taskProcess.WaitForExit()
                Write-Output "END process=$($taskProcess.Id) exit=$($taskProcess.ExitCode)"
            }
        } finally {
            foreach ($taskProcess in $taskProcesses) {
                if (-not $taskProcess.HasExited) { Stop-Process -Id $taskProcess.Id }
            }
        }
    }
}

foreach ($taskEncoding in @('delta', 'full')) {
    Invoke-Check "entropy-$taskEncoding-60s" @('-duration', '60s', '-workload', 'entropy', '-encoding', $taskEncoding)
    Invoke-Check "boundary-512-600-$taskEncoding-60s" @('-duration', '60s', '-workload', 'motion', '-encoding', $taskEncoding,
        '-state-bytes', '512', '-datagram-size', '600')
}

Invoke-Check 'motion-delta-10m' @('-duration', '10m', '-workload', 'motion', '-encoding', 'delta')
Invoke-Check 'resume-8x16-delta-60s' @('-duration', '60s', '-rooms', '8', '-resume-every', '3s', '-workload', 'motion', '-encoding', 'delta')
Invoke-Check 'arena-delta-20s' @('-duration', '20s', '-workload', 'arena', '-encoding', 'delta', '-resume-every', '3s')
& "$PSScriptRoot/compare.ps1"
exit $LASTEXITCODE
