# Run from the repository root. These 30-second samples are diagnostic, not acceptance.
param([string[]]$Variants = @('baseline', 'cubic', 'cubic-wide32', 'reno-wide32', 'bbr', 'sager-bbr'))
$ErrorActionPreference = 'Stop'
$taskOutput = (Resolve-Path $PSScriptRoot).Path
foreach ($taskVariant in $Variants) {
    $taskBinary = (Resolve-Path "bin/check-$taskVariant-lab.exe").Path
    foreach ($taskEncoding in @('delta', 'full')) {
        $taskName = "diagnostic-$taskVariant-$taskEncoding-300ms-30s"
        Write-Output "START $taskName"
        & $taskBinary -isolate -clients 16 -entities 256 -state-bytes 32 -workload motion -encoding $taskEncoding `
            -duration 30s -rtt 300ms -jitter 30ms -loss .05 -duplicate .02 -reorder .02 -seed 7 `
            -report "$taskOutput/$taskName.json" 2>&1 | Tee-Object -FilePath "$taskOutput/$taskName.txt"
        Write-Output "END $taskName exit=$LASTEXITCODE"
    }
}
