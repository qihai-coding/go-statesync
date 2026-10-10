# Recalculate acceptance from the saved formal reports; failure returns exit 1.
$ErrorActionPreference = 'Stop'
$taskPairs = @(
    @('normal', 'normal-delta-60s', 'normal-full-60s', .5),
    @('weak-150ms-seed7', 'weak-150ms-seed7-delta-5m', 'weak-150ms-seed7-full-5m', .5),
    @('weak-150ms-seed701', 'weak-150ms-seed701-delta-5m', 'weak-150ms-seed701-full-5m', .5),
    @('weak-300ms-seed7', 'weak-300ms-seed7-delta-5m', 'weak-300ms-seed7-full-5m', .5),
    @('weak-300ms-seed701', 'weak-300ms-seed701-delta-5m', 'weak-300ms-seed701-full-5m', .5),
    @('entropy', 'entropy-delta-60s', 'entropy-full-60s', -.05)
)
$taskComparisons = foreach ($taskPair in $taskPairs) {
    $taskDelta = Get-Content -LiteralPath "$PSScriptRoot/$($taskPair[1]).json" -Raw | ConvertFrom-Json
    $taskFull = Get-Content -LiteralPath "$PSScriptRoot/$($taskPair[2]).json" -Raw | ConvertFrom-Json
    if ($taskDelta.TotalApplicationBytes -le 0 -or $taskFull.TotalApplicationBytes -le 0) {
        throw "Missing application byte count: $($taskPair[0])"
    }
    $taskReduction = 1 - $taskDelta.TotalApplicationBytes / $taskFull.TotalApplicationBytes
    [ordered]@{
        Condition = $taskPair[0]; DeltaReport = "$($taskPair[1]).json"; FullReport = "$($taskPair[2]).json"
        DeltaBytes = $taskDelta.TotalApplicationBytes; FullBytes = $taskFull.TotalApplicationBytes
        ReductionFraction = $taskReduction; RequiredReductionFraction = $taskPair[3]
        ByteTargetPassed = ($taskReduction -ge $taskPair[3])
        DeltaDisplayAgeP95Seconds = $taskDelta.WorstEntityDisplayAgeP95Seconds
        FullDisplayAgeP95Seconds = $taskFull.WorstEntityDisplayAgeP95Seconds
        DeltaSentEntityUpdates = $taskDelta.SentEntityUpdates; FullSentEntityUpdates = $taskFull.SentEntityUpdates
        DeltaAppliedEntityUpdates = $taskDelta.AppliedEntityUpdates; FullAppliedEntityUpdates = $taskFull.AppliedEntityUpdates
        DeltaRecoverySeconds = $taskDelta.RecoverySeconds; FullRecoverySeconds = $taskFull.RecoverySeconds
        DeltaWithoutReliableCorrection = $taskDelta.RecoveryWithoutReliableCorrection
        FullWithoutReliableCorrection = $taskFull.RecoveryWithoutReliableCorrection
    }
}
$taskComparisons | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath "$PSScriptRoot/comparison.json" -Encoding utf8
$taskFailedMembers = 0
foreach ($taskName in (@($taskPairs | ForEach-Object { $_[1]; $_[2] }) + @(
    'boundary-512-600-delta-60s', 'boundary-512-600-full-60s', 'motion-delta-10m', 'resume-8x16-delta-60s', 'arena-delta-20s'))) {
    $taskReport = Get-Content -LiteralPath "$PSScriptRoot/$taskName.json" -Raw | ConvertFrom-Json
    if ($taskReport.Passed -ne $true) { $taskFailedMembers++ }
}
$taskFailedPairs = @($taskComparisons | Where-Object { -not $_.ByteTargetPassed }).Count
Write-Output "Failed byte pairs: $taskFailedPairs; failed formal members: $taskFailedMembers"
if ($taskFailedPairs -gt 0 -or $taskFailedMembers -gt 0) { exit 1 }
