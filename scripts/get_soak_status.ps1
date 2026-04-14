param(
    [string]$WorkspaceRoot = (Get-Location).Path,
    [string]$RunLabel,
    [switch]$AsJson
)

$ErrorActionPreference = "Stop"

function Remove-AnsiEscapeCodes {
    param([string]$Line)

    if ([string]::IsNullOrEmpty($Line)) {
        return $Line
    }

    $escape = [char]27
    $ansiPattern = "${escape}\[[0-9;]*[ -/]*[@-~]"
    return [regex]::Replace($Line, $ansiPattern, "")
}

function Extract-Int64 {
    param(
        [string]$Line,
        [string]$Pattern
    )

    $match = [regex]::Match($Line, $Pattern)
    if (-not $match.Success) {
        return $null
    }

    return [int64]$match.Groups[1].Value
}

function Extract-Double {
    param(
        [string]$Line,
        [string]$Pattern
    )

    $match = [regex]::Match($Line, $Pattern)
    if (-not $match.Success) {
        return $null
    }

    return [double]$match.Groups[1].Value
}

$resolvedWorkspace = (Resolve-Path $WorkspaceRoot).Path
$soakRoot = Join-Path $resolvedWorkspace "logs\\soak"

if (-not (Test-Path $soakRoot)) {
    throw "No soak directory found at $soakRoot"
}

if ([string]::IsNullOrWhiteSpace($RunLabel)) {
    $latest = Get-ChildItem -Path $soakRoot -Directory | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if ($null -eq $latest) {
        throw "No soak runs found under $soakRoot"
    }

    $RunLabel = $latest.Name
}

$runDir = Join-Path $soakRoot $RunLabel
if (-not (Test-Path $runDir)) {
    throw "Requested soak run does not exist: $runDir"
}

$stdoutPath = Join-Path $runDir "stdout.log"
$stderrPath = Join-Path $runDir "stderr.log"
$statePath = Join-Path $runDir "run-state.json"
$reportPath = Join-Path $runDir "gate-report.json"

$telemetryLineCount = 0
$latestTelemetry = $null
if (Test-Path $stdoutPath) {
    $telemetryLines = @(Select-String -Path $stdoutPath -Pattern "runtime telemetry" | ForEach-Object { $_.Line })
    $telemetryLineCount = $telemetryLines.Count

    if ($telemetryLineCount -gt 0) {
        $latestTelemetry = Remove-AnsiEscapeCodes -Line $telemetryLines[$telemetryLineCount - 1]
    }
}

$telemetrySummary = $null
if ($null -ne $latestTelemetry) {
    $telemetrySummary = [ordered]@{
        total_depth_events = Extract-Int64 -Line $latestTelemetry -Pattern 'total_depth_events=(\d+)'
        sequence_gap_events = Extract-Int64 -Line $latestTelemetry -Pattern 'sequence_gap_events=(\d+)'
        loss_rate = Extract-Double -Line $latestTelemetry -Pattern 'loss_rate=([-+0-9.eE]+)'
        signals_emitted = Extract-Int64 -Line $latestTelemetry -Pattern 'signals_emitted=(\d+)'
        stale_signals_emitted = Extract-Int64 -Line $latestTelemetry -Pattern 'stale_signals_emitted=(\d+)'
        p99_latency_ms = Extract-Int64 -Line $latestTelemetry -Pattern 'p99_latency_ms=(?:Some\()?([0-9]+)'
    }
}

$gateReport = $null
$gateReportExists = Test-Path $reportPath
if ($gateReportExists) {
    $gateReport = Get-Content -Raw -Path $reportPath | ConvertFrom-Json
}

$state = $null
$stateExists = Test-Path $statePath
if ($stateExists) {
    $state = Get-Content -Raw -Path $statePath | ConvertFrom-Json
}

$result = [ordered]@{
    run_label = $RunLabel
    run_dir = $runDir
    stdout_log = $stdoutPath
    stderr_log = $stderrPath
    state_file = $statePath
    state_file_exists = $stateExists
    state_cargo_pid = if ($stateExists) { $state.cargo_pid } else { $null }
    state_stop_reason = if ($stateExists) { $state.stop_reason } else { $null }
    gate_report_exists = $gateReportExists
    telemetry_lines_observed = $telemetryLineCount
    latest_telemetry = $telemetrySummary
    report_stop_reason = if ($gateReportExists) { $gateReport.stop_reason } else { $null }
    report_overall_pass = if ($gateReportExists) { $gateReport.gate.overall_pass } else { $null }
}

if ($AsJson) {
    $result | ConvertTo-Json -Depth 6
    exit 0
}

Write-Host "Run label: $($result.run_label)"
Write-Host "Run dir: $($result.run_dir)"
Write-Host "Gate report exists: $($result.gate_report_exists)"
Write-Host "Telemetry lines observed: $($result.telemetry_lines_observed)"

if ($null -ne $result.latest_telemetry) {
    Write-Host "Latest telemetry:"
    Write-Host "  total_depth_events=$($result.latest_telemetry.total_depth_events)"
    Write-Host "  sequence_gap_events=$($result.latest_telemetry.sequence_gap_events)"
    Write-Host "  loss_rate=$($result.latest_telemetry.loss_rate)"
    Write-Host "  signals_emitted=$($result.latest_telemetry.signals_emitted)"
    Write-Host "  stale_signals_emitted=$($result.latest_telemetry.stale_signals_emitted)"
    Write-Host "  p99_latency_ms=$($result.latest_telemetry.p99_latency_ms)"
}

if ($result.gate_report_exists) {
    Write-Host "Report gate overall_pass: $($result.report_overall_pass)"
    Write-Host "Report stop_reason: $($result.report_stop_reason)"
}
