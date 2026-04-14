param(
    [int]$DurationMinutes = 1440,
    [string]$WorkspaceRoot = (Get-Location).Path,
    [string]$RunLabel = (Get-Date -Format "yyyyMMdd-HHmmss"),
    [switch]$Release
)

$ErrorActionPreference = "Stop"

if ($DurationMinutes -le 0) {
    throw "DurationMinutes must be greater than 0"
}

function Resolve-CargoExecutable {
    $rustup = Get-Command rustup -ErrorAction SilentlyContinue
    if ($null -ne $rustup) {
        try {
            $resolved = (& rustup which cargo 2>$null | Select-Object -First 1)
            if ($LASTEXITCODE -eq 0 -and -not [string]::IsNullOrWhiteSpace($resolved)) {
                $candidate = $resolved.Trim()
                if (Test-Path $candidate) {
                    return $candidate
                }
            }
        }
        catch {
        }
    }

    $cargo = Get-Command cargo -ErrorAction Stop
    return $cargo.Source
}

function Get-DescendantProcessIds {
    param(
        [int]$RootPid
    )

    $descendants = New-Object System.Collections.Generic.List[int]
    $queue = New-Object System.Collections.Generic.Queue[int]
    $queue.Enqueue($RootPid)

    while ($queue.Count -gt 0) {
        $parentPid = $queue.Dequeue()
        $children = @(Get-CimInstance Win32_Process -Filter "ParentProcessId = $parentPid" | Select-Object -ExpandProperty ProcessId)

        foreach ($childPid in $children) {
            if (-not $descendants.Contains([int]$childPid)) {
                $descendants.Add([int]$childPid)
                $queue.Enqueue([int]$childPid)
            }
        }
    }

    return @($descendants)
}

function Stop-ProcessTree {
    param(
        [int]$RootPid
    )

    $allPids = @($RootPid) + (Get-DescendantProcessIds -RootPid $RootPid)
    $ordered = $allPids | Sort-Object -Descending -Unique

    foreach ($processId in $ordered) {
        Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue
    }
}

$resolvedWorkspace = (Resolve-Path $WorkspaceRoot).Path
$runDir = Join-Path $resolvedWorkspace ("logs\\soak\\" + $RunLabel)
New-Item -ItemType Directory -Path $runDir -Force | Out-Null

Write-Host "Soak run label: $RunLabel"
Write-Host "Soak output directory: $runDir"

$stdoutPath = Join-Path $runDir "stdout.log"
$stderrPath = Join-Path $runDir "stderr.log"
$stateJsonPath = Join-Path $runDir "run-state.json"
$reportJsonPath = Join-Path $runDir "gate-report.json"
$reportMarkdownPath = Join-Path $runDir "gate-report.md"

$cargoArgs = @("run")
if ($Release) {
    $cargoArgs += "--release"
}
$argumentList = $cargoArgs -join " "

$cargoExecutable = Resolve-CargoExecutable
Write-Host "Resolved cargo executable: $cargoExecutable"

$startedUtc = (Get-Date).ToUniversalTime()
Write-Host "Starting soak run with command: cargo $argumentList"

$process = $null
$rootPid = $null
$stopReason = "process_exited"
$cancelRequested = $false
$cancelHandler = [ConsoleCancelEventHandler] {
    param($sender, $eventArgs)
    $eventArgs.Cancel = $true
    $script:cancelRequested = $true
    Write-Host "Ctrl+C received. Stopping soak child process and finalizing report..."
}

$cancelHandlerRegistered = $false
try {
    [Console]::add_CancelKeyPress($cancelHandler)
    $cancelHandlerRegistered = $true
}
catch {
    Write-Warning "Console cancel handler is unavailable in this host; Ctrl+C passthrough is not guaranteed."
}

try {
    $process = Start-Process -FilePath $cargoExecutable -ArgumentList $argumentList -WorkingDirectory $resolvedWorkspace -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath -PassThru
    $rootPid = $process.Id

    $state = [ordered]@{
        run_label = $RunLabel
        started_utc = $startedUtc.ToString("o")
        workspace_root = $resolvedWorkspace
        cargo_executable = $cargoExecutable
        cargo_pid = $rootPid
        stop_reason = "running"
    }
    $state | ConvertTo-Json -Depth 4 | Set-Content -Path $stateJsonPath -Encoding UTF8

    $deadlineUtc = $startedUtc.AddMinutes($DurationMinutes)

    while ($true) {
        if ($cancelRequested) {
            $stopReason = "ctrl_c_requested"
            break
        }

        $remainingMs = [int64]($deadlineUtc - (Get-Date).ToUniversalTime()).TotalMilliseconds
        if ($remainingMs -le 0) {
            $stopReason = "duration_elapsed"
            break
        }

        $waitMs = [int][Math]::Min($remainingMs, 1000)
        if ($process.WaitForExit($waitMs)) {
            $stopReason = "process_exited"
            break
        }
    }

    if (($stopReason -eq "ctrl_c_requested" -or $stopReason -eq "duration_elapsed") -and -not $process.HasExited) {
        if ($stopReason -eq "duration_elapsed") {
            Write-Host "Configured duration reached; stopping process id $($process.Id)"
        }

        Stop-ProcessTree -RootPid $process.Id
        $null = $process.WaitForExit(5000)
    }
}
finally {
    if ($cancelHandlerRegistered) {
        [Console]::remove_CancelKeyPress($cancelHandler)
    }
}

$finishedUtc = (Get-Date).ToUniversalTime()
$exitCode = -1

if ($null -ne $process) {
    $process.Refresh()
}
if ($null -ne $process -and $process.HasExited) {
    try {
        $exitCode = $process.ExitCode
    }
    catch {
        $exitCode = -1
    }
}

if ($null -eq $exitCode) {
    $exitCode = -1
}

$stoppedByUser = $stopReason -eq "ctrl_c_requested"

if (Test-Path $stateJsonPath) {
    $state = [ordered]@{
        run_label = $RunLabel
        started_utc = $startedUtc.ToString("o")
        finished_utc = $finishedUtc.ToString("o")
        workspace_root = $resolvedWorkspace
        cargo_executable = $cargoExecutable
        cargo_pid = $rootPid
        stop_reason = $stopReason
        process_exit_code = $exitCode
    }
    $state | ConvertTo-Json -Depth 4 | Set-Content -Path $stateJsonPath -Encoding UTF8
}

$telemetryLines = @()
if (Test-Path $stdoutPath) {
    $telemetryLines = @(Select-String -Path $stdoutPath -Pattern "runtime telemetry" | ForEach-Object { $_.Line })
}

function Get-CapturedValue {
    param(
        [string]$Line,
        [string]$Pattern
    )

    $match = [regex]::Match($Line, $Pattern)
    if ($match.Success) {
        return $match.Groups[1].Value
    }

    return $null
}

function Remove-AnsiEscapeCodes {
    param(
        [string]$Line
    )

    if ([string]::IsNullOrEmpty($Line)) {
        return $Line
    }

    $escape = [char]27
    $ansiPattern = "${escape}\[[0-9;]*[ -/]*[@-~]"
    return [regex]::Replace($Line, $ansiPattern, "")
}

function Get-CapturedInt64 {
    param(
        [string]$Line,
        [string]$Pattern
    )

    $value = Get-CapturedValue -Line $Line -Pattern $Pattern
    if ([string]::IsNullOrWhiteSpace($value)) {
        return $null
    }

    return [int64]$value
}

function Get-CapturedDouble {
    param(
        [string]$Line,
        [string]$Pattern
    )

    $value = Get-CapturedValue -Line $Line -Pattern $Pattern
    if ([string]::IsNullOrWhiteSpace($value)) {
        return $null
    }

    return [double]$value
}

$summary = $null
if ($telemetryLines.Count -gt 0) {
    $lastTelemetry = Remove-AnsiEscapeCodes -Line $telemetryLines[$telemetryLines.Count - 1]
    $summary = [ordered]@{
        total_depth_events = Get-CapturedInt64 -Line $lastTelemetry -Pattern 'total_depth_events=(\d+)'
        sequence_gap_events = Get-CapturedInt64 -Line $lastTelemetry -Pattern 'sequence_gap_events=(\d+)'
        loss_rate = Get-CapturedDouble -Line $lastTelemetry -Pattern 'loss_rate=([-+0-9.eE]+)'
        signals_emitted = Get-CapturedInt64 -Line $lastTelemetry -Pattern 'signals_emitted=(\d+)'
        stale_signals_emitted = Get-CapturedInt64 -Line $lastTelemetry -Pattern 'stale_signals_emitted=(\d+)'
        p99_latency_ms = Get-CapturedInt64 -Line $lastTelemetry -Pattern 'p99_latency_ms=(?:Some\()?([0-9]+)'
    }
}

$lossSloMax = 0.0001
$p99SloMaxMs = 60

$telemetryObserved = $telemetryLines.Count -gt 0
$depthEventsObserved = $false
$lossRatePass = $false
$p99LatencyPass = $false

if ($summary -ne $null) {
    if ($summary.total_depth_events -ne $null -and $summary.total_depth_events -gt 0) {
        $depthEventsObserved = $true
    }

    if ($summary.loss_rate -ne $null -and $summary.loss_rate -le $lossSloMax) {
        $lossRatePass = $true
    }

    if ($summary.p99_latency_ms -ne $null -and $summary.p99_latency_ms -le $p99SloMaxMs) {
        $p99LatencyPass = $true
    }
}

$overallPass = $telemetryObserved -and $depthEventsObserved -and $lossRatePass -and $p99LatencyPass

$gate = [ordered]@{
    loss_rate_max = $lossSloMax
    p99_latency_max_ms = $p99SloMaxMs
    telemetry_observed = $telemetryObserved
    depth_events_observed = $depthEventsObserved
    loss_rate_pass = $lossRatePass
    p99_latency_pass = $p99LatencyPass
    overall_pass = $overallPass
}

$report = [ordered]@{
    run_label = $RunLabel
    workspace_root = $resolvedWorkspace
    started_utc = $startedUtc.ToString("o")
    finished_utc = $finishedUtc.ToString("o")
    configured_duration_minutes = $DurationMinutes
    command = "cargo $argumentList"
    cargo_executable = $cargoExecutable
    process_id = $rootPid
    stop_reason = $stopReason
    stopped_by_user = $stoppedByUser
    process_exit_code = $exitCode
    state_file = $stateJsonPath
    stdout_log = $stdoutPath
    stderr_log = $stderrPath
    telemetry_lines_observed = $telemetryLines.Count
    telemetry_summary = $summary
    gate = $gate
}

$report | ConvertTo-Json -Depth 6 | Set-Content -Path $reportJsonPath -Encoding UTF8

$markdown = @(
    "# Soak Gate Report",
    "",
    "- Run label: $RunLabel",
    "- Started UTC: $($report.started_utc)",
    "- Finished UTC: $($report.finished_utc)",
    "- Configured duration minutes: $DurationMinutes",
    "- Command: cargo $argumentList",
    "- Stop reason: $($report.stop_reason)",
    "- Stopped by user: $($report.stopped_by_user)",
    "- Process exit code: $exitCode",
    "- Telemetry lines observed: $($telemetryLines.Count)",
    "- Loss rate: $($summary.loss_rate)",
    "- p99 latency ms: $($summary.p99_latency_ms)",
    "- Gate overall pass: $($gate.overall_pass)",
    "",
    "## Gate Checks",
    "- telemetry_observed: $($gate.telemetry_observed)",
    "- depth_events_observed: $($gate.depth_events_observed)",
    "- loss_rate_pass: $($gate.loss_rate_pass) (<= $lossSloMax)",
    "- p99_latency_pass: $($gate.p99_latency_pass) (<= $p99SloMaxMs ms)",
    "",
    "## Outputs",
    "- stdout log: $stdoutPath",
    "- stderr log: $stderrPath",
    "- json report: $reportJsonPath"
)

$markdown | Set-Content -Path $reportMarkdownPath -Encoding UTF8

Write-Host "Soak JSON report written to $reportJsonPath"
Write-Host "Soak markdown report written to $reportMarkdownPath"

if ($stopReason -eq "ctrl_c_requested") {
    exit 130
}

if ($overallPass) {
    exit 0
}

exit 2
