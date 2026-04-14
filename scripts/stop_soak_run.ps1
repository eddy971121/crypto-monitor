param(
    [string]$WorkspaceRoot = (Get-Location).Path,
    [string]$RunLabel
)

$ErrorActionPreference = "Stop"

function Get-DescendantProcessIds {
    param([int]$RootPid)

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
    param([int]$RootPid)

    $allPids = @($RootPid) + (Get-DescendantProcessIds -RootPid $RootPid)
    $ordered = $allPids | Sort-Object -Descending -Unique

    foreach ($processId in $ordered) {
        Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue
    }

    return $ordered.Count
}

function Resolve-RunDirectory {
    param(
        [string]$SoakRoot,
        [string]$RunLabel
    )

    if (-not (Test-Path $SoakRoot)) {
        throw "No soak directory found at $SoakRoot"
    }

    if ([string]::IsNullOrWhiteSpace($RunLabel)) {
        $latest = Get-ChildItem -Path $SoakRoot -Directory | Sort-Object LastWriteTime -Descending | Select-Object -First 1
        if ($null -eq $latest) {
            throw "No soak runs found under $SoakRoot"
        }

        return $latest.FullName
    }

    $runDir = Join-Path $SoakRoot $RunLabel
    if (-not (Test-Path $runDir)) {
        throw "Requested soak run does not exist: $runDir"
    }

    return $runDir
}

function Stop-ByStateFile {
    param([string]$StatePath)

    if (-not (Test-Path $StatePath)) {
        return $false
    }

    $state = Get-Content -Raw -Path $StatePath | ConvertFrom-Json
    if ($null -eq $state.cargo_pid) {
        return $false
    }

    $rootPid = [int]$state.cargo_pid
    $target = Get-Process -Id $rootPid -ErrorAction SilentlyContinue
    if ($null -eq $target) {
        return $false
    }

    $count = Stop-ProcessTree -RootPid $rootPid
    Write-Host "Stopped process tree via state file cargo_pid=$rootPid (count=$count)"
    return $true
}

function Stop-ByWorkspaceProcessScan {
    param([string]$WorkspaceRoot)

    $workspaceNormalized = (Resolve-Path $WorkspaceRoot).Path.ToLowerInvariant()
    $targets = @(Get-CimInstance Win32_Process -Filter "Name = 'crypto-monitor.exe'")

    if ($targets.Count -eq 0) {
        Write-Host "No crypto-monitor.exe process found for fallback stop."
        return
    }

    $stoppedCount = 0
    foreach ($process in $targets) {
        $exePath = ($process.ExecutablePath | ForEach-Object { $_ })
        if ([string]::IsNullOrWhiteSpace($exePath)) {
            continue
        }

        if (-not $exePath.ToLowerInvariant().StartsWith($workspaceNormalized)) {
            continue
        }

        $count = Stop-ProcessTree -RootPid ([int]$process.ProcessId)
        $stoppedCount += $count
        Write-Host "Stopped workspace process tree root pid=$($process.ProcessId) (count=$count)"
    }

    if ($stoppedCount -eq 0) {
        Write-Host "No workspace-owned crypto-monitor process trees were stopped."
    }
}

$resolvedWorkspace = (Resolve-Path $WorkspaceRoot).Path
$soakRoot = Join-Path $resolvedWorkspace "logs\\soak"
$runDir = Resolve-RunDirectory -SoakRoot $soakRoot -RunLabel $RunLabel
$statePath = Join-Path $runDir "run-state.json"

$stopped = Stop-ByStateFile -StatePath $statePath
if (-not $stopped) {
    Stop-ByWorkspaceProcessScan -WorkspaceRoot $resolvedWorkspace
}

if (Test-Path $statePath) {
    $state = Get-Content -Raw -Path $statePath | ConvertFrom-Json

    if ($state.PSObject.Properties.Name -contains "stop_reason") {
        $state.stop_reason = "manual_stop_script"
    }
    else {
        $state | Add-Member -NotePropertyName "stop_reason" -NotePropertyValue "manual_stop_script"
    }

    if ($state.PSObject.Properties.Name -contains "stopped_utc") {
        $state.stopped_utc = (Get-Date).ToUniversalTime().ToString("o")
    }
    else {
        $state | Add-Member -NotePropertyName "stopped_utc" -NotePropertyValue ((Get-Date).ToUniversalTime().ToString("o"))
    }

    $state | ConvertTo-Json -Depth 6 | Set-Content -Path $statePath -Encoding UTF8
    Write-Host "Updated state file: $statePath"
}
