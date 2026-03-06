<#
.SYNOPSIS
Create or print weekday Task Scheduler entries for the scheduled dual momentum comparison run.

.DESCRIPTION
Creates two weekday tasks in Windows Task Scheduler:
- 08:25 local time -> morning_0825
- 15:35 local time -> afternoon_1535

Both tasks call `trading_codex_scheduled_dual_compare.ps1`, which launches the
WSL-side orchestration and writes durable logs/snapshots/review artifacts.

.PARAMETER FolderName
Task Scheduler folder prefix. Default: `TradingCodex`.

.PARAMETER WslDistro
WSL distro name. Default: `Ubuntu`.

.PARAMETER WslRepoPath
Repo path inside WSL. Default: `~/trading_codex`.

.PARAMETER WslPython
Python path inside WSL. Defaults to `<WslRepoPath>/.venv/bin/python`.

.PARAMETER PresetsFile
Optional presets file override. Windows paths are converted by the wrapper.

.PARAMETER BaseDir
Durable WSL base directory for scheduled comparison artifacts.
Default: `~/.trading_codex/scheduled_runs`

.PARAMETER PrintOnly
Print the `schtasks.exe` commands without executing them.

.PARAMETER RunNow
After creating each task, run it once immediately.

.EXAMPLE
./install_dual_mom_compare_tasks.ps1 -PrintOnly

.EXAMPLE
./install_dual_mom_compare_tasks.ps1 -RunNow
#>
[CmdletBinding()]
param(
  [string]$FolderName = "TradingCodex",
  [string]$WslDistro = "Ubuntu",
  [string]$WslRepoPath = "~/trading_codex",
  [string]$WslPython,
  [string]$PresetsFile,
  [string]$BaseDir = "~/.trading_codex/scheduled_runs",
  [switch]$PrintOnly,
  [switch]$RunNow
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($WslPython)) {
  $WslPython = "$($WslRepoPath.TrimEnd('/'))/.venv/bin/python"
}

$wrapperPath = Join-Path $PSScriptRoot "trading_codex_scheduled_dual_compare.ps1"
if (-not (Test-Path -LiteralPath $wrapperPath)) {
  throw "Wrapper not found: $wrapperPath"
}

function New-TaskSpec {
  param(
    [string]$Window,
    [string]$StartTime
  )

  $taskName = "{0}\{1}_dual_compare" -f $FolderName, $Window
  $taskRun = @(
    "powershell.exe",
    "-NoProfile",
    "-ExecutionPolicy",
    "Bypass",
    "-File",
    "`"$wrapperPath`"",
    "-Window",
    $Window,
    "-WslDistro",
    "`"$WslDistro`"",
    "-WslRepoPath",
    "`"$WslRepoPath`"",
    "-WslPython",
    "`"$WslPython`"",
    "-BaseDir",
    "`"$BaseDir`""
  )
  if (-not [string]::IsNullOrWhiteSpace($PresetsFile)) {
    $taskRun += @("-PresetsFile", "`"$PresetsFile`"")
  }

  $taskRunString = [string]::Join(" ", $taskRun)
  $printable = "schtasks.exe /Create /TN `"$taskName`" /TR `"$taskRunString`" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST $StartTime /F"

  return [pscustomobject]@{
    Window = $Window
    StartTime = $StartTime
    TaskName = $taskName
    TaskRun = $taskRunString
    PrintableCreate = $printable
  }
}

$tasks = @(
  (New-TaskSpec -Window "morning_0825" -StartTime "08:25"),
  (New-TaskSpec -Window "afternoon_1535" -StartTime "15:35")
)

foreach ($task in $tasks) {
  if ($PrintOnly) {
    Write-Output $task.PrintableCreate
    if ($RunNow) {
      Write-Output "schtasks.exe /Run /TN `"$($task.TaskName)`""
    }
    Write-Output ""
    continue
  }

  & schtasks.exe /Create /TN $task.TaskName /TR $task.TaskRun /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST $task.StartTime /F
  if ($LASTEXITCODE -ne 0) {
    throw "Failed to create task: $($task.TaskName)"
  }

  if ($RunNow) {
    & schtasks.exe /Run /TN $task.TaskName
    if ($LASTEXITCODE -ne 0) {
      throw "Failed to run task: $($task.TaskName)"
    }
  }
}
