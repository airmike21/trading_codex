<#
.SYNOPSIS
Create or print a weekday Task Scheduler entry for the Stage 2 shadow daily ops lane.

.DESCRIPTION
Creates one weekday Task Scheduler task that launches
`trading_codex_stage2_shadow_daily_ops.ps1` from a staged local Windows path as
the single scheduled entrypoint for the bounded local-only Stage 2 shadow daily
ops lane. The default install mode uses the current logged-on interactive user
session. This avoids relying on Task Scheduler launching a PowerShell script
directly from `\\wsl$\...`.

This installer only schedules the existing runner. It does not mutate
`configs/stage2_shadow_ops.json`; `active_pair` must already be intentionally
configured if unattended refresh should do real work instead of the fail-closed
no-op path.

.PARAMETER FolderName
Task Scheduler folder prefix. Default: `TradingCodex`.

.PARAMETER StartTime
Weekday local start time in `HH:mm` 24-hour format. Default: `16:10`.

.PARAMETER WslDistro
WSL distro name. Default: `Ubuntu`.

.PARAMETER WslRepoPath
Repo path inside WSL. Default: `~/trading_codex`.

.PARAMETER WslPython
Python path inside WSL. Defaults to `<WslRepoPath>/.venv/bin/python`.

.PARAMETER Provider
Data provider for `update_data_eod.py`. Default: `stooq`.

.PARAMETER ShadowOpsConfig
Optional shadow ops config override persisted into the task action.

.PARAMETER DataDir
Optional cached market-data directory override persisted into the task action.

.PARAMETER ArchiveRoot
Optional archive root override persisted into the task action.

.PARAMETER PaperBaseDir
Optional local shadow replay paper-lane base-dir override persisted into the
task action.

.PARAMETER InstallMode
Task registration mode:
- `Interactive`: register with the current logged-on user session. This is the
  default Stage 2 acceptance path for this machine/context.
- `Background`: register with non-interactive S4U. This preserves the prior
  background-only behavior used by other Trading Codex task installers.

.PARAMETER PrintOnly
Print the install plan without executing it.

.PARAMETER RunNow
After creating the task, run it once immediately.

.EXAMPLE
./install_stage2_shadow_daily_ops_task.ps1 -PrintOnly -WslRepoPath ~/trading_codex

.EXAMPLE
./install_stage2_shadow_daily_ops_task.ps1 -InstallMode Background -StartTime 16:10 -RunNow
#>
[CmdletBinding()]
param(
  [string]$FolderName = "TradingCodex",
  [string]$StartTime = "16:10",
  [string]$WslDistro = "Ubuntu",
  [string]$WslRepoPath = "~/trading_codex",
  [string]$WslPython,
  [ValidateSet("stooq", "tiingo")]
  [string]$Provider = "stooq",
  [string]$ShadowOpsConfig,
  [string]$DataDir,
  [string]$ArchiveRoot,
  [string]$PaperBaseDir,
  [ValidateSet("Interactive", "Background")]
  [string]$InstallMode = "Interactive",
  [switch]$PrintOnly,
  [switch]$RunNow
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ($StartTime -notmatch '^(?:[01][0-9]|2[0-3]):[0-5][0-9]$') {
  throw "StartTime must use HH:mm 24-hour format. Got: $StartTime"
}

if ([string]::IsNullOrWhiteSpace($WslPython)) {
  $WslPython = "$($WslRepoPath.TrimEnd('/'))/.venv/bin/python"
}

$wrapperPath = Join-Path $PSScriptRoot "trading_codex_stage2_shadow_daily_ops.ps1"
if (-not (Test-Path -LiteralPath $wrapperPath)) {
  throw "Wrapper not found: $wrapperPath"
}

$currentUserName = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$currentUserSid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value

function Get-InstallModeSpec {
  param(
    [string]$ModeName
  )

  switch ($ModeName) {
    "Interactive" {
      return [pscustomobject]@{
        ModeName = "Interactive"
        LogonType = "InteractiveToken"
        PrincipalPreview = "$currentUserName (InteractiveToken logged-on session; runs only while signed in)"
        XmlFileName = "stage2_shadow_daily_ops_interactive.xml"
      }
    }
    "Background" {
      return [pscustomobject]@{
        ModeName = "Background"
        LogonType = "S4U"
        PrincipalPreview = "$currentUserName (S4U non-interactive; local resources only)"
        XmlFileName = "stage2_shadow_daily_ops_background.xml"
      }
    }
    default {
      throw "Unsupported InstallMode: $ModeName"
    }
  }
}

function Quote-WindowsArg {
  param(
    [string]$Value
  )
  if ($null -eq $Value) {
    return '""'
  }
  if ($Value -notmatch '[\s"]') {
    return $Value
  }
  return '"' + ($Value -replace '"', '\"') + '"'
}

function Join-WindowsCommandLine {
  param(
    [string[]]$Values
  )
  $quoted = foreach ($value in $Values) {
    Quote-WindowsArg -Value $value
  }
  return [string]::Join(" ", $quoted)
}

function ConvertTo-XmlText {
  param(
    [string]$Value
  )
  if ($null -eq $Value) {
    return ""
  }
  return [System.Security.SecurityElement]::Escape($Value)
}

function Get-LauncherDirectory {
  if (-not [string]::IsNullOrWhiteSpace($env:LOCALAPPDATA)) {
    return (Join-Path $env:LOCALAPPDATA "TradingCodex\stage2_shadow_ops\launcher")
  }
  return (Join-Path $env:TEMP "TradingCodex\stage2_shadow_ops\launcher")
}

function Get-StagedWrapperPath {
  $launcherDir = Get-LauncherDirectory
  return (Join-Path $launcherDir "trading_codex_stage2_shadow_daily_ops.ps1")
}

function New-TaskSpec {
  $installModeSpec = Get-InstallModeSpec -ModeName $InstallMode
  $taskName = "{0}\stage2_shadow_daily_ops" -f $FolderName
  $scheduledWrapperPath = Get-StagedWrapperPath
  $argList = @(
    "-NoLogo",
    "-NoProfile",
    "-NonInteractive",
    "-ExecutionPolicy",
    "Bypass",
    "-File",
    $scheduledWrapperPath,
    "-Provider",
    $Provider,
    "-WslDistro",
    $WslDistro,
    "-WslRepoPath",
    $WslRepoPath,
    "-WslPython",
    $WslPython
  )
  if (-not [string]::IsNullOrWhiteSpace($ShadowOpsConfig)) {
    $argList += @("-ShadowOpsConfig", $ShadowOpsConfig)
  }
  if (-not [string]::IsNullOrWhiteSpace($DataDir)) {
    $argList += @("-DataDir", $DataDir)
  }
  if (-not [string]::IsNullOrWhiteSpace($ArchiveRoot)) {
    $argList += @("-ArchiveRoot", $ArchiveRoot)
  }
  if (-not [string]::IsNullOrWhiteSpace($PaperBaseDir)) {
    $argList += @("-PaperBaseDir", $PaperBaseDir)
  }

  $arguments = Join-WindowsCommandLine -Values $argList
  return [pscustomobject]@{
    TaskName = $taskName
    StartTime = $StartTime
    InstallMode = $installModeSpec.ModeName
    PrincipalPreview = $installModeSpec.PrincipalPreview
    TaskLogonType = $installModeSpec.LogonType
    XmlFileName = $installModeSpec.XmlFileName
    ActionExecute = "powershell.exe"
    ActionArguments = $arguments
    ScheduledWrapperPath = $scheduledWrapperPath
    PrintableCreate = "schtasks.exe /Create /TN `"$taskName`" /XML `"%TEMP%\$($installModeSpec.XmlFileName)`" /F"
  }
}

function Write-TaskPreview {
  param(
    [pscustomobject]$Task
  )
  Write-Output "# mode=$($Task.InstallMode)"
  Write-Output "# principal=$($Task.PrincipalPreview)"
  Write-Output "# schedule=Mon-Fri $($Task.StartTime)"
  Write-Output "# action=$($Task.ActionExecute) $($Task.ActionArguments)"
  Write-Output $Task.PrintableCreate
  if ($RunNow) {
    Write-Output "schtasks.exe /Run /TN `"$($Task.TaskName)`""
  }
}

function New-TaskXml {
  param(
    [pscustomobject]$Task
  )

  $startBoundary = "{0}T{1}:00" -f (Get-Date).ToString("yyyy-MM-dd"), $Task.StartTime
  $taskUri = "\" + $Task.TaskName

  $escapedAuthor = ConvertTo-XmlText -Value $currentUserName
  $escapedUri = ConvertTo-XmlText -Value $taskUri
  $escapedSid = ConvertTo-XmlText -Value $currentUserSid
  $escapedLogonType = ConvertTo-XmlText -Value $Task.TaskLogonType
  $escapedExecute = ConvertTo-XmlText -Value $Task.ActionExecute
  $escapedArguments = ConvertTo-XmlText -Value $Task.ActionArguments
  $escapedStartBoundary = ConvertTo-XmlText -Value $startBoundary

  return @"
<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Author>$escapedAuthor</Author>
    <URI>$escapedUri</URI>
    <Description>Trading Codex Stage 2 shadow daily ops</Description>
  </RegistrationInfo>
  <Principals>
    <Principal id="Author">
      <UserId>$escapedSid</UserId>
      <LogonType>$escapedLogonType</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <DisallowStartIfOnBatteries>true</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>true</StopIfGoingOnBatteries>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <StartWhenAvailable>true</StartWhenAvailable>
  </Settings>
  <Triggers>
    <CalendarTrigger>
      <StartBoundary>$escapedStartBoundary</StartBoundary>
      <ScheduleByWeek>
        <WeeksInterval>1</WeeksInterval>
        <DaysOfWeek>
          <Monday />
          <Tuesday />
          <Wednesday />
          <Thursday />
          <Friday />
        </DaysOfWeek>
      </ScheduleByWeek>
    </CalendarTrigger>
  </Triggers>
  <Actions Context="Author">
    <Exec>
      <Command>$escapedExecute</Command>
      <Arguments>$escapedArguments</Arguments>
    </Exec>
  </Actions>
</Task>
"@
}

function Install-Task {
  param(
    [pscustomobject]$Task
  )

  $xmlPath = Join-Path $env:TEMP $Task.XmlFileName
  try {
    $launcherDir = Split-Path -Parent $Task.ScheduledWrapperPath
    New-Item -ItemType Directory -Force -Path $launcherDir | Out-Null
    Copy-Item -LiteralPath $wrapperPath -Destination $Task.ScheduledWrapperPath -Force

    $xml = New-TaskXml -Task $Task
    Set-Content -LiteralPath $xmlPath -Value $xml -Encoding Unicode
    & schtasks.exe /Create /TN $Task.TaskName /XML $xmlPath /F
    if ($LASTEXITCODE -ne 0) {
      throw "Failed to create Stage 2 shadow daily ops task: $($Task.TaskName)"
    }
  }
  finally {
    Remove-Item -LiteralPath $xmlPath -ErrorAction SilentlyContinue
  }
}

function Run-TaskNow {
  param(
    [pscustomobject]$Task
  )

  & schtasks.exe /Run /TN $Task.TaskName
  if ($LASTEXITCODE -ne 0) {
    throw "Failed to run task: $($Task.TaskName)"
  }
}

$task = New-TaskSpec

if ($PrintOnly) {
  Write-TaskPreview -Task $task
  exit 0
}

Install-Task -Task $task
if ($RunNow) {
  Run-TaskNow -Task $task
}

Write-Output "Installed Stage 2 shadow daily ops task in $($task.InstallMode) mode: $($task.TaskName)"
