<#
.SYNOPSIS
Create or print weekday Task Scheduler entries for the scheduled dual momentum comparison run.

.DESCRIPTION
Creates two weekday tasks in Windows Task Scheduler:
- 08:25 local time -> morning_0825
- 15:35 local time -> afternoon_1535

The installer defaults to `Auto` mode:
- First it attempts a true non-interactive S4U task registration.
- If the host rejects that registration, it falls back to an interactive
  hidden launcher that keeps the compare task visually silent.

Both task modes launch the WSL-side orchestration and write durable
logs/snapshots/review artifacts.

.PARAMETER FolderName
Task Scheduler folder prefix. Default: `TradingCodex`.

.PARAMETER WslDistro
WSL distro name. Default: `Ubuntu`.

.PARAMETER WslRepoPath
Repo path inside WSL. Default: `~/trading_codex`.

.PARAMETER WslPython
Python path inside WSL. Defaults to `<WslRepoPath>/.venv/bin/python`.

.PARAMETER PresetsFile
Optional presets file override. Windows paths are converted to WSL paths.

.PARAMETER BaseDir
Durable WSL base directory for scheduled comparison artifacts.
Default: `~/.trading_codex/scheduled_runs`

.PARAMETER InstallMode
Task registration mode:
- `Auto`: try non-interactive S4U first, then fall back to hidden interactive.
- `Background`: require non-interactive S4U registration.
- `Hidden`: install the interactive hidden-launch fallback only.

.PARAMETER PrintOnly
Print the install plan without executing it.

.PARAMETER RunNow
After creating each task, run it once immediately.

.EXAMPLE
./install_dual_mom_compare_tasks.ps1 -PrintOnly

.EXAMPLE
./install_dual_mom_compare_tasks.ps1 -InstallMode Hidden -RunNow
#>
[CmdletBinding()]
param(
  [string]$FolderName = "TradingCodex",
  [string]$WslDistro = "Ubuntu",
  [string]$WslRepoPath = "~/trading_codex",
  [string]$WslPython,
  [string]$PresetsFile,
  [string]$BaseDir = "~/.trading_codex/scheduled_runs",
  [ValidateSet("Auto", "Background", "Hidden")]
  [string]$InstallMode = "Auto",
  [switch]$PrintOnly,
  [switch]$RunNow
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$DefaultWslRepoPath = "~/trading_codex"

if ([string]::IsNullOrWhiteSpace($WslPython)) {
  $WslPython = "$($WslRepoPath.TrimEnd('/'))/.venv/bin/python"
}
$DefaultWslPython = "$($DefaultWslRepoPath.TrimEnd('/'))/.venv/bin/python"

$wrapperPath = Join-Path $PSScriptRoot "trading_codex_scheduled_dual_compare.ps1"
if (-not (Test-Path -LiteralPath $wrapperPath)) {
  throw "Wrapper not found: $wrapperPath"
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

function Resolve-WslPath {
  param(
    [string]$PathValue,
    [string]$Distro
  )
  if ([string]::IsNullOrWhiteSpace($PathValue)) {
    return $null
  }

  if ($PathValue -like "~*") {
    $resolved = & wsl.exe -d $Distro -- bash -lc "realpath -m $PathValue" 2>$null
    $resolved = ($resolved | Out-String).Trim()
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($resolved)) {
      throw "Failed to resolve WSL path: $PathValue"
    }
    return $resolved
  }

  if ($PathValue.StartsWith("/")) {
    return $PathValue
  }

  $candidate = $PathValue
  if (Test-Path -LiteralPath $PathValue) {
    $candidate = (Resolve-Path -LiteralPath $PathValue).Path
  }

  $resolvedWindows = & wsl.exe -d $Distro -- wslpath -a $candidate 2>$null
  $resolvedWindows = ($resolvedWindows | Out-String).Trim()
  if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($resolvedWindows)) {
    throw "Failed to convert path to WSL path: $PathValue"
  }
  return $resolvedWindows
}

function Resolve-PresetsFileArg {
  param(
    [string]$Distro,
    [string]$ExplicitPresetsFile
  )
  if (-not [string]::IsNullOrWhiteSpace($ExplicitPresetsFile)) {
    return Resolve-WslPath -PathValue $ExplicitPresetsFile -Distro $Distro
  }
  return $null
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

$ResolvedWslRepoPath = $null
$shellRunnerPath = $null
if ($InstallMode -ne "Hidden") {
  $ResolvedWslRepoPath = Resolve-WslPath -PathValue $WslRepoPath -Distro $WslDistro
  $shellRunnerPath = "$($ResolvedWslRepoPath.TrimEnd('/'))/scripts/windows/trading_codex_scheduled_dual_compare.sh"
  if (-not $PrintOnly) {
    & wsl.exe -d $WslDistro -- test -f $shellRunnerPath *> $null
    if ($LASTEXITCODE -ne 0) {
      throw "WSL shell runner not found: $shellRunnerPath"
    }
  }
}

$resolvedPresetsFile = Resolve-PresetsFileArg -Distro $WslDistro -ExplicitPresetsFile $PresetsFile
$currentUserName = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$currentUserSid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value

function New-TaskSpec {
  param(
    [string]$Window,
    [string]$StartTime
  )

  $taskName = "{0}\{1}_dual_compare" -f $FolderName, $Window
  $wslArgList = @()
  if ($null -ne $shellRunnerPath) {
    $wslArgList = @(
      "-d",
      $WslDistro,
      "--",
      "bash",
      $shellRunnerPath,
      "--window",
      $Window,
      "--base-dir",
      $BaseDir
    )
    if ($WslPython -ne $DefaultWslPython) {
      $wslArgList += @("--python", $WslPython)
    }
  }
  $hiddenArgList = @(
    "powershell.exe",
    "-NoLogo",
    "-NoProfile",
    "-NonInteractive",
    "-WindowStyle",
    "Hidden",
    "-ExecutionPolicy",
    "Bypass",
    "-File",
    $wrapperPath,
    "-Window",
    $Window
  )
  if ($WslDistro -ne "Ubuntu") {
    $hiddenArgList += @("-WslDistro", $WslDistro)
  }
  if ($WslRepoPath -ne $DefaultWslRepoPath) {
    $hiddenArgList += @("-WslRepoPath", $WslRepoPath)
  }
  if ($WslPython -ne $DefaultWslPython) {
    $hiddenArgList += @("-WslPython", $WslPython)
  }
  if ($BaseDir -ne "~/.trading_codex/scheduled_runs") {
    $hiddenArgList += @("-BaseDir", $BaseDir)
  }
  if ($null -ne $resolvedPresetsFile) {
    if ($null -ne $shellRunnerPath) {
      $wslArgList += @("--presets-file", $resolvedPresetsFile)
    }
    $hiddenArgList += @("-PresetsFile", $resolvedPresetsFile)
  }
  $wslArguments = $null
  if ($wslArgList.Count -gt 0) {
    $wslArguments = Join-WindowsCommandLine -Values $wslArgList
  }
  $hiddenTaskRun = Join-WindowsCommandLine -Values $hiddenArgList
  $printableHiddenTaskRun = $hiddenTaskRun.Replace('"', '`"')
  $printableHidden = "schtasks.exe /Create /TN `"$taskName`" /TR `"$printableHiddenTaskRun`" /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST $StartTime /RL LIMITED /F"
  $printableBackground = "schtasks.exe /Create /TN `"$taskName`" /XML `"%TEMP%\$($Window)_dual_compare_background.xml`" /F"

  return [pscustomobject]@{
    Window = $Window
    StartTime = $StartTime
    TaskName = $taskName
    ActionExecute = "wsl.exe"
    ActionArguments = $wslArguments
    HiddenTaskRun = $hiddenTaskRun
    PrintableHiddenCreate = $printableHidden
    PrintableBackgroundCreate = $printableBackground
  }
}

function Write-TaskPreview {
  param(
    [pscustomobject]$Task,
    [string]$ModeName
  )

  if ($ModeName -eq "Background") {
    Write-Output "# mode=Background"
    Write-Output "# principal=$currentUserName (S4U non-interactive; local resources only)"
    Write-Output "# action=$($Task.ActionExecute) $($Task.ActionArguments)"
    Write-Output $Task.PrintableBackgroundCreate
  }
  elseif ($ModeName -eq "Hidden") {
    Write-Output "# mode=Hidden"
    Write-Output "# action=$($Task.HiddenTaskRun)"
    Write-Output $Task.PrintableHiddenCreate
  }
  else {
    Write-Output "# mode=Auto"
    Write-Output "# background principal=$currentUserName (S4U non-interactive; local resources only)"
    Write-Output "# background action=$($Task.ActionExecute) $($Task.ActionArguments)"
    Write-Output $Task.PrintableBackgroundCreate
    Write-Output "# fallback=Hidden"
    Write-Output "# fallback action=$($Task.HiddenTaskRun)"
    Write-Output $Task.PrintableHiddenCreate
  }
  if ($RunNow) {
    Write-Output "schtasks.exe /Run /TN `"$($Task.TaskName)`""
  }
  Write-Output ""
}

function New-BackgroundTaskXml {
  param(
    [pscustomobject]$Task
  )

  $startBoundary = "{0}T{1}:00" -f (Get-Date).ToString("yyyy-MM-dd"), $Task.StartTime
  $taskUri = "\" + $Task.TaskName

  $escapedAuthor = ConvertTo-XmlText -Value $currentUserName
  $escapedUri = ConvertTo-XmlText -Value $taskUri
  $escapedSid = ConvertTo-XmlText -Value $currentUserSid
  $escapedExecute = ConvertTo-XmlText -Value $Task.ActionExecute
  $escapedArguments = ConvertTo-XmlText -Value $Task.ActionArguments
  $escapedStartBoundary = ConvertTo-XmlText -Value $startBoundary

  return @"
<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.2" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Author>$escapedAuthor</Author>
    <URI>$escapedUri</URI>
  </RegistrationInfo>
  <Principals>
    <Principal id="Author">
      <UserId>$escapedSid</UserId>
      <LogonType>S4U</LogonType>
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

function Install-HiddenTask {
  param(
    [pscustomobject]$Task
  )

  if ($Task.HiddenTaskRun.Length -gt 261) {
    throw "Hidden task command exceeds schtasks /TR 261 character limit: $($Task.HiddenTaskRun.Length)"
  }

  & schtasks.exe /Create /TN $Task.TaskName /TR $Task.HiddenTaskRun /SC WEEKLY /D MON,TUE,WED,THU,FRI /ST $Task.StartTime /RL LIMITED /F
  if ($LASTEXITCODE -ne 0) {
    throw "Failed to create hidden compare task: $($Task.TaskName)"
  }
}

function Install-BackgroundTask {
  param(
    [pscustomobject]$Task
  )

  $xmlPath = Join-Path $env:TEMP ("trading_codex_{0}_background.xml" -f $Task.Window)
  try {
    $xml = New-BackgroundTaskXml -Task $Task
    Set-Content -LiteralPath $xmlPath -Value $xml -Encoding Unicode
    & schtasks.exe /Create /TN $Task.TaskName /XML $xmlPath /F
    if ($LASTEXITCODE -ne 0) {
      throw "Failed to create background task: $($Task.TaskName)"
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

$tasks = @(
  (New-TaskSpec -Window "morning_0825" -StartTime "08:25"),
  (New-TaskSpec -Window "afternoon_1535" -StartTime "15:35")
)

if ($PrintOnly) {
  foreach ($task in $tasks) {
    Write-TaskPreview -Task $task -ModeName $InstallMode
  }
  exit 0
}

$effectiveMode = $InstallMode
$backgroundFailure = $null

switch ($InstallMode) {
  "Background" {
    foreach ($task in $tasks) {
      Install-BackgroundTask -Task $task
    }
  }
  "Hidden" {
    foreach ($task in $tasks) {
      Install-HiddenTask -Task $task
    }
  }
  "Auto" {
    try {
      foreach ($task in $tasks) {
        Install-BackgroundTask -Task $task
      }
      $effectiveMode = "Background"
    }
    catch {
      $backgroundFailure = $_
      $effectiveMode = "Hidden"
      Write-Warning ("Background registration failed for scheduled dual compare tasks: {0}" -f $_.Exception.Message)
      Write-Warning "Falling back to the hidden interactive launcher. This avoids the visible popup, but it is not a true background task."
      foreach ($task in $tasks) {
        Install-HiddenTask -Task $task
      }
    }
  }
}

if ($RunNow) {
  foreach ($task in $tasks) {
    Run-TaskNow -Task $task
  }
}

Write-Output ("Installed scheduled dual compare tasks in {0} mode." -f $effectiveMode)
if ($null -ne $backgroundFailure) {
  Write-Output ("Background registration error: {0}" -f $backgroundFailure.Exception.Message)
}
