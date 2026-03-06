<#
.SYNOPSIS
Launch the read-only Trading Codex daily summary inside WSL Ubuntu.

.DESCRIPTION
Runs `scripts/daily_summary.py` via WSL using the repo venv Python.
If `~/trading_codex/configs/presets.json` exists inside WSL, the wrapper
passes it through as `--presets-file`; otherwise it falls back to the
Python script defaults.

.PARAMETER Json
Pass `--emit json` to `scripts/daily_summary.py`.

.PARAMETER Preset
Preset name(s) to summarize. Repeat or pass an array.

.PARAMETER PresetsFile
Explicit presets file override. Windows paths are converted to WSL paths.

.PARAMETER WslDistro
WSL distro name. Default: `Ubuntu`.

.PARAMETER WslRepoPath
Repo path inside WSL. Default: `~/trading_codex`.

.PARAMETER WslPython
Python path inside WSL. Defaults to `<WslRepoPath>/.venv/bin/python`.

.PARAMETER Help
Show this help text.

.EXAMPLE
./trading_codex_daily_summary.ps1

.EXAMPLE
./trading_codex_daily_summary.ps1 -Json

.EXAMPLE
./trading_codex_daily_summary.ps1 -Preset vm_core -Preset vm_core_due
#>
[CmdletBinding()]
param(
  [switch]$Json,
  [string[]]$Preset = @(),
  [string]$PresetsFile,
  [string]$WslDistro = "Ubuntu",
  [string]$WslRepoPath = "~/trading_codex",
  [string]$WslPython,
  [switch]$Help
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function ConvertTo-BashArg {
  param(
    [string]$Value
  )
  if ($null -eq $Value) {
    return "''"
  }
  $escaped = $Value.Replace("'", "'`"`'`"`'")
  return "'$escaped'"
}

function ConvertTo-BashPathExpr {
  param(
    [string]$Value
  )
  if ($null -eq $Value) {
    return "''"
  }
  if ($Value -like "~*") {
    return $Value
  }
  return ConvertTo-BashArg $Value
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

function Test-WslFileExists {
  param(
    [string]$Distro,
    [string]$PathValue
  )
  $pathExpr = ConvertTo-BashPathExpr $PathValue
  & wsl.exe -d $Distro -- bash -lc "test -f $pathExpr" *> $null
  return ($LASTEXITCODE -eq 0)
}

function Resolve-PresetsFileArg {
  param(
    [string]$Distro,
    [string]$RepoPath,
    [string]$ExplicitPresetsFile
  )
  if (-not [string]::IsNullOrWhiteSpace($ExplicitPresetsFile)) {
    return Resolve-WslPath -PathValue $ExplicitPresetsFile -Distro $Distro
  }

  $candidate = "$($RepoPath.TrimEnd('/'))/configs/presets.json"
  if (Test-WslFileExists -Distro $Distro -PathValue $candidate) {
    return Resolve-WslPath -PathValue $candidate -Distro $Distro
  }

  return $null
}

function Build-BashCommand {
  param(
    [string]$RepoPath,
    [string]$PythonCmd,
    [string[]]$SummaryArgs
  )
  $parts = @(
    (ConvertTo-BashPathExpr $PythonCmd),
    "scripts/daily_summary.py"
  )
  foreach ($arg in $SummaryArgs) {
    $parts += (ConvertTo-BashArg ([string]$arg))
  }
  $summaryCmd = [string]::Join(" ", $parts)
  return "cd $(ConvertTo-BashPathExpr $RepoPath) && $summaryCmd"
}

if ($Help) {
  Get-Help -Full $PSCommandPath | Out-String | Write-Output
  exit 0
}

if ([string]::IsNullOrWhiteSpace($WslPython)) {
  $WslPython = "$($WslRepoPath.TrimEnd('/'))/.venv/bin/python"
}

$summaryArgs = @()
$resolvedPresetsFile = Resolve-PresetsFileArg -Distro $WslDistro -RepoPath $WslRepoPath -ExplicitPresetsFile $PresetsFile
if ($null -ne $resolvedPresetsFile) {
  $summaryArgs += @("--presets-file", $resolvedPresetsFile)
}
if ($Json) {
  $summaryArgs += @("--emit", "json")
}
foreach ($name in $Preset) {
  if (-not [string]::IsNullOrWhiteSpace($name)) {
    $summaryArgs += @("--preset", [string]$name)
  }
}

$bashCommand = Build-BashCommand -RepoPath $WslRepoPath -PythonCmd $WslPython -SummaryArgs $summaryArgs
& wsl.exe -d $WslDistro -- bash -lc $bashCommand
exit $LASTEXITCODE
