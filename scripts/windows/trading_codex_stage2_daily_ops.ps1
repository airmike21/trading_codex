<#
.SYNOPSIS
Launch the Stage 2 paper-lane daily ops routine inside WSL.

.DESCRIPTION
Runs `scripts/paper_lane_daily_ops.py` via WSL using the repo venv Python.
If `<repo>/configs/presets.json` exists inside WSL, the wrapper passes it
through as `--presets-file`; otherwise it falls back to the Python script
defaults.

.PARAMETER Preset
Paper-lane preset name. Default: `dual_mom_vol10_cash_core`.

.PARAMETER Provider
Data provider for `update_data_eod.py`. Default: `stooq`.

.PARAMETER PresetsFile
Explicit presets file override. Windows paths are converted to WSL paths.

.PARAMETER ArchiveRoot
Optional archive root override passed to `paper_lane_daily_ops.py`.
Windows paths are converted to WSL paths.

.PARAMETER PaperBaseDir
Optional paper-lane base dir override passed to `paper_lane_daily_ops.py`.
Windows paths are converted to WSL paths.

.PARAMETER Timestamp
Optional ISO timestamp override for deterministic testing.

.PARAMETER WslDistro
WSL distro name. Default: `Ubuntu`.

.PARAMETER WslRepoPath
Repo path inside WSL. Default: `~/trading_codex`.

.PARAMETER WslPython
Python path inside WSL. Defaults to `<WslRepoPath>/.venv/bin/python`.

.PARAMETER PrintOnly
Print the resolved WSL command without executing it.

.PARAMETER Help
Show this help text.

.EXAMPLE
./trading_codex_stage2_daily_ops.ps1

.EXAMPLE
./trading_codex_stage2_daily_ops.ps1 -PrintOnly -WslRepoPath ~/trading_codex
#>
[CmdletBinding()]
param(
  [string]$Preset = "dual_mom_vol10_cash_core",
  [ValidateSet("stooq", "tiingo")]
  [string]$Provider = "stooq",
  [string]$PresetsFile,
  [string]$ArchiveRoot,
  [string]$PaperBaseDir,
  [string]$Timestamp,
  [string]$WslDistro = "Ubuntu",
  [string]$WslRepoPath = "~/trading_codex",
  [string]$WslPython,
  [switch]$PrintOnly,
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
  if ($candidate -match '^[A-Za-z]:[\\/]') {
    $candidate = $candidate -replace '\\', '/'
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
    [string[]]$ScriptArgs
  )
  $parts = @(
    (ConvertTo-BashPathExpr $PythonCmd),
    "scripts/paper_lane_daily_ops.py"
  )
  foreach ($arg in $ScriptArgs) {
    $parts += (ConvertTo-BashArg ([string]$arg))
  }
  $cmd = [string]::Join(" ", $parts)
  return "cd $(ConvertTo-BashPathExpr $RepoPath) && $cmd"
}

if ($Help) {
  Get-Help -Full $PSCommandPath | Out-String | Write-Output
  exit 0
}

if ([string]::IsNullOrWhiteSpace($WslPython)) {
  $WslPython = "$($WslRepoPath.TrimEnd('/'))/.venv/bin/python"
}

$scriptArgs = @("--preset", $Preset, "--provider", $Provider)
$resolvedPresetsFile = Resolve-PresetsFileArg -Distro $WslDistro -RepoPath $WslRepoPath -ExplicitPresetsFile $PresetsFile
if ($null -ne $resolvedPresetsFile) {
  $scriptArgs += @("--presets-file", $resolvedPresetsFile)
}
if (-not [string]::IsNullOrWhiteSpace($ArchiveRoot)) {
  $scriptArgs += @("--archive-root", (Resolve-WslPath -PathValue $ArchiveRoot -Distro $WslDistro))
}
if (-not [string]::IsNullOrWhiteSpace($PaperBaseDir)) {
  $scriptArgs += @("--paper-base-dir", (Resolve-WslPath -PathValue $PaperBaseDir -Distro $WslDistro))
}
if (-not [string]::IsNullOrWhiteSpace($Timestamp)) {
  $scriptArgs += @("--timestamp", $Timestamp)
}

$bashCommand = Build-BashCommand -RepoPath $WslRepoPath -PythonCmd $WslPython -ScriptArgs $scriptArgs
if ($PrintOnly) {
  Write-Output "repo_path=$WslRepoPath"
  Write-Output "python_path=$WslPython"
  Write-Output "command=$bashCommand"
  exit 0
}

& wsl.exe -d $WslDistro -- bash -lc $bashCommand
exit $LASTEXITCODE
