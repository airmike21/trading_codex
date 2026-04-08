<#
.SYNOPSIS
Launch the Stage 2 shadow-only daily ops routine inside WSL.

.DESCRIPTION
Runs `scripts/stage2_shadow_daily_ops.py` via WSL using the repo venv Python.
The tracked repo config `configs/stage2_shadow_ops.json` is used by default.
This wrapper can override the config path and other local-only runtime paths when
needed.

.PARAMETER Provider
Data provider for `update_data_eod.py`. Default: `stooq`.

.PARAMETER ShadowOpsConfig
Optional shadow ops config JSON override. Windows paths are converted to WSL
paths.

.PARAMETER DataDir
Optional cached market-data directory override. Windows paths are converted to
WSL paths.

.PARAMETER ArchiveRoot
Optional archive root override passed to `stage2_shadow_daily_ops.py`.
Windows paths are converted to WSL paths.

.PARAMETER PaperBaseDir
Optional local shadow replay paper-lane state dir override passed to
`stage2_shadow_daily_ops.py`. Windows paths are converted to WSL paths.

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
./trading_codex_stage2_shadow_daily_ops.ps1

.EXAMPLE
./trading_codex_stage2_shadow_daily_ops.ps1 -PrintOnly -WslRepoPath ~/trading_codex
#>
[CmdletBinding()]
param(
  [ValidateSet("stooq", "tiingo")]
  [string]$Provider = "stooq",
  [string]$ShadowOpsConfig,
  [string]$DataDir,
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

function Build-BashCommand {
  param(
    [string]$RepoPath,
    [string]$PythonCmd,
    [string[]]$ScriptArgs
  )
  $parts = @(
    (ConvertTo-BashPathExpr $PythonCmd),
    "scripts/stage2_shadow_daily_ops.py"
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

$scriptArgs = @("--provider", $Provider)
if (-not [string]::IsNullOrWhiteSpace($ShadowOpsConfig)) {
  $scriptArgs += @("--shadow-ops-config", (Resolve-WslPath -PathValue $ShadowOpsConfig -Distro $WslDistro))
}
if (-not [string]::IsNullOrWhiteSpace($DataDir)) {
  $scriptArgs += @("--data-dir", (Resolve-WslPath -PathValue $DataDir -Distro $WslDistro))
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
