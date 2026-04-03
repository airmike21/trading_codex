<#
.SYNOPSIS
Launch the Stage 2 IBKR PaperTrader daily ops routine inside WSL.

.DESCRIPTION
Runs a fail-closed preflight first, then the existing
`scripts/ibkr_paper_lane_daily_ops.py` runner via WSL using the repo venv
Python. The wrapper defaults to the repo-managed
`configs/presets.example.json` path so the scheduled command stays
deterministic instead of silently switching between preset files.

.PARAMETER Preset
IBKR paper-lane preset name. Default: `dual_mom_vol10_cash_core`.

.PARAMETER Provider
Data provider for `update_data_eod.py`. Default: `stooq`.

.PARAMETER PresetsFile
Explicit presets file override. Windows paths are converted to WSL paths.
Default: `<WslRepoPath>/configs/presets.example.json`.

.PARAMETER ArchiveRoot
Optional archive root override passed to `ibkr_paper_lane_daily_ops.py`.
Windows paths are converted to WSL paths.

.PARAMETER IbkrBaseDir
Optional IBKR paper lane base dir override passed to
`ibkr_paper_lane_daily_ops.py`. Windows paths are converted to WSL paths.

.PARAMETER Timestamp
Optional ISO timestamp override for deterministic testing.

.PARAMETER IbkrAccountId
Explicit IBKR PaperTrader account id. If omitted, the wrapper falls back to
`IBKR_PAPER_ACCOUNT_ID` from Windows first, then from the selected WSL distro.

.PARAMETER IbkrBaseUrl
IBKR Client Portal Gateway base URL. Default:
`https://127.0.0.1:5000/v1/api`.

.PARAMETER IbkrTimeoutSeconds
IBKR Client Portal Gateway timeout. Default: `15`.

.PARAMETER VerifyIbkrSsl
Enable TLS verification for the IBKR base URL. Default is disabled because
the local gateway commonly uses a self-signed certificate.

.PARAMETER LogDir
Windows log directory for wrapper, preflight, and daily-ops output.
Default: `%LOCALAPPDATA%\TradingCodex\stage2_ibkr_paper_ops\logs`.

.PARAMETER PreflightOnly
Run only the fail-closed preflight stage and stop before daily ops.

.PARAMETER WslDistro
WSL distro name. Default: `Ubuntu`.

.PARAMETER WslRepoPath
Repo path inside WSL. Default: `~/.codex-workspaces/trading-builder`.

.PARAMETER WslPython
Python path inside WSL. Defaults to `<WslRepoPath>/.venv/bin/python`.

.PARAMETER PrintOnly
Print the resolved commands without executing them.

.PARAMETER Help
Show this help text.

.EXAMPLE
./trading_codex_stage2_ibkr_paper_daily_ops.ps1 -IbkrAccountId DUPXXXXXXX

.EXAMPLE
./trading_codex_stage2_ibkr_paper_daily_ops.ps1 -PrintOnly -WslRepoPath ~/.codex-workspaces/trading-builder
#>
[CmdletBinding()]
param(
  [string]$Preset = "dual_mom_vol10_cash_core",
  [ValidateSet("stooq", "tiingo")]
  [string]$Provider = "stooq",
  [string]$PresetsFile,
  [string]$ArchiveRoot,
  [string]$IbkrBaseDir,
  [string]$Timestamp,
  [string]$IbkrAccountId,
  [string]$IbkrBaseUrl = "https://127.0.0.1:5000/v1/api",
  [double]$IbkrTimeoutSeconds = 15.0,
  [switch]$VerifyIbkrSsl,
  [string]$LogDir,
  [switch]$PreflightOnly,
  [string]$WslDistro = "Ubuntu",
  [string]$WslRepoPath = "~/.codex-workspaces/trading-builder",
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

function Resolve-LocalPath {
  param(
    [string]$PathValue
  )
  if ([string]::IsNullOrWhiteSpace($PathValue)) {
    return $null
  }
  if (Test-Path -LiteralPath $PathValue) {
    return (Resolve-Path -LiteralPath $PathValue).Path
  }
  return [System.IO.Path]::GetFullPath($PathValue)
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

function Test-WslDirectoryExists {
  param(
    [string]$Distro,
    [string]$PathValue
  )
  $pathExpr = ConvertTo-BashPathExpr $PathValue
  & wsl.exe -d $Distro -- bash -lc "test -d $pathExpr" *> $null
  return ($LASTEXITCODE -eq 0)
}

function Resolve-ExpectedPresetsFileArg {
  param(
    [string]$Distro,
    [string]$RepoPath,
    [string]$ExplicitPresetsFile
  )
  if (-not [string]::IsNullOrWhiteSpace($ExplicitPresetsFile)) {
    return Resolve-WslPath -PathValue $ExplicitPresetsFile -Distro $Distro
  }

  return Resolve-WslPath -PathValue "$($RepoPath.TrimEnd('/'))/configs/presets.example.json" -Distro $Distro
}

function Resolve-WslEnvValue {
  param(
    [string]$Distro,
    [string]$Name
  )
  $escapedName = ConvertTo-BashArg $Name
  $resolved = & wsl.exe -d $Distro -- bash -lc "printenv $escapedName" 2>$null
  if ($LASTEXITCODE -ne 0) {
    return ""
  }
  return (($resolved | Out-String).Trim())
}

function Resolve-IbkrAccountIdValue {
  param(
    [string]$ExplicitValue,
    [string]$Distro
  )
  if (-not [string]::IsNullOrWhiteSpace($ExplicitValue)) {
    return [pscustomobject]@{
      Value = $ExplicitValue.Trim()
      Source = "parameter"
    }
  }
  if (-not [string]::IsNullOrWhiteSpace($env:IBKR_PAPER_ACCOUNT_ID)) {
    return [pscustomobject]@{
      Value = $env:IBKR_PAPER_ACCOUNT_ID.Trim()
      Source = "windows_env"
    }
  }
  $wslValue = Resolve-WslEnvValue -Distro $Distro -Name "IBKR_PAPER_ACCOUNT_ID"
  if (-not [string]::IsNullOrWhiteSpace($wslValue)) {
    return [pscustomobject]@{
      Value = $wslValue.Trim()
      Source = "wsl_env"
    }
  }
  return [pscustomobject]@{
    Value = ""
    Source = "missing"
  }
}

function Build-BashCommand {
  param(
    [string]$RepoPath,
    [string]$PythonCmd,
    [string]$ScriptPath,
    [string[]]$ScriptArgs
  )
  $parts = @(
    (ConvertTo-BashPathExpr $PythonCmd),
    $ScriptPath
  )
  foreach ($arg in $ScriptArgs) {
    $parts += (ConvertTo-BashArg ([string]$arg))
  }
  $cmd = [string]::Join(" ", $parts)
  return "cd $(ConvertTo-BashPathExpr $RepoPath) && $cmd"
}

function New-DefaultLogDir {
  if (-not [string]::IsNullOrWhiteSpace($env:LOCALAPPDATA)) {
    return (Join-Path $env:LOCALAPPDATA "TradingCodex\stage2_ibkr_paper_ops\logs")
  }
  return (Join-Path $env:TEMP "TradingCodex\stage2_ibkr_paper_ops\logs")
}

function Write-LauncherLog {
  param(
    [string]$Message,
    [string]$Level = "INFO"
  )
  $timestampIso = (Get-Date).ToString("yyyy-MM-ddTHH:mm:sszzz")
  $line = "[$timestampIso] [$Level] $Message"
  Write-Output $line
  if (-not [string]::IsNullOrWhiteSpace($script:LauncherLogPath)) {
    Add-Content -LiteralPath $script:LauncherLogPath -Value $line
  }
}

function Invoke-WslLoggedCommand {
  param(
    [string]$Description,
    [string]$Distro,
    [string]$BashCommand
  )

  Write-LauncherLog -Message "starting $Description"
  & wsl.exe -d $Distro -- bash -lc $BashCommand 2>&1 | Tee-Object -FilePath $script:LauncherLogPath -Append
  $exitCode = $LASTEXITCODE
  if ($exitCode -ne 0) {
    Write-LauncherLog -Level "ERROR" -Message "$Description failed exit_code=$exitCode"
  }
  else {
    Write-LauncherLog -Message "$Description completed exit_code=0"
  }
  return $exitCode
}

if ($Help) {
  Get-Help -Full $PSCommandPath | Out-String | Write-Output
  exit 0
}

if ([string]::IsNullOrWhiteSpace($WslPython)) {
  $WslPython = "$($WslRepoPath.TrimEnd('/'))/.venv/bin/python"
}
if ([string]::IsNullOrWhiteSpace($LogDir)) {
  $LogDir = New-DefaultLogDir
}

$resolvedLogDir = Resolve-LocalPath -PathValue $LogDir
$logDate = (Get-Date).ToString("yyyyMMdd")
$script:LauncherLogPath = Join-Path $resolvedLogDir "stage2_ibkr_paper_daily_ops-$logDate.log"
$resolvedPresetsFile = Resolve-ExpectedPresetsFileArg -Distro $WslDistro -RepoPath $WslRepoPath -ExplicitPresetsFile $PresetsFile
$resolvedAccountIdInfo = Resolve-IbkrAccountIdValue -ExplicitValue $IbkrAccountId -Distro $WslDistro
$resolvedAccountId = $resolvedAccountIdInfo.Value

$preflightArgs = @(
  "--preset",
  $Preset,
  "--presets-file",
  $resolvedPresetsFile,
  "--ibkr-base-url",
  $IbkrBaseUrl,
  "--ibkr-timeout-seconds",
  $IbkrTimeoutSeconds.ToString([System.Globalization.CultureInfo]::InvariantCulture)
)
if (-not [string]::IsNullOrWhiteSpace($resolvedAccountId)) {
  $preflightArgs += @("--ibkr-account-id", $resolvedAccountId)
}
if ($VerifyIbkrSsl) {
  $preflightArgs += "--ibkr-verify-ssl"
}
else {
  $preflightArgs += "--no-ibkr-verify-ssl"
}

$dailyOpsArgs = @(
  "--preset",
  $Preset,
  "--provider",
  $Provider,
  "--presets-file",
  $resolvedPresetsFile,
  "--ibkr-base-url",
  $IbkrBaseUrl,
  "--ibkr-timeout-seconds",
  $IbkrTimeoutSeconds.ToString([System.Globalization.CultureInfo]::InvariantCulture)
)
if (-not [string]::IsNullOrWhiteSpace($resolvedAccountId)) {
  $dailyOpsArgs += @("--ibkr-account-id", $resolvedAccountId)
}
if ($VerifyIbkrSsl) {
  $dailyOpsArgs += "--ibkr-verify-ssl"
}
else {
  $dailyOpsArgs += "--no-ibkr-verify-ssl"
}
if (-not [string]::IsNullOrWhiteSpace($ArchiveRoot)) {
  $dailyOpsArgs += @("--archive-root", (Resolve-WslPath -PathValue $ArchiveRoot -Distro $WslDistro))
}
if (-not [string]::IsNullOrWhiteSpace($IbkrBaseDir)) {
  $dailyOpsArgs += @("--ibkr-base-dir", (Resolve-WslPath -PathValue $IbkrBaseDir -Distro $WslDistro))
}
if (-not [string]::IsNullOrWhiteSpace($Timestamp)) {
  $dailyOpsArgs += @("--timestamp", $Timestamp)
}

$preflightCommand = Build-BashCommand `
  -RepoPath $WslRepoPath `
  -PythonCmd $WslPython `
  -ScriptPath "scripts/ibkr_paper_lane_daily_ops_preflight.py" `
  -ScriptArgs $preflightArgs
$dailyOpsCommand = Build-BashCommand `
  -RepoPath $WslRepoPath `
  -PythonCmd $WslPython `
  -ScriptPath "scripts/ibkr_paper_lane_daily_ops.py" `
  -ScriptArgs $dailyOpsArgs

if ($PrintOnly) {
  Write-Output "repo_path=$WslRepoPath"
  Write-Output "python_path=$WslPython"
  Write-Output "presets_file=$resolvedPresetsFile"
  Write-Output "log_path=$script:LauncherLogPath"
  Write-Output "ibkr_account_id_source=$($resolvedAccountIdInfo.Source)"
  Write-Output "preflight_command=$preflightCommand"
  Write-Output "command=$dailyOpsCommand"
  exit 0
}

New-Item -ItemType Directory -Force -Path $resolvedLogDir | Out-Null

if ([string]::IsNullOrWhiteSpace($resolvedAccountId)) {
  Write-LauncherLog -Level "ERROR" -Message "IBKR PaperTrader account id is required. Pass -IbkrAccountId or set IBKR_PAPER_ACCOUNT_ID."
  exit 2
}
if (-not (Test-WslDirectoryExists -Distro $WslDistro -PathValue $WslRepoPath)) {
  Write-LauncherLog -Level "ERROR" -Message "WSL repo path not found: $WslRepoPath"
  exit 2
}
if (-not (Test-WslFileExists -Distro $WslDistro -PathValue $WslPython)) {
  Write-LauncherLog -Level "ERROR" -Message "WSL Python not found: $WslPython"
  exit 2
}
if (-not (Test-WslFileExists -Distro $WslDistro -PathValue $resolvedPresetsFile)) {
  Write-LauncherLog -Level "ERROR" -Message "Expected presets file not found: $resolvedPresetsFile"
  exit 2
}

Write-LauncherLog -Message "log_path=$script:LauncherLogPath"
Write-LauncherLog -Message "preset=$Preset provider=$Provider presets_file=$resolvedPresetsFile account_id_source=$($resolvedAccountIdInfo.Source)"
Write-LauncherLog -Message "ibkr_account_id=$resolvedAccountId base_url=$IbkrBaseUrl verify_ssl=$($VerifyIbkrSsl.IsPresent) timeout_seconds=$IbkrTimeoutSeconds"

$preflightExit = Invoke-WslLoggedCommand -Description "stage2_ibkr_paper_preflight" -Distro $WslDistro -BashCommand $preflightCommand
if ($preflightExit -ne 0) {
  exit $preflightExit
}

if ($PreflightOnly) {
  Write-LauncherLog -Message "preflight_only=true daily_ops_skipped=true"
  exit 0
}

$dailyOpsExit = Invoke-WslLoggedCommand -Description "stage2_ibkr_paper_daily_ops" -Distro $WslDistro -BashCommand $dailyOpsCommand
exit $dailyOpsExit
