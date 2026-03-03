param(
  [string]$ConfigPath,
  [string]$MonitorName
)

$ErrorActionPreference = "Stop"

function Show-FallbackMessage {
  param(
    [string]$Message
  )
  msg.exe $env:USERNAME $Message
}

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

function Get-DefaultMonitorSettings {
  return @{
    WslDistro = "Ubuntu"
    WslRepoPath = "~/trading_codex"
    WslPython = "python3"
    NextActionArgs = @(
      "--emit", "json",
      "--",
      "--strategy", "dual_mom",
      "--symbols", "SPY", "QQQ", "IWM", "EFA",
      "--defensive", "TLT",
      "--start", "2005-01-01",
      "--end", "2005-05-02",
      "--no-plot",
      "--vol-target", "0.10",
      "--vol-update", "rebalance"
    )
  }
}

function Get-ConfiguredMonitor {
  param(
    [string]$ConfigPathValue,
    [string]$MonitorNameValue
  )
  if (-not (Test-Path $ConfigPathValue)) {
    throw "Config file not found: $ConfigPathValue"
  }
  $cfg = Get-Content -Raw $ConfigPathValue | ConvertFrom-Json -ErrorAction Stop
  if ($null -eq $cfg.monitors) {
    throw "Config missing monitors array: $ConfigPathValue"
  }
  $matches = @($cfg.monitors | Where-Object { [string]$_.name -eq $MonitorNameValue })
  if ($matches.Count -eq 0) {
    throw "Monitor '$MonitorNameValue' not found in $ConfigPathValue"
  }
  return $matches[0]
}

function Test-HasEmitArg {
  param(
    [string[]]$Args
  )
  foreach ($arg in $Args) {
    if ($arg -eq "--emit" -or $arg.StartsWith("--emit=")) {
      return $true
    }
  }
  return $false
}

function Ensure-EmitArg {
  param(
    [string[]]$Args,
    [string]$EmitValue
  )
  if (Test-HasEmitArg -Args $Args) {
    return $Args
  }
  $normalized = if ($EmitValue -in @("json", "text")) { $EmitValue } else { "json" }
  return @("--emit", $normalized) + $Args
}

function Build-BashCommand {
  param(
    [string]$RepoPath,
    [string]$PythonCmd,
    [string[]]$WrapperArgs
  )
  $cdTarget = if ($RepoPath -like "~*") { $RepoPath } else { ConvertTo-BashArg $RepoPath }
  $parts = @(
    (ConvertTo-BashArg $PythonCmd),
    "scripts/next_action_alert.py"
  )
  foreach ($arg in $WrapperArgs) {
    $parts += (ConvertTo-BashArg ([string]$arg))
  }
  $wrapperCmd = [string]::Join(" ", $parts)
  return "cd $cdTarget && $wrapperCmd"
}

# Ensure BurntToast is available (toast notifications)
if (-not (Get-Module -ListAvailable -Name BurntToast)) {
  try {
    Install-Module BurntToast -Scope CurrentUser -Force -AllowClobber -ErrorAction Stop
  } catch {
    # If install fails, fall back to msg.exe so you still get something
    $Global:BurntToastUnavailable = $true
  }
}
if (-not $Global:BurntToastUnavailable) {
  Import-Module BurntToast -ErrorAction SilentlyContinue
  if (-not (Get-Command New-BurntToastNotification -ErrorAction SilentlyContinue)) {
    $Global:BurntToastUnavailable = $true
  }
}

$settings = Get-DefaultMonitorSettings
if ($PSBoundParameters.ContainsKey("ConfigPath") -or $PSBoundParameters.ContainsKey("MonitorName")) {
  if (-not ($PSBoundParameters.ContainsKey("ConfigPath") -and $PSBoundParameters.ContainsKey("MonitorName"))) {
    throw "Provide both -ConfigPath and -MonitorName, or neither for default behavior."
  }
  $monitor = Get-ConfiguredMonitor -ConfigPathValue $ConfigPath -MonitorNameValue $MonitorName
  if ($null -ne $monitor.wsl_distro -and [string]$monitor.wsl_distro) {
    $settings.WslDistro = [string]$monitor.wsl_distro
  }
  if ($null -ne $monitor.wsl_repo_path -and [string]$monitor.wsl_repo_path) {
    $settings.WslRepoPath = [string]$monitor.wsl_repo_path
  }
  if ($null -ne $monitor.wsl_python -and [string]$monitor.wsl_python) {
    $settings.WslPython = [string]$monitor.wsl_python
  }
  if ($null -ne $monitor.next_action_args) {
    $settings.NextActionArgs = @($monitor.next_action_args | ForEach-Object { [string]$_ })
  }
  $settings.NextActionArgs = Ensure-EmitArg -Args $settings.NextActionArgs -EmitValue ([string]$monitor.emit)
}

$settings.NextActionArgs = Ensure-EmitArg -Args $settings.NextActionArgs -EmitValue "json"

$bash = Build-BashCommand -RepoPath $settings.WslRepoPath -PythonCmd $settings.WslPython -WrapperArgs $settings.NextActionArgs
$out = & wsl.exe -d $settings.WslDistro -e bash -lc $bash 2>$null
$out = ($out | Out-String).Trim()

if (-not $out) { exit 0 }

# Parse JSON payload
try {
  $obj = $out | ConvertFrom-Json -ErrorAction Stop
} catch {
  # If parsing fails, just show raw output
  $obj = $null
}

$ts = Get-Date -Format s
Add-Content -Path "$env:USERPROFILE\trading_codex_alert.log" -Value "$ts $out"

if ($null -eq $obj) {
  if ($Global:BurntToastUnavailable) {
    Show-FallbackMessage "TradingCodex alert: $out"
  } else {
    try {
      New-BurntToastNotification -Text "TradingCodex alert", $out
    } catch {
      Show-FallbackMessage "TradingCodex alert: $out"
    }
  }
  exit 0
}

# Build richer message from payload
$strategy = [string]$obj.strategy
$action = [string]$obj.action
$symbol = [string]$obj.symbol
$nextRebalance = if ($null -eq $obj.next_rebalance) { "" } else { [string]$obj.next_rebalance }
$eventId = if ($null -eq $obj.event_id) { "" } else { [string]$obj.event_id }
$targetShares = if ($null -eq $obj.target_shares) { "" } else { [string]$obj.target_shares }
$resizePrev = if ($null -eq $obj.resize_prev_shares) { "" } else { [string]$obj.resize_prev_shares }
$resizeNew = if ($null -eq $obj.resize_new_shares) { "" } else { [string]$obj.resize_new_shares }
$leverage = if ($null -eq $obj.leverage) { "" } else { [string]$obj.leverage }
$volTarget = if ($null -eq $obj.vol_target) { "" } else { [string]$obj.vol_target }

$title = "$strategy · $symbol"

switch ($action) {
  "RESIZE" {
    $line1 = "RESIZE $resizePrev→$resizeNew / $targetShares"
    if ($leverage) { $line1 += " lev=$leverage" }
    if ($volTarget) { $line1 += " vol_target=$volTarget" }
  }
  "HOLD" {
    $line1 = "HOLD (no trade)"
  }
  default {
    if ($targetShares) {
      $line1 = "$action to $targetShares"
    } else {
      $line1 = "$action"
    }
  }
}

$line2 = "next_rebalance=$nextRebalance | event_id=$eventId"

if ($Global:BurntToastUnavailable) {
  Show-FallbackMessage "$title`n$line1`n$line2"
  exit 0
}

$bt = Get-Command New-BurntToastNotification -ErrorAction SilentlyContinue
if ($null -eq $bt) {
  Show-FallbackMessage "$title`n$line1`n$line2"
  exit 0
}

$toastParams = @{
  Text = @($title, $line1, $line2)
}
if ($bt.Parameters.ContainsKey("Sound")) {
  $toastParams["Sound"] = "Default"
}
if ($bt.Parameters.ContainsKey("Duration")) {
  $toastParams["Duration"] = "Long"
} elseif ($bt.Parameters.ContainsKey("ExpirationTime")) {
  $toastParams["ExpirationTime"] = (Get-Date).AddMinutes(1)
}

try {
  New-BurntToastNotification @toastParams
} catch {
  Show-FallbackMessage "$title`n$line1`n$line2"
}
