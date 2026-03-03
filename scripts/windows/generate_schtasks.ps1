param(
  [string]$ConfigPath = "$env:USERPROFILE\trading_codex_alerts.json"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $ConfigPath)) {
  throw "Config file not found: $ConfigPath"
}

$cfg = Get-Content -Raw $ConfigPath | ConvertFrom-Json -ErrorAction Stop
if ($null -eq $cfg.monitors) {
  throw "Config missing monitors array: $ConfigPath"
}

$monitors = @($cfg.monitors)
if ($monitors.Count -lt 1 -or $monitors.Count -gt 3) {
  throw "Config must include between 1 and 3 monitors. Found: $($monitors.Count)"
}

$runner = Join-Path $env:USERPROFILE "Scripts\trading_codex_next_action_alert.ps1"

foreach ($m in $monitors) {
  $name = [string]$m.name
  if ([string]::IsNullOrWhiteSpace($name)) {
    throw "Each monitor must include a non-empty name."
  }

  $interval = 30
  if ($null -ne $m.interval_minutes) {
    $interval = [int]$m.interval_minutes
  }
  if ($interval -lt 1) {
    throw "Monitor '$name' has invalid interval_minutes: $interval"
  }

  $taskName = "TradingCodex\$name"
  $taskRun = "powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$runner`" -ConfigPath `"$ConfigPath`" -MonitorName `"$name`""
  $createCmd = "schtasks.exe /Create /TN `"$taskName`" /TR '$taskRun' /SC MINUTE /MO $interval /F"
  $runCmd = "schtasks.exe /Run /TN `"$taskName`""

  Write-Output $createCmd
  Write-Output $runCmd
  Write-Output ""
}
