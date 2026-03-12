<#
.SYNOPSIS
Open the local read-only Trading Codex review dashboard from a dedicated WSL workspace.

.DESCRIPTION
Uses a clean WSL review workspace to run `scripts/review_dashboard.py` with Streamlit.
The launcher only binds Streamlit to `127.0.0.1`, refuses the normal `~/trading_codex`
checkout, and only reuses a launcher-owned Trading Codex review dashboard instance
for the same repo and port instead of trusting any generic healthy Streamlit server.

.PARAMETER WslDistro
WSL distro name. Default: `Ubuntu`.

.PARAMETER WslRepoPath
Dedicated clean WSL workspace used to run the dashboard.
Default: `~/.codex-workspaces/trading-review`.

.PARAMETER WslPython
Python path inside WSL. Defaults to `<WslRepoPath>/.venv/bin/python`.

.PARAMETER CacheDir
Local-only WSL cache directory used for launcher logs and instance metadata.
Default: `~/.cache/trading_codex/review_dashboard`.

.PARAMETER Port
Local dashboard port. Default: `8501`.

.PARAMETER StartupTimeoutSeconds
Seconds to wait for a new dashboard instance to become healthy before failing.
Default: `20`.

.PARAMETER NoBrowser
Do not open the browser after validating or launching the dashboard.

.PARAMETER ValidateOnly
Validate the configured WSL review workspace and exit without launching.

.PARAMETER PrintOnly
Print the validated launch plan and exit without launching.

.PARAMETER ShowErrorDialog
Show a Windows message box on launcher errors. Intended for desktop shortcut use.

.PARAMETER Help
Show this help text.

.EXAMPLE
./trading_codex_review_dashboard.ps1

.EXAMPLE
./trading_codex_review_dashboard.ps1 -ValidateOnly -WslRepoPath ~/.codex-workspaces/trading-review

.EXAMPLE
./trading_codex_review_dashboard.ps1 -PrintOnly -Port 8502 -NoBrowser
#>
[CmdletBinding()]
param(
  [string]$WslDistro = "Ubuntu",
  [string]$WslRepoPath = "~/.codex-workspaces/trading-review",
  [string]$WslPython,
  [string]$CacheDir = "~/.cache/trading_codex/review_dashboard",
  [ValidateRange(1, 65535)]
  [int]$Port = 8501,
  [ValidateRange(1, 120)]
  [int]$StartupTimeoutSeconds = 20,
  [switch]$NoBrowser,
  [switch]$ValidateOnly,
  [switch]$PrintOnly,
  [switch]$ShowErrorDialog,
  [switch]$Help
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$DashboardHost = "127.0.0.1"
$DefaultReviewWorkspace = "~/.codex-workspaces/trading-review"
$MetadataSchema = "trading_codex_review_dashboard_launcher_v1"

function Normalize-WslOutput {
  param(
    [string]$Text
  )
  if ([string]::IsNullOrWhiteSpace($Text)) {
    return ""
  }
  $lines = @(
    $Text -split "\r?\n" |
    Where-Object { $_ -notmatch 'screen size is bogus\. expect trouble$' }
  )
  return (($lines -join [Environment]::NewLine).Trim())
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
  return ConvertTo-BashArg -Value $Value
}

function Invoke-WslCapture {
  param(
    [string]$Distro,
    [string[]]$Command,
    [switch]$AllowFailure
  )
  $output = & wsl.exe -d $Distro -- @Command 2>&1
  $text = Normalize-WslOutput -Text (($output | Out-String).Trim())
  if (-not $AllowFailure -and $LASTEXITCODE -ne 0) {
    if ([string]::IsNullOrWhiteSpace($text)) {
      throw "WSL command failed (exit code $LASTEXITCODE)."
    }
    throw $text
  }
  return $text
}

function Invoke-WslStdout {
  param(
    [string]$Distro,
    [string[]]$Command,
    [switch]$AllowFailure
  )
  $output = & wsl.exe -d $Distro -- @Command 2>$null
  $text = Normalize-WslOutput -Text (($output | Out-String).Trim())
  if (-not $AllowFailure -and $LASTEXITCODE -ne 0) {
    if ([string]::IsNullOrWhiteSpace($text)) {
      throw "WSL command failed (exit code $LASTEXITCODE)."
    }
    throw $text
  }
  return $text
}

function Resolve-WslPath {
  param(
    [string]$PathValue,
    [string]$Distro
  )
  if ([string]::IsNullOrWhiteSpace($PathValue)) {
    return $null
  }

  if ($PathValue -like "~*" -or $PathValue.StartsWith("/")) {
    $pathExpr = ConvertTo-BashPathExpr -Value $PathValue
    return Invoke-WslCapture -Distro $Distro -Command @("sh", "-lc", "realpath -m $pathExpr")
  }

  $candidate = $PathValue
  if (Test-Path -LiteralPath $PathValue) {
    $candidate = (Resolve-Path -LiteralPath $PathValue).Path
  }
  return Invoke-WslCapture -Distro $Distro -Command @("wslpath", "-a", $candidate)
}

function Convert-WslPathToWindowsPath {
  param(
    [string]$Distro,
    [string]$PathValue
  )
  return Invoke-WslCapture -Distro $Distro -Command @("wslpath", "-w", $PathValue)
}

function Get-WslHomeDirectory {
  param(
    [string]$Distro
  )
  return Invoke-WslCapture -Distro $Distro -Command @("sh", "-lc", 'printf %s "$HOME"')
}

function Test-WslDirectoryExists {
  param(
    [string]$Distro,
    [string]$PathValue
  )
  $pathExpr = ConvertTo-BashPathExpr -Value $PathValue
  & wsl.exe -d $Distro -- sh -lc "test -d $pathExpr" *> $null
  return ($LASTEXITCODE -eq 0)
}

function Test-WslFileExists {
  param(
    [string]$Distro,
    [string]$PathValue
  )
  $pathExpr = ConvertTo-BashPathExpr -Value $PathValue
  & wsl.exe -d $Distro -- sh -lc "test -f $pathExpr" *> $null
  return ($LASTEXITCODE -eq 0)
}

function Read-JsonFile {
  param(
    [string]$PathValue
  )
  if (-not (Test-Path -LiteralPath $PathValue)) {
    return $null
  }
  try {
    return (Get-Content -LiteralPath $PathValue -Raw -Encoding UTF8 | ConvertFrom-Json)
  } catch {
    return $null
  }
}

function Write-JsonFile {
  param(
    [string]$PathValue,
    [hashtable]$Payload
  )
  $directory = Split-Path -Path $PathValue -Parent
  if (-not (Test-Path -LiteralPath $directory)) {
    New-Item -ItemType Directory -Path $directory -Force | Out-Null
  }
  $Payload | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $PathValue -Encoding UTF8
}

function Remove-FileIfExists {
  param(
    [string]$PathValue
  )
  if (Test-Path -LiteralPath $PathValue) {
    Remove-Item -LiteralPath $PathValue -Force
  }
}

function Get-WslGitStatus {
  param(
    [string]$Distro,
    [string]$RepoPath
  )
  $repoExpr = ConvertTo-BashArg -Value $RepoPath
  return Invoke-WslCapture -Distro $Distro -AllowFailure -Command @(
    "sh",
    "-lc",
    "cd $repoExpr && git status --porcelain --untracked-files=all"
  )
}

function Test-HealthyDashboard {
  param(
    [string]$Url
  )
  try {
    $response = Invoke-WebRequest -Uri "$Url/_stcore/health" -UseBasicParsing -TimeoutSec 2
    return ($response.StatusCode -eq 200 -and ($response.Content | Out-String).Trim() -eq "ok")
  } catch {
    return $false
  }
}

function Test-LocalPortInUse {
  param(
    [string]$Address,
    [int]$PortNumber
  )
  $client = New-Object System.Net.Sockets.TcpClient
  try {
    $task = $client.ConnectAsync($Address, $PortNumber)
    if (-not $task.Wait(1000)) {
      return $false
    }
    return $client.Connected
  } catch {
    return $false
  } finally {
    $client.Dispose()
  }
}

function Wait-ForHealthyDashboard {
  param(
    [string]$Url,
    [int]$TimeoutSeconds
  )
  $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
  while ((Get-Date) -lt $deadline) {
    if (Test-HealthyDashboard -Url $Url) {
      return $true
    }
    Start-Sleep -Seconds 1
  }
  return $false
}

function Get-WslProcessSnapshot {
  param(
    [string]$Distro,
    [int]$ProcessId
  )
  $psCommand = @'
if [ -r /proc/{0}/stat ] && [ -r /proc/{0}/cmdline ]; then
  sed 's/^[^)]*) //' /proc/{0}/stat | cut -d' ' -f20
  tr '\000' ' ' < /proc/{0}/cmdline
else
  exit 1
fi
'@ -f $ProcessId
  $output = Invoke-WslStdout -Distro $Distro -AllowFailure -Command @("sh", "-lc", $psCommand)
  if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($output)) {
    return $null
  }
  $lines = @($output -split "\r?\n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
  if ($lines.Count -lt 2) {
    return $null
  }
  $processStartTicks = $lines[$lines.Count - 2].Trim()
  $commandLine = $lines[$lines.Count - 1].Trim()
  if ([string]::IsNullOrWhiteSpace($processStartTicks) -or [string]::IsNullOrWhiteSpace($commandLine)) {
    return $null
  }
  return [pscustomobject]@{
    ProcessStartTicks = $processStartTicks
    CommandLine = $commandLine
  }
}

function Wait-ForWslProcessSnapshot {
  param(
    [string]$Distro,
    [int]$ProcessId,
    [int]$TimeoutSeconds
  )
  $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
  while ((Get-Date) -lt $deadline) {
    $snapshot = Get-WslProcessSnapshot -Distro $Distro -ProcessId $ProcessId
    if ($null -ne $snapshot) {
      return $snapshot
    }
    Start-Sleep -Milliseconds 200
  }
  return $null
}

function Test-ReviewDashboardCommandLine {
  param(
    [string]$CommandLine,
    [string]$RepoPath,
    [int]$PortNumber
  )
  if ([string]::IsNullOrWhiteSpace($CommandLine)) {
    return $false
  }
  $scriptPath = "$($RepoPath.TrimEnd('/'))/scripts/review_dashboard.py"
  $hasScriptPath = $CommandLine.Contains($scriptPath)
  $hasStreamlit = $CommandLine.Contains("streamlit")
  $hasPortArg = $CommandLine.Contains("--server.port $PortNumber") -or $CommandLine.Contains("--server.port=$PortNumber")
  return ($hasScriptPath -and $hasStreamlit -and $hasPortArg)
}

function Get-LauncherInstanceState {
  param(
    [string]$Distro,
    [string]$MetadataWindowsPath,
    [string]$RepoPath,
    [string]$PythonPath,
    [int]$PortNumber,
    [string]$Url
  )
  $metadata = Read-JsonFile -PathValue $MetadataWindowsPath
  $markerMatchesRequest = $false
  $ownedProcess = $false

  if ($null -ne $metadata) {
    $metadataPort = -1
    $trackedProcessId = 0
    [int]::TryParse([string]$metadata.port, [ref]$metadataPort) | Out-Null
    if (
      [string]$metadata.schema -eq $MetadataSchema -and
      [string]$metadata.repo_path -eq $RepoPath -and
      [string]$metadata.python_path -eq $PythonPath -and
      $metadataPort -eq $PortNumber -and
      [int]::TryParse([string]$metadata.pid, [ref]$trackedProcessId)
    ) {
      $markerMatchesRequest = $true
      $snapshot = Get-WslProcessSnapshot -Distro $Distro -ProcessId $trackedProcessId
      if (
        $null -ne $snapshot -and
        [string]$metadata.process_start_ticks -eq $snapshot.ProcessStartTicks -and
        (Test-ReviewDashboardCommandLine -CommandLine $snapshot.CommandLine -RepoPath $RepoPath -PortNumber $PortNumber)
      ) {
        $ownedProcess = $true
      }
    }
  }

  $healthy = Test-HealthyDashboard -Url $Url
  $portInUse = if ($healthy) { $true } else { Test-LocalPortInUse -Address $DashboardHost -PortNumber $PortNumber }

  return [pscustomobject]@{
    Metadata = $metadata
    MarkerMatchesRequest = $markerMatchesRequest
    OwnedProcess = $ownedProcess
    Healthy = $healthy
    PortInUse = $portInUse
    Reusable = ($healthy -and $ownedProcess)
  }
}

function Join-BashCommand {
  param(
    [string[]]$Values
  )
  return [string]::Join(" ", $Values)
}

function Build-LaunchCommand {
  param(
    [string]$RepoPath,
    [string]$PythonPath,
    [string]$DashboardScriptPath,
    [string]$LogPath,
    [int]$PortNumber
  )
  $logDirectory = Split-Path -Path $LogPath -Parent
  $streamlitParts = @(
    (ConvertTo-BashArg -Value $PythonPath),
    "-m",
    "streamlit",
    "run",
    (ConvertTo-BashArg -Value $DashboardScriptPath),
    "--server.address",
    $DashboardHost,
    "--server.port",
    [string]$PortNumber,
    "--server.headless",
    "true",
    "--browser.gatherUsageStats",
    "false"
  )
  $streamlitCommand = Join-BashCommand -Values $streamlitParts
  return "mkdir -p $(ConvertTo-BashArg -Value $logDirectory) && cd $(ConvertTo-BashArg -Value $RepoPath) && nohup $streamlitCommand > $(ConvertTo-BashArg -Value $LogPath) 2>&1 < /dev/null & printf '%s' " + '$!'
}

function Assert-SafeReviewWorkspace {
  param(
    [string]$Distro,
    [string]$RepoPath,
    [string]$PythonPath
  )
  $wslHome = Get-WslHomeDirectory -Distro $Distro
  $unsafeRepoPath = Resolve-WslPath -PathValue "$wslHome/trading_codex" -Distro $Distro
  if ($RepoPath -eq $unsafeRepoPath) {
    throw "Refusing to use $RepoPath because it resolves to ~/trading_codex. Use a dedicated clean review workspace such as $DefaultReviewWorkspace."
  }

  if (-not (Test-WslDirectoryExists -Distro $Distro -PathValue $RepoPath)) {
    throw "WSL review workspace not found: $RepoPath. Create or update a clean review workspace such as $DefaultReviewWorkspace before launching."
  }

  $requiredFiles = @(
    "$RepoPath/pyproject.toml",
    "$RepoPath/scripts/review_dashboard.py",
    "$RepoPath/src/trading_codex/review_dashboard_data.py"
  )
  foreach ($pathValue in $requiredFiles) {
    if (-not (Test-WslFileExists -Distro $Distro -PathValue $pathValue)) {
      throw "WSL review workspace is missing a required file: $pathValue"
    }
  }

  if (-not (Test-WslFileExists -Distro $Distro -PathValue $PythonPath)) {
    throw "Configured WSL Python was not found: $PythonPath"
  }

  $repoExpr = ConvertTo-BashArg -Value $RepoPath
  & wsl.exe -d $Distro -- sh -lc "cd $repoExpr && git rev-parse --show-toplevel >/dev/null 2>&1" *> $null
  if ($LASTEXITCODE -ne 0) {
    throw "WSL review workspace is not a git checkout: $RepoPath"
  }

  $gitStatus = Get-WslGitStatus -Distro $Distro -RepoPath $RepoPath
  if (-not [string]::IsNullOrWhiteSpace($gitStatus)) {
    $firstLines = ($gitStatus -split "\r?\n" | Select-Object -First 5) -join "; "
    throw "WSL review workspace must be clean before launching the dashboard: $RepoPath. Pending changes: $firstLines"
  }
}

function Show-ErrorDialog {
  param(
    [string]$Message
  )
  try {
    Add-Type -AssemblyName System.Windows.Forms -ErrorAction Stop
    [System.Windows.Forms.MessageBox]::Show(
      $Message,
      "Trading Codex Review Dashboard",
      [System.Windows.Forms.MessageBoxButtons]::OK,
      [System.Windows.Forms.MessageBoxIcon]::Error
    ) | Out-Null
  } catch {
    # Best effort only.
  }
}

try {
  if ($Help) {
    Get-Help -Full $PSCommandPath | Out-String | Write-Output
    exit 0
  }

  if ([string]::IsNullOrWhiteSpace($WslPython)) {
    $WslPython = "$($WslRepoPath.TrimEnd('/'))/.venv/bin/python"
  }

  $resolvedRepoPath = Resolve-WslPath -PathValue $WslRepoPath -Distro $WslDistro
  $resolvedPythonPath = Resolve-WslPath -PathValue $WslPython -Distro $WslDistro
  $resolvedCacheDir = Resolve-WslPath -PathValue $CacheDir -Distro $WslDistro
  $resolvedDashboardScriptPath = Resolve-WslPath -PathValue "$($resolvedRepoPath.TrimEnd('/'))/scripts/review_dashboard.py" -Distro $WslDistro
  $resolvedLogPath = Resolve-WslPath -PathValue "$($resolvedCacheDir.TrimEnd('/'))/streamlit-$Port.log" -Distro $WslDistro
  $resolvedMetadataPath = Resolve-WslPath -PathValue "$($resolvedCacheDir.TrimEnd('/'))/instance-$Port.json" -Distro $WslDistro
  $metadataWindowsPath = Convert-WslPathToWindowsPath -Distro $WslDistro -PathValue $resolvedMetadataPath
  $dashboardUrl = "http://${DashboardHost}:$Port"

  Assert-SafeReviewWorkspace -Distro $WslDistro -RepoPath $resolvedRepoPath -PythonPath $resolvedPythonPath

  if ($PrintOnly) {
    $launchCommand = Build-LaunchCommand -RepoPath $resolvedRepoPath -PythonPath $resolvedPythonPath -DashboardScriptPath $resolvedDashboardScriptPath -LogPath $resolvedLogPath -PortNumber $Port
    Write-Output "# review_dashboard_launcher"
    Write-Output "url=$dashboardUrl"
    Write-Output "wsl_distro=$WslDistro"
    Write-Output "repo_path=$resolvedRepoPath"
    Write-Output "python_path=$resolvedPythonPath"
    Write-Output "cache_dir=$resolvedCacheDir"
    Write-Output "log_path=$resolvedLogPath"
    Write-Output "instance_path=$resolvedMetadataPath"
    Write-Output "browser=$([bool](-not $NoBrowser))"
    Write-Output "action=reuse-only-if-launcher-marker-and-process-match-else-fail-or-launch"
    Write-Output "command=$launchCommand"
    exit 0
  }

  if ($ValidateOnly) {
    Write-Output "Review dashboard workspace OK: $resolvedRepoPath"
    exit 0
  }

  $instanceState = Get-LauncherInstanceState -Distro $WslDistro -MetadataWindowsPath $metadataWindowsPath -RepoPath $resolvedRepoPath -PythonPath $resolvedPythonPath -PortNumber $Port -Url $dashboardUrl
  if ($instanceState.Reusable) {
    if (-not $NoBrowser) {
      Start-Process $dashboardUrl | Out-Null
    }
    Write-Output "Review dashboard already running at $dashboardUrl"
    exit 0
  }

  if ($instanceState.MarkerMatchesRequest -and $instanceState.OwnedProcess) {
    throw "A launcher-owned Trading Codex review dashboard process is already present for $resolvedRepoPath on port $Port, but it is not healthy. Check the WSL log at $resolvedLogPath before retrying."
  }

  if ($instanceState.PortInUse) {
    throw "Port $Port is already in use on $DashboardHost, but the running service does not match a launcher-owned Trading Codex review dashboard instance for $resolvedRepoPath. Refusing to reuse or overwrite it."
  }

  if ($null -ne $instanceState.Metadata) {
    Remove-FileIfExists -PathValue $metadataWindowsPath
  }

  $bashCommand = Build-LaunchCommand -RepoPath $resolvedRepoPath -PythonPath $resolvedPythonPath -DashboardScriptPath $resolvedDashboardScriptPath -LogPath $resolvedLogPath -PortNumber $Port
  $launchOutput = Invoke-WslStdout -Distro $WslDistro -Command @("sh", "-lc", $bashCommand)
  $launchLines = @($launchOutput -split "\r?\n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
  $launchPidText = if ($launchLines.Count -gt 0) { $launchLines[$launchLines.Count - 1].Trim() } else { "" }
  $launchedProcessId = 0
  if (-not [int]::TryParse($launchPidText, [ref]$launchedProcessId)) {
    throw "Failed to capture the WSL review dashboard process id."
  }

  $processSnapshot = Wait-ForWslProcessSnapshot -Distro $WslDistro -ProcessId $launchedProcessId -TimeoutSeconds 5
  if ($null -eq $processSnapshot -or -not (Test-ReviewDashboardCommandLine -CommandLine $processSnapshot.CommandLine -RepoPath $resolvedRepoPath -PortNumber $Port)) {
    throw "Failed to verify the WSL review dashboard process identity after launch."
  }

  Write-JsonFile -PathValue $metadataWindowsPath -Payload @{
    schema = $MetadataSchema
    repo_path = $resolvedRepoPath
    python_path = $resolvedPythonPath
    port = $Port
    url = $dashboardUrl
    pid = $launchedProcessId
    process_start_ticks = $processSnapshot.ProcessStartTicks
    command_line = $processSnapshot.CommandLine
    updated_at = (Get-Date).ToString("o")
  }

  if (-not (Wait-ForHealthyDashboard -Url $dashboardUrl -TimeoutSeconds $StartupTimeoutSeconds)) {
    Remove-FileIfExists -PathValue $metadataWindowsPath
    throw "Review dashboard did not become healthy within $StartupTimeoutSeconds seconds. Check the WSL log at $resolvedLogPath. If Streamlit is missing, install the dashboard extras in the review workspace with 'pip install -e .[dashboard]'."
  }

  if (-not $NoBrowser) {
    Start-Process $dashboardUrl | Out-Null
  }
  Write-Output "Opened review dashboard at $dashboardUrl"
  exit 0
} catch {
  $message = $_.Exception.Message
  Write-Error $message
  if ($ShowErrorDialog -and -not $ValidateOnly -and -not $PrintOnly -and -not $Help) {
    Show-ErrorDialog -Message $message
  }
  exit 1
}
