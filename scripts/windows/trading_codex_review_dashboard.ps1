<#
.SYNOPSIS
Open the local read-only Trading Codex review dashboard from a dedicated WSL workspace.

.DESCRIPTION
Uses a clean WSL review workspace to run `scripts/review_dashboard.py` with Streamlit.
The launcher only binds Streamlit to `127.0.0.1`, refuses the normal `~/trading_codex`
checkout, and reuses an already-running healthy dashboard on the expected local port
instead of starting a duplicate instance.

.PARAMETER WslDistro
WSL distro name. Default: `Ubuntu`.

.PARAMETER WslRepoPath
Dedicated clean WSL workspace used to run the dashboard.
Default: `~/.codex-workspaces/trading-review`.

.PARAMETER WslPython
Python path inside WSL. Defaults to `<WslRepoPath>/.venv/bin/python`.

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
  $text = ($output | Out-String).Trim()
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
    return Invoke-WslCapture -Distro $Distro -Command @("bash", "-lc", "realpath -m $pathExpr")
  }

  $candidate = $PathValue
  if (Test-Path -LiteralPath $PathValue) {
    $candidate = (Resolve-Path -LiteralPath $PathValue).Path
  }
  return Invoke-WslCapture -Distro $Distro -Command @("wslpath", "-a", $candidate)
}

function Get-WslHomeDirectory {
  param(
    [string]$Distro
  )
  return Invoke-WslCapture -Distro $Distro -Command @("bash", "-lc", 'printf %s "$HOME"')
}

function Test-WslDirectoryExists {
  param(
    [string]$Distro,
    [string]$PathValue
  )
  $pathExpr = ConvertTo-BashPathExpr -Value $PathValue
  & wsl.exe -d $Distro -- bash -lc "test -d $pathExpr" *> $null
  return ($LASTEXITCODE -eq 0)
}

function Test-WslFileExists {
  param(
    [string]$Distro,
    [string]$PathValue
  )
  $pathExpr = ConvertTo-BashPathExpr -Value $PathValue
  & wsl.exe -d $Distro -- bash -lc "test -f $pathExpr" *> $null
  return ($LASTEXITCODE -eq 0)
}

function Get-WslGitStatus {
  param(
    [string]$Distro,
    [string]$RepoPath
  )
  $repoExpr = ConvertTo-BashArg -Value $RepoPath
  return Invoke-WslCapture -Distro $Distro -AllowFailure -Command @(
    "bash",
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
    [string]$LogPath,
    [int]$PortNumber
  )
  $logDirectory = Split-Path -Path $LogPath -Parent
  $streamlitParts = @(
    (ConvertTo-BashArg -Value $PythonPath),
    "-m",
    "streamlit",
    "run",
    "scripts/review_dashboard.py",
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
  return "cd $(ConvertTo-BashArg -Value $RepoPath) && mkdir -p $(ConvertTo-BashArg -Value $logDirectory) && nohup $streamlitCommand > $(ConvertTo-BashArg -Value $LogPath) 2>&1 < /dev/null &"
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
  & wsl.exe -d $Distro -- bash -lc "cd $repoExpr && git rev-parse --show-toplevel >/dev/null 2>&1" *> $null
  if ($LASTEXITCODE -ne 0) {
    throw "WSL review workspace is not a git checkout: $RepoPath"
  }

  $gitStatus = Get-WslGitStatus -Distro $Distro -RepoPath $RepoPath
  if (-not [string]::IsNullOrWhiteSpace($gitStatus)) {
    $firstLines = ($gitStatus -split [Environment]::NewLine | Select-Object -First 5) -join "; "
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
  $resolvedLogPath = Resolve-WslPath -PathValue "~/.cache/trading_codex/review_dashboard/streamlit-$Port.log" -Distro $WslDistro
  $dashboardUrl = "http://${DashboardHost}:$Port"

  Assert-SafeReviewWorkspace -Distro $WslDistro -RepoPath $resolvedRepoPath -PythonPath $resolvedPythonPath

  if ($PrintOnly) {
    $launchCommand = Build-LaunchCommand -RepoPath $resolvedRepoPath -PythonPath $resolvedPythonPath -LogPath $resolvedLogPath -PortNumber $Port
    Write-Output "# review_dashboard_launcher"
    Write-Output "url=$dashboardUrl"
    Write-Output "wsl_distro=$WslDistro"
    Write-Output "repo_path=$resolvedRepoPath"
    Write-Output "python_path=$resolvedPythonPath"
    Write-Output "log_path=$resolvedLogPath"
    Write-Output "browser=$([bool](-not $NoBrowser))"
    Write-Output "action=reuse-if-healthy-else-launch"
    Write-Output "command=$launchCommand"
    exit 0
  }

  if ($ValidateOnly) {
    Write-Output "Review dashboard workspace OK: $resolvedRepoPath"
    exit 0
  }

  if (Test-HealthyDashboard -Url $dashboardUrl) {
    if (-not $NoBrowser) {
      Start-Process $dashboardUrl | Out-Null
    }
    Write-Output "Review dashboard already running at $dashboardUrl"
    exit 0
  }

  if (Test-LocalPortInUse -Address $DashboardHost -PortNumber $Port) {
    throw "Port $Port is already in use on $DashboardHost by a non-dashboard process. Refusing to start another service on that port."
  }

  $bashCommand = Build-LaunchCommand -RepoPath $resolvedRepoPath -PythonPath $resolvedPythonPath -LogPath $resolvedLogPath -PortNumber $Port
  & wsl.exe -d $WslDistro -- bash -lc $bashCommand *> $null
  if ($LASTEXITCODE -ne 0) {
    throw "Failed to launch the WSL review dashboard process."
  }

  if (-not (Wait-ForHealthyDashboard -Url $dashboardUrl -TimeoutSeconds $StartupTimeoutSeconds)) {
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
