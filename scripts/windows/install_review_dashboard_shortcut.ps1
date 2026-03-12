<#
.SYNOPSIS
Create or update the Windows desktop shortcut for the local Trading Codex review dashboard.

.DESCRIPTION
Creates a desktop shortcut named `Trading Codex Review Hub` that launches the
PowerShell review dashboard wrapper from a dedicated WSL review workspace.
The shortcut uses a hidden PowerShell window and enables the wrapper's
error dialog so failures still surface clearly.

.PARAMETER ShortcutName
Desktop shortcut display name. Default: `Trading Codex Review Hub`.

.PARAMETER DesktopPath
Destination desktop folder. Defaults to the current user's desktop.

.PARAMETER WslDistro
WSL distro name. Default: `Ubuntu`.

.PARAMETER WslRepoPath
Dedicated clean WSL review workspace used to run the dashboard.
Default: `~/.codex-workspaces/trading-review`.

.PARAMETER Port
Local dashboard port. Default: `8501`.

.PARAMETER PrintOnly
Print the shortcut plan without writing the `.lnk` file.

.PARAMETER Help
Show this help text.

.EXAMPLE
./install_review_dashboard_shortcut.ps1

.EXAMPLE
./install_review_dashboard_shortcut.ps1 -PrintOnly -WslRepoPath ~/.codex-workspaces/trading-review
#>
[CmdletBinding()]
param(
  [string]$ShortcutName = "Trading Codex Review Hub",
  [string]$DesktopPath = [Environment]::GetFolderPath("Desktop"),
  [string]$WslDistro = "Ubuntu",
  [string]$WslRepoPath = "~/.codex-workspaces/trading-review",
  [ValidateRange(1, 65535)]
  [int]$Port = 8501,
  [switch]$PrintOnly,
  [switch]$Help
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

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
  return "'" + $Value.Replace("'", "'`"`'`"`'") + "'"
}

function Invoke-WslCapture {
  param(
    [string]$Distro,
    [string[]]$Command
  )
  $output = & wsl.exe -d $Distro -- @Command 2>&1
  $text = ($output | Out-String).Trim()
  if ($LASTEXITCODE -ne 0) {
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

function Convert-WslPathToWindowsPath {
  param(
    [string]$Distro,
    [string]$PathValue
  )
  return Invoke-WslCapture -Distro $Distro -Command @("wslpath", "-w", $PathValue)
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

if ($Help) {
  Get-Help -Full $PSCommandPath | Out-String | Write-Output
  exit 0
}

$resolvedRepoPath = Resolve-WslPath -PathValue $WslRepoPath -Distro $WslDistro
$launcherWslPath = "$($resolvedRepoPath.TrimEnd('/'))/scripts/windows/trading_codex_review_dashboard.ps1"
if (-not (Test-WslFileExists -Distro $WslDistro -PathValue $launcherWslPath)) {
  throw "Launcher script not found in the configured WSL review workspace: $launcherWslPath"
}

$launcherWindowsPath = Convert-WslPathToWindowsPath -Distro $WslDistro -PathValue $launcherWslPath
$shortcutPath = Join-Path $DesktopPath "$ShortcutName.lnk"
$powershellExe = Join-Path $PSHOME "powershell.exe"
$arguments = @(
  "-NoLogo",
  "-NoProfile",
  "-WindowStyle",
  "Hidden",
  "-ExecutionPolicy",
  "Bypass",
  "-File",
  $launcherWindowsPath,
  "-ShowErrorDialog",
  "-WslDistro",
  $WslDistro,
  "-WslRepoPath",
  $WslRepoPath,
  "-Port",
  [string]$Port
)
$argumentLine = Join-WindowsCommandLine -Values $arguments

if ($PrintOnly) {
  Write-Output "# review_dashboard_shortcut"
  Write-Output "shortcut_path=$shortcutPath"
  Write-Output "target_path=$powershellExe"
  Write-Output "launcher_path=$launcherWindowsPath"
  Write-Output "wsl_repo_path=$resolvedRepoPath"
  Write-Output "arguments=$argumentLine"
  exit 0
}

if (-not (Test-Path -LiteralPath $DesktopPath)) {
  throw "Desktop destination not found: $DesktopPath"
}

& $powershellExe -NoLogo -NoProfile -ExecutionPolicy Bypass -File $launcherWindowsPath -ValidateOnly -WslDistro $WslDistro -WslRepoPath $WslRepoPath -Port $Port
if ($LASTEXITCODE -ne 0) {
  throw "Launcher validation failed for $resolvedRepoPath"
}

$shell = New-Object -ComObject WScript.Shell
$shortcut = $shell.CreateShortcut($shortcutPath)
$shortcut.TargetPath = $powershellExe
$shortcut.Arguments = $argumentLine
$shortcut.WorkingDirectory = $env:USERPROFILE
$shortcut.Description = "Open the local read-only Trading Codex review dashboard."
$shortcut.IconLocation = "$powershellExe,0"
$shortcut.Save()

Write-Output "Created desktop shortcut: $shortcutPath"
