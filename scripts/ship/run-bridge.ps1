param(
  [string]$BindHost = "127.0.0.1",
  [int]$Port = 8877,
  [string]$DataDir = ""
)

$ErrorActionPreference = "Stop"
$PythonLauncher = Get-Command py -ErrorAction SilentlyContinue
$PythonCommand = "python"
$PythonArgs = @()
if ($PythonLauncher) {
  $PythonCommand = $PythonLauncher.Source
  $PythonArgs = @("-3")
}
$Root = if (Test-Path (Join-Path $PSScriptRoot "scripts")) {
  $PSScriptRoot
} else {
  (Resolve-Path (Join-Path $PSScriptRoot "..\\..")).Path
}
if ([string]::IsNullOrWhiteSpace($DataDir)) {
  $DataDir = Join-Path $Root "data"
}
$Manager = Join-Path $Root "scripts\ship\update_manager.py"
$Launcher = Join-Path $Root "scripts\ship\runtime_launcher.py"
if (-not (Test-Path $Manager)) {
  throw "Update manager not found: $Manager"
}
if (-not (Test-Path $Launcher)) {
  throw "Runtime launcher not found: $Launcher"
}

& $PythonCommand @PythonArgs $Manager startup-check --root $Root --data-dir $DataDir
if ($LASTEXITCODE -ne 0) {
  throw "Startup validation failed. See error above."
}

$CurrentPointer = Join-Path $Root "app\current.txt"
$CurrentVersion = (Get-Content $CurrentPointer -Raw).Trim()
$ActiveRoot = Join-Path $Root "app\versions\$CurrentVersion"
if (-not (Test-Path $ActiveRoot)) {
  throw "Active version directory not found: $ActiveRoot"
}

$env:BALUFFO_DATA_DIR = $DataDir
Write-Host "[baluffo-ship] Starting admin bridge..." -ForegroundColor Cyan
Write-Host "[baluffo-ship] URL: http://$BindHost`:$Port" -ForegroundColor Gray
Write-Host "[baluffo-ship] Data dir: $DataDir" -ForegroundColor Gray
Write-Host "[baluffo-ship] Version: $CurrentVersion" -ForegroundColor Gray
Write-Host "[baluffo-ship] Python: $PythonCommand $($PythonArgs -join ' ')" -ForegroundColor Gray

Push-Location $ActiveRoot
try {
  & $PythonCommand @PythonArgs $Launcher bridge --root $Root --bind-host $BindHost --port $Port --data-dir $DataDir
}
finally {
  Pop-Location
}
