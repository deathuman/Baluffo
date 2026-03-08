param(
  [string]$Host = "127.0.0.1",
  [int]$Port = 8878,
  [string]$DataDir = ""
)

$ErrorActionPreference = "Stop"
$Root = if (Test-Path (Join-Path $PSScriptRoot "scripts")) {
  $PSScriptRoot
} else {
  (Resolve-Path (Join-Path $PSScriptRoot "..\\..")).Path
}
if ([string]::IsNullOrWhiteSpace($DataDir)) {
  $DataDir = Join-Path $Root "data"
}
$Manager = Join-Path $Root "scripts\ship\update_manager.py"
if (-not (Test-Path $Manager)) {
  throw "Update manager not found: $Manager"
}

python $Manager startup-check --root $Root --data-dir $DataDir
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
Write-Host "[baluffo-ship] URL: http://$Host`:$Port" -ForegroundColor Gray
Write-Host "[baluffo-ship] Data dir: $DataDir" -ForegroundColor Gray
Write-Host "[baluffo-ship] Version: $CurrentVersion" -ForegroundColor Gray

Push-Location $ActiveRoot
try {
  python scripts/admin_bridge.py --host $Host --port $Port --data-dir $DataDir --log-format human --log-level info
}
finally {
  Pop-Location
}
