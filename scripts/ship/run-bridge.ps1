param(
  [string]$Host = "127.0.0.1",
  [int]$Port = 8877,
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

$env:BALUFFO_DATA_DIR = $DataDir
Write-Host "[baluffo-ship] Starting admin bridge..." -ForegroundColor Cyan
Write-Host "[baluffo-ship] URL: http://$Host`:$Port" -ForegroundColor Gray
Write-Host "[baluffo-ship] Data dir: $DataDir" -ForegroundColor Gray

Push-Location $Root
try {
  python scripts/admin_bridge.py --host $Host --port $Port --data-dir $DataDir --log-format human --log-level info
}
finally {
  Pop-Location
}
