param(
  [string]$BindHost = "127.0.0.1",
  [int]$Port = 8877,
  [string]$DataDir = ""
)

$ErrorActionPreference = "Stop"
$PythonCommand = "python"
$PythonArgs = @()
$Root = if (Test-Path (Join-Path $PSScriptRoot "src")) {
  $PSScriptRoot
} else {
  (Resolve-Path (Join-Path $PSScriptRoot "..\\..")).Path
}
if ([string]::IsNullOrWhiteSpace($DataDir)) {
  $DataDir = Join-Path $Root "data"
}
& $PythonCommand @PythonArgs -m src.ship.update_manager startup-check --root $Root --data-dir $DataDir
if ($LASTEXITCODE -ne 0) {
  throw "Startup validation failed. See error above."
}

$env:BALUFFO_DATA_DIR = $DataDir
Write-Host "[baluffo-ship] Starting admin bridge..." -ForegroundColor Cyan
Write-Host "[baluffo-ship] URL: http://$BindHost`:$Port" -ForegroundColor Gray
Write-Host "[baluffo-ship] Data dir: $DataDir" -ForegroundColor Gray
Write-Host "[baluffo-ship] Ship root: $Root" -ForegroundColor Gray
Write-Host "[baluffo-ship] Python: $PythonCommand $($PythonArgs -join ' ')" -ForegroundColor Gray

& $PythonCommand @PythonArgs -m src.ship.runtime_launcher bridge --root $Root --bind-host $BindHost --port $Port --data-dir $DataDir
