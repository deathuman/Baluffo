param(
  [int]$Port = 8080
)

$ErrorActionPreference = "Stop"
$PythonCommand = "python"
$PythonArgs = @()
$Root = if (Test-Path (Join-Path $PSScriptRoot "src")) {
  $PSScriptRoot
} else {
  (Resolve-Path (Join-Path $PSScriptRoot "..\\..")).Path
}
Write-Host "[baluffo-ship] Starting static site..." -ForegroundColor Cyan
Write-Host "[baluffo-ship] URL: http://127.0.0.1:$Port" -ForegroundColor Gray
Write-Host "[baluffo-ship] Ship root: $Root" -ForegroundColor Gray
Write-Host "[baluffo-ship] Python: $PythonCommand $($PythonArgs -join ' ')" -ForegroundColor Gray

& $PythonCommand @PythonArgs -m src.ship.runtime_launcher site --root $Root --port $Port
