param(
  [int]$Port = 8080
)

$ErrorActionPreference = "Stop"
$Root = if (Test-Path (Join-Path $PSScriptRoot "scripts")) {
  $PSScriptRoot
} else {
  (Resolve-Path (Join-Path $PSScriptRoot "..\\..")).Path
}
Write-Host "[baluffo-ship] Starting static site..." -ForegroundColor Cyan
Write-Host "[baluffo-ship] URL: http://127.0.0.1:$Port" -ForegroundColor Gray
Write-Host "[baluffo-ship] Root: $Root" -ForegroundColor Gray

Push-Location $Root
try {
  python -m http.server $Port --directory .
}
finally {
  Pop-Location
}
