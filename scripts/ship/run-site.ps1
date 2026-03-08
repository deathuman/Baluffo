param(
  [int]$Port = 8080
)

$ErrorActionPreference = "Stop"
$Root = if (Test-Path (Join-Path $PSScriptRoot "scripts")) {
  $PSScriptRoot
} else {
  (Resolve-Path (Join-Path $PSScriptRoot "..\\..")).Path
}
$CurrentPointer = Join-Path $Root "app\current.txt"
if (-not (Test-Path $CurrentPointer)) {
  throw "Missing app current pointer: $CurrentPointer"
}
$CurrentVersion = (Get-Content $CurrentPointer -Raw).Trim()
if ([string]::IsNullOrWhiteSpace($CurrentVersion)) {
  throw "Current version pointer is empty."
}
$ActiveRoot = Join-Path $Root "app\versions\$CurrentVersion"
if (-not (Test-Path $ActiveRoot)) {
  throw "Active version directory not found: $ActiveRoot"
}
Write-Host "[baluffo-ship] Starting static site..." -ForegroundColor Cyan
Write-Host "[baluffo-ship] URL: http://127.0.0.1:$Port" -ForegroundColor Gray
Write-Host "[baluffo-ship] Root: $ActiveRoot" -ForegroundColor Gray
Write-Host "[baluffo-ship] Version: $CurrentVersion" -ForegroundColor Gray

Push-Location $ActiveRoot
try {
  python -m http.server $Port --directory .
}
finally {
  Pop-Location
}
