param(
  [int]$Port = 8080
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
$Launcher = Join-Path $Root "scripts\ship\runtime_launcher.py"
$CurrentPointer = Join-Path $Root "app\current.txt"
if (-not (Test-Path $Launcher)) {
  throw "Runtime launcher not found: $Launcher"
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
Write-Host "[baluffo-ship] Python: $PythonCommand $($PythonArgs -join ' ')" -ForegroundColor Gray

Push-Location $ActiveRoot
try {
  & $PythonCommand @PythonArgs $Launcher site --root $Root --port $Port
}
finally {
  Pop-Location
}
