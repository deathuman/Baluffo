param(
  [int]$SitePort = 8080,
  [string]$BridgeHost = "127.0.0.1",
  [int]$BridgePort = 8877,
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

$BridgeScript = Join-Path $PSScriptRoot "run-bridge.ps1"
$SiteScript = Join-Path $PSScriptRoot "run-site.ps1"

Write-Host "[baluffo-ship] Launching site + bridge..." -ForegroundColor Cyan
Write-Host "[baluffo-ship] Site:   http://127.0.0.1:$SitePort" -ForegroundColor Gray
Write-Host "[baluffo-ship] Bridge: http://$BridgeHost`:$BridgePort" -ForegroundColor Gray
Write-Host "[baluffo-ship] Data:   $DataDir" -ForegroundColor Gray

Start-Process powershell -ArgumentList "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", "`"$SiteScript`"", "-Port", "$SitePort"
Start-Process powershell -ArgumentList "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", "`"$BridgeScript`"", "-Host", "$BridgeHost", "-Port", "$BridgePort", "-DataDir", "`"$DataDir`""

Write-Host "[baluffo-ship] Started. Use Task Manager or PowerShell Stop-Process to stop spawned windows." -ForegroundColor Yellow
