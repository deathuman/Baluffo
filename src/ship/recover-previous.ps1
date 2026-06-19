param(
  [string]$Root = ""
)

$ErrorActionPreference = "Stop"
if ([string]::IsNullOrWhiteSpace($Root)) {
  $Root = if (Test-Path (Join-Path $PSScriptRoot "app")) { $PSScriptRoot } else { (Resolve-Path (Join-Path $PSScriptRoot "..\\..")).Path }
}

$managerCli = Join-Path $Root "src\ship\update_manager_cli.py"
if (-not (Test-Path $managerCli)) {
  throw "Update manager CLI not found: $managerCli"
}

Push-Location -LiteralPath $Root
try {
  python -m src.ship.update_manager_cli recover --root $Root
} finally {
  Pop-Location
}
