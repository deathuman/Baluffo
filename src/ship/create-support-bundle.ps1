param(
  [string]$Root = "",
  [string]$Output = ""
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
  if ([string]::IsNullOrWhiteSpace($Output)) {
    python -m src.ship.update_manager_cli support-bundle --root $Root
  } else {
    python -m src.ship.update_manager_cli support-bundle --root $Root --output $Output
  }
} finally {
  Pop-Location
}
