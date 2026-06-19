param(
  [Parameter(Mandatory = $true)][string]$BundleZip,
  [Parameter(Mandatory = $true)][string]$Manifest,
  [string]$Root = "",
  [string]$SigningKey = ""
)

$ErrorActionPreference = "Stop"
if ([string]::IsNullOrWhiteSpace($Root)) {
  $Root = if (Test-Path (Join-Path $PSScriptRoot "app")) { $PSScriptRoot } else { (Resolve-Path (Join-Path $PSScriptRoot "..\\..")).Path }
}
if ([string]::IsNullOrWhiteSpace($SigningKey)) {
  $SigningKey = [Environment]::GetEnvironmentVariable("BALUFFO_UPDATE_SIGNING_KEY")
}

if ([string]::IsNullOrWhiteSpace($SigningKey)) {
  throw "Missing signing key. Set -SigningKey or BALUFFO_UPDATE_SIGNING_KEY."
}

$managerCli = Join-Path $Root "src\ship\update_manager_cli.py"
if (-not (Test-Path $managerCli)) {
  throw "Update manager CLI not found: $managerCli"
}

Push-Location -LiteralPath $Root
try {
  python -m src.ship.update_manager_cli apply --root $Root --bundle-zip $BundleZip --manifest $Manifest --signing-key $SigningKey
} finally {
  Pop-Location
}
