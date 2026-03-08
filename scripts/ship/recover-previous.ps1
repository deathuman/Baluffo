param(
  [string]$Root = ""
)

$ErrorActionPreference = "Stop"
if ([string]::IsNullOrWhiteSpace($Root)) {
  $Root = if (Test-Path (Join-Path $PSScriptRoot "app")) { $PSScriptRoot } else { (Resolve-Path (Join-Path $PSScriptRoot "..\\..")).Path }
}

$manager = Join-Path $Root "scripts\ship\update_manager.py"
if (-not (Test-Path $manager)) {
  throw "Update manager not found: $manager"
}

python $manager recover --root $Root
