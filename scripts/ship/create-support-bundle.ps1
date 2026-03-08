param(
  [string]$Root = "",
  [string]$Output = ""
)

$ErrorActionPreference = "Stop"
if ([string]::IsNullOrWhiteSpace($Root)) {
  $Root = if (Test-Path (Join-Path $PSScriptRoot "app")) { $PSScriptRoot } else { (Resolve-Path (Join-Path $PSScriptRoot "..\\..")).Path }
}

$manager = Join-Path $Root "scripts\ship\update_manager.py"
if (-not (Test-Path $manager)) {
  throw "Update manager not found: $manager"
}

if ([string]::IsNullOrWhiteSpace($Output)) {
  python $manager support-bundle --root $Root
} else {
  python $manager support-bundle --root $Root --output $Output
}
