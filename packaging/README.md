# GitHub App Sync Packaging

The source sync runtime now expects a packaged GitHub App config file instead of user-entered PAT settings.

## Packaged file

Ship a file named `github-app-sync-config.json` with the app bundle, or set `BALUFFO_SYNC_APP_CONFIG_PATH` to its location.

Expected JSON shape:

```json
{
  "schemaVersion": 1,
  "appId": "123456",
  "installationId": "98765432",
  "repo": "your-org/job-sources-backup",
  "branch": "main",
  "path": "baluffo/source-sync.json",
  "allowedRepo": "your-org/job-sources-backup",
  "allowedBranch": "main",
  "allowedPathPrefix": "baluffo/source-sync.json",
  "keyDerivation": "embedded",
  "embeddedKeyHint": "build-generated-hint",
  "embeddedKeyVersion": "v1",
  "keySalt": "base64url-random-salt",
  "privateKeyPemEnc": "base64url-encrypted-private-key"
}
```

Runtime requirement for passphrase mode:

- set `BALUFFO_SYNC_KEY_PASSPHRASE` on machines that should use sync
- optional emergency kill switch: set `BALUFFO_SYNC_DISABLE=1`

## Encryption format

The encrypted key supports three derivation modes implemented in `scripts/source_sync.py`:

- `machine`: key material is derived from machine identity, app id, installation id, and `keySalt`
- `passphrase`: key material is derived from app id, installation id, `keySalt`, and `BALUFFO_SYNC_KEY_PASSPHRASE`
- `embedded`: key material is derived from `embeddedKeyHint` + embedded runtime constants (deterrence only)
- the private key PEM is XOR-encrypted with a SHA-256 keystream
- plaintext private keys should not be shipped in production bundles
- on Windows, decrypted key material is re-wrapped into a local DPAPI cache for subsequent launches

For local tests only, `privateKeyPem` may be supplied instead of `privateKeyPemEnc`.

## Build-time notes

- Do not commit production `github-app-sync-config.json`
- Do not commit generated `github-app-sync-config.localkey.json`
- Generate a unique `keySalt` per distribution target
- Encrypt the GitHub App private key PEM before bundling
- Bundle the config next to the app or point `BALUFFO_SYNC_APP_CONFIG_PATH` to the deployed file

## Root config defaults

Packaged sync defaults now come from:

- `baluffo.config.json`
- optional `baluffo.config.local.json`

Build-time precedence is:

- CLI
- env
- `baluffo.config.local.json`
- `baluffo.config.json`
- code fallback

That root config now provides the default packaged sync path, repo/branch/path defaults, allowlist defaults,
and default build key derivation. `BALUFFO_SYNC_BUILD_*` env vars still override those values when present.

## Helper command

You can generate the packaged config locally with:

```powershell
python scripts/build_sync_app_config.py `
  --app-id 123456 `
  --installation-id 98765432 `
  --repo owner/repo `
  --allowed-repo owner/repo `
  --allowed-branch main `
  --allowed-path-prefix baluffo/source-sync.json `
  --key-derivation embedded `
  --private-key C:\path\to\github-app-private-key.pem `
  --embedded-key-version v1
```

For portable passphrase mode instead of embedded mode, set:

```powershell
$env:BALUFFO_SYNC_KEY_PASSPHRASE="replace-with-strong-passphrase"
python scripts/build_sync_app_config.py --key-derivation passphrase --portable-passphrase-env BALUFFO_SYNC_KEY_PASSPHRASE ...
```

## Automatic build generation

`scripts/build_ship_bundle.py` and `scripts/build_portable_exe.py` now auto-generate
`packaging/github-app-sync-config.json` during the build when a prebuilt config file is
not already present.

Supported build-time inputs:

- `BALUFFO_SYNC_BUILD_APP_ID`
- `BALUFFO_SYNC_BUILD_INSTALLATION_ID`
- `BALUFFO_SYNC_BUILD_REPO`
- `BALUFFO_SYNC_BUILD_PRIVATE_KEY_PATH` or `BALUFFO_SYNC_BUILD_PRIVATE_KEY_PEM`
- optional: `BALUFFO_SYNC_BUILD_BRANCH`
- optional: `BALUFFO_SYNC_BUILD_PATH`
- optional: `BALUFFO_SYNC_BUILD_ALLOWED_REPO`
- optional: `BALUFFO_SYNC_BUILD_ALLOWED_BRANCH`
- optional: `BALUFFO_SYNC_BUILD_ALLOWED_PATH_PREFIX`
- optional: `BALUFFO_SYNC_BUILD_KEY_DERIVATION` (`embedded` default, `machine`, `passphrase`, or `plaintext`)
- optional: `BALUFFO_SYNC_BUILD_PASSPHRASE_ENV`
- optional: `BALUFFO_SYNC_BUILD_EMBEDDED_KEY_HINT`
- optional: `BALUFFO_SYNC_BUILD_EMBEDDED_KEY_VERSION`
- optional: `BALUFFO_SYNC_BUILD_KEY_SALT`

Defaults used by the build:

- key derivation: `embedded`
- branch: `main`
- path: `baluffo/source-sync.json`
- allowlist repo/branch/path-prefix: mirrors the target repo/branch/path unless explicitly overridden

Example:

```powershell
$env:BALUFFO_SYNC_BUILD_APP_ID="123456"
$env:BALUFFO_SYNC_BUILD_INSTALLATION_ID="98765432"
$env:BALUFFO_SYNC_BUILD_REPO="your-org/job-sources-backup"
$env:BALUFFO_SYNC_BUILD_PRIVATE_KEY_PATH="C:\secrets\github-app-private-key.pem"
npm run build:ship-bundle
```

For local-only testing, add `--plaintext` to write `privateKeyPem` directly instead of the encrypted form.

## Security note

Embedded mode is obfuscation and blast-radius reduction, not strong secret protection on client-owned machines.
