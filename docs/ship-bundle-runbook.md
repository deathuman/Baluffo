# Ship Bundle Runbook (Windows Zip-First)

Full operator guide: `docs/deployment-and-update-guide.md`.
Versioning and release policy: `docs/versioning-policy-and-release-checklist.md`.

## Build

```powershell
python scripts/build_ship_bundle.py
```

Default output:

- `dist/baluffo-ship`

Optional output override:

```powershell
python scripts/build_ship_bundle.py --output-dir C:\temp\baluffo-ship
```

Optional bundle version:

```powershell
python scripts/build_ship_bundle.py --bundle-version 1.2.3
```

## Start from bundle

From bundle root:

```powershell
.\run-site.ps1 -Port 8080
.\run-bridge.ps1 -Host 127.0.0.1 -Port 8877
```

Or launch both:

```powershell
.\run-all.ps1 -SitePort 8080 -BridgeHost 127.0.0.1 -BridgePort 8877
```

## Bundle layout

- `app\versions\<version>`: immutable application files
- `app\current.txt`: active version pointer
- `app\update-state.json`: updater state (`current_version`, `previous_version`, `last_update_status`, `last_error_code`)
- `app\staging`: temporary extraction area for in-progress updates
- `data`: persistent user/runtime data (outside app version folders)
- `data\backups`: pre-migration snapshots
- `data\migration-reports`: migration apply/verify/rollback reports

## Apply update atomically

1) Build or receive a zip artifact containing `app\versions\<targetVersion>`.
2) Build a manifest JSON containing:
- `version`
- `artifact_url`
- `sha256`
- `signature` (HMAC-SHA256 over `version:sha256`)
- `min_updater_version`
- `migration_plan` (array)
- `rollback_allowed` (bool)

Schema reference: `docs/update-manifest.schema.json`.

Apply:

```powershell
$env:BALUFFO_UPDATE_SIGNING_KEY="replace-with-release-key"
.\apply-update.ps1 -BundleZip .\baluffo-update.zip -Manifest .\update-manifest.json
```

The updater validates checksum/signature, stages extract in `app\staging`, snapshots `data` before migrations, runs migration apply+verify, then atomically switches `app\current.txt`. On failure it restores data and keeps previous version active.

## Recovery and support bundle

Manual recovery:

```powershell
.\run-all.ps1 -RecoverPrevious
```

Support bundle:

```powershell
.\run-all.ps1 -CreateSupportBundle
```

Support bundle includes current/previous version state, updater log, and latest migration report.

## Runtime overrides

Bridge supports CLI and env configuration with precedence:

- `CLI > env > defaults`

CLI options:

- `--host`
- `--port`
- `--data-dir`
- `--log-format` (`human|jsonl`)
- `--log-level` (`info|debug`)
- `--quiet-requests`

Env options:

- `BALUFFO_BRIDGE_HOST`
- `BALUFFO_BRIDGE_PORT`
- `BALUFFO_DATA_DIR`
- `BALUFFO_BRIDGE_LOG_FORMAT`
- `BALUFFO_BRIDGE_LOG_LEVEL`

Example:

```powershell
$env:BALUFFO_DATA_DIR="C:\baluffo\data"
python scripts/admin_bridge.py --port 9988
```

## Troubleshooting

- `Address already in use`:
  - change `--port` or stop previous bridge process.
- Admin page shows bridge unavailable:
  - verify URL/port and local firewall prompts.
  - check bridge startup logs for bind or path errors.
- No files under expected data dir:
  - confirm `--data-dir` or `BALUFFO_DATA_DIR` and CLI/env precedence.
- Discovery/fetch launched from bridge writes unexpected location:
  - verify bridge `data_dir` in startup banner and child process args in task logs.
- Startup fails with data-dir safety error:
  - choose a `--data-dir` that is not inside `app\versions\...`.
