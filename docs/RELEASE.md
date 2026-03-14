# Release Guide

This is the authoritative release document for Baluffo.

## Distribution Channels

Baluffo ships through two distribution channels:

- Ship bundle: the canonical zip-first release channel built around a versioned `app\versions\<version>` layout, PowerShell launchers, and the updater/recovery flow.
- Portable EXE: a Windows desktop wrapper built with `PyInstaller` and the desktop launcher runtime that embeds the ship bundle under `ship\` and uses the ship bundle as its runtime payload.

Important rules:

- The ship bundle is the canonical update channel.
- The portable EXE is a packaged distribution built on top of the ship bundle, not a separate updater model.
- Persistent runtime data must remain outside versioned app folders in the ship bundle, and inside `ship\data\` for the portable EXE layout.

## Versioning Policy

This policy applies to both distribution channels.

Every release must track these versions explicitly:

- `app_version`: end-user release version, using SemVer (`MAJOR.MINOR.PATCH`)
- `updater_version`: capability version of `scripts/ship/update_manager.py`
- `data_schema_version`: version of persisted data expectations and migrations
- `manifest_schema_version`: version of the update manifest contract
- The default `app_version` used by local build/package workflows is defined in `scripts/app_version.py`.

Compatibility rules:

- Patch and minor upgrades are allowed by default if checksum/signature validation, startup health checks, and declared migrations pass.
- Major upgrades require explicit compatibility sign-off, a documented migration plan, rollback criteria, and a successful staging rehearsal.
- Downgrades are blocked by default and only allowed when `rollback_allowed=true`, the target version passes health checks, and the data rollback path has been validated.

Updater and manifest rules:

- The canonical manifest contract is `docs/update-manifest.schema.json`.
- Required manifest fields are `version`, `artifact_url`, `sha256`, `signature`, `min_updater_version`, `migration_plan`, and `rollback_allowed`.
- `min_updater_version` must be compared numerically as SemVer, not lexically.
- Current stable signing is HMAC-SHA256 over `version:sha256` using `BALUFFO_UPDATE_SIGNING_KEY`.

Migration and retention rules:

- Each declared migration must provide `apply`, `verify`, and `rollback`.
- Data backup must be created before migrations run.
- On migration or startup-health failure, data restore and version rollback must complete automatically.
- Operational retention target is the last `N=3` app versions and `N=3` data backups after a release reaches stable `ready` state.

## Build Procedures

### Ship Bundle

Preferred build command:

```powershell
npm run build:ship-bundle
```

Direct Python entrypoint for operator/debug use:

```powershell
python scripts/build_ship_bundle.py --bundle-version 1.2.3
```

Default output:

- `dist\baluffo-ship`

Bundle layout:

- `app\versions\<version>`: immutable app payload
- `app\current.txt`: active version pointer
- `app\update-state.json`: updater state
- `app\staging`: temporary update extraction area
- `data\`: persistent runtime and user data
- `data\backups`: pre-migration snapshots
- `data\migration-reports`: migration apply/verify/rollback reports

Release preparation:

1. Build the target version.
2. Verify the bundle contains launcher scripts and seeded runtime defaults, not repo-local runtime JSON/CSV/log state.
3. Create the update artifact zip containing `app\versions\<targetVersion>`.
4. Compute the artifact `sha256`.
5. Sign the manifest:

```powershell
$env:BALUFFO_UPDATE_SIGNING_KEY="replace-with-release-key"
python scripts/ship/update_manager.py sign-manifest --version 1.2.4 --sha256 <artifact_sha256>
```

Ship-bundle update/apply path:

```powershell
$env:BALUFFO_UPDATE_SIGNING_KEY="replace-with-release-key"
.\apply-update.ps1 -BundleZip .\baluffo-update.zip -Manifest .\update-manifest.json
```

What the updater does:

1. Validates manifest structure.
2. Validates artifact checksum and signature.
3. Extracts into `app\staging\<version>`.
4. Creates a data backup.
5. Runs migrations (`apply` then `verify`).
6. Runs startup health checks on the target version.
7. Atomically switches `app\current.txt`.

Recovery and diagnostics:

```powershell
.\run-all.ps1 -RecoverPrevious
.\run-all.ps1 -CreateSupportBundle
```

### Portable EXE

Prerequisites:

```powershell
python -m pip install -r requirements-desktop.txt
```

Preferred build command:

```powershell
npm run build:portable-exe -- --bundle-version 1.2.3
```

Direct Python entrypoint for operator/debug use:

```powershell
python scripts/build_portable_exe.py --bundle-version 1.2.3
```

Optional icon override:

```powershell
python scripts/build_portable_exe.py --bundle-version 1.2.3 --icon C:\path\to\Baluffo.ico
```

Current environment baseline:

- Use Python 3.13.x for ship bundle and portable EXE build/test workflows in this repo.
- Use `python` commands consistently so shell/tooling behavior matches local and CI execution.

Default outputs:

- `dist\baluffo-portable`
- `dist\baluffo-portable-<version>.zip`

Portable layout:

- `Baluffo.exe`: desktop entrypoint
- `ship\`: embedded ship bundle
- `ship\data\`: runtime and user data
- `ship\data\local-user-data\`: desktop-specific saved jobs, notes, activity, attachments, and profile data

Runtime notes:

- The executable starts the local static site and admin bridge in the background.
- Desktop runtime waits for `jobs.html` and `/ops/health` readiness before opening the window.
- Child processes shut down with the desktop window.
- Desktop local data uses the bridge-backed file store instead of browser-local IndexedDB/localStorage.

## Verification Checklist

### Shared Release Gates

Before any release:

1. Record `app_version`, `updater_version`, `data_schema_version`, and `manifest_schema_version`.
2. Run required validation at minimum:
   - `npm run test:py`
   - `npm run test:frontend:unit`
3. Validate any declared migrations and rollback behavior.
4. Rehearse the release on a staging machine before publish.

### Ship Bundle Verification

1. Build the ship bundle for the target version.
2. Confirm launcher scripts exist in the bundle root.
3. Confirm `app\versions\<app_version>\packaging\github-app-sync-config.template.json` exists.
4. Confirm `data\` contains seeded defaults, not repo-local runtime artifacts.
5. Validate the manifest against `docs/update-manifest.schema.json`.
6. Apply the update on staging with `.\apply-update.ps1`.
7. Confirm:
   - `app\update-state.json` ends in `ready`
   - `app\current.txt` points to the target version
   - persisted user/runtime data remains intact
   - support bundle generation works

### Portable EXE Verification

1. Build the portable EXE for the target version.
2. Confirm:
   - `dist\baluffo-portable\Baluffo.exe` exists
   - `dist\baluffo-portable-<version>.zip` exists
   - the embedded `ship\` bundle exists
3. Run packaged desktop smoke validation:

```powershell
npm run test:frontend:packaged
```

Optional rebuild-backed smoke validation:

```powershell
python scripts/packaged_desktop_smoke.py --rebuild
```

4. Confirm desktop startup, bridge readiness, and admin page readiness all pass in the smoke output.
5. If sync credentials are packaged, confirm the packaged runtime still resolves the expected sync config and smoke remains green.

### Post-Release / Incident Checks

After release:

1. Confirm healthy steady state for the shipped channel.
2. Keep the previous ship-bundle version available for rollback.
3. For a failed ship-bundle rollout, recover with `.\run-all.ps1 -RecoverPrevious` and capture diagnostics with `.\run-all.ps1 -CreateSupportBundle`.
