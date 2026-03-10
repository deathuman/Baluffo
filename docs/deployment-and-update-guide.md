# Deployment and Update Guide (Windows Zip-First)

This guide defines how Baluffo should be deployed and updated for end users.

Versioning and release policy reference:

- `docs\versioning-policy-and-release-checklist.md`

## 1) Deployment model

Baluffo ship bundles use:

- `app\versions\<version>` for immutable app binaries/files
- `app\current.txt` as the active-version pointer
- `app\update-state.json` for updater state
- `app\staging` for temporary update extraction
- `data\` for persistent runtime/user data

Important rule:

- `data\` must stay outside `app\versions\...`.

## 2) Build a deployable bundle

From repo root:

```powershell
py -3 scripts/build_ship_bundle.py --bundle-version 1.2.3
```

Output (default):

- `dist\baluffo-ship`

Zip and distribute the full `dist\baluffo-ship` directory to users.

The bundle seeds a clean `data\` runtime. Do not package repo-local `data\*.json`, `data\*.csv`, or log files as release state.

## 3) First-time deployment on end-user machine

1. Unzip bundle to a stable path (example: `C:\baluffo\baluffo-ship`).
2. Start app:

```powershell
.\run-all.ps1 -SitePort 8080 -BridgeHost 127.0.0.1 -BridgePort 8877
```

3. Optional persistent data location:

```powershell
.\run-all.ps1 -DataDir C:\baluffo\data
```

## 4) How updates must be prepared

Each update requires:

- Update artifact zip containing `app\versions\<targetVersion>`
- Manifest JSON with fields:
  - `version`
  - `artifact_url`
  - `sha256`
  - `signature`
  - `min_updater_version`
  - `migration_plan`
  - `rollback_allowed`

Schema:

- `docs\update-manifest.schema.json`

Signature format:

- HMAC-SHA256 of `version:sha256` using release key (`BALUFFO_UPDATE_SIGNING_KEY`)

Example signing command:

```powershell
$env:BALUFFO_UPDATE_SIGNING_KEY="replace-with-release-key"
py -3 scripts/ship/update_manager.py sign-manifest --version 1.2.4 --sha256 <artifact_sha256>
```

## 5) How updates must be applied

On user machine (bundle root):

```powershell
$env:BALUFFO_UPDATE_SIGNING_KEY="replace-with-release-key"
.\apply-update.ps1 -BundleZip .\baluffo-update.zip -Manifest .\update-manifest.json
```

What happens automatically:

1. Validates manifest structure
2. Validates artifact checksum
3. Verifies signature
4. Extracts into `app\staging\<version>`
5. Creates data backup in `data\backups`
6. Runs migrations (`apply` + `verify`)
7. Runs startup health checks on target version
8. Atomically switches `app\current.txt`

Failure behavior:

- Update is marked failed in `app\update-state.json`
- Data is restored from backup
- Previous version remains active

## 6) Recovery and diagnostics

Recover previous version:

```powershell
.\run-all.ps1 -RecoverPrevious
```

Create support bundle:

```powershell
.\run-all.ps1 -CreateSupportBundle
```

Support bundle includes update state, update events log, and latest migration report.

## 7) Operational checklist

Before releasing:

1. Build bundle with target version.
2. Verify tests pass (`npm run test:py` at minimum).
3. Produce update zip + manifest + signature.
4. Validate update in a staging machine.

After releasing:

1. Confirm updater status is `ready` in `app\update-state.json`.
2. Confirm user data remained intact.
3. Keep previous version available for rollback.

## 8) Sync credential incident response (desktop packaged mode)

Baluffo desktop sync packaging is deterrence-oriented. If package extraction is suspected:

1. Immediately disable sync on affected machines:

```powershell
$env:BALUFFO_SYNC_DISABLE="1"
```

2. Revoke/rotate GitHub App private key.
3. Verify dedicated sync repo integrity (`baluffo/source-sync.json` path only).
4. Build and ship a new package with refreshed sync credentials.
5. Re-enable sync only after rollout completes.

Risk note:

- Out-of-box embedded sync credentials are not strong protection against determined reverse engineering on client-owned devices.
