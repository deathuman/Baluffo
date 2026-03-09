# Versioning Policy and Release Checklist

This policy defines how Baluffo versions and updates must be managed for the zip-first ship deployment model.

## 1) Version model

Every release must define these versions explicitly:

- `app_version`: end-user application release version (bundled under `app\versions\<app_version>`)
- `updater_version`: capability version of `scripts/ship/update_manager.py`
- `data_schema_version`: version of persisted data expectations/migrations
- `manifest_schema_version`: version of update-manifest contract

Rules:

1. All four versions must be captured in release notes/checklist.
2. `app_version` must follow SemVer (`MAJOR.MINOR.PATCH`).
3. `updater_version` changes whenever updater behavior/validation changes.
4. `manifest_schema_version` changes whenever manifest format/validation contract changes.

## 2) Compatibility policy (Strict SemVer major gates)

### Patch/minor upgrades

- Allowed by default.
- Must pass standard update gates:
  - artifact checksum + signature verification
  - startup health check on target version
  - migration `apply` + `verify` (if declared)

### Major upgrades

- Not a routine path.
- Require explicit compatibility sign-off in release checklist:
  - documented migration plan
  - rollback criteria documented and tested
  - staging-machine update rehearsal completed

### Downgrades

- Blocked by default.
- Allowed only when all conditions are true:
  - manifest sets `rollback_allowed=true`
  - target version exists and passes health checks
  - data rollback path is documented and validated for that release

## 3) Update state machine contract

Allowed update statuses in `app\update-state.json`:

- `ready`: normal steady state, safe to operate/update
- `updating`: update in progress, do not run concurrent update
- `failed`: update failed; current pointer remains previous stable version
- `auto_rolled_back`: startup failed on active version and automatic fallback to previous succeeded
- `recovered`: operator-triggered manual switch to previous version

State transitions:

- `ready -> updating -> ready` (success path)
- `ready -> updating -> failed` (update failure)
- `ready -> auto_rolled_back` (startup safety rollback)
- `ready -> recovered` (manual recovery action)

Operator action policy:

1. Never force edits to `current.txt` or `update-state.json`.
2. Use launcher/update commands only (`apply-update.ps1`, `run-all.ps1 -RecoverPrevious`).
3. Investigate non-`ready` states before the next rollout.

## 4) Manifest and updater policy

Required manifest fields:

- `version`
- `artifact_url`
- `sha256`
- `signature`
- `min_updater_version`
- `migration_plan`
- `rollback_allowed`

Contract source:

- `docs/update-manifest.schema.json`

Validation policy:

1. Manifest must validate against schema before release.
2. `min_updater_version` comparison must be numeric SemVer comparison, not lexical string comparison.
3. Signature must be verified before staging or install.

## 5) Migration policy

Every migration must provide:

- `apply(data_path)`
- `verify(data_path)`
- `rollback(data_path, backup_ref)`

Release gates:

1. If a release declares migrations, each migration must have explicit verify criteria.
2. Release is blocked if migration verification behavior is undefined.
3. Data backup must be created before migration execution.
4. On migration/health failure, data restore + version rollback must complete automatically.

## 6) Retention policy

Default retention:

- Keep last `N=3` app versions under `app\versions`.
- Keep last `N=3` data backups under `data\backups`.
- Keep latest migration reports for troubleshooting.

Operational rule:

- Prune only after a release has reached stable `ready` state and post-release checks are complete.

## 7) Signing policy (Stable channel, HMAC now)

Release channel policy:

- Stable channel only (no beta/internal channel in current process).

Current signing method:

- HMAC-SHA256 over `version:sha256` using `BALUFFO_UPDATE_SIGNING_KEY`.

Current signing command:

```powershell
$env:BALUFFO_UPDATE_SIGNING_KEY="replace-with-release-key"
py -3 scripts/ship/update_manager.py sign-manifest --version 1.2.4 --sha256 <artifact_sha256>
```

Roadmap to asymmetric signing:

1. Add public-key verification support in updater (keep HMAC for transition period).
2. Dual-sign manifests (HMAC + asymmetric) for compatibility window.
3. Require asymmetric verification for stable releases.
4. Decommission HMAC verification after migration window.

## 8) Stable release checklist

### Pre-release gates

1. Build bundle with target `app_version`:
   - `py -3 scripts/build_ship_bundle.py --bundle-version <app_version>`
2. Run required validations/tests:
   - `npm run test:py` (minimum gate)
   - `npm run test:frontend:unit`
3. Verify packaged output layout before publish:
   - launcher scripts exist in bundle root
   - `app\versions\<app_version>\packaging\github-app-sync-config.template.json` exists
   - `data\` contains seeded defaults, not repo-local runtime artifacts
4. Prepare update artifact zip and manifest.
5. Compute `sha256` and signature.
6. Validate manifest fields and schema.
7. Rehearse update on staging machine:
   - apply update
   - confirm startup passes
   - verify user data integrity
   - verify recovery flow

### Publish

1. Publish update artifact and manifest.
2. Apply update in production/user environment using:
   - `.\apply-update.ps1 -BundleZip <zip> -Manifest <manifest>`

### Post-release checks

1. Confirm `app\update-state.json` reports `ready`.
2. Confirm `app\current.txt` points to target version.
3. Confirm no data loss in persisted user/runtime data.
4. Confirm support bundle can be generated:
   - `.\run-all.ps1 -CreateSupportBundle`

### Incident rollback path

1. Trigger recovery:
   - `.\run-all.ps1 -RecoverPrevious`
2. Capture diagnostics:
   - `.\run-all.ps1 -CreateSupportBundle`
3. Record root cause and corrective action before next release.

## 9) Operational metrics

Track per stable release:

- Update success rate
- Auto-rollback rate
- Data-restore incident count
- Mean time to recovery (MTTR) from failed update to healthy `ready` state

## 10) Policy compliance checks (mapping to current implementation)

Current implementation references:

- Bundle creation/version layout: `scripts/build_ship_bundle.py`
- Update application/signing/recovery/startup checks: `scripts/ship/update_manager.py`
- Launcher/recovery entry points:
  - `scripts/ship/run-all.ps1`
  - `scripts/ship/run-site.ps1`
  - `scripts/ship/run-bridge.ps1`
  - `scripts/ship/apply-update.ps1`
  - `scripts/ship/recover-previous.ps1`
  - `scripts/ship/create-support-bundle.ps1`

Compliance status:

1. Atomic update flow with staged extraction and pointer switch: implemented.
2. Data outside versioned app path guard: implemented.
3. Manifest required-field validation: implemented.
4. Checksum + signature verification: implemented (HMAC).
5. Backup/rollback/recovery flows: implemented.
6. Numeric SemVer check for `min_updater_version`: implemented.
7. Retention pruning automation (`N=3` policy): not yet implemented (policy requirement for follow-up automation).
