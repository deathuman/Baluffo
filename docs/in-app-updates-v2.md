# In-App Updates For `Baluffo.exe` — V2

This document now serves two purposes:

- it preserves the validated V2 target design for in-app updates in Baluffo's Windows portable desktop distribution
- it records the current implementation status in the repo as of `2026-04-14`

It is intentionally aligned with the repo's current release and runtime model documented in `docs/RELEASE.md`:

- the ship bundle remains the canonical runtime and update model
- the portable desktop build remains a packaged distribution built on top of that ship bundle
- persistent runtime and user data remain under `ship\data`, with desktop profile data under `ship\data\local-user-data`

V2 adds a separate desktop update flow for end users without redefining the existing ship-bundle updater contract in place.

## Current Status

### Implemented in the repo

- A desktop update service exists under `src/ship/desktop_update.py`
- A packaged updater helper entrypoint exists under `src/ship/desktop_updater.py`
- New bridge routes exist for:
  - `GET /app/update-status`
  - `POST /app/check-for-update`
  - `POST /app/download-update`
  - `POST /app/install-update`
- `/ops/health` now exposes:
  - top-level `appVersion`
  - top-level `startupReady`
  - top-level `updater`
- Desktop updater state is persisted under `ship\data\updater\`
- Portable packaging now builds and includes `BaluffoUpdater.exe`
- A dedicated desktop manifest schema exists at `docs/desktop-update-manifest.schema.json`
- Targeted backend, bridge, packaging, and desktop-runtime tests were added and are passing

### Partially implemented

- Manifest verification now supports a packaged embedded public-key fallback and release-time signing flow, but key rotation and long-term operator workflow hardening still rely on disciplined release configuration rather than a broader key-management system
- The helper performs install, rollback snapshotting, relaunch, and startup verification, but resumability and stage-marked recovery are not yet complete to the level described below
- The desktop launcher writes the post-install success marker when the bridge reaches startup readiness, but the install-success handshake has not yet been rehearsed through a true packaged `N -> N+1` helper-driven upgrade on disk

### Not yet complete

- Optional routes such as cancel or dismiss are not implemented
- The helper does not yet show the final minimal staged progress UI described in the target flow
- A real packaged upgrade rehearsal that installs one built version over another through the shipped helper path is still outstanding

## Summary

`Baluffo.exe` is now functionally wired for a prompted, helper-driven desktop updater built for the existing portable desktop layout.

The repo currently has the full user-visible app path and release-manifest path for this desktop update flow:

- the app checks GitHub Releases for a desktop update manifest
- the jobs-page desktop UI can check, download, and install the target version
- the app downloads the full portable ZIP for the target version
- the installed app launches a temp-copied `BaluffoUpdater.exe`
- the helper replaces runtime files while preserving `ship\data`
- the helper restarts the target version and confirms startup success before deleting rollback state
- release automation now generates and signs `baluffo-desktop-update-manifest.json` and publishes it with the release assets

What is still missing for the summary above to be fully closed against the full target design:

- helper resume/stage recovery hardening
- final helper progress UI
- packaged true-upgrade rehearsal coverage

## Product Decisions

### Scope

- Platform: Windows portable desktop only
- Update source: GitHub Releases
- Release channel: `stable` only
- UX: prompted only
- Desktop install artifact: full portable ZIP
- Ship ZIP: retained for operator/manual recovery
- Auto behavior: auto-check allowed, auto-install not allowed

### Core trust and persistence rules

- Desktop manifest verification uses an asymmetric public-key signature
- The existing ship manifest contract is not replaced in place
- `ship\data` is never overwritten from the downloaded artifact
- `ship\data` may still be explicitly mutated later by declared migrations under apply/verify/rollback rules, consistent with the release guide's migration model

## Final User Flow

### 1. Startup / manual check

Baluffo checks for updates:

- on startup using a reasonable throttle window
- when the user clicks "Check for updates"

### 2. Update available

If a newer stable version exists, the app shows:

- current version
- latest version
- update size
- optional release notes link

Primary actions:

- Download
- Later

### 3. Download phase

When the user clicks Download:

- the app downloads the portable ZIP
- updater state is persisted under `ship\data\updater\`
- the user can continue using the app while the download runs

UI states:

- checking
- update available
- downloading
- download failed
- ready to install

### 4. Install confirmation

When download finishes, the CTA becomes:

- Install and restart

The app must clearly state:

- Baluffo will close
- the update will be installed
- the app will reopen automatically
- saved data and profiles will be preserved

### 5. Handoff

When the user confirms install:

- the app writes an install plan
- the app copies `BaluffoUpdater.exe` to a temp location outside the install root
- the app launches that temp copy with the install plan
- the app exits cleanly

### 6. Helper install

The helper shows a minimal progress UI:

- Preparing update
- Closing Baluffo
- Installing update
- Restarting Baluffo

### 7. Success

The helper launches the new version, verifies startup success, deletes rollback state, and exits.

### 8. Failure

If install or first launch fails:

- the helper restores the previous runtime snapshot
- preserves `ship\data`
- relaunches the previous version if possible
- leaves diagnostics in updater logs/state
- shows a simple failure message

## Architecture

### 1. Existing contracts stay intact

The current ship updater and ship manifest schema remain supported and unchanged for ship and manual recovery purposes. V2 does not overload or silently redefine the existing ship manifest contract described in `docs/RELEASE.md`.

### 2. New desktop manifest contract

Status: implemented in schema form and wired into release workflow.

The repo now includes a separate desktop manifest contract and schema:

- `docs/desktop-update-manifest.schema.json`
- release asset name: `baluffo-desktop-update-manifest.json`

This is a new contract, separate from the current ship manifest defined in `docs/update-manifest.schema.json`.

### 3. App-side update service

Status: implemented.

The repo now has a desktop update service under `src/ship/` responsible for:

- fetching the desktop manifest
- verifying its signature
- comparing remote version vs local version
- caching updater state under `ship\data\updater\`
- downloading the portable ZIP
- preparing helper handoff

This service does not mutate installed runtime files directly.

### 4. Packaged updater helper

Status: implemented in packaging and helper entrypoint.

The portable build now produces a packaged `BaluffoUpdater.exe`.

Important rule:

The installed app must launch a temp-copied helper from outside the install root so the helper can replace the installed `BaluffoUpdater.exe` during update.

## Desktop Manifest V2

### Release assets

Each stable GitHub release should publish:

- `baluffo-desktop-update-manifest.json`
- `baluffo-portable-<version>.zip`
- `baluffo-ship-update-<version>.zip`
- optional release notes asset or URL

### Asset roles

- desktop manifest: source of truth for the in-app desktop updater
- portable ZIP: the only runtime artifact used by the desktop updater
- ship update ZIP: manual and operator recovery artifact

GitHub release resolution rules:

- the app resolves the latest stable release only
- drafts and prereleases are ignored
- the desktop manifest must come from that selected stable release
- the portable ZIP and optional ship recovery ZIP must be resolved from the same release as the manifest

### Required desktop manifest fields

The desktop manifest should include:

- `schema_version`
- `key_id`
- `channel`
- `version`
- `published_at`
- `release_notes_url`
- `min_desktop_updater_version`
- `min_supported_current_version`
- `data_schema_version`
- `rollback_allowed`
- `portable_artifact`
- optional `ship_recovery_artifact`
- `migration_plan`
- `signature`

### Example shape

```json
{
  "schema_version": 2,
  "key_id": "desktop-ed25519-2026-01",
  "channel": "stable",
  "version": "1.4.0",
  "published_at": "2026-04-14T12:00:00Z",
  "release_notes_url": "https://github.com/.../releases/tag/v1.4.0",
  "min_desktop_updater_version": "2.0.0",
  "min_supported_current_version": "1.2.0",
  "data_schema_version": "2",
  "rollback_allowed": true,
  "portable_artifact": {
    "url": "https://github.com/.../baluffo-portable-1.4.0.zip",
    "sha256": "<64-hex>",
    "size_bytes": 123456789
  },
  "ship_recovery_artifact": {
    "url": "https://github.com/.../baluffo-ship-update-1.4.0.zip",
    "sha256": "<64-hex>",
    "size_bytes": 45678901
  },
  "migration_plan": [],
  "signature": "<base64-signature>"
}
```

### Manifest trust model

Desktop manifest verification must use an asymmetric signature model:

- canonicalize the manifest payload without `signature`
- sign at release time with a private key
- verify in app and helper with an embedded public key
- select the embedded public key using manifest `key_id`

Recommended practical choice:

- Ed25519 signatures

Signature format is fixed for implementation:

- algorithm: `Ed25519`
- encoding: `base64`
- canonicalization rules:
- remove the `signature` field
- serialize the remaining manifest as UTF-8 JSON
- sort object keys recursively
- use no insignificant whitespace
- preserve array order exactly

GitHub and TLS remain transport security only, not the trust root.

Implementation note:

- canonicalization and Ed25519 verification code now exist in the repo
- packaged builds now embed desktop update public keys for app/helper verification fallback
- the GitHub release workflow now generates and signs the desktop manifest during release preparation

### Version gating

If `min_desktop_updater_version` is higher than the installed helper version:

- the app must block in-app install
- the UI must direct the user to manually download the latest portable release

If the installed app version is lower than `min_supported_current_version`:

- the app must block in-app install
- the UI must direct the user to manually download the latest portable release
- the app must not attempt helper handoff

If the manifest version is lower than the installed app version and `rollback_allowed` is `false`:

- the updater must refuse install
- the app must not offer in-app install for that manifest

## Persistence And Mutation Rules

### Hard persistence boundary

`ship\data` is the persistence boundary for portable desktop installs.

The desktop updater must never overwrite `ship\data` from the downloaded portable ZIP.

That includes:

- `ship\data\local-user-data`
- user profiles
- saved jobs
- activity
- attachment metadata
- attachment files
- runtime and admin state
- updater cache and state
- backups
- migration reports

This matches the documented portable layout in `docs/RELEASE.md`.

### What may still change under controlled migration

Preserving `ship\data` does not mean the directory is frozen forever.

If a release declares explicit app or data migrations, they may mutate the preserved data after install, but only under the same release-guide safeguards already used by the repo:

- backup first
- apply
- verify
- rollback on failure

#### Desktop migration ownership

Desktop V2 assigns migration execution to the updater helper.

- the helper owns execution of manifest `migration_plan`
- if `migration_plan` is non-empty, the helper must create a data backup before migration work begins
- after runtime replacement and before final launch success is accepted, the helper must invoke the target version's migration entrypoint against the preserved `ship\data`
- migrations must run with explicit `apply`, `verify`, and `rollback` semantics
- if migration apply or verify fails, the helper must run rollback and restore from backup as needed, then restore the previous runtime snapshot
- the newly launched target app does not own `migration_plan` execution; its role is startup, health reporting, and writing the post-install success marker after startup is ready

### Updater working area

Persist updater state under:

- `ship\data\updater\manifest-cache.json`
- `ship\data\updater\downloads\`
- `ship\data\updater\install-plan.json`
- `ship\data\updater\install-state.json`
- `ship\data\updater\rollback\`
- `ship\data\updater\post-install-success.json`

`post-install-success.json` is the durable helper and app handshake marker confirming a successful first launch of the target version.

Rollback snapshots should be stored under:

- `ship\data\updater\rollback\<target-version>-<timestamp>\`

## Install Plan Contract

The app and helper must share a fixed `install-plan.json` schema so the handoff contract does not drift.

At minimum, the plan must include:

- `planVersion`
- `installRoot`
- `tempHelperPath`
- `targetVersion`
- `currentVersion`
- `manifestPath`
- `downloadedZipPath`
- `expectedZipSha256`
- `manifestKeyId`
- `rollbackPath`
- `updaterWorkingDir`
- `createdAt`
- `launcherPid`
- `launcherToken`
- `desktopSessionRoot`

## Install Flow

### Phase A - Check

Status: implemented in backend route, service, and desktop UI.

1. Resolve the latest stable desktop release from GitHub Releases.
2. Ignore drafts and prereleases.
3. Fetch the desktop manifest from that selected stable release.
4. Verify manifest signature with the embedded public key.
5. Compare remote version against the local app version.
6. Cache the result under `ship\data\updater\`.

### Phase B - Download

Status: implemented in backend route, background download state, and desktop UI.

1. Download the portable ZIP into `ship\data\updater\downloads\`.
2. Verify ZIP SHA256 using the verified manifest payload.
3. Mark updater state as ready to install.

### Phase C - Handoff

Status: implemented in backend/helper contract.

1. App writes `install-plan.json`.
2. App copies `BaluffoUpdater.exe` to a temp path outside the install root.
3. App launches the temp helper with the install plan.
4. App exits cleanly.

Before helper launch, the app should run preflight checks for:

- enough disk space for extracted ZIP plus rollback snapshot
- write access to the install root
- write access to `ship\data\updater`
- a writable temp directory for helper and extraction work

If any preflight check fails:

- block install
- keep the current runtime untouched
- show a clear user-facing error

### Phase D - Helper gate

Status: partially implemented.

The helper must not require a generic "scan every child process" rule.

Instead, it should use the existing launcher and session model as the install gate:

- wait for the main launcher process to exit
- wait for the instance lock and session state to clear or become reclaimable
- use normal file-lock retries when replacing files

This is intentionally aligned with the existing desktop runtime behavior.

The target design still requires the helper to be idempotent and resumable:

- it must persist stage-based progress in `ship\data\updater\install-state.json`
- it must resume safely after interruption
- interrupted installs must recover on the next helper run
- swap and rollback phases must be stage-marked explicitly

### Phase E - Replacement

Status: implemented in helper form, with some recovery/resume gaps still remaining.

The helper:

1. Re-validates the install plan.
2. Re-verifies manifest signature and ZIP hash.
3. Extracts the portable ZIP to a temp directory.
4. Creates a rollback snapshot of the currently installed runtime, excluding `ship\data`.
5. Creates a data backup if `migration_plan` is non-empty.
6. Syncs the extracted target build into the install root, removing stale files that are absent from the target build, except:
   - `ship\data\`
   - the currently running temp helper executable
   - updater working files under the temp helper execution path
7. Runs `migration_plan` via the target version migration entrypoint with apply and verify semantics, and rollback on failure.
8. Keeps the existing `ship\data` directory intact except for explicit declared migrations executed under the helper-owned migration flow.
9. Starts the new `Baluffo.exe`.

### Phase F - First-launch success check

Status: mostly implemented in backend/helper/runtime contract.

A desktop install is successful only if all of the following are true:

- target process started
- session state was written by the target launch
- `/ops/health` is reachable
- `/ops/health.service == "baluffo-bridge"`
- `/ops/health.desktopMode == true`
- `/ops/health.appVersion == targetVersion`
- `/ops/health.startupReady == true`
- `ship\data\updater\post-install-success.json` is written by the target version

Generic `/ops/health.status` must not be used as the install success gate, because it reflects broader operational health that may be degraded for reasons unrelated to desktop startup readiness.

Ownership of the success marker is explicit:

- before launching the target version, the helper must remove any stale `post-install-success.json`
- the newly launched target version must write a fresh `ship\data\updater\post-install-success.json` only after:
  - session state is written
  - `/ops/health.service == "baluffo-bridge"`
  - `/ops/health.desktopMode == true`
  - `/ops/health.appVersion == targetVersion`
  - `/ops/health.startupReady == true`
- the helper waits for that file in addition to the runtime checks above
- the helper deletes or archives the success marker during finalization

Only then may the helper:

- delete rollback state
- clear install state
- exit successfully

### Phase G - Recovery

Status: partially implemented.

If any install or first-launch step fails, the helper must:

- restore the rollback snapshot
- preserve `ship\data`
- relaunch the previous version when possible
- leave logs and state for diagnostics
- surface a plain recovery message to the user

## Bridge And Ops Interfaces

### New bridge routes

Status: implemented.

Add:

- `GET /app/update-status`
- `POST /app/check-for-update`
- `POST /app/download-update`
- `POST /app/install-update`

Optional later:

- `POST /app/cancel-update-download`
- `POST /app/dismiss-update`

### `GET /app/update-status` payload

Status: route implemented.

Return:

- current version
- latest version
- channel
- update availability
- download state
- progress bytes and percent
- install state
- release notes URL
- last checked timestamp
- last error

### `/ops/health` additions

Status: implemented.

Add:

- top-level `appVersion`
- top-level `startupReady`
- top-level `updater` object containing:
- `currentVersion`
- `latestVersion`
- `availability`
- `downloadState`
- `installState`
- `lastCheckedAt`
- `lastError`

This extends the existing ops health surface rather than inventing a separate diagnostic model.

Install success uses a dedicated startup-readiness contract:

- `service == "baluffo-bridge"`
- `desktopMode == true`
- `appVersion == targetVersion`
- `startupReady == true`

The generic ops `status` field remains an operational signal and is not the desktop install-success gate.

## Release Automation

Status: implemented for manifest generation/signing/publish, with follow-up validation still desirable.

Update release automation so publish-time scripts:

1. build the portable app
2. build the updater helper
3. create the portable ZIP
4. create the ship recovery ZIP
5. compute hashes for release assets
6. generate the desktop manifest with final asset URLs
7. sign the desktop manifest with the private key
8. upload desktop manifest, portable ZIP, ship recovery ZIP, and release notes to GitHub Releases

The existing ship-bundle updater remains supported and is not removed.

Current repo state:

- portable packaging now builds the updater helper alongside `Baluffo.exe`
- ship packaging now includes the desktop update schema and helper sources
- ship builds can embed desktop update public keys for app/helper verification
- publish-time desktop manifest generation and signing now run from `scripts/build_desktop_update_release.py`
- the GitHub release workflow uploads desktop manifest, portable ZIP, and ship ZIP together

## Test Plan

### Unit tests

Status: substantially implemented, with a few deeper recovery/rehearsal gaps still open.

Add tests for:

- desktop manifest canonicalization
- asymmetric signature verification
- semver and version-floor gating
- install-plan validation
- updater state transitions
- rollback snapshot excluding `ship\data`
- hash and signature mismatch handling
- helper resume behavior after interruption

### Integration tests

Status: partially implemented.

Add tests for:

- `GET /app/update-status`
- `POST /app/check-for-update`
- `POST /app/download-update`
- `POST /app/install-update`
- persisted updater state across restart
- helper handoff and resume
- failed first launch causing rollback
- `/ops/health.updater` and `appVersion` payloads

### Packaged desktop E2E

Status: partially implemented.

1. Install version `N`.
2. Create real profiles, jobs, and attachments under `ship\data\local-user-data`.
3. Download `N+1`.
4. Install through the real helper flow.
5. Verify:
   - target app launched
   - app version updated
   - `/ops/health` is healthy in desktop mode
   - user data is preserved
6. Simulate first-launch failure.
7. Verify rollback of runtime with preserved data intact.

### Release workflow validation

Status: partially implemented.

Assert that the GitHub release publishes:

- desktop manifest
- portable ZIP
- ship recovery ZIP
- release notes

Then verify:

- manifest URLs match uploaded assets
- manifest hashes match uploaded assets
- manifest signature verifies with the embedded public key

## Assumptions

- `stable` is the only in-app channel in V2
- the existing ship-bundle updater remains supported for ship and manual recovery
- the updater helper runs from a temp copy during install
- desktop manifest verification uses asymmetric signatures, not HMAC
- preserving `ship\data` means never overwriting it from the downloaded artifact, while still allowing explicit declared migrations under the existing release-guide safeguards

## Acceptance Criteria

Current assessment:

- backend and helper implementation are in place
- bridge, ops, and user-facing jobs-page update UI are in place
- packaging and release-manifest generation/signing are in place
- the feature is close to the target acceptance bar, with remaining work concentrated in helper resume/progress polish and a true packaged upgrade rehearsal

V2 is complete when:

- desktop users can check, download, and install updates without manual ZIP extraction
- install always requires explicit user consent
- runtime mutation is helper-owned
- `ship\data` is never overwritten from downloaded artifacts
- declared migrations still work with backup, apply, verify, and rollback semantics
- failed installs restore the previous runtime automatically
- updater state is visible in user UI and ops health
- GitHub Releases publish the correct desktop manifest and assets
- packaged end-to-end update rehearsals pass
