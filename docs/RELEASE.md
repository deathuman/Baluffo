# Release Guide

This is the authoritative release document for Baluffo.

## Distribution Channels

Baluffo ships through three distribution channels:

- Ship bundle: the canonical zip-first release channel built around a versioned `app\versions\<version>` layout, PowerShell launchers, and the updater/recovery flow.
- Portable EXE: a Windows desktop wrapper built with `PyInstaller` and the desktop launcher runtime that embeds the ship bundle under `ship\` and uses the ship bundle as its runtime payload.
- Container / Umbrel: a Linux container published to `ghcr.io/deathuman/baluffo` for private Umbrel community app-store installs and other same-origin HTTP service deployments.

Important rules:

- The ship bundle is the canonical update channel.
- The portable EXE is a packaged distribution built on top of the ship bundle, not a separate updater model.
- The container channel is service-style deployment. It does not use the desktop updater, desktop session lifecycle, host-browser open behavior, or packaged desktop data migration.
- Persistent runtime data must remain outside versioned app folders. Windows packaged desktop defaults to `%APPDATA%\Baluffo\`; `ship\data\` is only the legacy packaged migration source.
  Container runtime data defaults to `/data`.

## Cross-Channel Release Checkpoint

Before publishing a desktop release after a run of container/Umbrel-only patches, compare the latest public desktop tag with current `main`. If shared fixes accumulated since that tag, write a desktop-facing rollup changelog entry and use a new release identity rather than tagging a narrow container patch whose notes understate the desktop-visible change set.

For example, when the latest public desktop release is behind `main`, the release owner should explicitly decide whether the next release is:

- a desktop rollup release with full packaged preflight, tag, signed manifest, and GitHub release assets;
- a container-only patch with no desktop tag; or
- a deferred desktop release, with the gap recorded in Basic Memory or the release handoff.

Do not move or recreate an existing desktop tag to fix release-note scope. Bump forward and keep the old tag as historical evidence.

## Versioning Policy

This policy applies to both distribution channels.

Every release must track these versions explicitly:

- `app_version`: end-user release version, using Baluffo's public release ordering
- `updater_version`: capability version of `src/ship/update_manager.py`
- `data_schema_version`: version of persisted data expectations and migrations
- `manifest_schema_version`: version of the update manifest contract
- The default `app_version` used by local build/package workflows is defined in `src/app_version.py`.
- Git tags should use the `v<app_version>` form.
- The public release history should follow the same `app_version` line; updater/schema versions are documented separately and should not introduce a second public version family.
- GitHub release notes must be generated from the matching versioned section of `docs/CHANGELOG.md`; `Unreleased` may sit above tagged releases, but the versioned section remains the single release-note source of truth.

Baluffo release ordering (train-agnostic, implemented in `src/baluffo_version.py`):

- The first two segments compare numerically, so a higher minor is newer regardless of the last segment.
- The final dotted segment is Baluffo-specific:
  - first digit = release-major within that train
  - remaining digits = sub-increment within that release-major
- Examples (patch rank shown alongside the full comparison key):
  - `0.2.3` => patch rank `(3, 0)`, key `(0, 2, 3, 0, 1)`
  - `0.2.25` => patch rank `(2, 5)`, key `(0, 2, 2, 5, 2)`
  - `0.2.83` => patch rank `(8, 3)`, key `(0, 2, 8, 3, 2)`
  - `0.2.152` => patch rank `(1, 52)`, key `(0, 2, 1, 52, 3)`
  - `0.3.0` => patch rank `(0, 0)`, key `(0, 3, 0, 0, 1)`
- Two consequences worth internalising before picking a version:
  - `0.2.3` is newer than `0.2.25` through `0.2.29`, because a single-digit release-major outranks a two-digit one.
  - `0.3.0` is newer than **every** `0.2.x`, because the minor segment decides first. A version is not "bigger" because its last segment has more digits: `0.2.200` parses to key `(0, 2, 2, 0, 3)` and is *older* than the already-published `0.2.25`.
- `v0.1.31` is the historical compatibility bridge release that both the older semver clients and the newer Baluffo-order clients must accept; the desktop updater, downgrade checks, recovery selection, and release tooling must all use the same ordering.

Compatibility rules:

- Patch and minor upgrades are allowed by default if checksum/signature validation, startup health checks, and declared migrations pass.
- Major upgrades require explicit migration sign-off, a documented migration plan, rollback criteria, and a successful staging rehearsal.
- Downgrades are blocked by default and only allowed when `rollback_allowed=true`, the target version passes health checks, and the data rollback path has been validated.

Updater and manifest rules:

- The ship bundle update contract remains `docs/update-manifest.schema.json`.
- The portable desktop in-app update contract is `docs/desktop-update-manifest.schema.json`.
- `src/ship/update_manager.py` remains the canonical updater API compatibility facade; operator CLI entrypoints use `python -m src.ship.update_manager_cli`, and implementation belongs in the `src/ship/update_manager_*.py` leaves.
- `src/ship/desktop_updater.py` remains the stable helper executable and test patch surface; helper implementation belongs in `src/ship/desktop_updater_{ui,release,install}.py`.
- Desktop update manifests are generated by `scripts/build_desktop_update_release.py` and must include the portable ZIP, optional ship recovery ZIP, release notes URL, version floors, and rollback policy.
- Desktop update manifests are signed with Ed25519 using `BALUFFO_DESKTOP_UPDATE_PRIVATE_KEY_B64` and `BALUFFO_DESKTOP_UPDATE_KEY_ID`, and are verified at runtime against the embedded public-key set.
- The canonical signed-release path is `.github/workflows/build-portable-exe.yml`: tagged or workflow-dispatch release runs read the desktop update signing values from GitHub repository secrets and publish the signed manifest plus release assets. Local shells are not expected to have that private key unless explicitly configured for emergency/manual release work.
- Release secrets must stay in GitHub encrypted secrets or local environment variables. Do not commit `packaging\github-app-sync-config.json`, `packaging\github-app-sync-config.localkey.json`, `baluffo.config.local.json`, private key PEMs, desktop update signing keys, or copied release secret values.
- Desktop release selection must ignore GitHub drafts and prereleases and compare Baluffo release versions with the shared Baluffo-specific ordering, not lexically.
- The packaged desktop updater must download only the portable ZIP, preserve the resolved external data root, hand off runtime mutation to a temp-copied `BaluffoUpdater.exe`, and require both `/ops/health.startupReady == true` and `%APPDATA%\Baluffo\updater\post-install-success.json` before finalizing success on Windows packaged installs. During migration from older helpers that do not understand `dataDir`, the target app may also write a transition-only `ship\data\updater\post-install-success.json` marker for legacy source-helper verification.

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
python scripts/build_ship_bundle.py --bundle-version <version>
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
python -m src.ship.update_manager_cli sign-manifest --version 1.2.4 --sha256 <artifact_sha256>
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

Release-note extraction:

```powershell
python scripts/extract_release_notes.py --version <version> --changelog docs/CHANGELOG.md --output release-notes.md
```

### Portable EXE

Prerequisites:

```powershell
python -m pip install -r requirements-lock.txt
```

Preferred build command:

```powershell
npm run build:portable-exe -- --bundle-version <version>
```

Direct Python entrypoint for operator/debug use:

```powershell
python scripts/build_portable_exe.py --bundle-version <version>
```

Default icon:

- Portable EXE builds use the checked-in root `favicon.ico` for both `Baluffo.exe` and `BaluffoUpdater.exe`.

Optional icon override:

```powershell
python scripts/build_portable_exe.py --bundle-version <version> --icon C:\path\to\Baluffo.ico
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
- `%APPDATA%\Baluffo\`: Windows packaged runtime and user data
- `%APPDATA%\Baluffo\local-user-data\`: desktop-specific saved jobs, notes, activity, attachments, and profile data
- `%LOCALAPPDATA%\Baluffo\`: desktop session, browser profile, and transient local state
- `%LOCALAPPDATA%\Baluffo\cache\`: cache directory
- `ship\data\`: legacy packaged data source copied to `%APPDATA%\Baluffo\` on first Windows packaged launch when no completed migration report exists
- `_internal\playwright\driver\package\.local-browsers\`: exactly one `chromium_headless_shell-<revision>` directory matching packaged Playwright `browsers.json`

Runtime notes:

- The executable starts the local static site and admin bridge in the background.
- Desktop runtime waits for `jobs.html` and `/ops/health` readiness before opening the window.
- Child processes shut down with the desktop window.
- Desktop local data uses the bridge-backed file store instead of browser-local IndexedDB/localStorage.
- The shipped `packaging\github-app-sync-config.json` must stay portable in the artifact (`embedded` or an explicitly approved `passphrase` flow), never `machine`.

Portable desktop in-app update flow:

- Baluffo performs one fresh, silent desktop update check on Jobs-page startup and when the user clicks `Check for updates`; non-forced internal checks may still reuse the manifest cache.
- Update state lives under the resolved data root, `%APPDATA%\Baluffo\updater\` for Windows packaged installs.
- The jobs-page desktop UI must surface `Check for updates`, `Download`, `Install and restart`, install-progress, and failure/retry states.
- Background download failures must remain visible in the Jobs-page updater panel, using the persisted updater `lastError` and a retry download action instead of reverting to the generic available-update CTA.
- The updater downloads the portable ZIP from the selected GitHub release and never overwrites the resolved data root from the downloaded artifact.
- Install handoff writes `install-plan.json`, copies `BaluffoUpdater.exe` to a temp path outside the install root, and closes the running app before the helper mutates the runtime.
- Install handoff liveness checks must work without optional `psutil`; the Windows fallback uses process handles, not `os.kill(pid, 0)`.
- Handoff failures before helper launch write `%APPDATA%\Baluffo\updater\handoff-diagnostics.json` on Windows packaged installs with non-secret verifier predicates.
- The helper owns extraction, rollback snapshotting, optional migrations, target relaunch, and rollback-on-failure.
- First-launch success requires desktop session state, `baluffo-bridge` health in desktop mode, `startupReady == true`, the target app version, and a fresh `%APPDATA%\Baluffo\updater\post-install-success.json` on Windows packaged installs. `ship\data\updater\post-install-success.json` is compatibility-only for old source helpers with legacy handoff artifacts.
- The release workflow must publish the portable ZIP, ship recovery ZIP, desktop manifest, and release notes together for desktop in-app updates to work.
- Desktop update status may include cached stable GitHub release-note history as `releaseNotesHistory`; the scalar latest-release `releaseNotes*` fields remain the compatibility contract.
- Releases that require the fixed source-side handoff checker must set `min_desktop_updater_version` to `2.0.1` or newer; do not move or replace an already published release tag to recover affected installs.

### Container / Umbrel

Prerequisites:

```bash
docker buildx version
```

Local build command:

```bash
docker build -t ghcr.io/deathuman/baluffo:local .
```

Prefer the normal live-workspace build above. On Windows workspaces where Docker context transfer fails on `.venv` reparse points or other live-tree artifacts, build committed `HEAD` from a clean `git archive` context instead:

```bash
python scripts/docker_build_clean_context.py --tag ghcr.io/deathuman/baluffo:local
```

The helper is a fallback for release-parity smoke builds. It does not include uncommitted changes; commit first or use the normal `docker build .` path when testing local edits.

Local run command:

```bash
docker run --rm -p 8877:8080 -v baluffo-data:/data ghcr.io/deathuman/baluffo:local
```

Runtime notes:

- The container starts `python -m src.container_entrypoint --host 0.0.0.0 --port 8080 --data-dir /data`.
- The container exposes one same-origin UI/API service on port `8080`.
- `frontend-runtime-config.js` is generated dynamically with `bridge.sameOrigin: true`, `runtime.mode: "container"`, and `runtime.localDataMode: "bridge"`.
- Persistent runtime state belongs under `/data`, including `baluffo-runtime.db`.
- Desktop-only routes, updater behavior, owner-session lifecycle, and host-browser open behavior are disabled in container mode.
- Playwright Chromium is baked into the image for deterministic first run.
- Official GHCR publishes generate `packaging/github-app-sync-config.json` inside the image from GitHub Actions BuildKit secrets, using the same portable encrypted `embedded` sync config model as desktop packages.
- Pull request and local container builds without sync build secrets still build, but `/sync/status` remains misconfigured until a publish build embeds the packaged GitHub App config.
- The `.dockerignore` file must keep local secrets, sync config, local profiles, DBs, logs, `_out`, fetched artifacts, docs, and tests out of the image context.

The GitHub workflow `.github/workflows/build-container.yml` publishes the public multi-arch image `ghcr.io/deathuman/baluffo` for `linux/amd64` and `linux/arm64`.
Docs, tests, and repo-process-only branch/PR changes are path-filtered out of the container workflow, so they should not republish the current app-version or `latest` GHCR tags. Tag pushes and manual `workflow_dispatch` runs still publish normally.

Raw-LAN Umbrel installs are unauthenticated for anyone who can reach the app port. The embedded source-sync config follows the existing desktop packaging deterrence model and should use the same least-privilege GitHub App allowlist.

Umbrel private app-store metadata lives at `umbrel-app-store.yml` and `deathuman-baluffo/`. The Compose file uses `app_proxy` with `APP_PORT: 8080`, `PROXY_AUTH_ADD: "false"`, and `${APP_DATA_DIR}/data:/data`. Do not add a `web` service `ports: "8877:8080"` mapping; Umbrel binds the manifest `port: 8877` through `app_proxy`.

Container / Umbrel ship checklist:

1. Bump `src/app_version.py`, `deathuman-baluffo/umbrel-app.yml`, and `deathuman-baluffo/docker-compose.yml` to the same patch version, and add the matching changelog entry.
2. Run the focused tests for the changed runtime surface, then the broader frontend/refactor/lint gates requested by the release plan.
3. Build locally with `docker build -t ghcr.io/deathuman/baluffo:local .`; on Windows context-transfer failures, commit first and use `python scripts/docker_build_clean_context.py --tag ghcr.io/deathuman/baluffo:local`.
4. Smoke the local image with a fresh `/data` mount: `/ops/health`, UI load, same-origin runtime config, no wildcard CORS, disabled desktop-only routes, profile/job persistence, and source-sync status expectations for the build type.
5. Push `main`, then verify normal main checks and `Build Container` are green when the change touches image-relevant paths. For docs/test/process-only changes, the container workflow may be intentionally skipped.
6. Confirm `ghcr.io/deathuman/baluffo:<version>` exists with `linux/amd64` and `linux/arm64` manifests, and record the multi-arch digest.
7. Smoke the published image far enough to prove `/sync/status.config.ready == true`, `credentialsPackaged == true`, and `missing == []` for official publishes.
8. Update or reinstall the Umbrel app, then verify the live raw-LAN app before declaring shipped.

If live Umbrel smoke exposes a blocker after an image has published, ship a new patch version instead of reusing the failed image identity. Do not move or recreate desktop release tags for container-only recovery.

#### Shipped-code version gate

The `release` repo guardrail (`container_version_policy.py`) fails when container-affecting commits land on `main` after the most recent version bump without either advancing the version or declaring explicit release-tag intent. The `0.2.140` reuse trap it prevents: a version was bumped once and the Umbrel box updated to it, then container-affecting commits kept landing under the same version string — every `main` push republished the same `0.2.140`/`latest` tags with newer code, but Umbrel's app-store update detection keys purely on the `version:` string in `umbrel-app.yml`, so the box never re-pulled and silently ran an older build while the image tag drifted forward.

How to satisfy the gate:

- Bump the version in the same commit batch as the container-affecting code (`python scripts/bump_version.py <next>`), or
- Declare release-tag intent in a commit message with a `Release-tag: vX.Y.Z` line (or a `release(vX.Y.Z):` / `chore(release):` subject) naming a version strictly newer than the current one.

The window the gate evaluates is the commits after the last commit that changed `src/app_version.py` or `deathuman-baluffo/umbrel-app.yml`. Any shipped commit (everything except `docs/`, `tests/`, `tools/`, `.github/`, and root docs/identity files) in that window triggers the failure unless the window carries a bump or an explicit intent declaration. The lint CI checkout fetches full history (`fetch-depth: 0`) so the gate evaluates the real window on every push/PR.

#### Amending a version bump into an existing commit is a catch-22

The gate reads committed history only. When you try to `git commit --amend` a version bump into the tip commit, HEAD at gate time is still the pre-amend commit (shipped code, no bump, no intent), so the gate fails inside the amend's own pre-commit hook and the amend silently aborts — while the git command itself may report success because the hook consumed the failure. Always verify the amended commit's tree actually contains the bump (`git show HEAD:src/app_version.py`) before assuming an amend landed.

The clean pattern to add a version bump to already-made commits:

1. `git reset --soft <last commit that carries a valid bump/intent>` — the tip before the shipped-code commits.
2. Recommit the staged stack as one commit that carries the bump (`python scripts/bump_version.py <next>` before staging, or as part of the same staged set).
3. At commit time the gate window is empty (HEAD already carries the previous bump), and at push time HEAD carries the new bump, so both gates pass.

This collapses the intervening commits into one; only do it with unpushed commits. If the commits are already pushed, bump forward in a new commit instead of rewriting history.

### Linux AppImage

Prerequisites:

```bash
python -m pip install -r requirements-lock.txt
# appimagetool is auto-fetched from GitHub Releases and cached at _out/appimagetool/
```

Preferred build command:

```bash
npm run build:linux
```

Direct Python entrypoint for operator/debug use:

```bash
python scripts/build_portable_linux.py
```

Default icon:

- AppImage builds use the checked-in `packaging/baluffo.png` (converted from `favicon.ico`).

Default output:

- `dist/Baluffo-{version}-x86_64.AppImage`

AppDir layout before appimagetool packaging:

- `dist/baluffo-linux/baluffo` — ELF executable
- `dist/baluffo-linux/_internal/` — Python runtime and dependencies
- `dist/baluffo-linux/ship/` — embedded ship bundle with `.sh` launcher scripts

Key differences from the Windows portable EXE build:

| Aspect | Windows | Linux |
|--------|---------|-------|
| PyInstaller flags | `--windowed --onedir` | `--onedir` (no `--windowed`) |
| Entry point | Same `desktop_app/__main__.py` | Same, with `_linux.py` platform dispatch |
| Updater | Separate `BaluffoUpdater.exe` (`--onefile`) | Folded into main binary |
| Output binary | `Baluffo.exe` | `baluffo` (ELF) |
| Icon | `favicon.ico` | `packaging/baluffo.png` |
| Launcher scripts | `.ps1` in `ship/` | `.sh` in `ship/` |
| Distribution format | `.zip` | `.AppImage` |
| CI runner | `windows-2022` | `ubuntu-latest` |
| CI workflow | `.github/workflows/build-portable-exe.yml` | `.github/workflows/build-linux.yml` |

Runtime notes:

- The AppImage is self-contained and runs on any glibc-compatible Linux distribution.
- Desktop runtime starts the local static site and admin bridge in the background, same as Windows.
- No separate updater binary; the updater logic is folded into the main ELF binary.
- On headless systems (no `$DISPLAY` or `$WAYLAND_DISPLAY`), the launcher runs in service-only mode.
- Playwright Chromium is deferred until upstream v1.61; AppRun sets `PLAYWRIGHT_SYSTEM_CHROMIUM=1`, relying on system `chromium-browser` for frontend smoke tests.

Linux AppImage smoke test:

```bash
bash scripts/smoke_test_appimage.sh
```

This launches the AppImage in headless mode, polls the bridge and site HTTP endpoints, verifies responses, and terminates cleanly.

AppImage execution notes:

- Direct execution requires FUSE: `sudo apt install libfuse2` on Ubuntu 24.04+. Use `--appimage-extract-and-run` as a FUSE-free workaround.
- CI workflows use `--appimage-extract-and-run` to avoid FUSE dependencies on runner images.

## Verification Checklist

### Shared Release Gates

Before any release:

1. Record `app_version`, `updater_version`, `data_schema_version`, and `manifest_schema_version`.
2. Run the canonical release preflight on the exact commit you plan to push or tag:
   - `npm run release:preflight`
   This includes the pre-commit `gitleaks` scan through the existing lint lane.
   Local preflight does not fully exercise GitHub secret-backed packaged sync config generation unless a valid non-secret test PEM path or env value is provided for that run. Treat packaged sync private-key handling as uncovered until the local lane or the release CI lane has executed that path.
3. If you need to debug a failing lane individually, rerun the underlying command directly:
   - `npm run lint:precommit`
   - `npm run test:py:extended`
   - `npm run test:frontend:unit`
   - `npm run security:js`
   - `npm run test:frontend:packaged`
   - `npm run test:frontend:packaged:admin-startup`
   - `npm run test:frontend:packaged:fetch-evidence`
   - `npm run test:frontend:packaged:sync-rehearsal`
   - `npm run test:frontend:packaged:update-rehearsal`
   - `npm run test:frontend:packaged:orphan-reclaim-rehearsal`
   - `npm run test:frontend:packaged:browser-job-rehearsal`
   - `npm run test:frontend:packaged:desktop-lifecycle-rehearsal`
   - `npm run test:frontend:packaged:active-task-close-rehearsal`
   - `npm run test:frontend:packaged:task-abort-schedule-rehearsal`
   - `npm run test:frontend:packaged:first-run`
   - `npm run test:frontend:packaged:admin-active-run`
   - `npm run test:frontend:packaged:jobs-pipeline`
    - `npm run probe:desktop:startup:jobs:cold`
    - Linux lanes:
      - `npm run test:py:linux`
      - `npm run test:frontend:linux`
      - `npm run build:linux`
      - `bash scripts/smoke_test_appimage.sh`
    For CI release failures, inspect artifacts before inferring root cause:
   - `gh run view <run-id> --log-failed`
   - `gh run download <run-id> --dir .tmp/release-run-<run-id>`
   - Inspect the packaged smoke report JSON first when a packaged desktop lane fails.
4. Validate any declared migrations and rollback behavior.
5. Rehearse the release on a staging machine before publish.

### Shared Desktop + Umbrel Public Closeout

When one version is intended to be both the public desktop release and the Umbrel Docker release:

1. Confirm `main`, `origin/main`, `src/app_version.py`, `docs/CHANGELOG.md`, and release metadata all name the same version, and confirm the target tag/release does not already exist.
2. Run `npm run release:preflight` on the exact commit to tag, then confirm the repo is still clean.
3. Build local desktop artifacts for the same version: `npm run build:portable-exe -- --bundle-version <version>` and `npm run build:ship-bundle -- --bundle-version <version>`, then verify the portable ZIP and ship ZIP embed `app/current.txt == <version>` and `APP_VERSION = "<version>"`.
4. Create an annotated `v<version>` tag only after local gates and artifact checks pass, push only that tag, and watch every tag-triggered workflow that can publish artifacts: `build-portable-exe`, `Build Container`, and `build-linux`. Pushing the tag now runs the same pre-push gate as `main` (`lint:precommit:ci` plus `lint:deadcode:js`), so a green local run is still the only thing standing between you and a published release — the tag push is not a bypass.
5. Verify the GitHub release is published, not draft, not prerelease, and that release notes came from the matching `docs/CHANGELOG.md` version section.
6. Verify release assets: portable ZIP, ship ZIP, desktop update manifest, and Linux AppImage when the Linux workflow publishes it.
7. Download `baluffo-desktop-update-manifest.json` and confirm its version, channel, schema, key id, signature, minimum updater version, rollback flag, portable artifact URL, checksum, and size match the release asset.
8. Reconfirm GHCR `ghcr.io/deathuman/baluffo:<version>` after tag-side workflows finish, recording the final index digest and `linux/amd64` plus `linux/arm64` platforms.
9. Reconfirm live Umbrel on the shipped version with `/app/ready`, `/ops/health`, `/tasks/run-jobs-pipeline-status`, `/ops/task-state?view=summary`, and `/sync/status?view=summary`.
10. Record commit, tag, release URL, workflow run ids, asset names/sizes, manifest evidence, GHCR digest/platforms, Umbrel endpoint status, and residual risks in Basic Memory, then commit and push the curated BaluffoMemory repo.

### Protecting Repo-Local Runtime Data During Release Prep

Runtime-owned `data/` files are rewritten by the app during normal local use, so they are **not** release content: never stage them in a release commit.

`scripts/precommit_gate.py` excludes this set from its changed-mode collection, so an ordinary commit is no longer blocked by their working-tree churn. Seven of the eight are **untracked** — they were deliberately untracked so a stale multi-megabyte ledger can never be committed or shipped — and only the tombstones file is still tracked, because it is canonical registry state that must survive:

- untracked: `data/desktop-startup-metrics.jsonl`, `data/jobs-fetch-tasks.json`, `data/jobs-lifecycle-state.json`, `data/jobs-source-state.json`, `data/jobs-success-cache.json`, `data/source-discovery-candidates.json`, `data/source-discovery-report.json`
- tracked: `data/source-registry-tombstones.json.gz`

`data/jobs-fetch-report.json` is also excluded when present, and is untracked in this repo.

CI scopes the same gate with `--exclude-root data`, so both paths agree. `tools/repo_health/workflow_policy.py` holds both declared sets and fails if a runtime artifact is re-tracked (`test_runtime_owned_untracked_data_files_stay_untracked`) or if a genuinely tracked one is dropped from `EXCLUDED_FILES` (`test_runtime_owned_data_files_are_tracked`).

When a release commit must be built from a clean tree, preserve local runtime state rather than discarding it:

1. Stash only the runtime files before release prep:
   `git stash push -m "pre-release local runtime data v<version>" -- data/`
2. Do the release work on the clean tree.
3. Restore afterwards with `git stash pop`, then verify each file matches the preserved blob:
   `git rev-parse "stash@{0}:<path>"` against `git hash-object -- <path>`.
4. Expect those files to stay dirty afterwards. Dirty runtime data is the normal local state, not a failed restore.

### Recovering A Release Without Moving Its Tag

Release tags are immutable. When a tagged workflow fails and the fix belongs on `main`, rebuild the release assets from `main` while keeping the original tag and commit:

```powershell
gh workflow run build-portable-exe.yml --ref main `
  -f release_tag=v<version> -f bundle_version=<version>
```

- `--ref main` dispatches from the **fixed** `main`, not the tag, so the recovery run uses current workflow code. Dispatching without it checks out the tag's original commit and reproduces the same failure.
- `release_tag` targets the existing release; nothing is deleted, moved, or recreated.
- `bundle_version` must match the tag's version or the published asset names will disagree with the release.

After the run completes, re-verify the published assets against the refreshed desktop update manifest: version, channel, schema, key id, signature, `portable_artifact` and `ship_recovery_artifact` URLs, checksums, and sizes.

`docs/RELEASE.md` forbids moving or recreating tags for both desktop and container recovery; container-side fixes ship as a **new patch version** instead.

### A Live Container Tag Is Mutable Until The Next Bump

Tags are immutable for *release assets*. A **published container tag is not**: GHCR lets any
`Build Container` run overwrite `ghcr.io/deathuman/baluffo:<version>` in place, and the workflow fires on
any push touching shipped code while the version in `umbrel-app.yml` is unchanged.

This is not hypothetical. On 2026-10-01, pushing the Q4/Q5 CSS deletion (`3f33e089`) republished the
already-released `0.3.001` tag and moved it from `sha256:f6fa5b37` to `sha256:0d0f194c`. Nothing failed, no
lane went red, and the `v0.3.001` **release assets** (built earlier, at `af63e63b`) do not contain the
change — so the GitHub release and the container now hold **different code under one version string**.

The version gate did not catch it, by construction. `container_version_policy.py` evaluated the window
anchored at the last version bump, and `evaluate_window` returned clean if **any** shipped commit in that
window declared release-tag intent:

```python
if any(_has_valid_release_tag_intent(commit, current_version) for commit in shipped):
    return []
```

One intent line discharged the whole window, and the window only reset at a bump. So once a commit declared
`Release-tag: v<next>` while `<current>` was still published, **every later shipped push republished the live
tag silently** — and because Umbrel's update check is string inequality, none of those republishes was ever
offered to an existing install. That gate is fixed; see below.

- **Shipped code is not landable on `main` while the current version tag is published**, unless the same
  commit bumps the version. Either bump `src/app_version.py` in that commit, or keep the change on a branch
  and release it by tagging. Non-shipped work (`.github/`, `tools/`, `tests/`, `docs/`) is always safe.
- **Check the digest after pushing shipped code**, not only before a release:
  `python scripts/check_published_version.py --version <current>`. Run before the push it is a pre-flight
  warning; run after, it is a report about a fact that already exists.
- **Record the digest a version actually ends on.** `0.3.001` shipped as `f6fa5b37` and now resolves to
  `0d0f194c`; both are true at different times and only the second is current.

This is now **enforced**. `check_container_shipped_code_version_gate` computes whether the current version
carries a git release tag and, when it does, drops the shipped commits that are ancestors of that tag (they
are inside the published image already) and then fails on whatever shipped code remains — intent included. A
`Release-tag: v<next>` line no longer authorises overwriting a published `v<current>`; the version has to
actually move. The remedy is in the failure message: bump, or branch and release by tagging.

Two deliberate limits:

- **The git tag is the release marker, not a GHCR query.** That keeps the gate offline and deterministic in
  pre-commit and pre-push, where a network call would add two round-trips and fail without a registry — and
  because `AGENTS.md` bans `--no-verify`, a flaky gate means stuck commits rather than a bypass. The cost is
  that a version that reached GHCR without ever being tagged reads as unreleased; that is the inert window
  anyway, since nothing can hold the version string.
- **Pre-release republishing is still allowed.** Between a bump and its tag, any shipped push republishes the
  not-yet-released version. Nothing can install that string and Umbrel's string-equality check means it is
  never offered, so the churn is unreachable by users and blocking it would cost a release-window fix for
  nothing.

`package.json` and `package-lock.json` are both in `NON_SHIPPED_PATTERNS` for the related reason that a
dev-dependency bump should not rebuild a published image at all — see `NON_SHIPPED_PATTERNS` in
`tools/repo_health/container_version_policy.py`. The lockfile was registered first and the manifest that
produces it was not, so a grouped Dependabot bump of `eslint` and `knip` was still counted as shipped code
and still failed `check_container_shipped_code_version_gate` against a released tag (PR #12). A routine
toolchain bump must need neither a version bump nor release-tag intent to land.

The exclusion is path-level and therefore only as sound as the manifest it covers, so
`test_package_json_declares_no_runtime_dependencies` asserts that `package.json` carries no runtime
`dependencies` section. Dev deps install into the image's build stage only — `Dockerfile` runs `npm ci` in
a frontend stage and copies forward just the built `.container-frontend` bundle — so the file can change the
image digest (`.dockerignore` keeps it in the image) without changing what the app does. If a runtime
dependency is ever added, both files must leave `NON_SHIPPED_PATTERNS` and the `paths-ignore` lists in
`.github/workflows/build-container.yml` together, which
`test_container_workflow_paths_ignore_stays_aligned_with_guardrail` then enforces.

### Ship Bundle Verification

1. Build the ship bundle for the target version.
2. Confirm launcher scripts exist in the bundle root.
3. Confirm `app\versions\<app_version>\packaging\github-app-sync-config.template.json` exists.
4. Confirm `data\` contains seeded defaults, including `data\defaults\source-registry-*.seed.json`, not repo-local runtime registry artifacts such as `source-registry-active.json`, `source-registry-pending.json`, or `source-approval-state.json`.
5. Validate the manifest against `docs/update-manifest.schema.json`.
6. Apply the update on staging with `.\apply-update.ps1`.
7. Confirm:
   - `app\update-state.json` ends in `ready`
   - `app\current.txt` points to the target version
   - persisted user/runtime data remains intact
   - support bundle generation works

### Portable EXE Verification

1. Build the cache-backed portable EXE for the target version.
2. Confirm:
   - `dist\baluffo-portable\Baluffo.exe` exists
   - `dist\baluffo-portable-<version>.zip` exists
   - `_out\latest\build\portable\Baluffo.exe` was refreshed from the same portable output
   - the embedded `ship\` bundle exists
   - the embedded Playwright browser payload contains only the required `chromium_headless_shell-*` cache, not full Chromium, Firefox, WebKit, ffmpeg, winldd, or other local cache siblings
   - the packaged updater rehearsal source runtime exercises install handoff with optional `psutil` removed
3. Run packaged desktop smoke validation:

```powershell
npm run test:frontend:packaged
npm run test:frontend:packaged:sync-rehearsal
npm run test:frontend:packaged:orphan-reclaim-rehearsal
npm run test:frontend:packaged:browser-job-rehearsal
npm run test:frontend:packaged:desktop-lifecycle-rehearsal
npm run test:frontend:packaged:active-task-close-rehearsal
npm run test:frontend:packaged:task-abort-schedule-rehearsal
npm run test:frontend:packaged:first-run
npm run test:frontend:packaged:admin-active-run
npm run test:frontend:packaged:jobs-pipeline
npm run test:frontend:packaged:update-rehearsal
npm run probe:desktop:startup:jobs:cold
```

The `npm run test:frontend:packaged:update-rehearsal` lane uses a locally generated, self-signed manifest. As a release-closeout step, additionally validate the updater end-to-end against the real published manifest and assets (signed by the release key) once the GitHub release is live, so the signed publish path — not only the local rehearsal — is confirmed accepted by the updater.

These packaged smoke commands validate the direct `dist\baluffo-portable\Baluffo.exe` artifact. Normal release and perf lanes reuse the same content-addressed portable build when its fingerprint is current; cold startup coverage gets a fresh runtime data/profile state rather than a rebuilt executable. Local portable builds also mirror the successful output to `_out\latest\build\portable\Baluffo.exe` so the familiar latest path does not remain stale after `npm run build:portable-exe`.
The first-run packaged smoke is deterministic: it opens Jobs from a cold isolated runtime, lets the real Jobs UI start the bootstrap route with `BALUFFO_PACKAGED_SMOKE_BOOTSTRAP_MODE=controlled-heartbeat-success`, avoids live Google Sheets, keeps the backend active past a smoke-shortened UI timeout boundary with fresh task-live heartbeats, asserts the running report/task state, renders the promoted one-row feed, verifies no duplicate post-success bootstrap starts, and captures computed-style checked light/dark popup artifacts.
The Jobs-page packaged smoke is now a terminal-success gate: it must launch the Jobs pipeline, observe visible running progress, and then reach a non-error terminal state. It uses a smoke-only stub-success pipeline mode so the lane stays deterministic and does not run the full real discovery/fetch/sync workload.
The task-abort/scheduler packaged rehearsal drives the shipped bridge through `POST /tasks/run-jobs-bootstrap`, `POST /tasks/abort`, and `POST /tasks/jobs-pipeline-schedule`, then verifies canceled lifecycle evidence with `user_abort_requested` and one scheduled pipeline terminal success in smoke-only `stub-success` mode.
The packaged sync rehearsal gate validates the shipped `github-app-sync-config.json` inside the artifact, fails if it is machine-derived, and then drives `/sync/test` against a local fake GitHub App endpoint so the release gate exercises packaged auth/read portability without hitting real GitHub.
The updater rehearsal gate exercises the real packaged `N -> N+1` helper-driven install path, including portable ZIP download staging, relaunch verification, and preservation of the resolved local-data root.
The orphan-reclaim rehearsal gate seeds stale packaged `site` / `bridge` children plus stale desktop session state, relaunches the packaged app on the same ports, and fails unless startup metrics prove the launcher reclaimed both stale children instead of retrying or silently degrading.
The browser-job rehearsal gate forces managed Chromium app-mode launch, requires early browser job-attachment telemetry, and then kills only `Baluffo.exe` to prove the attached/live browser PID exits before any smoke cleanup backstop runs.
The desktop lifecycle rehearsal gate blocks desktop lifecycle POST/beacon traffic from a controlled page while non-health page traffic continues, proves the owner session does not false-idle-close past a short smoke-only timeout, then proves real page/window shutdown releases the launcher, browser proof PID, and desktop ports.
The active-task close rehearsal gate starts a deterministic active bootstrap task, accepts the real desktop close confirmation through smoke-only CDP, and fails if the launcher reopens the browser or enters the fatal active-work path instead of exiting and releasing child processes.

Optional additional cache-backed smoke validation:

```powershell
npm run probe:desktop:startup:cold
```

Release preflight already includes `npm run probe:desktop:startup:jobs:cold` as the separate cold Jobs startup threshold gate. That probe is startup-profile coverage only; first-run bootstrap timeout/recovery correctness belongs to `npm run test:frontend:packaged:first-run`. For a warmer startup path, use `npm run probe:desktop:startup:warm`.
For the canonical startup measurement architecture and the preferred `perf:startup:*` command surface, see [`startup-probe-architecture.md`](startup-probe-architecture.md).

4. Confirm desktop startup, bridge readiness, the full packaged smoke, and the Jobs-page no-Admin pipeline smoke all pass in the smoke output.
   - The Jobs-page smoke should not be considered passed if the backend pipeline enters `stage=error` or reports a non-empty `error` after startup.
5. If sync credentials are packaged, confirm the packaged runtime still resolves the expected sync config and smoke remains green.

### Local Preflight vs CI Gate Coverage

`npm run release:preflight` runs the all-files pre-commit gate, the extended Python suite, frontend unit tests, the npm dependency security audit, the published-version check, the portable EXE build, the 17 packaged smoke and rehearsal lanes above (including `admin-startup`, `admin-active-run` and `fetch-evidence`), and the cold Jobs startup probe. It intentionally does not run every release gate:

- `npm run typecheck:py` (mypy) and `npm run lint:deadcode:js` (knip) are not part of `release:preflight`, but neither is unowned locally. Both are `stages: [pre-push]` hooks selected by `PRE_PUSH_HOOK_IDS` in `scripts/precommit_gate.py`, and `run_changed` (the commit path) runs them too, so a type or lint error fails the **commit** rather than surfacing seconds later at push. knip is additionally called directly by `.githooks/pre-push`, because it is not a `.pre-commit-config.yaml` hook at all. Both therefore stop a push to `main` or to a `v*` release tag before it reaches the remote; feature branches and `rollback-*` tags are deliberately ungated.
- The container Jobs boot performance gate (`jobs-boot-perf.yml`) builds and runs the real container; it is deliberately not part of local preflight because preflight has no live container.
- `eslint` has one definition: `.pre-commit-config.yaml` declares it `stages: [pre-push]` with `entry: npm run lint:js`, and `scripts/precommit_gate.py` selects that stage explicitly. The hook used to carry its own `npx eslint . --ignore-pattern ...` line whose patterns were already a strict subset of `eslint.config.js`, so it could drift away from the config it shadowed; it now calls the same npm script everything else uses. It is enforced by the commit hook (`lint:precommit:changed`), by `lint:precommit:ci` in CI, on `main` and `v*` tag pushes, and by preflight. The only bypass is `--no-verify`, which is forbidden.

`npm run check:published-version` (lane 4 of preflight) queries GHCR for the current `APP_VERSION` and warns when that version is already published. It exists because the container version gate validates release *intent* but is deliberately offline — it cannot see whether the version it is asking you to publish has already shipped, which is the 0.2.140 reuse trap in another disguise. It is warn-only by default (republishing inside an open release window is sometimes intentional); `--strict` turns an already-published version into a failure. A network failure degrades to a "verify manually" notice rather than breaking preflight, and it is intentionally **not** part of the always-on `repo_guardrails` set so that gate stays fast and offline.

Do not read "preflight passed" as "all gates passed"; the tag-push workflows run the remaining lanes.

#### Local Preflight vs Tag Workflow: Known Divergences

`release:preflight` and the tag workflow's `Run release gates` step are **not** the same command set, and a green preflight does not predict a green tag run. The concrete differences:

| Lane | `release:preflight` | `build-portable-exe.yml` release gates | Enforced elsewhere |
| --- | --- | --- | --- |
| `npm run lint:precommit` (all-files gate) | yes | no | `lint.yml` runs `lint:precommit:ci`; the pre-push hook runs it on `main` and `v*` tags |
| `npm run security:js` | yes | no | `lint.yml` |
| `npm run check:published-version` | yes | no | **nowhere — manual lanes only** |
| Portable prepare (`--skip-zip`) | yes | yes | — |
| `test:frontend:unit`, `test:py:extended` | yes | yes | `test.yml` also runs both |
| Packaged smoke + rehearsal lanes (17 commands) | yes | yes | **nowhere else — tag-time only, by decision** |
| `npm run probe:desktop:startup:jobs:cold` | yes | yes | — |
| `npm run typecheck:py`, `lint:deadcode:js` | no | no | `lint.yml`, plus the commit hook and the pre-push hook on `main` and `v*` tags |

Two rows deserve to be read as decisions, not gaps:

- **`check:published-version` has no automated home.** It is the one lane that exists only in the manual preflight. It is warn-only by design and cannot meaningfully gate an automated run, so this is intentional — but it means the "is this version already published?" question is asked by a human, before tagging, and nowhere else.
- **The 17 packaged desktop lanes run only at tag time.** This is a deliberate trade-off, not an oversight: they are the slowest and most environment-sensitive lanes (measured `Run release gates` at 17m26s in CI), and the cost of running them on every push was judged higher than the benefit. The accepted consequence is that a regression in packaged desktop behaviour can reach `main` and is only caught by the next tag's release gate. If a packaged regression ever ships, that is where to look first.

The Windows release runner needs **both** Playwright runtimes installed, because the frontend unit lane imports `@playwright/test` while the portable builder imports Python Playwright, and each resolves a different browser revision. `build-portable-exe.yml` installs both and fails fast naming a missing cache entry; a workflow that installs only one will hang the unit lane rather than fail it.

Before running `python scripts/bump_version.py <version>`, author the release compatibility phrases into the `[Unreleased]` changelog section. `repo_guardrails` does not require a "compatibility sentence": `test_release_docs_cover_the_current_public_release_line` asserts six literal substrings — `same-origin Linux container`, `Umbrel raw-LAN installs`, `GHCR multi-arch image publishing`, `private community app-store metadata`, `wildcard browser CORS allow headers`, `desktop localhost bridge compatibility` — inside the new `## [<app_version>] - <date>` section, which must also carry a Keep-a-Changelog `###` subsection and at least one `- ` bullet. Bumping prose-free `[Unreleased]` content therefore fails `release:preflight` on the docs guardrail.

`test_top_release_notes_stay_user_readable` additionally bounds the shape of that top section — at most 2,000 words and no bullet over 1,200 characters — and prints a non-blocking advisory when implementation detail leaks in. It never scans historical sections.

The same release commit is also where the changelog's **clamp** is applied. `docs/CHANGELOG.md` holds only the newest 5 released versions plus `[Unreleased]`; `git log` is the source of truth for anything older. `test_changelog_clamps_released_sections` fails once a sixth released section appears, so adding the new `## [<app_version>]` section means dropping the oldest one in the same commit — otherwise `release:preflight` fails on the docs guardrail.

Bumping straight from one released section to the next (as 0.3.004 and 0.3.005 did) is also how a change ends up described twice. `test_no_change_is_claimed_by_two_release_sections` fails when the same bullet appears in two release sections, because it makes each of them appear to ship a change only one of them did. Fold the unreleased sections into the new one and keep each change described once. The single exception is a deliberate **rollup** release that restates an earlier release's notes for users skipping versions: it may duplicate bullets provided it declares that in a blockquote under its heading, exactly as `## [0.3.001]` does.

### Container / Umbrel Verification

Container builds use the checked-in `Dockerfile` and run:

```powershell
python -m src.container_entrypoint --host 0.0.0.0 --port 8080 --data-dir /data
```

Runtime contract:

- The container exposes one HTTP service on port `8080` inside the container.
- UI, static assets, runtime data, and bridge API routes are served same-origin.
- Official container images build generated frontend bundles with `npm run build:container-frontend`; hashed bundle assets are immutable/gzip-capable, while desktop/local HTML continues using checked-in unbundled modules when the generated container asset directory is absent.
- Static `/data` serving is allowlisted to public runtime reports, registry/discovery exports, contracts, and defaults; user profile and attachment data stays behind bridge routes.
- `frontend-runtime-config.js` is generated dynamically with `bridge.sameOrigin: true`, `runtime.mode: "container"`, and `runtime.localDataMode: "bridge"`.
- `/data` is the only persistent runtime volume and owns profiles, saved jobs, attachments, reports, logs, source registries, and `baluffo-runtime.db`.
- Desktop-only routes return HTTP 409 with `not available in container mode`.
- Playwright Chromium is baked into the image for deterministic first run.

The public image target is:

```text
ghcr.io/deathuman/baluffo
```

The GHCR workflow builds `linux/amd64` and `linux/arm64` using Docker buildx and QEMU. Pull request builds validate the image without pushing; default-branch and tag builds publish to GHCR when the workflow runs. Docs, tests, and repo-process-only branch/PR changes are path-filtered out so they do not republish the current app-version or `latest` tags. Tag pushes and manual `workflow_dispatch` publishes are not skipped by those path filters.

Umbrel private app-store metadata lives at:

- `umbrel-app-store.yml`
- `deathuman-baluffo/umbrel-app.yml`
- `deathuman-baluffo/docker-compose.yml`
- `deathuman-baluffo/exports.sh`

Umbrel Compose uses `app_proxy` with `APP_PORT: 8080` and `PROXY_AUTH_ADD: "false"`, and mounts `${APP_DATA_DIR}/data:/data`. The intended raw LAN URL is `http://192.168.50.61:8877/`; Umbrel binds that host port from `umbrel-app.yml`, so the `web` service must not publish the same port.

Raw LAN exposure is intentional for this channel. Anyone who can reach the host port can access Baluffo UI, Admin, and local-data routes, so do not expose the port to the Internet, public Wi-Fi, or broad VPN peers. The container service is still browser same-origin and must not emit wildcard CORS allow headers for arbitrary external origins.

Umbrel live smoke is required for Umbrel release closeout. Verify:

1. `/ops/health.appVersion` matches the shipped version, `startupReady == true`, and the pipeline is idle unless the smoke is intentionally running.
2. `admin.html` and `jobs.html` return HTTP 200 with fresh cache-busted frontend modules.
3. `/registry/summary` and `/registry/sources?buckets=active,pending,rejected&includeHiddenPending=1` match the latest terminal discovery report's `runtime.registryFinalization` counts.
4. `/sync/status.config.ready == true`, `credentialsPackaged == true`, and `missing == []`.
5. The jobs data feed loads and its count matches the completed pipeline `finalOutputCount` after a pipeline smoke.
6. A manual or scheduled Jobs pipeline terminalizes cleanly, leaves `/ops/task-state?view=summary.count == 0`, writes fetch/discovery evidence under `/data`, and does not fail through an absolute safety-cap timeout.

Record the final commit SHA, workflow run ids, GHCR digest/platforms, Umbrel installed version, registry/sync/jobs/pipeline smoke results, and residual risk in Basic Memory.

### Post-Release / Incident Checks

If a tagged release workflow fails:

1. Inspect failed logs and downloaded artifacts before changing code.
2. Confirm whether GitHub release assets, notes, or manifests were published.
3. Do not move, delete, or recreate the release tag without explicit user approval.
4. If tag movement is approved, recreate the annotated tag on the fixed release commit and force-push only that tag.
5. Watch the fresh release workflow and re-check assets, release notes, manifest publication, and rollback availability.

After release:

1. Confirm healthy steady state for the shipped channel.
2. Keep the previous ship-bundle version available for rollback.
3. For a failed ship-bundle rollout, recover with `.\run-all.ps1 -RecoverPrevious` and capture diagnostics with `.\run-all.ps1 -CreateSupportBundle`.
4. If a release secret may have leaked, rotate it before any cleanup work. Run a manual full-history `gitleaks git` audit to classify findings, and only rewrite Git history or tags after an explicit separate approval.
