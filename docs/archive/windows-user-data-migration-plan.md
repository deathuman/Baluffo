# Windows User Data Migration Plan

> - **Status:** Archived — implemented and shipped to `main` (2026-05-25). See git history for implementation detail.
> - **Not canonical for:** Linux/XDG/AppImage behavior; use [`linux-compatibility-plan.md`](linux-compatibility-plan.md) for that

## Summary

Baluffo's Windows packaged runtime stores persistent data outside the install root:

- Config/data: `%APPDATA%\Baluffo\`
- Session/browser transient data: `%LOCALAPPDATA%\Baluffo\`
- Cache subdir: `%LOCALAPPDATA%\Baluffo\cache\`

Repo/source defaults and explicit overrides still use the configured `data` root unless packaged desktop startup resolves a Windows packaged default. `--data-dir` and `BALUFFO_DATA_DIR` remain supported for dev, smoke tests, and recovery.

## Implemented Behavior

- Windows packaged desktop default `data_dir` resolves to `%APPDATA%\Baluffo`.
- `ship\data` remains a legacy migration source, not the packaged runtime data root.
- First packaged Windows launch copies legacy `<install root>\ship\data` into `%APPDATA%\Baluffo` when no completed migration report exists.
- Migration leaves legacy `ship\data` intact, never overwrites existing target files, and writes `%APPDATA%\Baluffo\migration-reports\windows-user-data-migration.json`.
- Updater install state, downloads, rollback metadata, success markers, handoff diagnostics, and helper logs use the resolved external data root.
- Desktop install plans include `dataDir` so the helper uses the same data root for install, rollback, migrations, and relaunch verification.
- New helpers use `%APPDATA%\Baluffo\updater\post-install-success.json` as canonical; old source helpers can receive a transition-only `ship\data\updater\post-install-success.json` marker when legacy handoff artifacts are present.

## Verification

- Unit coverage includes Windows path helper env/fallback resolution, packaged default selection, explicit override preservation, migration copy/idempotency/conflict reporting, updater path resolution, install-plan `dataDir`, relaunch success-marker lookup, legacy success-marker compatibility, and split data-root/install-root free-space preflight.
- Release-level gates for this surface remain:
  - `npm run test:frontend:packaged:update-rehearsal`
  - `npm run test:frontend:packaged:first-run`
  - `npm run test:frontend:packaged:admin-startup`
  - `npm run build:portable-exe`

## Assumptions

- Scope is Windows only.
- Linux/XDG/AppImage behavior is documented in [`linux-compatibility-plan.md`](linux-compatibility-plan.md) (archived).
- Migration copies data and leaves legacy `ship\data` untouched.
- No new dependency is introduced for Windows path resolution.
