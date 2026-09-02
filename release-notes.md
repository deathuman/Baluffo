## [0.2.142] - 2026-09-02
### Fixed

- Storage: `append_task_event` no longer seeds orphaned active task rows. The
  task_events foreign key required a placeholder `task_runs` row for event-only
  runs, but it was inserted with `status='running'` and no owner_pid/owner_kind;
  when a run's real lifecycle row lived only in JSON (pre-cutover or shadow
  mode), the SQLite placeholder survived as an unreapable "running" zombie that
  blocked subsequent task launches. The placeholder is now terminal
  (`succeeded`, `terminal_reason='event_only'`); a real lifecycle start still
  flips it to running through the normal upsert path.


### Changed

- Bridge task lifecycle: pid-less running rows in the SQLite task_runs projection (rows
  written without an ownerKind or ownerPid, e.g. by pre-migration or crashed bridge
  versions) are now reaped at startup once their heartbeat goes cold (1h threshold,
  far above any real worker cadence). Previously neither the pid check nor the
  owner-kind allowlist could stale them, so they persisted as active tasks forever,
  blocking every later pipeline run behind "Updating local jobs..." (observed live:
  two zombie sync rows, one six days old, disabled the Update-jobs button for hours).

- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.


- Container shipped-code version gate (`tools/repo_health/container_version_policy.py`): the `release` repo guardrail now fails when container-affecting commits land after the last version bump without either advancing the version or declaring explicit release-tag intent (`Release-tag: vX.Y.Z` line, or `release(vX.Y.Z):` / `chore(release):` subject, naming a version newer than the current one). This closes the 0.2.140 reuse trap — code shipped to the Umbrel container channel while the `umbrel-app.yml` version string stayed frozen, so Umbrel's app-store update detection never offered the newer build. The lint CI checkout now fetches full history so the gate evaluates the real commit window.
- Container `paths-ignore` alignment (`.github/workflows/build-container.yml`): the republish trigger's `paths-ignore` now matches the guardrail's shipped-path list exactly (`tools/**`, `.github/**`, `release-notes.md`, `umbrel-app-store.yml` added), so pure-tooling commits stop triggering no-op image republishes; a pinned test keeps the two lists in lockstep.
- Workflow-syntax gate (`tools/repo_health/workflow_syntax_policy.py`): the `workflow` repo guardrail now runs actionlint over every `.github/workflows/*.yml` so workflow YAML is semantically validated in the pre-commit/pre-push gates and CI Lint on every change. The pinned actionlint binary is located on PATH or provisioned as a checksum-verified release into the gitignored `.tmp/actionlint/` cache, and the gate fails — never silently skips — if it cannot be obtained.
