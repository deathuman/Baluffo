## [0.3.065] - 2026-11-17

### Added

- A desktop update that fails its startup check now has a **Repair install** action in the update panel. It restores the retained copy of the previous install or, when no usable copy is left, downloads the signed release again and rebuilds the install from it. Saved jobs, notes, attachments and other local data are kept, and the app reopens when the repair finishes.

### Fixed

- A desktop update that fails its startup check now stops the app it just launched before rolling back, then reads the restored install back from disk. If `ship\app\current.txt` or any file from the previous install is missing, the update reports a failed restore and keeps the rollback snapshot instead of leaving a half-restored folder that still answers some requests and then fails on the next module it loads. The updater also reports a specific restore failure rather than ending quietly in a failed state.
- Automatic database storage reclaim now actually arms itself. 0.3.063 described the self-arming rehearsal, but the pre-flight refused that first automatic measurement, so a host that never set `BALUFFO_RECLAIM_AUTO_ENABLED` stayed unarmed. The rehearsal now runs at startup from the configured data directory, and an adequate measurement starts the first automatic shrink once a run is safe. Setting `BALUFFO_RECLAIM_AUTO_ENABLED=0` still blocks the arming rehearsal and every automatic start, and Rehearse, Run and Stop keep working.
- Closing a completed repair result now keeps the panel closed on later launches. The panel reopened on every start because the dismissal lived only in the page; the dismissal is remembered, a newer repair result still opens, and the update check resumes after a dismissal so the repaired state clears instead of making every later launch look repaired.

### Notes

- The same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility are unchanged. Existing feeds, saved jobs, notes, and compatibility exports are preserved; a failed update keeps the previous install intact and relaunches it.
- Release preflight now runs `npm run test:frontend:packaged:update-path-rehearsal`, which updates an installed copy from a version that reports 0.0.9 and requires the relaunched bridge to answer `/ops/health` with `startupReady: true` over a complete installed tree, not just a reachable process.
- Release preflight now also runs `npm run test:frontend:packaged:update-rollback-rehearsal`, which publishes a target version the build cannot report so the real readiness check fails, then requires the rollback to stop the relaunched app, restore every previous install file, and bring the previous app back with `startupReady: true`.
- Release preflight now runs `npm run test:frontend:packaged:update-repair-rehearsal` and `npm run test:frontend:packaged:update-reextract-rehearsal`, which prove both repair paths against a packaged install: the retained-snapshot restore, and a verified download-and-rebuild that keeps `ship/data`.
- Release preflight now runs `npm run test:frontend:packaged:admin-reclaim-startup`, which starts a packaged install with an unarmed, bloated runtime and no browser, and fails unless the startup rehearsal arms automatic reclaim and a real automatic run begins without a Rehearse click.
