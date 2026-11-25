## [0.3.069] - 2026-11-25

> This is a rollup release: it gathers everything that reached `main` after
> 0.3.068 — the Admin reporting fixes first planned for this version, the
> storage, discovery and container-browser work that followed, and the desktop
> update-lock fixes.

### Fixed

- The probe-wave line in Admin no longer reads as a current all-clear. It showed a "clean" chip from whatever receipt was on disk, and the live container's came from a wave 16 days old, so a lane nobody had exercised looked verified. The row now prints when that wave finished and warns on a receipt older than a week or one with no finish time; a fresh wave still reads clean.
- A large database now gets the integrity check it used to defer forever. Above 64 MiB the live check was skipped for size, so "healthy" only meant writes were succeeding and nothing had ever read the file back. It is now verified in the background once a day, and the storage panel says when that last happened — or that it never has. A failed check marks the store unhealthy with the database's own error.
- A source whose browser fallback attempt was refused or blocked now records why. The row already said "browser fallback unavailable (cooldown active)" but not what closed the lane, so a run-wide cooldown read as an unexplained refusal; the cause is captured at the moment of the refusal and kept per attempt, so neither a later recovery nor a second failure in the same run rewrites it. A source that only recommended fallback, or whose page rendered with no jobs, is not stamped.
- Admin's fetch duration reports the middle of recent runs instead of their average. One long run, or a group of quick aborts, could move an average far from how long a run usually takes — the live container read about 50 minutes while its runs took around 2.5 hours. The card headlines the median, with the 90th percentile and the number of runs behind it underneath, so the figure can be checked rather than trusted.
- Admin's 7-day fetch success figure no longer turns an empty week into a total failure. With no run in the window it showed 0.0%, the same reading as a week where every run failed; the card now says the week was not measured and names the empty window, while a real 0.0% still means runs happened and all of them failed.
- Storage maintenance, a current-feed rebuild and a reclaim copy now appear as their own rows in Admin's Current Runs, with the state and age of the work instead of running invisibly. Starting a fetch, discovery, sync or pipeline while a rebuild or reclaim copy owns the database is refused, naming the running copy, and a second rebuild, compaction or retention pass is held or refused while a copy is being written, so nothing deletes rows out from under it. A reclaim copy also outlives the page that started it.
- Stale task rows are now closed by the lifecycle reaper. A row whose owning process is gone no longer sits active forever; it is finished through the same transitions a normal completion uses, so Admin stops showing work that nothing is running.
- Discovery results now reach the source registry through the registry's own merge instead of a direct pending-file write. A run that fails after discovery no longer loses the pending rows it found: they are restaged through the same merge, a board an admin deleted during the run cannot be resurrected, and a run can fill blank display fields without overwriting a fresher value another process owns. Conflicts and dropped rows are recorded in the run report.
- A registry merge that cannot get the lock is no longer dropped. The report waits in a retry queue drained on the next bridge start and when a discovery run finishes; the queue is capped at 25 entries and 2 MiB uncompressed, gzip-backed, with age and attempt limits and a marker so a crash mid-merge charges only the entry it was working on.
- Discovery counters in Admin are now stamped with when the shown values last moved, not when the run last touched them. A running pass that finds nothing new no longer makes unchanged counts look freshly updated, and a quiet stretch no longer makes them look stalled.
- Desktop update state writes are serialized across processes. A stale download-progress update can no longer overwrite a newer install status, so a completed install cannot fall back to "downloading".
- The desktop update, source registry and job-feed reconciliation locks now keep their lock file empty until the lock itself is taken. Writing a placeholder byte before locking put it inside the range the current holder had locked, so on Windows a waiting writer could crash with a permission error instead of waiting.
- The storage panel keeps its last complete table-size reading when a new one times out, reconciles capped table sizes so the rows still add up, and drops entries for tables that belonged to a database file that has since been replaced. Admin's health summary is also invalidated when the database is rewritten, so it cannot report health from before a rebuild.
- Container Chromium launches no longer trade one crash for another. Forcing the GPU path in-process replaced a GPU-helper crash with a browser crash on the first page, so the zygote is now bypassed only on root or container Linux; desktop Linux keeps its zygote and the sandbox it carries.

### Changed

- Admin's dashboard summary now reads storage and browser health directly, and ops diagnostics can probe the browser lane on demand instead of waiting for a run to exercise it.
- The Action Center names the recommended, attempted and failed browser-fallback source counts separately, and splits failures into environment-blocked and other causes, so "failed" no longer carries two different scopes.

### Notes

- This release is a rollup: Admin reporting and storage maintenance, the discovery registry merge and its retry queue, discovery counter stamps, the container browser launch fix, and the desktop update, registry and reconciliation locks. The same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility all stay as they are. No feed, saved job, note, or synced row changes format.
