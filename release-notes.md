## [0.3.043] - 2026-11-09

### Added

- Ops can now inspect a legacy browser-fallback queue before it is read: a new diagnostic reports pages listed more than once, which copy will be kept, and parser-error rows that are not eligible for browser retry. It is read-only and never changes the queue.
- Admin now shows a per-table breakdown of the runtime database, so where a growing store spends its space is visible without opening a shell.

### Changed

- Browser fallback queue entries now say whether fallback was attempted, whether it supplied content, and why it was not attempted. A run-wide cooldown is recorded separately, so one breaker refusal is no longer mistaken for a refusal of every queued source.
- The runtime database now keeps only the newest 30 fetch runs and 30 days of per-source fetch detail, and compacts itself after pruning, so a long-lived install no longer accumulates every fetch report it has ever written. The compaction is bounded by time and free disk space and reports its outcome in storage health.

### Fixed

- Browser fallback queue rows now deduplicate by canonical page across source IDs, and `parse_error` rows outside the documented static-queue classification allowlist are excluded from new writes and legacy queue reads.
- Admin's runtime storage panel now loads once when startup hydration finishes instead of firing a second read, so it no longer stalls the panel when the bridge is still finishing startup work.
- Browser fallback can now reopen during a long fetch. When the cooldown refuses a fallback request, the app checks whether a browser can render again and, if it can, retries that request immediately instead of leaving every later source refused until the 30-minute window ends. The check runs at most once a minute, so a browser that is still broken is not relaunched for every source that asks for fallback.

- Static fetch diagnostics no longer count sources excluded before evaluation as zero-kept
  review entries. A targeted `--only-sources` run had reported every unselected loader as
  blank residue, turning one evaluated source into more than 1,600 phantom reviews.
- Game-role detection now keeps lighting artists, QA/test roles and narrative writer titles
  that were previously dropped, recovering roles from Housemarque, KRAFTON India, 2K Vegas,
  Scape, JustPlay, Beamdog, 2K and Cloud Chamber while still rejecting technical, content and
  marketing writers.
- SmartRecruiters boards that were registered with only a compact source identifier now fetch
  correctly, and their postings link back to the right company.
- Kojima Productions, Raven Software/Activision and Eidos Montreal cross-postings now merge into
  one entry with both source links, instead of showing the same opening twice.
- Distinct Epic, Riot and Scopely postings with different requisitions or work arrangements no
  longer collapse into one entry.
- Epic job links and Greenhouse job-detail links now count as provider identity, so distinct
  provider job IDs cannot merge solely on title and location.

- After a browser health probe confirms Chromium can launch and render, browser fallback reopens for the next run instead of keeping a stale cooldown from one failed page navigation.
- Browser fallback no longer queues static fetch failures a browser cannot repair, including DNS resolution, certificate/TLS validation, and exhausted time or failure budgets. Existing fallback queues are filtered when read, so those rows stop running after updating.
- Ops no longer repeats the app version in the health header; the header shows the running build separately.

### Notes

- Distribution compatibility is unchanged: the same-origin Linux container, GHCR multi-arch image publishing, wildcard browser CORS allow headers, and desktop localhost bridge compatibility remain supported.
