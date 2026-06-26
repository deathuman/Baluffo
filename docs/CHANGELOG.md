# Changelog

> All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and Baluffo desktop releases use the project-specific `0.1.x` ordering documented in
[`RELEASE.md`](RELEASE.md).

---

## [Unreleased]

## [0.2.89] - 2026-06-26

### Fixed
- Admin no longer renders a false-empty Stored Profile Overview from degraded Umbrel bootstrap data; missing overview data now appears as delayed and retries the fast local-data overview before reporting an authoritative empty profile list.
- Bumped the Admin cache chain so existing Umbrel browser sessions load the 0.2.89 overview fix.

### Notes
- This remains on the current shared release line covering the same-origin Linux container, Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.88] - 2026-06-26

### Fixed
- Admin source-table startup now requests bounded registry table rows with `limitPerBucket`, reducing idle Umbrel registry payload pressure while keeping the existing full `/registry/sources` compatibility route available.
- The registry table view now supports an additive `limitPerBucket` query for bounded Admin loads and reports truncation metadata in the existing summary envelope.

### Notes
- This remains on the current shared release line covering the same-origin Linux container, Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.87] - 2026-06-25

### Fixed
- The Umbrel container gateway now returns bounded degraded-idle payloads for Admin bootstrap, task-state summary, task-live summary, dashboard summary, and pipeline schedule reads when the internal bridge is slow, preventing idle Admin boot from collapsing into repeated `HTTP 504` errors.
- Admin now treats recent bridge-heavy read timeouts as a degraded-idle state, keeps the shell usable, and delays registry source-table reads with explicit retry placeholders instead of immediately reloading `/registry/summary` and `/registry/sources?view=table`.

### Notes
- This remains on the current shared release line covering the same-origin Linux container, Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.86] - 2026-06-25

### Fixed
- Bounded fetcher and discovery log reads now preserve existing UTF-8 cursor behavior when a log ends with an incomplete multi-byte sequence, keeping `nextOffset` aligned with consumed text instead of raw partial bytes.

### Notes
- This remains on the current shared release line covering the same-origin Linux container, Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.85] - 2026-06-25

### Fixed
- Admin live fetch and discovery log polling now uses bounded log slices instead of unbounded offset reads, preventing large active logs from triggering Umbrel gateway timeouts while preserving the existing log payload shape.
- Fetcher and discovery log routes now enforce bounded offset and tail reads server-side, so stale cursors cannot return multi-megabyte responses.
- The Admin Action Center now delays health, sync, and storage probes while active job updates are known, relying on compact task-status routes until active work returns idle.

### Notes
- This remains on the current shared release line covering the same-origin Linux container, Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.84] - 2026-06-24

### Fixed
- Jobs pipeline progress labels now tolerate browser/server clock skew by using the live server snapshot timestamp when it is newer than the browser clock, preventing active updates from appearing stuck at `Checking sources... 0s`.
- The Jobs update button continues to show the current pipeline stage from `/tasks/run-jobs-pipeline-status`, so active Discovery, Fetch, and Sync work remain visibly distinct while elapsed time advances correctly.
- Aborting a Jobs pipeline now keeps issuing abort requests to the active fetch/discovery child while it remains live, surfaces child abort warnings, and clears or fails the `Aborting...` state after verification instead of leaving the UI stuck.
- Admin active-run polling now defers heavy fetch KPI, dashboard, storage-health, registry-summary, and bootstrap lifecycle reads when compact pipeline/task-state evidence is available, preventing repeated Umbrel `HTTP 504` timeouts during broad job updates.

### Notes
- This is a forward shared desktop and Umbrel patch after `0.2.83`; no existing release tags are moved or recreated.
- Container/Umbrel compatibility from the current public release line remains intact: same-origin Linux container mode, Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, avoidance of wildcard browser CORS allow headers, and desktop localhost bridge compatibility are all preserved.
- Route payloads, SQLite schema, persisted JSON contracts, Umbrel metadata shape, and public CLI surfaces remain compatible.

## [0.2.83] - 2026-06-24

### Fixed
- Admin now truly defers registry source-table loading while a job update or discovery pipeline is active, avoiding repeated `/registry/sources` and `/registry/summary` bridge pressure that could surface as Umbrel `HTTP 504` errors.
- Source tables still recover after the active run returns idle, preserving the delayed-state copy during active work and the existing registry/source-table payload contracts after recovery.

### Notes
- This is a forward shared desktop and Umbrel patch after `0.2.82`; no existing release tags are moved or recreated.
- Container/Umbrel compatibility from the current public release line remains intact: same-origin Linux container mode, Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, avoidance of wildcard browser CORS allow headers, and desktop localhost bridge compatibility are all preserved.
- Route payloads, SQLite schema, persisted JSON contracts, Umbrel metadata shape, and public CLI surfaces remain compatible.

## [0.2.82] - 2026-06-24

### Fixed
- Shared desktop and Umbrel rollup from the post-`v0.2.43` release line, including the Admin/Umbrel active-work responsiveness fixes, bounded task/status routes, compact source-table loading, and Jobs feed refresh recovery.
- Desktop packaged startup and session reliability are hardened: corrupted session JSON no longer breaks active-session recovery, recent invalid lock files are no longer reclaimed too aggressively, lock contention uses bounded backoff, and lock/session failures emit better startup diagnostics.
- Runtime SQLite storage is more resilient under contention: bridge-owned stores can configure busy timeout/retry settings through environment variables, reads reuse a cached connection, and transient read-side busy errors retry before surfacing failure.
- Source discovery fetch retries now use one shared sync/async timing policy with capped exponential backoff and jitter, while preserving existing retry counts, HTTP retry codes, and unexpected-exception propagation.
- Source discovery configuration drift is reduced by centralizing adapter scoring sets and env integer parsing without changing confidence values, concurrency defaults, or compatibility exports.
- Jobs and Admin continue to recover from active pipeline/fetch pressure using hot task snapshots, lightweight task-live summaries, and gateway control-plane fallbacks instead of loading large reports during active work.
- QLOC/Elevato recovery from the Umbrel patch series is included: Elevato boards and comma-style job URLs are parsed, stale Google Sheets evidence is replaced, and runtime registry-backed sources are selected by normal Jobs updates.
- Container/Umbrel compatibility from the current public release line remains intact: same-origin Linux container mode, Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, avoidance of wildcard browser CORS allow headers, and desktop localhost bridge compatibility are all preserved.

### Changed
- Desktop, bridge, updater, source-discovery, storage, and jobs internals have been split into narrower leaves with stronger guardrails and exception-ratchet coverage, reducing broad fallback behavior without changing public API or persisted data contracts.
- Release and packaged smoke coverage now exercises storage health, source-run/job-feed/source-registry SQLite authority, updater handoff/recovery paths, desktop lifecycle behavior, and active-task close/abort scheduling.

### Notes
- This is the next shared desktop and Umbrel release after the long container/Umbrel patch series. `v0.2.43` remains the previous public desktop baseline.
- No existing release tags are moved or recreated for this rollup.
- Route payloads, SQLite schema, persisted JSON contracts, Umbrel metadata shape, and public CLI surfaces remain compatible.

## [0.2.81] - 2026-06-16

### Fixed
- Active Discovery, Fetch, and Sync operator routes now publish a bounded hot task snapshot, so compact Admin/Jobs polling can read current progress without rebuilding lifecycle projections or hydrating large reports during active work.
- `/ops/task-state?view=summary` and `/ops/task-live/<task>?view=summary` now prefer the hot active-task snapshot while it is fresh, preserving existing route shapes while stripping full work items, source lists, registry diagnostics, and unbounded event arrays.
- The Umbrel container gateway now serves compact task-state and task-live summaries directly from the hot snapshot or pipeline control fallback when active work is in progress, reducing exposure to internal bridge slowness and avoiding active-route 504s.
- Runtime startup/cleanup now seeds and clears `admin-active-task-snapshot.json`, preventing stale active rows from surviving restarts while keeping full run history and diagnostics on the existing authoritative idle paths.

### Notes
- This is a forward Umbrel/container release candidate before the public desktop tag. Do not reuse `0.2.80`; use `0.2.81` for the next clean Umbrel smoke and, if that smoke passes, the later public desktop tag.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.80] - 2026-06-16

### Fixed
- Admin remains populated and responsive during active work, with hydrated Stored Profiles, Source Sync, Pipeline schedule, Action Center, source-table placeholders, compact tab badges, bounded Older Runs, and consistent polished controls instead of blank or stale panels.
- Jobs updates, search, and feed publication now recover cleanly after broad fetches: multi-term searches match across job fields, completed updates refresh the visible feed automatically, gzip feed responses preserve their headers, and the Jobs table keeps a stable visual height at narrower desktop widths.
- QLOC/Elevato source recovery is included end to end: Elevato boards and comma-style job URLs are parsed, expired detail pages are filtered, live QLOC `technical-artist,j,240` rows replace stale Google Sheets `j,229` evidence, and active runtime registry sources are selected by normal Jobs updates.
- Desktop packaged runtime fixes cover keyboard reloads, idle liveness, false first-run modal behavior with existing data, packaged Scrapy/lxml metadata, source-sync configuration loading, and bounded portable build-cache retention.
- Umbrel/Admin performance and reliability improvements from the container patch series are included, including compact registry source-table payloads, lightweight task-live summary polling, active-fetch timeout backoff, pipeline abort recovery, and active-run-safe bootstrap behavior.

### Notes
- This is the shared desktop and Umbrel Docker release candidate after the container/Umbrel-only patch series from `0.2.44` through `0.2.79`; `v0.2.43` remains the previous public desktop release.
- The container image preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.
- No existing release tags are moved or recreated for this rollup.

## [0.2.79] - 2026-06-16

### Fixed
- Jobs search now tokenizes multi-term queries across a combined title, company, location, source, and URL search index, so searches such as `QLOC Technical Artist` match the recovered QLOC Technical Artist row.
- Jobs updates now refresh the visible feed automatically after a completed update reports fresh data, replacing stale startup/cache rows without requiring a manual Reload.
- Gzip-backed Jobs feed serving now has regression coverage for the expected `Content-Encoding: gzip` response header.

### Notes
- This is a forward container/Umbrel patch for the post-`0.2.78` Jobs UX gap. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the existing QLOC/Elevato ingestion, registry, sync, and source-selection behavior from `0.2.78`.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.78] - 2026-06-16

### Fixed
- Jobs updates now activate the runtime active source registry from the explicit fetch output directory before default loader selection, so container fetch children consume the live SQLite/JSON registry instead of an import-time packaged fallback.
- Targeted `onlySources` selection now resolves dynamic registry-backed static loaders such as `static_source::static:listing_url:https://qloc.elevato.net/en/` before task launch, avoiding zero-loader targeted runs for valid active sources.
- Dynamic `static_source::...` loaders are classified as `static` for incremental cache decisions and source reports, so source-check-only freshness cannot hide QLOC when the published feed has no QLOC row.

### Notes
- This is a forward container/Umbrel correction after the `0.2.77` live QLOC smoke still excluded QLOC as `cache_within_freshness_window` and left `j,240` out of the feed. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.77] - 2026-06-16

### Fixed
- Normal Jobs updates now build static source loaders from the SQLite-backed active source-registry authority before falling back to JSON exports, so active Source Sync/Admin rows such as QLOC are actually selected by the fetcher child process.
- QLOC feed recovery now covers the live `0.2.76` miss where QLOC was active with `jobsFound: 9` but the full Jobs update selected only built-in provider-family loaders, leaving the published feed on stale Google Sheets `j,229` rows and missing the live Elevato `j,240` opening.

### Notes
- This is a forward container/Umbrel correction after the `0.2.76` live QLOC smoke exposed a source-selection gap. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.76] - 2026-06-16

### Fixed
- Static source freshness now ignores source-check-only `nextEligibleCheckAt` state that has no successful feed-producing fetch history, so active Elevato sources such as QLOC run in the next normal Jobs update instead of being skipped as fresh with `lastJobsKept: 0`.
- QLOC feed recovery now covers the live `0.2.75` failure mode where the active registry row had `jobsFound: 9` but the published feed still carried stale Google Sheets `j,229` evidence and lacked the live Elevato `j,240` opening.

### Notes
- This is a forward container/Umbrel correction after the `0.2.75` live QLOC smoke failed. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.75] - 2026-06-16

### Fixed
- Jobs updates now distinguish source-check freshness from feed-producing freshness, so active static sources such as QLOC are not skipped unless their exact source identity is already represented in the published feed.
- Explicit fetch `onlySources` requests now fail fast when no selector matches and otherwise bypass incremental freshness, cadence, and circuit-breaker skips for the selected source.
- Elevato static rows now win over stale Google Sheets Elevato detail rows for the same opening, keeping the live QLOC Technical Artist `j,240` link as the public primary job and hiding the expired `j,229` detail link from the public bundle sample.
- The locked Python dependency set now carries `cryptography 48.0.1` and compatible `pyOpenSSL 26.2.0`, clearing the current pip-audit advisory for container and desktop package builds.

### Notes
- This is a forward container/Umbrel patch for QLOC/Elevato feed recovery. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.74] - 2026-06-15

### Fixed
- Admin/source-check validation now recognizes Elevato comma-style `,j,<id>` job links, so targeted QLOC checks report live job evidence instead of leaving QLOC pending with `jobsFound: 0`.
- Elevato source-check link extraction filters generic "Join <company>" anchors while preserving real openings such as QLOC Technical Artist.

### Notes
- This is a forward container/Umbrel correction for the `0.2.73` QLOC source-check smoke gap. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.73] - 2026-06-15

### Fixed
- Static source discovery and fetching now support Elevato-hosted job boards such as QLOC, including comma-style `,j,<id>` job URLs so the live QLOC Technical Artist opening is detected from the English board.
- Expired Elevato detail pages are treated as empty/removed evidence, while generic "Join <company>" pages and privacy-policy links are filtered out of static job output.

### Notes
- This is a forward container/Umbrel patch for QLOC/Elevato source recovery. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.72] - 2026-06-15

### Fixed
- Admin Ops startup now avoids false-empty and false-healthy panels: Action Center shows a neutral checking state until required signals complete, Source Sync hydrates from live sync status, source-table delayed states stay visible during active work, tab badges show bounded delayed/unavailable states, and Older Runs uses a contained scroll area without broken inline detail rows.
- Desktop lifecycle handling now keeps idle packaged windows alive by sending regular owner heartbeats, while keyboard reloads continue to bypass close shutdown handling.
- Portable desktop builds now keep Scrapy/lxml package metadata needed by fetch subprocesses and prune `_out/portable-build-cache` to a bounded set of recent bundle caches.

### Notes
- This is the Umbrel Docker release candidate used to validate the current Admin, Jobs, source sync, and desktop rollup before a later public desktop tag. No desktop release tag is created by this container publish; `v0.2.43` remains the latest public desktop release until explicit tag approval.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.71] - 2026-06-14

### Fixed
- Admin active-fetch fallback hydration now keeps Stored Profiles, Source Sync, Pipeline schedule, and source-table delayed placeholders populated when `/admin/bootstrap` is unavailable during active job updates, with a smoke-only fail-once gate and in-app Browser proof helper for visual regression checks.

### Notes
- This is a forward container/Umbrel patch for Admin active-fetch false-empty recovery. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.70] - 2026-06-14

### Fixed
- Admin source tables now request the compact `/registry/sources?view=table` payload, preserving table actions and filters while avoiding full source diagnostic fields that made idle Umbrel source loads too large and slow.

### Notes
- This is a forward container/Umbrel patch for bounded Admin registry source-table payloads. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- The registry source-table view is additive; default `/registry/sources` remains full-fidelity and backward compatible.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.69] - 2026-06-13

### Fixed
- Container Admin and Jobs active-fetch polling now use the additive `/ops/task-live/<task>?view=summary` route, keeping live task progress bounded without hydrating full fetch work-item payloads.
- `/ops/task-live/<task>?view=summary` now returns lightweight task identity, status, progress, counts, timestamps, summary, and bounded recent events while preserving the full default task-live payload for diagnostics.
- `/ops/health` now avoids expensive active-run detail work while a pipeline or fetch is active, keeping the existing route shape responsive during broad Umbrel fetches.
- Source-sync shard pushes now serialize GitHub Contents writes to avoid branch-head conflicts when publishing multiple changed shards to the same remote branch.

### Notes
- This is a forward container/Umbrel patch for active-fetch route performance and source-sync recovery. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- The task-live summary route is additive; default `/ops/task-live/<task>` remains full-fidelity and backward compatible.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.68] - 2026-06-13

### Fixed
- Container Admin source tables now treat terminal pipeline control-plane stages such as `canceled` as idle, so a post-abort refresh loads registry source rows instead of staying stuck on "Source tables delayed while job update is running."
- Source-table recovery now clears the recent active-pipeline marker when the fast pipeline status route reports an inactive terminal state, preventing fresh Admin pages from inheriting stale active-fetch deferral.

### Notes
- This is a forward container/Umbrel recovery patch for the incomplete `0.2.67` live smoke. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.67] - 2026-06-13

### Fixed
- Container Admin now replaces successful-but-partial fetch KPI payloads with terminal "No successful fetch yet" or "Not available" copy instead of leaving "Loading latest fetch KPI..." in KPI cards forever.
- Admin source tables now preflight the fast pipeline status route before starting full `/registry/sources` loads, so active Discovery to Fetch transitions render delayed source-table copy without waiting on heavy registry reads.
- Registry source-table HTTP 504s during active pipeline/fetch work now downgrade to the bounded delayed state instead of logging a blocking Admin registry source-table error.
- Admin now stays on compact active-run polling when pipeline or task-state control routes time out during possible active Fetch, Pipeline, or Abort work, avoiding repeated dashboard, registry conflict, and tab-count route pressure.
- Fetch log polling now backs off after repeated timeouts while preserving the last visible progress and log text.
- Pipeline Abort now renders queued/aborting state immediately and keeps active child Fetch rows visible until backend evidence shows they have actually settled.

### Notes
- This is a forward container/Umbrel recovery patch for the incomplete `0.2.66` live smoke. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- Full registry source tables remain diagnostic/operator data and may be delayed while a job update is running; current pipeline visibility and usable Admin navigation remain prioritized.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.66] - 2026-06-13

### Fixed
- Container Admin now keeps the gateway pipeline status snapshot fresh during active Fetch child waits, so `/tasks/run-jobs-pipeline-status` does not freeze on a stale `snapshotAt` while fetch progress continues.
- Admin current runs now lets fresher pipeline status replace stale Discovery child rows when the pipeline advances to Fetch, while preserving richer matching task-state rows.
- Admin source tables and fetch KPI cards now show bounded delayed copy during active pipeline/fetch work instead of indefinite loading placeholders when registry or summary routes are delayed.
- Admin now suppresses Abort buttons for pipeline-owned child rows and keeps Abort scoped to standalone Fetch/Discovery runs plus the Pipeline parent.
- Admin bridge status checks now use `/app/ready` and accept container-gateway ready/degraded payloads so the badge does not briefly report offline while lightweight gateway routes are healthy.

### Notes
- This is a container/Umbrel active-fetch recovery patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- Full registry source tables remain deferred while a job update is running; active pipeline visibility, current Fetch state, and Pipeline Abort stay prioritized through the gateway control plane.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.65] - 2026-06-12

### Fixed
- Container Admin now uses the gateway pipeline status as the authoritative active-task source during running pipelines, including bounded display-only child rows for Fetch, Discovery, and Sync progress.
- Pipeline status snapshots now expose bounded `activeChildren` rows so the gateway can keep Admin task visibility and Pipeline Abort available even when slow internal Ops routes are delayed.
- Admin active-pipeline polling no longer depends on dashboard-health, task-state, fetch KPI, storage-health, or full diagnostics routes for current task rendering.

### Notes
- This is a container/Umbrel control-plane recovery patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- Full Ops diagnostics may still be delayed during running pipelines; task visibility, navigation, and Pipeline Abort are prioritized through the gateway control plane.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.64] - 2026-06-12

### Fixed
- Container gateway proxied bridge responses now strip the upstream `Content-Length` before writing the gateway response length, avoiding duplicate content-length headers that Umbrel's proxy rejected with `HPE_UNEXPECTED_CONTENT_LENGTH`.

### Notes
- This is a forward fix for the failed live smoke of `0.2.63`, where gateway-native routes were healthy but proxied bridge routes returned Umbrel HTML `502` pages. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.63] - 2026-06-12

### Fixed
- Container gateway readiness now distinguishes an alive internal bridge process from a bridge socket that is actually listening, so `/app/ready` reports degraded until proxied bridge routes can respond.
- Container gateway routing now treats `/admin/*` as API traffic instead of static fallback HTML, restoring `/admin/bootstrap` through the internal bridge.
- Container bridge startup no longer performs source-registry ensure work before binding the internal bridge socket, reducing the chance that live `/data` registry reads leave gateway-only control routes up while bridge APIs refuse connections.

### Notes
- This is a forward fix for the failed live smoke of `0.2.62`, where the public gateway was installed but proxied internal bridge routes returned immediate `504 bridge_degraded` responses. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.62] - 2026-06-12

### Changed
- Container/Umbrel runtime now uses a lightweight public gateway in front of the internal bridge so `/app/ready`, pipeline status, startup static assets, and pipeline abort intake remain responsive while heavier diagnostics or pipeline work are busy.
- Container Jobs startup now keeps the first page on the bounded startup feed and defers the full light-feed refresh until explicit reload or a later safe refresh path, avoiding automatic multi-MiB feed reads before first usable UI.

### Notes
- This is a container/Umbrel control-plane recovery patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- Pipeline status and pipeline abort are now resilient through the public gateway; fetch/discovery child abort still depends on the internal bridge.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.61] - 2026-06-12

### Fixed
- Added a minimal `/app/ready` liveness route and made `/ops/health?view=ready` use the same in-memory readiness payload so bridge badges do not wait on Ops/dashboard/report reads during active pipelines.
- Admin now requests `/tasks/run-jobs-pipeline-status` immediately on boot and keeps a current Pipeline row plus Abort action visible when bootstrap, task-state, health, or dashboard routes are delayed.
- Admin bootstrap and task-state failures no longer clear an active pipeline fallback row or force the bridge badge into a blocking offline state while the lightweight pipeline status route remains responsive.
- Admin and Jobs bridge status checks now degrade gracefully during running-task contention instead of blocking navigation or clearing running/abort controls.

### Notes
- This is a container/Umbrel running-task stability patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.60] - 2026-06-12

### Fixed
- Admin deferred panels now avoid false empty or misleading status values while source tables, registry/sync diagnostics, discovery review, dedup lists, and fetch/discovery logs are still loading.
- Admin Ops health now keeps KPI, warning, badge, and schedule state truthful across automatic summary polling and manual refreshes.
- Jobs and Saved navigation keep the Admin entry point available during transient bridge delays, and Jobs preserves active pipeline/Abort state from the lightweight pipeline status route while optional Ops detail is delayed.
- Desktop packaging now includes `admin.html` in the embedded static payload so Admin navigation does not return the generic packaged 404 page.

### Notes
- This is a container/Umbrel smoke build for the latest Admin truthfulness and running-task stability fixes. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.59] - 2026-06-11

### Fixed
- Jobs no longer loads the full fetch report during normal container page startup or navigation; source metadata is deferred until the Data Sources panel is opened.
- Admin Fetcher and Discovery sections now use bounded summaries and short log tails by default, keeping full diagnostics manual or active-task-only.
- Admin discovery/source-table loading no longer marks task and source action buttons as running work when backend task state is idle.
- Jobs idle pipeline checks and the shared Admin bridge button now avoid overlapping status polling once idle state is confirmed.

### Notes
- This is a container/Umbrel frontend data-flow recovery patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- Chrome DevTools traces on live Umbrel remain the acceptance signal for user-visible Admin and Jobs page-load performance.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.58] - 2026-06-11

### Fixed
- Container Admin now applies active Fetcher and Discovery task progress directly from the bounded bootstrap task rows, so current work is visible immediately while full reports hydrate in the background.
- Frontend smoke coverage now matches the load-on-view Admin contract: full Fetcher diagnostics are verified through explicit manual refresh instead of first-load auto fan-out.

### Notes
- This supersedes the unpublished-to-Umbrel `0.2.57` container image, whose GitHub Tests workflow failed on the old Admin diagnostics smoke expectation. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.57] - 2026-06-11

### Changed
- Container Admin startup now uses one bounded `/admin/bootstrap` control-plane route for first-use data instead of fanning out across Ops, Sync, Registry, Discovery, and dashboard routes during first render.
- Admin boot now renders overview summary, current running tasks, two recent runs, and sync readiness from the bootstrap payload, while full diagnostics remain tab-open or manual-refresh work.
- Task lifecycle current/recent reads now trust SQLite authority without falling back to stale JSON lifecycle rows.

### Notes
- This is a container/Umbrel Admin startup recovery patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- Chrome DevTools traces on live Umbrel remain the acceptance signal for user-visible Admin and Jobs page-load performance.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.56] - 2026-06-07

### Fixed
- Container Jobs startup feed export now writes only the bounded startup preview instead of duplicating the full light feed.
- Container static serving repairs upgraded `/data/jobs-unified-startup.json` artifacts that are malformed or larger than the startup preview contract, so upgraded Umbrel installs recover without waiting for another pipeline.

### Notes
- This is a container/Umbrel startup-feed recovery patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.55] - 2026-06-07

### Changed
- Container/Umbrel bridge-started fetch runs now use conservative default concurrency so Admin, Jobs, and lightweight Ops routes remain responsive while a fetch is active.
- Container/Umbrel fetch defaults are now `--max-workers 4`, `--max-per-domain 2`, `--adapter-http-concurrency 16`, and `--static-detail-concurrency 4`; explicit payload overrides still win.

### Notes
- This is a container/Umbrel runtime-pressure recovery patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- Desktop bridge defaults remain unchanged, and the `uncapped` preset remains intentionally aggressive in container mode.
- Chrome DevTools traces during an active fetch remain the primary acceptance signal for Umbrel page-load performance; backend route profiles remain supporting diagnostics.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.54] - 2026-06-07

### Changed
- Container Admin Fetcher and Discovery sections now load from explicit navigation, hash focus, or manual action instead of near-viewport observation.
- Container Admin Fetcher and Discovery focused sections now request bounded recent log tails before continuing live polling from the returned offset.

### Fixed
- Container Admin Discovery manual refresh now uses the bounded log-tail path instead of rendering full historical log DOM.

### Notes
- This is a container/Umbrel Admin log-tail recovery patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- Chrome DevTools traces remain the primary acceptance signal for Umbrel page-load performance; backend route profiles remain supporting diagnostics.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.53] - 2026-06-07

### Changed
- Container Admin now loads deferred panels on view: Ops recent history, Fetcher output, Discovery/source tables, and Sync diagnostics load when their section is focused or near the viewport instead of relying on delayed full diagnostics.
- Ops history now requests only the two most recent completed runs for the initial Admin view; older run history loads only when the older-runs disclosure is opened while current running tasks remain visible from the task summary.
- Deferred Fetcher, Discovery, Sources, and Sync panels now show truthful animated loading states instead of blank static areas or false empty copy.

### Fixed
- Admin run history no longer shows `No run history yet` before the recent-history request has completed.

### Notes
- This is a container/Umbrel Admin load-on-view recovery patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- Chrome DevTools traces remain the primary acceptance signal for Umbrel page-load performance; backend route profiles remain supporting diagnostics.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.52] - 2026-06-06

### Changed
- Container Admin startup now avoids automatic full diagnostics fan-out after first render, keeps Fetcher and Discovery log DOM bounded and lazy, and deduplicates lightweight summary/ready bridge requests.
- Jobs idle polling now avoids repeated task-state and dashboard-health summary calls after the initial idle check while preserving active pipeline, abort, bootstrap, and completion behavior.

### Fixed
- `/discovery/report?view=summary` now uses a bounded startup projection instead of loading and normalizing the full discovery report or materializing large candidate/failure arrays.

### Notes
- This is a container/Umbrel frontend-pressure recovery patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- Chrome DevTools traces remain the primary acceptance signal for Umbrel page-load performance; backend route profiles remain supporting diagnostics.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.51] - 2026-06-06

### Fixed
- Container static serving now handles upgraded Umbrel installs whose backing light jobs feed is gzip-backed, and still returns a bounded generated startup preview if persisting `data/jobs-unified-startup.json` fails.
- This corrects the live `0.2.50` acceptance failure where `data/jobs-unified-startup.json` could remain `404` after update even though `jobs-unified-light.json` was available.

### Notes
- This is a container/Umbrel startup-feed recovery correction. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- Follow-up Chrome DevTools traces should be captured on live `0.2.51` for Admin cold/warm, Jobs cold/warm, Jobs-to-Admin, and Admin-to-Jobs before choosing the next page-load patch.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.50] - 2026-06-06

### Fixed
- Container static serving now backfills a missing `data/jobs-unified-startup.json` from the existing light jobs feed on upgraded Umbrel installs, so Jobs can render a bounded startup preview before the next pipeline run writes the artifact.
- Existing startup artifacts are preserved, and full `jobs-unified-light.json`, `jobs-unified.json`, and CSV contracts remain unchanged.

### Notes
- This is a container/Umbrel startup-feed recovery patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- Follow-up Chrome DevTools traces should be captured on live `0.2.50` for Admin cold/warm, Jobs cold/warm, Jobs-to-Admin, and Admin-to-Jobs before choosing the next page-load patch.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.49] - 2026-06-06

### Changed
- Admin first-load behavior now keeps the initial route set lightweight, with core panels restored and full diagnostics deferred until tab/manual paths.
- Admin source tables now render large Active, Pending, and Rejected source buckets through virtualized rows so source lists remain usable without thousands of DOM nodes.
- Jobs startup now uses a startup feed path and shared feed loading to reduce repeated large-feed work and avoid missing fallback probes.

### Fixed
- Source sync summary status now preserves the resolved enabled state during Admin boot so saving the form cannot accidentally disable sync from a lightweight summary payload.
- Jobs pipeline starts are no longer blocked solely because source sync is degraded; sync failures remain visible while fetch/discovery pipeline work can proceed.
- Bootstrap tests and release checks now account for the generated startup jobs artifact used by the container startup path.

### Notes
- This is a container/Umbrel page-load recovery patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- Chrome-visible Admin and Jobs behavior remains the primary acceptance signal for future Umbrel page-load performance work; backend route profiles are supporting diagnostics.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.48] - 2026-06-06

### Added
- Added a container-only frontend bundling path for Umbrel images. Docker now builds hashed, minified ESM assets for `admin.html`, `jobs.html`, and `saved.html`, serves gzip sidecars when accepted, and keeps checked-in desktop/local HTML behavior as the fallback.
- Added `GET /ops/dashboard-health?view=summary` for Admin first paint. The default `/ops/dashboard-health` route remains the full compatibility payload.

### Changed
- Admin boot now uses the lightweight dashboard summary first, keeps heavy diagnostics deferred until manual/detail paths, and no longer restores full fetch/discovery reports unconditionally on page load.
- `/ops/task-state?view=summary` now builds a true compact projection instead of compacting the full diagnostic task payload.

### Fixed
- Stale running lifecycle rows with terminal progress, stale heartbeat, and no live task evidence are repaired through the task lifecycle path so old sync rows no longer keep Admin in a fake active state.
- Container static serving now prefers generated container frontend assets when present while preserving no-store behavior for HTML/runtime config and immutable caching for hashed bundles.

### Notes
- This is a container/Umbrel performance recovery patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- Chrome DevTools trace evidence is the release acceptance signal for Umbrel page-load performance; backend route profiles remain supporting diagnostics.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.47] - 2026-06-06

### Added
- Added a Chrome DevTools trace summary tool for `.json` and `.json.gz` Performance exports, and optional `perf:complete` ingestion so LCP elements, slow browser resources, user timing spans, and long main-thread tasks are visible beside backend profiling.

### Fixed
- Rolled back the Umbrel container runtime to the `0.2.44` Admin readiness code path after live Chrome traces showed the later Ops route cache/coalescing stack could leave Admin waiting on slow discovery, registry, sync, and dashboard routes for many seconds.
- Restored the earlier Admin behavior where profile overview and sync status render without being blocked by first-load diagnostics fan-out.
- Stopped the Admin first-load path from automatically loading full discovery source/report data; operators can still load source tables manually or through task-completion refreshes.

### Notes
- This is a container/Umbrel recovery patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.
- `0.2.45` and `0.2.46` remain historical evidence, but should be treated as degraded for the private Umbrel install until the Admin boot path is redesigned around Chrome-trace acceptance criteria.

## [0.2.44] - 2026-06-05

### Added
- `/registry/summary?view=exact` now exposes normalized registry summary counts without source rows for diagnostics, while the default `/registry/summary` remains a lightweight storage snapshot.

### Changed
- Admin registry diagnostics now label storage snapshot counts versus normalized counts so duplicate/pending evidence is not overstated.
- Admin now loads local profile overview summary data first, defers exact attachment-size filesystem work to a background full refresh, and exposes bounded overview performance labels for container/Admin profiling.

### Fixed
- Admin Ops now renders a neutral readiness shell during the first dashboard-health request instead of leaving `Loading operations health...` visible while slower Umbrel containers finish the health snapshot.
- Jobs, Saved Jobs, and Admin now avoid passive first-load placeholder copy such as `Loading jobs...`, `Loading saved jobs...`, `Admin Checking...`, and empty discovery/activity text while background startup data is still settling.

### Notes
- This is a container/Umbrel patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.43] - 2026-06-04

### Added
- Desktop release rollup from the last public desktop build, bringing the shared task lifecycle hardening, pipeline start-race handling, packaged source-sync config parity, discovery/report diagnostics, and job company repair work into the packaged desktop channel.

### Changed
- Admin now keeps restore, demote, and delete source bulk actions collapsed as advanced actions before runtime JavaScript finishes loading.
- Saved Jobs now hides workspace metrics while guest, restoring, or waiting for profile rows, avoiding prominent zero-value metrics before the local profile has loaded.

### Fixed
- Source-sync shard garbage collection now ignores malformed remote content entries that do not include a path, removing the blank `skipped invalid source-sync shard GC path:` warning while preserving warnings for real invalid shard paths.

### Notes
- This is the next desktop-facing release identity after `v0.2.25`; `0.2.26` through `0.2.42` were primarily Umbrel/container patch identities but included shared fixes that desktop packaging now receives.
- Live Umbrel evidence for `duplicatePendingCount` remains operator registry state, not a deterministic release-blocking code repair. No live registry files were edited.
- This rollup preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.42] - 2026-06-04

### Fixed
- Discovery failure-attempt diagnostics now classify permanent GameDevMap homepage and directory website DNS/404/410 misses as expected negatives, reducing live Umbrel actionable discovery diagnostics without hiding transient, TLS, 403/5xx, parser, or provider-validation failures.

### Notes
- This is a diagnostics-only Umbrel/container patch. Fetcher parsing, provider scoring, source policy, source sync, public job data contracts, same-origin raw-LAN behavior, and desktop packaging behavior are unchanged.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.41] - 2026-06-04

### Fixed
- Discovery failure-attempt diagnostics now separate expected negative GameDevMap recovery and static probe misses from actionable discovery diagnostics, so generated `/careers` or `/jobs` 404s and stale inferred `careers.*` DNS misses no longer inflate the high-priority failure count.
- GameDevMap recovery planning now carries bounded URL-source metadata, uses path-only recovery labels, and skips secondary generated recovery paths when primary generated paths only returned 404/410 for that studio homepage.

### Notes
- Fetcher parsing, provider scoring, source policy, source sync, public job data contracts, same-origin raw-LAN behavior, and desktop packaging behavior are unchanged.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.40] - 2026-06-04

### Fixed
- Task failure-attempt diagnostics now redact URL-like substrings from bounded example labels, closing the live `0.2.39` smoke blocker where GameDevMap recovery example names could expose raw URLs.

### Notes
- This is a corrective Umbrel/container patch for the `0.2.39` diagnostics route. Fetcher parsing, discovery queue policy, provider scoring, source sync, public job data contracts, same-origin raw-LAN behavior, and desktop packaging behavior are unchanged.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.39] - 2026-06-04

### Added
- Admin Ops now exposes bounded task failure-attempt diagnostics through `/ops/task-failure-attempts`, separating expected fetch cache skips and discovery dedupe/queue/static skips from hard fetch failures and actionable discovery diagnostics.
- The Admin Fetcher diagnostics panel now lazy-loads and renders the failure-attempt summary with copy/refresh support, including high-priority discovery buckets without exposing raw artifact bodies or URLs.

### Notes
- This is a diagnostics-only Umbrel/container patch. Fetcher parsing, discovery queue policy, provider scoring, source sync, public job data contracts, same-origin raw-LAN behavior, and desktop packaging behavior are unchanged.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.
- Live `0.2.38` evidence showed no hard fetch failures, one partial static-source warning, and elevated discovery diagnostics in dedupe skips, GameDevMap recovery fetches, and probes; this patch makes those buckets visible before any behavior-changing follow-up.

## [0.2.38] - 2026-06-04

### Fixed
- Google Sheets company repair now recognizes structured LinkedIn detail URLs with numeric job ids and a small set of first-party game-studio career hosts, repairing currently observed `Unknown company` rows for Scopely, Activision, Techland, Wargaming, Rockstar Games, Santa Monica Studio, Believer, and Rovio when the job link itself carries strong company evidence.
- The shipped-artifact quality gate now checks direct structured job-link company evidence before requiring Grackle bundle evidence, so stale feeds with repairable `Unknown company` rows are classified as blockers instead of weak warnings.

### Notes
- Live Umbrel `0.2.37` audit evidence found 135 `Unknown company` rows; 118 are repairable by this patch and 17 remain weak-evidence rows, mostly generic LinkedIn search/expired redirect URLs plus one Jobvite and one Dayforce URL without safe company evidence.
- Fetch attempt audit found no real fetch failures: 22 sources ran successfully and 2,127 were expected `cache_within_freshness_window` exclusions.
- Discovery failure-attempt audit found high diagnostic buckets in dedupe skips, GameDevMap recovery fetches, and static probes, but no queue-policy or provider-scoring change is justified by this patch.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.37] - 2026-06-04

### Fixed
- Jobs pipeline child waits now extend the absolute report wait cap while the discovery/fetch child has live heartbeat or lifecycle evidence, preventing long but healthy Umbrel fetch merges from failing the parent pipeline before the terminal report is written.

### Notes
- This is a corrective container patch for the 0.2.36 Umbrel manual pipeline smoke failure where fetch completed all 555 source tasks and entered merge, but the parent pipeline failed with `fetch_wait: fetch report exceeded absolute safety cap`.
- Terminal child lifecycle rows still fail or cancel the parent promptly when the expected report is missing or unfinished; stale children without live evidence still hit the quiet timeout path.
- Fetcher parsing, provider quality rules, source policy, sync contracts, raw-LAN same-origin behavior, and desktop packaging behavior are unchanged.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.36] - 2026-06-03

### Fixed
- Load-time registry safe-demotion now preserves active rows that were approved by discovery auto-approval, so terminal discovery report reconciliation is not immediately undone by routine registry normalization or auto-sync reads.
- Terminal discovery registry reconciliation now stays durable across the normal registry service load path when completed reports declare auto-approved duplicate candidates as active.

### Notes
- This is a corrective container patch for the 0.2.35 Umbrel verification failure where the registry briefly repaired to the completed report counts and then reverted after load-time safe demotion.
- Manual Admin conflict safe-demotion remains available; this change only protects discovery auto-approved active rows from automatic load-time cleanup.
- Fetcher parsing, provider quality rules, source policy, sync contracts, raw-LAN same-origin behavior, and desktop packaging behavior are unchanged.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.35] - 2026-06-03

### Fixed
- Terminal discovery registry reconciliation now also replays report-declared `discovery_auto_approve` promotions that were already stamped into the completed discovery report, repairing stale active/pending counts when eligibility replay alone cannot reconstruct the worker's final registry state.

### Notes
- This is a corrective container patch for the 0.2.34 Umbrel verification failure where `/discovery/report` still declared `active=2301/pending=811` while registry routes remained at `active=2289/pending=823` after update.
- Fetcher parsing, provider quality rules, source policy, sync contracts, raw-LAN same-origin behavior, and desktop packaging behavior are unchanged.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.34] - 2026-06-03

### Fixed
- Terminal discovery reports now reconcile report-declared auto-approval through the bridge registry authority, repairing stale registry bucket counts before `/discovery/report` is served or a new discovery starts.
- Jobs pipeline child waits now stop promptly when discovery or fetch child lifecycle rows terminalize without a matching terminal report, avoiding long absolute safety-cap waits.

### Notes
- Fetcher parsing, provider quality rules, source policy, sync contracts, raw-LAN same-origin behavior, and desktop packaging behavior are unchanged.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.33] - 2026-06-03

### Fixed
- Discovery auto-sync watching now waits for terminal registry finalization and auto-approval status before processing completed reports, preventing the bridge watcher from overwriting the final discovery report with an intermediate `running` finalization payload.

### Notes
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.32] - 2026-06-03

### Added
- Admin Bridge now exposes a lightweight `/registry/summary` response and a combined `/registry/sources` source-table response so Admin can refresh registry views without three separate full registry loads.

### Fixed
- Discovery completion watching now waits for registry finalization and auto-approval terminal status before refreshing source tables, avoiding misleading post-discovery registry timeout warnings on Umbrel.
- Admin background source-table refreshes now use a longer bounded timeout, preserve existing rows on delayed refreshes, and log delayed refreshes separately from discovery worker failures.

### Notes
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.31] - 2026-06-02

### Added
- Admin Ops now exposes bounded discovery audit artifact diagnostics for known audit files under the active data directory.
- Windows Docker smoke builds can use a clean committed `git archive` context when live workspace reparse points block `docker build .`.

### Fixed
- Jobs pipeline starts now verify live pipeline status before showing a start failure, avoiding a false error toast when the start POST times out after the bridge has accepted the run.
- Published container images now generate the portable encrypted GitHub App source-sync config from BuildKit secrets, matching desktop packaged sync behavior for Umbrel installs.

### Notes
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.30] - 2026-06-02

### Fixed
- Umbrel discovery tasks now write sheet-directory and web-search audit artifacts under the `/data` volume in container mode instead of the unwritable app directory.
- Discovery task reports now self-repair from terminal lifecycle state after child crashes, avoiding stale active `/discovery/report` payloads and long pipeline safety-cap waits.
- POSIX bridge PID checks now reject zombie child processes so container task lifecycle liveness is not falsely extended.

### Notes
- Fetcher parsing, provider quality rules, source policy, and desktop/non-container discovery audit path behavior are unchanged.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.29] - 2026-06-02

### Fixed
- Umbrel container Admin now preserves the explicit same-origin bridge base, fixing Admin panels that incorrectly called the visitor browser's `127.0.0.1:8877` instead of the LAN app origin.

### Notes
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.28] - 2026-06-02

### Fixed
- Umbrel app metadata now lets `app_proxy` own raw-LAN port `8877` and removes the duplicate `web` container host-port mapping that caused Docker install failures with `port is already allocated`.

### Notes
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.27] - 2026-06-01

### Fixed
- Umbrel container startup now prepares the `/data` bind mount before dropping to the non-root runtime user, fixing first-run seeding on root-owned Umbrel app data directories.

### Notes
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.26] - 2026-06-01

### Added
- Baluffo can now run as a same-origin Linux container for private Umbrel raw-LAN installs, with GHCR multi-arch image publishing and private community app-store metadata.

### Fixed
- Umbrel app metadata initially added a direct host-port mapping for `8877`; this was corrected in `0.2.28` to avoid conflicting with Umbrel's `app_proxy` port ownership.
- Container mode no longer emits wildcard browser CORS allow headers, while desktop localhost bridge compatibility keeps its existing split-origin behavior.

## [0.2.25] - 2026-06-01

### Added
- Admin Ops can now enable a bridge-owned recurring full Jobs pipeline schedule with a configurable whole-hour interval.
- Jobs and Admin Ops now expose confirmed abort controls for active discovery, fetch/bootstrap, and full Jobs pipeline runs through runId-scoped task cancellation.
- Release preflight now includes a packaged bridge/runtime rehearsal that proves task abort lifecycle evidence and one recurring Jobs pipeline scheduler trigger.
- Source-discovery hardening now includes broader ATS HTML-signature detection, independent `/jobs` Playwright fallback checks, and a jobs artifact quality gate for title/location contamination.

### Changed
- Jobs no longer shows the recent views bar, keeping the page focused on current pipeline and feed state.
- Static source inference now treats dead-listing retries, SPA shell signals, non-English career terms, and custom-domain ATS signatures as first-class discovery evidence.

### Fixed
- Task abort lifecycle closeout now keeps user-canceled evidence sticky across late fetch/discovery reports, watcher cleanup, startup cleanup, and pipeline child terminal races.
- Packaged desktop Jobs-to-Admin navigation no longer sends a regular desktop-close lifecycle signal, and the packaged lifecycle rehearsal now covers the navigation path.
- Desktop close cleanup now keeps packaged shutdown tied to real lifecycle state and avoids lingering active-task close rehearsal failures.
- Packaged desktop updater rehearsal now waits for the updater helper to finish and fails on helper terminal errors instead of reporting a false pass.
- Google Sheets and static-source cleanup now repairs category-style titles, redirect-derived company leaks, and container artifact titles before downstream job output.
- Remote Python CI and packaged rehearsal data-root checks no longer fail on stale runtime assumptions.

## [0.2.18] - 2026-05-25

### Added
- Linux packaged desktop support now includes platform abstraction, credential storage support, launch scripts, AppRun/desktop metadata, AppImage packaging, and Linux smoke tooling.
- Release automation now publishes a Linux AppImage alongside the Windows portable and ship-bundle assets for `v*` release tags.

### Changed
- Windows packaged desktop data now defaults to `%APPDATA%\Baluffo`, with first-launch legacy `ship\data` migration and migration reports.
- Desktop updater handoff, relaunch, rollback, and success-marker paths now preserve the planned external data root instead of deriving state from legacy `ship\data`.

### Fixed
- Linux CI no longer fails Windows desktop compat tests by resolving Windows-specific facade calls through Linux stubs.
- CI complexity checks now keep the Ruff baseline metadata aligned with the pinned Ruff version.

## [0.2.17] - 2026-05-23

### Fixed
- First-run Google Sheets bootstrap now avoids duplicate Retry launches after a feed exists and keeps/rechecks progress during long redirect and title-hydration phases before showing timeout.
- Jobs first-run Retry now loads an already completed runtime feed before trying to start another bootstrap.
- Packaged first-run smoke now exercises the real Jobs UI bootstrap request under a long-active heartbeat mode, catching timeout/recovery regressions without live Google Sheets.

## [0.2.16] - 2026-05-23

### Fixed
- Desktop launcher shutdown now pins post-handoff window liveness to the managed browser PID, so unrelated Baluffo-titled windows cannot keep packaged lifecycle shutdown alive.
- Remote Python CI now preserves carried `sourceBundle` evidence when seeding existing Jobs output and keeps source-policy review candidates blocked when provider validation evidence is explicitly not OK.
- First-run Google Sheets bootstrap no longer live-validates thousands of category rows that would be dropped anyway, and the UI now stays in progress while backend heartbeats remain fresh.
- Google Sheets category-style titles now must repair, hydrate, or drop, with bounded `404`/`410` link validation for suspicious category rows only.
- Google Sheets URL-derived title repair now strips opaque ATS/job ID affixes and skips pure posting-code path segments without hardcoding specific providers.
- Google Sheets provider title hydration now supports Ashby hosted-board pages for `jobs.ashbyhq.com/{board}/{posting_id}` links.
- Google Sheets provider title hydration now supports Workable widget feeds for `apply.workable.com/{account}/j/{shortcode}` links.
- Remote OK now reports a successful empty source when all valid feed rows are filtered out by sanitizer rules.
- Remote OK parser filtering now rejects generic community and open-pool non-job titles such as `Join Our Community`.
- Remote OK parser filtering now ignores description-only game keyword matches, reducing non-game remote job contamination before canonicalization.

## [0.2.15] - 2026-05-20

### Added
- Oracle HCM provider API support, including provider inference, JSON parsing, adapter registration, and fixture-backed coverage.
- Provider coverage migration tooling that stages pending provider candidates, reports validation gaps, and recommends focused next actions without requiring a full discovery rerun.
- Google Sheets and static-source title sanitization evidence, including an audit helper and regression corpus for noisy or source-name-only titles.
- Deterministic first-run Jobs regression coverage for packaged desktop bootstrap, retry, and feed-loading behavior.

### Changed
- Source-policy soak reports now distinguish provider migration staging, pending-provider fetch evidence, unsupported ATS advisories, and provider validation debugging.
- Jobs title normalization now preserves useful role specificity while rejecting source names, location fragments, and non-job boilerplate before rows reach reports or the frontend feed.
- Packaged first-run Jobs startup now uses tighter cache-busting, runtime-state, and bootstrap guards for stale bundled/runtime artifacts.
- Release and testing docs now describe the first-run packaged smoke lane and the Python dependency security audit path.

### Fixed
- Closing the packaged desktop browser window no longer leaves the launcher, site child, or bridge child running because `/ops/health` polling can no longer refresh desktop-window owner activity.
- First-run Jobs regressions after `0.2.1` no longer show stale packaged rows, loop bootstrap retries, or leave the page in a blank no-data state while the starter feed is being prepared.
- Google Sheets and static-source rows with source-name or boilerplate titles are sanitized or dropped consistently before dedup, reports, storage, and frontend rendering.
- Provider migration validation can now fetch explicitly staged pending provider rows without changing default fetch behavior or promoting local registry state.
- Remote CI gates are aligned with the new Oracle HCM provider defaults and the dependency security audit no longer fails on `idna`.

### Security
- Packaged source-sync private keys now use a `v2.` AES-GCM envelope with HKDF-SHA256 machine/embedded derivation and PBKDF2-HMAC-SHA256 passphrase derivation, while legacy no-prefix packaged configs remain decryptable.
- The sync config build helper no longer generates plaintext private-key configs, and sync/package warnings avoid echoing sensitive-looking build inputs or remote snapshot key names.
- Updated the locked Python dependency `idna` to `3.15` to resolve `CVE-2026-45409`.

## [0.2.1] - 2026-05-18

### Added
- Saved Jobs tracking polish, including phase history rendering, clearer action state, activity/timeline refinements, and attachment hardening.
- Previous release-note viewing in the desktop update UI, so users can inspect earlier published release details.
- Windows desktop sessions now flash the Baluffo taskbar button when a long Jobs pipeline run finishes in the background.
- A first-run Jobs notice that explains the starter Google Sheets bootstrap and its expected duration.

### Changed
- Saved Jobs action clarity and phase tracker presentation were tightened for repeated tracking workflows.
- AI/docs routing, Basic Memory closeout policy, and refactoring-analysis guidance were updated for future maintenance sessions.
- First-run Jobs pipeline tooltip and status copy now describe the bootstrap phase instead of the normal refresh cadence.

### Fixed
- First-run Jobs now suppresses stale packaged/runtime rows, starts one Google Sheets bootstrap, serves the promoted feed after success, and avoids the repeated fetch loop.
- Admin and Jobs navigation no longer pay the one-minute cold-start validation cost after first-run bootstrap recovery.
- Jobs rows with empty normalized titles are filtered before render, and the first-run empty state now explains that jobs are still being prepared.
- Saved Jobs attachment, tracking, grouping, and revert edge cases were hardened across browser and desktop local-data paths.

## [0.2.01] - 2026-05-16

### Changed
- Portable ZIP builds now embed only the required `chromium_headless_shell-*` Playwright browser payload, keeping offline browser fallback self-contained while avoiding unrelated browser cache siblings.
- No-openings detection now requires explicit visible empty-state evidence, and source reports keep hidden/script/template text and all-canonical-dropped rows in review instead of treating them as legitimate empty sources.
- Location sanity checks now preserve real city names such as Milan, Tel Aviv, and Frankfurt am Main, and treat `Unknown` country values as missing-country placeholders rather than contamination.

### Fixed
- Windows portable updater handoff confirmation no longer falsely rejects a live launcher when packaged runtimes lack optional `psutil`.
- Updater handoff failures now record non-secret diagnostics and clear stale post-install success markers before a fresh install handoff.
- Desktop update manifests for this release require updater capability `2.0.1`, so affected older clients stop attempting the broken automatic install path for future releases.

## [0.2.0] - 2026-05-15

### Added
- A more polished desktop Jobs experience, with denser job rows, clearer save/open actions, user-facing update controls, and quick-filter presets for common browsing flows.
- A safer Saved Jobs workflow, including contextual phase overrides, clearer remove/undo behavior, and an activity timeline that opens with useful defaults.
- A stronger Admin operations view with clearer run history, selected-run analysis, pipeline diagnostics, warning explanations, and advanced bulk actions kept behind an explicit disclosure.
- Runtime SQLite/WAL storage for task history, sync runs, source runs, jobs feed exports, and source registry rows, while keeping compatibility exports available for existing flows.
- Source-sync v3 with content-addressed shard bundles, changed-shard uploads, pull no-op detection, push progress, bounded cleanup, and stronger validation.
- New source-policy, provider/static, registry-conflict, and dedup review tools that make risky source changes easier to inspect before applying.
- Performance, release-safety, and repo-safety tooling, including startup probes, benchmark reporting, packaged desktop rehearsals, secret scanning, dependency audit wiring, and bridge route inventory checks.

### Changed
- Jobs discovery, fetching, sync, and lifecycle internals were split into smaller, more testable modules without changing the normal user workflow.
- Packaged desktop builds now include the storage/runtime pieces needed for the newer local storage and sync paths.
- Admin startup and heavy review panels now defer more expensive work, improving first-load behavior while preserving access to detailed diagnostics.
- Documentation was reorganized around the active docs index, release guide, storage/sync contracts, source-policy runbook, testing guide, and AI/tooling guardrails.

### Fixed
- Desktop startup, bridge ownership, browser shutdown, updater handoff, and packaged startup readiness are more reliable across Windows desktop sessions.
- Pipeline and fetch lifecycle tracking now uses stronger task authority and better evidence, so Admin progress and diagnostics avoid stale or placeholder state.
- Source-sync writes, retries, snapshot limits, checkpoint tagging, and source-health parity were hardened.
- Source registry conflicts, provider/static overlap, dedup review pressure, Google Sheets role buckets, and static-source conflict handling now produce clearer review evidence.
- Saved Jobs back navigation, activity timeline close behavior, phase override flow, remove action, and scrollbar styling were polished.
- Admin operations rows, completed-run ordering, pipeline summaries, and diagnostics copy now render more consistently.

### Security
- Added gitleaks-based secret scanning and Python dependency audit coverage to the local and release-safety workflow.
- Updated dependency and packaging guardrails used by the desktop release path.

## [0.1.33] - 2026-04-20

### Changed
- The desktop runtime has been modularized into focused `src/ship/desktop_app/` package modules (`launcher`, `startup`, `browser`, `session`, `_windows`, `config`, `process`) behind the existing `src.ship.desktop_app` compatibility facade, and the desktop ownership docs now point editors to those focused boundaries instead of the old monolithic module.
- Windows release-preflight now includes dedicated packaged rehearsal lanes for stale-runtime orphan reclaim and managed Chromium browser-job shutdown propagation, keeping the packaged smoke gate aligned with the hardened desktop supervision path.
- Uncapped fetch now reuses the regular fetch launch/runtime path with a narrower `50 / 5 / 10` overlay, seeds existing output during force-refresh runs, and enables a deeper uncapped static profile instead of maintaining a separate aggressive behavior tree.
- Packaged `scrapy_static_sources` fallback processing now runs as a bounded parallel queue with live heartbeat/progress reporting, and the Admin fetch UI surfaces that tail as an explicit `Browser fallback X/Y` progress badge instead of leaving the last running work item opaque.
- Portable builds now bundle the Scrapy fallback runtime stack needed by packaged child runners, including the `scrapy`, `scrapy_playwright`, and `twisted` runtime path.
- Jobs-page desktop updater install confirmation now falls back cleanly when the richer dialog hook is unavailable, and packaged updater rehearsal now proves `handoff-requested.json` plus an in-flight handoff state before treating launcher exit as a valid install transition.
- Desktop startup probing on the current public release line continues to use the more isolated policy and telemetry path introduced in the recent desktop startup hardening work.
- Packaged desktop smoke and CI release gates on the current public release line continue to isolate Playwright bridge local data from repo-local desktop session state so the bridge-release lane starts from a clean guest profile.

### Fixed
- Windows desktop supervision is now substantially harder to escape: launcher-managed `site`, `bridge`, and managed Chromium processes are attached more strictly to the desktop Job Object, stale runtime children can be reclaimed safely on startup, and detached Chromium handoff no longer leaves the launcher waiting for the bridge's two-minute owner-idle fallback after the Baluffo window is already gone.
- Linux CI desktop-app tests no longer fail spuriously on non-Windows runners by assuming Windows-only `src.ship.desktop_app` globals exist at import time; the Windows helper tests now inject their own shimmed surface instead.
- Desktop bridge/update imports on the current public release line no longer fail across source runtime startup, packaged updater handoff, or release-preflight test collection when `src.ship.desktop_app` and `src.ship.desktop_update` are loaded through different packaged surfaces.
- Packaged static-scrapy runners no longer relaunch `Baluffo.exe` as a second top-level desktop instance in frozen mode; packaged fallback execution now dispatches through the child-script path instead.
- Packaged uncapped fetch no longer leaves `scrapy_static_sources` looking frozen as an opaque final work item while the browser-fallback queue is still advancing.
- Desktop updater status no longer regresses handoff/install-ready state back to `ready` merely because the downloaded ZIP still exists while the updater is already in handoff/install states.
- Desktop update install start now refuses to report success unless durable launcher handoff is confirmed against the live launcher session, so first-click install attempts no longer silently no-op or snap back to `Install and restart` when handoff confirmation fails.
- Startup metrics on the current public release line continue to preserve the authoritative ordering for browser launch, shell-window visibility, and runtime readiness.

## [0.1.32] - 2026-04-19

### Changed
- Desktop update and release-note dialogs now use the newer polished popup presentation layer, and the Saved page received additional UI polish around the activity/workspace flow and local-profile modal presentation.
- Frontend styles now ship as split shared/page-scoped assets under `styles/` (`base.css`, `components.css`, `jobs.css`, `saved.css`, `admin.css`), and release/runtime packaging was updated to include that new asset layout.
- Desktop startup probing on the current public release line continues to use the more isolated policy and telemetry path introduced in the recent desktop startup hardening work.
- Packaged desktop smoke and CI release gates on the current public release line continue to isolate Playwright bridge local data from repo-local desktop session state so the bridge-release lane starts from a clean guest profile.

### Fixed
- Desktop update handoff and recovery no longer get stuck in a stale relaunch state after an install-ready update or updater transition.
- Packaged GitHub HTTPS traffic now shares the same trust fallback across source sync and desktop update flows, including the updater helper, and the preferred PEM override is `BALUFFO_GITHUB_CA_BUNDLE` with sync-only and update-only compatibility envs still supported.
- Packaged source sync no longer bypasses the shared GitHub TLS context on the normal runtime `urlopen` path, so the portable desktop now applies the same certificate trust fallback in real sync requests that desktop update already used.
- Startup metrics on the current public release line continue to preserve the authoritative ordering for browser launch, shell-window visibility, and runtime readiness.
- Jobs-page shared action styling was restored after the stylesheet split, including the `Refresh Jobs` / `Run Discovery + Fetch + Sync` buttons and the bottom `Admin Online` status pill.
- Jobs-page pagination spacing was corrected so the pager no longer sits flush against the end of the jobs table.

## [0.1.31] - 2026-04-19

### Changed
- Desktop release version ordering now follows Baluffo's `0.1.x` scheme across the updater, recovery manager, and release tooling, and `0.1.31` is the compatibility bridge that outranks both legacy semver releases like `0.1.23` and current Baluffo-ordered releases like `0.1.3` and `0.1.29`.
- `v0.1.31` is the first public release intentionally chosen to satisfy both the old semver updater population and the newer Baluffo-specific updater ordering.
- Desktop startup probing still uses the more isolated policy and telemetry path introduced on this release line, and the compatibility bridge keeps that runtime behavior as the current shipped desktop.
- Packaged desktop smoke and CI release gates continue to isolate Playwright bridge local data from repo-local desktop session state so the bridge-release lane starts from a clean guest profile.

### Fixed
- The packaged desktop now reports its intended `0.1.31` app version, and mixed-client update populations can converge on the same release without contradictory `Current` / `Latest` states.
- Startup metrics continue to preserve the authoritative ordering for browser launch, shell-window visibility, and runtime readiness on the current release line.

## [0.1.3] - 2026-04-19

### Changed
- Desktop startup probing now uses a more isolated policy and telemetry path, with tighter readiness checks, faster Chromium launch timing, and lower-overhead paired startup profiling.
- Portable release packaging now trims redundant payload size and hardens updater and runtime recovery behavior around staged startup ordering and launch diagnostics.
- Packaged desktop smoke and CI release gates now isolate Playwright bridge local data from repo-local desktop session state so the release lane starts from a clean guest profile.
- Packaged desktop startup probing, crash coverage, and updater finalize/retry behavior were hardened so release-preflight and smoke lanes stay aligned with the shipped runtime.
- Desktop first-use flow now explains guest-mode persistence, lists existing local desktop profiles before sign-in, shows the installed app version in page chrome, and reframes the initial Admin no-fetch state as guidance instead of an unexpected error.
- Release-notes and desktop update UI wording were tightened around finalize/retry and startup resilience.
- Static listing/detail completeness caps were removed so the fetcher can keep pursuing valid zero-yield and residual detail paths instead of cutting them off early.
- Static traversal now prioritizes recall again without giving up the async transport, capped Playwright, and packaged-runtime throughput improvements that stabilized cold fetches.

### Fixed
- Packaged desktop startup now keeps Jobs, Saved, and Admin navigation state stable during startup handoff and no longer regresses the unload prompt during in-app page switches.
- Startup metrics now preserve the authoritative ordering for browser launch, shell-window visibility, and runtime readiness so packaged startup smoke and profiling report the correct sequence.
- Local CI gate regressions across ship-bundle, runtime, and packaged smoke coverage are resolved so the canonical release-preflight lane stays green on the release commit.
- Desktop startup/update resilience regressions around launch handoff, stale launch retry paths, and packaged crash recovery were removed, including cleanup of the unused desktop launch retry helper.
- Desktop sign-in no longer falls back silently to blind profile-name entry when profile listing fails; it now requires explicit `Retry`, `Create new profile`, or `Cancel`.
- The first-run `fetch_never_run` Admin guidance can no longer be dismissed away before a successful fetch clears the condition.
- Packaged cold fetch validation stayed in the fast runtime class while slightly improving final merged output after the static completeness rollback.

## [0.1.23] - 2026-04-17

### Changed
- Desktop startup probing now uses a more isolated policy and telemetry path, with tighter readiness checks, faster Chromium launch timing, and lower-overhead paired startup profiling.
- Portable release packaging now trims redundant payload size and hardens updater and runtime recovery behavior around staged startup ordering and launch diagnostics.
- Packaged desktop smoke and CI release gates now isolate Playwright bridge local data from repo-local desktop session state so the release lane starts from a clean guest profile.

### Fixed
- Packaged desktop startup now keeps Jobs, Saved, and Admin navigation state stable during startup handoff and no longer regresses the unload prompt during in-app page switches.
- Startup metrics now preserve the authoritative ordering for browser launch, shell-window visibility, and runtime readiness so packaged startup smoke and profiling report the correct sequence.
- Local CI gate regressions across ship-bundle, runtime, and packaged smoke coverage are resolved so the canonical release-preflight lane stays green on the release commit.

## [0.1.22] - 2026-04-16

### Changed
- The desktop Jobs-page updater now surfaces persisted background download failures directly in the update panel instead of falling back to the generic available-update state.
- Release and troubleshooting documentation now describe the explicit failed-download retry path for the portable desktop updater.

### Fixed
- Desktop update downloads that fail in the background now keep the panel open, show the persisted updater error, and offer a direct `Try download again` action.
- Failed portable ZIP downloads now clear stale install-ready state and best-effort delete bad staged artifacts so retry starts from a clean updater state.

## [0.1.21] - 2026-04-16

### Fixed
- Jobs-page desktop job links now open in the default browser again instead of failing when the bridge request path duplicated the local bridge base URL.

## [0.1.2] - 2026-04-15

### Fixed
- Desktop navigation to Admin and Saved no longer prompts to save and closes the app window; the packaged desktop pages now retain the Baluffo window identity token during in-app page switches.

## [0.1.1] - 2026-04-15

### Added
- Desktop in-app update flow in the Jobs desktop UI, backed by a signed GitHub release-manifest pipeline for portable releases.
- Packaged updater rehearsal coverage and release diagnostics for the helper-driven `N -> N+1` install path.
- Shared city-noise and country-acceptance contracts, plus regression coverage for exact junk tokens, country promotion, and backend/frontend location parity.
- Jobs-page pipeline progress reporting, terminal-success packaged smoke coverage, and backend regression coverage for the worker path and bridge wiring.

### Changed
- City parsing now normalizes multi-location strings, dedupes bilingual variants, and rebuilds location summaries from the surviving normalized locations.
- Country-like city values such as `EU & NA` and `UK` are now promoted into the country field instead of being dropped, while valid cities remain untouched.
- Location normalization was consolidated into the canonical parsers path and mirrored in the frontend jobs domain so backend and UI stay aligned.
- Local portable builds now mirror successful `dist\baluffo-portable\Baluffo.exe` outputs to `_out\latest\build\portable\Baluffo.exe` so the latest path does not stay stale.
- Desktop updater install handoff, helper progress tracking, and packaged recovery behavior were hardened so portable releases update more reliably.
- Release tooling and packaged verification docs now reflect the current desktop build, smoke, and update pipeline.

### Fixed
- Exact city garbage, prose bleed, and chrome-like location fragments are now rejected consistently across the audit, canonicalization, and frontend normalization paths.
- The Sega M Electrical Products row no longer gets forced into the `Game` sector classification.
- Country picker dropdown now closes reliably when clicking outside it or pressing `Escape`, matching the shared popup behavior in the Jobs page.
- Source sync can now be pointed at a custom PEM CA bundle via `BALUFFO_SYNC_CA_BUNDLE` for machines with a nonstandard trust store or TLS-inspecting proxy.
- Jobs-page pipeline runs no longer fail at runtime with `'PipelineService' object has no attribute '_load_json_object'`.
- The packaged Jobs-page pipeline smoke now fails on backend worker errors after startup instead of passing once the button briefly enters a busy state.
- Packaged desktop update checks now resolve the correct release repo, avoid relaunch loops, and handle cross-platform release paths correctly.
- Closing the packaged desktop window now tears down the desktop session cleanly instead of leaving stray `Baluffo.exe` processes behind.
- Pre-submit parity and CI gate regressions that blocked the packaged release flow were corrected for the `0.1.1` release line.

## [0.1.0] - 2026-04-10

### Added
- Dedicated Jobs-page packaged smoke lane that proves the pipeline can be launched from Jobs without opening Admin.
- Changelog-backed release-note extraction for tagged releases.
- Shared dead-listing gate for static and generic careers extraction so regular pages reject as `dead_listing_page`
- Provenance-based game-sector normalization instead of a raw source-sector override
- Admin restore hooks for fetch and discovery progress after navigating away and back
- Better public-link rewriting for provider rows that exposed raw API URLs
- Transition-aware source registry sync with per-source merge, schema v2 snapshots, and local tombstone-backed deletes
- Explicit registry restore-deleted flow for locally removed sources

### Changed
- Discovery auto-approval now uses explicit eligibility rules and keeps `weakSignal` as diagnostics only.
- GitHub release notes are generated from the top versioned section of `docs/CHANGELOG.md`.
- Ship-bundle release builds use the canonical `python` entrypoint instead of `py -3.13`.
- Discovery preset semantics swapped in place: `default` now uses the former uncapped-lite behavior, and `uncapped` is the broader exploration preset
- Static plugin fallback metadata is now centralized in a shared helper to reduce duplicated boilerplate across host adapters
- Jobs UI link handling normalizes RemoteOK detail URLs to the safer listing page
- City and country filter normalization was tightened to reject obvious non-location contamination
- k-ID no longer needs a source-specific suppressor plugin; the shared dead-listing gate now handles it
- Source sync now pushes only active and pending rows; rejected stays local and tombstones are never serialized remotely
- Retired `scraping-pipeline-run-notes.md` from the docs archive; use git history for the outdated 2026-03-17 run notes.

### Fixed
- Legacy sync merge comparison no longer prefers stale remote rows when transition metadata is missing on the local side.
- SmartRecruiters API links now rewrite to the public posting URL
- Game-company rows now stay classified as `Game` when provenance or company evidence supports it
- Misclassified regular pages such as About / Contact / Careers landing pages no longer become synthetic job entries
- Static extraction now stops leaking a few repeated metadata payload shapes through copy-pasted per-plugin dict construction

## [0.0.15] - 2026-03-30

### Added
- Full Milestone 1-6 roadmap delivery (health scoring, taxonomy, discovery promotion, static adapter hardening)
- Enhanced static adapter with generic fallback heuristics and location fixes
- Provenance-based game classification
- Discovery promotion pipeline with structured migration
- Browser fallback circuit breaker
- Admin bridge refactoring with improved task lifecycle and busy-state handling
- M4-M6 social experiment reporting
- Complete lint infrastructure (Python + JavaScript/ESLint + pre-commit)
- Fetch artifacts refresh and audit tooling

### Changed
- Various bug fixes and code quality improvements

### Fixed
- Multiple bug fixes from M1-M6 delivery

---

## [0.0.10] — 2026-03-23

### Added
- Release 0.0.10 with sync, pipeline, and discovery fixes

### Notes
- The public app release line is `v0.0.x`.
- Git tags follow `v<app_version>` and, for this historical release entry, the tagged release was `v0.0.10`.

---

## Legacy notes

The notes below were retained from the earlier draft release history and are now treated as historical implementation notes, not separate shipped release lines.

### Admin bridge and runtime rewrite
- Admin bridge extracted to modular services (`src/bridge/`)
- Source check API with Playwright fallback for static sources
- Task history and run history API
- Ops health and alerts system
- Jobs pipeline refactored with separate loader selection and runtime phases
- Static adapter now dispatches to plugins via `AdapterPluginContext`
- Frontend state-hub for cross-module state management
- Browser queue URL collapse by source ID
- Activision canonical listing URL resolution

### Shipping and discovery foundation
- GitHub App-based source sync for multi-PC workflows
- Source discovery package (`src/source_discovery/`) reorganized
- Static adapter plugin system for studio-specific parsing

### Browser-required and initial release work
- Playwright fallback for static source discovery and scraping
- Scrapy-Playwright integration for browser-required sources
- Admin discovery log live tailing
- 403/timeout handling in discovery probe
- Generic static source classification
- Initial release: job aggregation from Google Sheets, Remote OK, provider APIs (Greenhouse, Lever, etc.)
- Static studio page scraping
- Source discovery with web search and probing
- Admin console for source management
- Saved jobs with notes and attachments
- Local-first storage (IndexedDB + file-based)

## Known Issues

| Issue | Status | Workaround |
|-------|--------|------------|
| Some static sources still return 0 jobs | Open | Use browser fallback queue |
| Social sources may miss recent posts | Open | Adjust lookback window |

---

## Version History

- [0.0.10] — 2026-03-23
- [0.0.9] — 2026-03-23
- [0.0.8] — 2026-03-20
- [0.0.7] — 2026-03-20

For older shipped tags, see `v0.0.1` through `v0.0.6`.

*For older releases, see the older versioned sections in this changelog.*
