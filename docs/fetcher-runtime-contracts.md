# Fetcher Runtime and Admin Contracts

> - **Status:** Active
> - **Use this when:** changing jobs fetcher CLI flags, admin fetch presets, runtime files, or source-state and circuit-breaker behavior
> - **Canonical for:** fetcher runtime options, admin preset wiring, and fetch-run artifacts consumed by admin flows
> - **Not canonical for:** full jobs pipeline ownership or broad testing strategy
> - **Then inspect:** `src/jobs_fetcher.py`, `src/jobs/fetcher_compat_{exports,runtime}.py`, `src/jobs/pipeline*.py`, `src/jobs/state*.py`, and [`testing.md`](testing.md)
> - **Last updated:** 2026-05-17

## CLI runtime options

- `--max-workers` (default `12`): max concurrent source loaders.
- `--max-per-domain` (default `3`): max concurrent requests per host across workers.
- `--fetch-strategy` (default `auto`): transport preference (`auto`, `http`, `browser`).
- `--adapter-http-concurrency` (default `48`): async HTTP client connection pool size.
- `--static-detail-concurrency` (default `10`): static detail-page fetch concurrency per source before per-domain limiting.
- `--skip-successful-sources`: incremental mode, skips sources recently successful within TTL.
- `--source-ttl-minutes` (default `360`): TTL window for incremental skip.
- `--respect-source-cadence`: applies hot/cold cadence skip based on source-state recency.
- `--hot-source-cadence-minutes` (default `15`): cadence for recently changed sources.
- `--cold-source-cadence-minutes` (default `60`): cadence for stable sources.
- `--only-sources`: comma-separated list of source loader names to run.
- `--no-seed-existing-output`: for targeted `--only-sources` runs, prevents carrying existing output into the new feed. This is required for first-run sheet bootstrap staging.
- `--circuit-breaker-failures` (default `3`): consecutive failures to trigger quarantine.
- `--circuit-breaker-cooldown-minutes` (default `180`): quarantine duration.
- `--browser-fallback-cooldown-minutes` (default `30`): short-lived cooldown applied after an environment-level Playwright/browser failure.
- `--ignore-circuit-breaker`: force run quarantined sources.
- `--social-enabled`: include social-source loaders (Reddit/X/Mastodon) in this run.
- `--social-config-path`: path to social source config JSON.
- `--social-lookback-minutes` (default `30`): recency window used by social source polling.
- `--quiet`: suppress per-source progress logs.

## Admin presets (`/tasks/run-fetcher`)

- `default`: full run with explicit runtime defaults.
- `incremental`: enables `--skip-successful-sources`, sets TTL, quiet mode.
- `retry_failed`: resolves failed sources from latest report, keeps deterministic ordering, filters unknown source names, runs with `--ignore-circuit-breaker --quiet`.
- `force_full`: full run with `--ignore-circuit-breaker --quiet`.
- `uncapped`: aggressive admin run that bypasses freshness skips, cadence skips, and circuit-breaker quarantine and avoids admin-imposed source/concurrency narrowing while still keeping hard transport safety.

Bridge defaults:

- Bridge-started fetch runs use the same runtime defaults as direct CLI fetches:
  - `--max-workers 12`
  - `--max-per-domain 3`
  - `--adapter-http-concurrency 48`
- Container/Umbrel bridge-started fetch runs use more conservative defaults to keep the raw-LAN UI responsive during active fetch work:
  - `--max-workers 4`
  - `--max-per-domain 2`
  - `--adapter-http-concurrency 16`
  - `--static-detail-concurrency 4`
- The `uncapped` preset remains intentionally aggressive in container mode (`--max-workers 50`, `--max-per-domain 5`, `--adapter-http-concurrency 48`, and default static detail concurrency).
- Bridge-started fetch runs include `--social-enabled` by default unless `socialEnabled: false` is passed.
- Jobs page `Run Discovery + Fetch + Sync` and Admin `Run Jobs Fetcher` share this same bridge-default behavior.
- `POST /tasks/run-jobs-bootstrap` is not a normal fetch preset. It is a first-run/retry bootstrap route that runs only `google_sheets`, `google_sheets_1er2oaxo`, and `google_sheets_1mvqhxat` into a private staging directory with no existing-output seed, no preserve-on-empty, forced refresh, circuit breaker ignored, and social disabled. It promotes `jobs-unified.json`, `jobs-unified-light.json`, `jobs-unified.csv`, and the report only when at least one sheet succeeds and output count is non-zero.

Optional overrides:

- `maxWorkers`
- `maxPerDomain`
- `fetchStrategy`
- `adapterHttpConcurrency`
- `staticDetailConcurrency`
- `sourceTtlMinutes`
- `respectSourceCadence`
- `hotSourceCadenceMinutes`
- `coldSourceCadenceMinutes`
- `circuitBreakerFailures`
- `circuitBreakerCooldownMinutes`
- `skipSuccessfulSources`
- `ignoreCircuitBreaker`
- `socialEnabled`
- `socialConfigPath`
- `socialLookbackMinutes`
- `quiet`
- `onlySources` (array)

## Runtime files consumed by admin

- `data/jobs-fetch-report.json`
  - contract keys: `runtime`, `summary`, `sources`.
  - includes output file paths under `outputs`.
  - `summary.okCleanSources` and `summary.okWithWarningSources` are additive success diagnostics; source rows still use `status: "ok"` for both.
  - `summary.needsReviewBreakdown` includes both shaped static diagnostic counts and raw comparison counters: `rawMarkerCount` and `includedCount`.
  - `summary.sizeGuardrails` reports per-artifact byte counts and limits for `json`, `lightJson`, and `csv`; `summary.sizeGuardrailExceeded` remains the aggregate compatibility flag.
  - after M4, bridge-started terminal reports are compact compatibility/debug exports when `sourceRuns=sqlite`: lean `sources` rows remain, bulky per-source `details` move to SQLite-backed source rows plus gzip evidence archives, and `sourceRuns.sourceDetailsArchive` references the archive. Direct CLI and old full reports remain valid JSON fallback.
- `GET /ops/fetch-report`
  - keeps the current payload shape.
  - when `sourceRuns=sqlite`, terminal source rows are hydrated from SQLite/archive; `?view=live` remains compact and omits bulky `details`.
- `GET /ops/fetch-report/sources`
  - additive bounded terminal-source query with `runId`, `limit`, `offset`, and optional `status`.
  - falls back to `jobs-fetch-report.json` rows when `sourceRuns` is not authoritative.
- Output size policy:
  - `jobs-unified.json`, `jobs-unified-light.json`, `jobs-source-state.json`, and `jobs-lifecycle-state.json` are stored through transparent gzip-backed paths while preserving their logical `.json` URLs and row fields.
  - `source-registry-active.json` and `source-registry-pending.json` store lean core rows with sparse metadata in `source-registry-metadata.json.gz`; readers reconstruct the full row shape on load, legacy monolithic registry files remain backward-compatible, and unchanged payloads now skip the rewrite path to reduce churn.
  - archived lifecycle rows are moved into yearly transparent gzip-backed cold files (`jobs-lifecycle-archive-{year}.json.gz`) and loaded on demand only.
  - report/debug JSON remains pretty-printed for operator readability.
  - warning limits are `80_000_000` bytes for full JSON, `60_000_000` bytes for light JSON, and `50_000_000` bytes for CSV.
  - package builds no longer seed row-bearing jobs artifacts (`jobs-unified*.json(.gz)`, `jobs-unified.csv`, or `jobs-unified-startup.json`). The desktop launcher quarantines stale row artifacts from upgraded installs when no successful runtime report proves a real local feed.
- Static HTTP fetch policy:
  - static listing and detail fetches should go through the shared `fetch_html_cached` path so cache, per-domain throttling, and redirect handling stay consistent.
  - one redirect hop is allowed for 301/302/303/307/308 when the target is HTTP(S), contains no credentials, does not downgrade HTTPS to HTTP, and stays on the same host or a `www.`/bare-host alias.
  - redirects without `Location`, redirect chains beyond one hop, credentialed targets, non-HTTP(S) schemes, unrelated cross-host targets, and HTTPS downgrades remain source diagnostics rather than silently followed traffic.
  - static zero-extract rows with redirect/status evidence are diagnosed as `site_changed`; generic static/manual no-jobs rows without redirect evidence remain `js_required` for browser/static extraction follow-up.
  - static plugins that need custom listing/detail handling receive the shared fetch helper from the static adapter surface; direct `fetch_text` usage is only acceptable for non-static-provider paths that intentionally bypass static report semantics.
- Scoped anti-bot browser retry policy:
  - registry rows may opt into higher-cost browser retry with `antiBotBrowserRetry: true`.
  - flagged static rows can retry listing fetches with Playwright for `HTTP 429` / "Too Many Requests"; non-flagged `429` rows keep the default HTTP failure behavior.
  - flagged HTML-board provider rows for Breezy, JazzHR, and Ashby can retry `403`, `429`, and timeout failures with the guarded Playwright helper before emitting diagnostics.
  - retry exhaustion keeps source status semantics unchanged, but diagnostics should include `classification: "anti_bot_or_challenge"`, `failureBucket: "anti_bot_or_challenge"` where source-report shaping supports it, `browserFallbackRecommended: true`, and queue-compatible `sourceId` / `pages` evidence.
- `data/jobs-source-state.json`
  - per-source state for TTL and circuit breaker decisions.
  - includes `consecutiveFailures`, `lastSuccessAt`, `quarantinedUntilAt`.
- `data/jobs-fetch-tasks.json`
  - live task/heartbeat state for source execution lifecycle.
  - includes task `status`, `startedAt`, `finishedAt`, `heartbeatAt`, and summary counters.
  - remains the active-run progress surface after M4; source-run bulk insert is terminal postprocessing, not streaming live progress.

## Source-state and circuit-breaker lifecycle

- On successful source run:
  - reset `consecutiveFailures` to `0`
  - set `lastSuccessAt`
  - clear quarantine/error fields.
- On failed source run:
  - increment `consecutiveFailures`
  - set `lastFailureAt` and `lastError`
  - if threshold is reached, set `quarantinedUntilAt`.
- During loader selection:
  - quarantined sources are marked as `excluded` until cooldown expires unless `--ignore-circuit-breaker` is set.
- During incremental mode:
  - sources with recent success within TTL are skipped.
- During admin uncapped mode:
  - bridge preset logic bypasses freshness/cadence/circuit-breaker gating in the launch args
  - this preset is intentionally more aggressive than `force_full`
