# Fetcher Runtime and Admin Contracts

> - **Status:** Active
> - **Use this when:** changing jobs fetcher CLI flags, admin fetch presets, runtime files, or source-state and circuit-breaker behavior
> - **Canonical for:** fetcher runtime options, admin preset wiring, and fetch-run artifacts consumed by admin flows
> - **Not canonical for:** full jobs pipeline ownership or broad testing strategy
> - **Then inspect:** `src/jobs_fetcher.py`, `src/jobs/fetcher_compat_{exports,runtime}.py`, `src/jobs/pipeline*.py`, `src/jobs/state*.py`, and [`testing.md`](testing.md)
> - **Last updated:** 2026-04-26

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
- Bridge-started fetch runs include `--social-enabled` by default unless `socialEnabled: false` is passed.
- Jobs page `Run Discovery + Fetch + Sync` and Admin `Run Jobs Fetcher` share this same bridge-default behavior.

Optional overrides:

- `maxWorkers`
- `maxPerDomain`
- `fetchStrategy`
- `adapterHttpConcurrency`
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
- `data/jobs-source-state.json`
  - per-source state for TTL and circuit breaker decisions.
  - includes `consecutiveFailures`, `lastSuccessAt`, `quarantinedUntilAt`.
- `data/jobs-fetch-tasks.json`
  - live task/heartbeat state for source execution lifecycle.
  - includes task `status`, `startedAt`, `finishedAt`, `heartbeatAt`, and summary counters.

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
