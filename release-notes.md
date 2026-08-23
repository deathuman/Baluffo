## [0.2.133] - 2026-08-23

> Shared Desktop + Umbrel performance patch: fetch-stage row streaming,
> lifecycle tree defer, jemalloc container allocator swap, LPT scheduling,
> browser-pool recycling + renderer caps, parser-noise classifier fix, and
> container concurrency profile raise.

### Changed

- Fetch-stage row streaming: seeded and fetched canonical rows deferred to
  finalize handoff via incremental sidecar; lifecycle tree deferred to finalize.
- Copy-on-write lifecycle rows and replace-based dedup renumbering.
- Container allocator swapped from glibc to jemalloc (`LD_PRELOAD`) with
  background page purging and forced mmap for large allocations.
- LPT scheduling: known-slow aggregate loaders (`google_sheets`,
  `scrapy_static_sources`) start first to overlap with fast statics.
- Browser pool recycling every N acquisitions with graceful close + lazy relaunch.
- Chromium renderer-process limit and V8 heap cap for tight cgroups.
- http2 transport attempt with graceful fallback on pooled HTTP clients.
- Compact hot-state JSON writes for task-state and progress reports.
- Heavy-host body caps (2 MiB) and listing-only enforcement for outlier domains.
- Parser-noise classifier tightened: single `{Token}` titles kept as real jobs.
- Container concurrency profile raised: mw=12, max_per_domain=3,
  static_detail_concurrency=6, adapter_http_concurrency cap=32.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.
