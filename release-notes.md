## [0.2.132] - 2026-08-18

> Desktop rollup: the jobs/discovery coverage batch (remote aggregator,
> discovery recovery escalation, provider feed liveness, Gamesmap default-on,
> and new seed coverage) plus the packaged portable EXE fix that restores the
> desktop platform modules the v0.2.131 frozen bundle was missing, and the
> tests/ mypy remediation that turns the type gate on for the whole tree.

### Added

- Remotive community-board loader (`remotive` source) with a game-job filter,
  mirroring the Remote OK loader; registered in the default source loaders and
  compat exports. Live-verified to fetch remote game roles (e.g. Mythwright
  Senior Technical Artist) that were previously missed.
- Discovery recovery escalation for directory rows that fail same-party careers
  recovery: bounded provider-pattern candidates (Workable/Greenhouse/Teamtailor
  etc.) are emitted from the studio name before rejection, and remaining
  `no_careers_evidence` rows are queued for web-search re-staging. Gated by
  `gamedevmap.activeAuditRecoveryEscalation*` settings.
- Personio feed liveness: feed URLs that redirect to the Personio marketing
  homepage now classify as `site_changed` and append the studio to
  `data/discovery-feed-recheck-queue.json` so the next discovery run re-stages
  the studio instead of erroring forever.
- Gamesmap directory adapter is enabled by default (`gamesmap.enabled=true`,
  `websiteOnlyFallback=true`, `activeAuditTtlMinutes=360`) with a new
  `--gamesmap-enabled` CLI flag.
- Seed-catalog coverage for NeoBards and Evolve (neobards static plugin), the
  Crater Studios JS-shell careers site (static plugin deriving titles from URL
  slugs), and a personio 429 recheck path that re-stages rate-limited sources
  on the next discovery run.

### Fixed

- Static/provider empty-source cache decisions require 2 consecutive zero-kept
  runs before skipping a source (`DEFAULT_INCREMENTAL_EMPTY_SOURCE_MIN_ZERO_RUNS`),
  so a single transient bad run no longer parks a parseable source.
- Source-discovery audit tests no longer write fixture artifacts into `data/`
  (all gamesmap/gameprog tests now pin `activeAuditPath` to temp locations);
  polluted `data/gameprog-`/`data/gamesmap-discovery-audit.json` artifacts were
  removed.
- Packaged portable EXE: PyInstaller now statically imports the desktop
  platform modules (`src.ship.desktop_app._windows` / `_linux`) so the frozen
  PYZ bundles them — the v0.2.131 bundle omitted them. Release verify fails
  fast when a required module is missing from the built EXE, and a regression
  test asserts both platform modules are present.
- `BrowserFallbackPool.close()` now captures the live browser/playwright
  handles before dropping pool references and closes the pool event loop, so
  playwright's subprocess pipe transports shut down through asyncio's own
  path instead of emitting unclosed-transport ResourceWarnings at GC time.

### Tooling

- The mypy gate now also type-checks `tests/`: 1,841 errors across 292 files
  remediated with honest annotations/casts and zero new suppressions
  (`files = src, tests` in `mypy.ini`), and the Linux CI typecheck step runs
  this gate for real.
- Native MCP stdio server config (`.agents/mcp.json`) registers Serena and
  Basic Memory with the same commands as `opencode.json`, loaded natively by
  the Freebuff CLI.

### Notes

- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.
