> - **Class:** coverage-gap
> - **Trigger:** three-opening coverage audit (Hatsu Labs/Dover apply URL, Vivid Games job `8351271`, Studio Gloam `#careers`) — only 1 of 3 openings is delivered, and the audit surfaced three further defects on the way
> - **Verified against:** 9b8901f8 (diagnosis); portable 0.3.017, fetch run `6138ba610943a2e9` completed 2026-10-12 16:11 UTC (live state); readiness re-verified at `3508b8f9` 2026-10-14
> - **Status:** F-1 `02ef7fcd`; F-2 `b7e97429`; F-3 `52024ae6` (rescoped to a packaging fix); F-4 `053cef5d`; F-5 implemented and live-accepted (Phase 6 on-box verification excluded from this slice)

# Opening coverage audit fixes

Canonical plan for closing the three audited openings and the defects the audit
proved. Umbrella coverage work stays in
[`catalogue-coverage-gap-plan.md`](catalogue-coverage-gap-plan.md); each slice below
adds an entry there before/after execution (one slice per commit).

## Baseline (measured 2026-10-12)

**Evidence source.** Fresh evidence comes from the portable install data root
`C:\Users\Andrea\AppData\Roaming\Baluffo` (app 0.3.017; unified feed published
2026-10-12 18:12; fetch report `jobs-fetch-report-6138ba610943a2e9-20261012-161154.json.gz`).
The repo checkout's `data/` is **stale at 2026-09-17** (`fetch_72084f81ca`) — it produced
"unverified" conclusions that the 10-12 run settled. Read the AppData root only; never
point repo tooling at it without isolation flags.

### The three openings

| # | Opening | Status on 2026-10-12 | Root cause |
|---|---|---|---|
| 1 | [Hatsu Labs — Dover apply URL](https://app.dover.com/apply/hatsulabs/8dbe43b3-3dc5-4812-bf95-fbb306d08e59) | ❌ absent | No Dover adapter (absent from `DEFAULT_SOURCE_LOADER_NAMES`, registry, `adapterTimings` — all three), studio never discovered, board is a client-rendered SPA shell (4,466 B, zero job content) |
| 2 | [Vivid Games — Senior Technical Artist](https://jobs.vividgames.com/jobs/8351271-senior-technical-artist-remote-hybrid-or-on-site) | ✅ delivered | Teamtailor row restored 2026-09-26, delivered `fetched 1 / kept 1` on the 10-12 run — **but stored title is the raw slug** (defect F-1) |
| 3 | [Studio Gloam — #careers](https://studiogloam.com/#careers) | ❌ absent | Never discovered (0 hits registry/candidates/reports); roles hardcoded in Vite JS bundle with in-page form; static reader sees an 8.5 KB shell with 0 job markers |

### Defects found during the audit

| ID | Defect | Evidence |
|---|---|---|
| F-1 | **JSON-LD `unescape`-before-parse breaks job titles feed-wide.** `parse_jobpostings_from_html` runs `html.unescape()` on the JSON-LD block *before* `json.loads`; any block whose escaped values contain entity-encoded quotes (`&quot;`/`&#34;`) becomes invalid JSON → parse returns `[]` → `_teamtailor_fallback_job` publishes the URL slug as the title | Reproduced: raw block parses (title = *"Senior Technical Artist (Remote, Hybrid or On-site)"*), unescaped block raises `JSONDecodeError line 5 col 747`. Fresh feed: **135 slug-id titles** (`"title":"<digits> …"`), 116 of them teamtailor rows (e.g. `8351271 senior technical artist remote hybrid or on site`, `6869078 technical artist`, `8438419 art director pocket champs`) |
| F-2 | **A dead-board sweep retired the only working fallback on a false premise.** `dead_board_sweep_20260906_batch3_vivid` tombstoned `static:listing_url:https://jobs.vividgames.com` (2026-09-07) citing "teamtailor … provider row already active and yielding (108 kept)" — the teamtailor row was still tombstoned at that moment, so the 09-17 run had **no `jobs.vividgames.com` row at all** | Tombstone reason text (`source-registry-tombstones.jsonl` line 8) vs the row's tombstone living in the compacted base until its removal 2026-09-26 07:50; the 09-17 report contains zero `jobs.vividgames.com` participants |
| F-3 | **Permanent redirect-error duplicate row still active.** `static:listing_url:https://vividgames.com/jobs` fails every run with `Unsafe static redirect from https://vividgames.com/jobs to https://jobs.vividgames.com` + `no jobs extracted` | 09-17 and 10-12 reports; the teamtailor provider row now delivers the same board |
| F-4 | **Dover is an unregistered, undetectable platform** (opening 1) | See baseline row 1; counter-example: 3 `app.dover.com/apply/…` links *do* enter the feed via Bandit Island's own static careers page — Hatsu's homepage links only to the Dover SPA |
| F-5 | **Studio Gloam invisible to discovery and to the static reader** (opening 3) | 0 hits everywhere on 10-12; roles are `<button>` toggles inside `QN` array of `/assets/index-C6_1yUeX.js`, application is an in-page form (`mailto:hello@studiogloam.com`) — no job anchors exist even after render |

The fragment-identity row `static:listing_url:https://jobs.vividgames.com/#jobs` is
already gone from the active registry — no action needed, recorded for completeness.

## Slice 1 — F-1: fix JSON-LD raw-parse (title hydration), unblock honest titles

**Change.** In `src/jobs/adapters/html_parsers.py` `parse_jobpostings_from_html` (line 171):

1. For each block from `extract_json_ld_blocks`: try `json.loads(raw_block)` **first**;
   only on `JSONDecodeError` fall back to `json.loads(unescape(raw_block))` (legacy
   double-escaped blocks keep their exact current behavior).
2. After a successful parse, deep-unescape the payload's string values once before the
   existing field extraction — this is byte-equivalent to today's behavior for every
   block that currently parses (proof: `&amp;quot;` → `&quot;` either way) while making
   single-escaped-quote blocks parseable at all.
3. Keep `_teamtailor_fallback_job` (`teamtailor_runner.py:42`) as the last resort, but
   strip a leading `\d+ ` segment from the slug-derived title so any residual fallback
   rows don't carry the numeric id prefix.

**Files.** `src/jobs/adapters/html_parsers.py`; fallback hardening in
`src/jobs/adapters/plugins/provider_api/teamtailor_runner.py`.

**Tests** (invariants, not tool output).

- New fixture with entity-encoded quotes + emoji in the JSON-LD description
  (Vivid-style, from the saved live page) → exactly 1 row, title equals the page `<h1>`.
- Existing `tests/test_jobs_fetcher_providers_individual.py::test_parse_jobpostings_from_html_teamtailor_fixture`
  stays green (legacy equivalence pin).
- Double-escaped fixture: assert output identical to pre-fix behavior (equivalence pin).
- Fallback: slug with leading id → title without the numeric prefix; slug without id unchanged.

**Acceptance.**

- Focused tests + `npm run test:py` + `npm run lint:repo-guardrails` green; `git status data/` clean.
- Isolated live check: `python src/jobs_fetcher.py --only-sources teamtailor_sources --ignore-circuit-breaker --force-refresh-all --output-dir _out/coverage/fetch-slice1` → row for `8351271-…` carries title *Senior Technical Artist (Remote, Hybrid or On-site)*.
- On the next full run: feed-wide `"title":"<digits> …"` count drops well below 135 (measure, do not pin).
- **Open question to settle in-slice:** whether lifecycle refreshes the stored title of
  already-published rows on refetch, or whether a one-off backfill is needed for the 135.
  If backfill is needed, it ships as its own commit with a public-text contract check
  (title-only change; locations/links untouched).

## Slice 2 — F-2: delivery-evidence guard before retiring a static fallback

**Change.** In `src/bridge/registry_conflicts_automation_static.py` (the durable home of
provider-over-static demotion logic; e.g. `_analyze_pending_provider_replacement_automation`
line 299), add a blocker before any action that removes/demotes the active static row of a
family:

- The replacement provider row must have **delivery evidence**, not probe evidence:
  latest source-state/run for the provider identity with `lastStatus: ok` (or a completed
  run record) and `lastKeptCount > 0`.
- `lastStatus: excluded`, cache skips, HTTP 304, and `lastKeptCount: 0` without a real
  fetch are **no evidence**, not disproof (repo guardrail) → block, keep the static row,
  surface `provider_missing_delivery_evidence`.
- `jobsFound` probe counts may support but never replace delivery evidence.

**Runbook twin (same commit, doc-only).** Out-of-band sweeps like
`dead_board_sweep_*` are not in this repo; record the rule in the
catalogue-coverage-gap plan: *never tombstone a static row for a host+tenant whose
provider row lacks a kept>0 run — verify with `tools/coverage_drain.py --verify-collected`
or a targeted fetch first.* The full rule: a provider identity replaces an active static
fallback only once it has **delivery** evidence — a real fetch with `lastStatus: ok` and
`lastKeptCount > 0`. `excluded` (not fetched this run), cache skips, HTTP 304, and
`lastKeptCount: 0` are no evidence, not disproof; a probe `jobsFound` count never
substitutes. Verify with an isolated targeted fetch before retiring the row:
`python src/jobs_fetcher.py --only-sources static_source::<id> --force-refresh-all --output-dir <dir>`.

**Tests.** Unit fixtures: provider with jobsFound>0 but no kept record → blocked;
provider with `keptCount>0` → allowed; excluded/304 rows → blocked. Assert the blocker's
presence/absence, never the tool's current report output.

**Acceptance.** Replaying the Vivid 09-07 decision through the guarded code yields
"blocked", while the real 10-12 state (teamtailor kept 1) yields "eligible".

**Landed.** `_has_delivery_evidence` in `registry_conflicts_row_core.py` (re-exported from
`registry_conflicts_row.py`); the guard in
`_analyze_pending_provider_replacement_automation` blocks with
`provider_missing_delivery_evidence`. Acceptance replay (`_out/coverage/f2_acceptance.py`):
09-07 probe-only (108) → `eligible=False` blocked; 10-12 real fetch kept 1 → `eligible=True`.
Six new tests assert the blocker's presence/absence; the three pre-existing promotion
fixtures now carry real delivery evidence, and the bamboohr weak-static fixture — which
promoted a *zero-delivery* provider — was converted to assert the new block, since that
shape is exactly the decision F-2 forbids.

## Slice 3 — F-3: retire the redirect-dead duplicate static row

**Change.** Tombstone `static:listing_url:https://vividgames.com/jobs` (host+tenant:
`vividgames.com` + root path, case-insensitive) in favor of the delivering
`teamtailor:listing_url:https://jobs.vividgames.com`. Rationale: provider row delivers
(`kept 1` on 10-12); the static row can never succeed (permanent cross-host redirect
rejected by the unsafe-redirect guard) — this is the plan's known duplicate-row class
(Ubisoft/WBD/EA), not adapter work.

**Procedure (repo guardrails, single-row but still a registry mutation).**

1. Print the plan: exact row id, expected mutation count **= 1**; assert before writing.
2. Back up registry state to `_out/coverage/`; write through the **runtime registry
   path** used by hygiene/prune (the state writer `src/jobs/registry_hygiene.py` uses) —
   never hand-edit jsonl deltas, never a path nothing calls.
3. Read back: row absent from active; teamtailor row and its `pages` untouched.
4. Alternative on record if retirement is refused: teach the static redirect guard to
   accept same-brand cross-host redirects (`vividgames.com` → `jobs.vividgames.com`);
   strictly lower priority than provider delivery.

**Acceptance.** Next run: zero `Unsafe static redirect … vividgames` entries in the
report; teamtailor board still `kept ≥ 1`; no other row changed (row-count delta = −1).

**BLOCKED on a decision (2026-10-14, live evidence).** The slice's premise is not what the
live run recorded, so the planned retirement cannot be executed honestly:

- Live 10-12 evidence for `static:listing_url:https://vividgames.com/jobs`
  (`jobs-source-state.json.gz`): `lastStatus: error`, `lastKeptCount: 0`,
  `consecutiveFailures: 4`, `cacheDecision: skip_fresh` (the row was **not fetched** on 10-12;
  last real attempt was 2026-06-12). Its recorded error is **not** a redirect:
  `[Errno 2] No such file or directory: '…\baluffo-portable-0.3.0\_internal\tldextract\.tld_set_snapshot'`
  + `no jobs extracted from source pages`.
- The 10-12 report contains **no `Unsafe static redirect … vividgames` pair** at all
  (checked directly; 60 redirect pairs, all `insecure_downgrade` except three
  `platform_migration`s). So `tools/coverage_retire.py` — the gated retirement tool —
  finds 0 rows to retire and `--expect-rows 1` refuses. There is no evidence-backed way to
  retire this row.
- The redirect *does* exist when probed live today: `https://vividgames.com/jobs` → **302 →
  `https://jobs.vividgames.com/`**. But the classifier returns
  `site_gone_or_moved` with reason *"redirect left the site for a host no platform owns"* —
  a **misdiagnosis**: `jobs.vividgames.com` is the same studio's own Teamtailor-hosted jobs
  subdomain (the delivering row). This is the slice's documented alternative (teach the
  redirect guard/classifier to accept same-brand cross-host moves), not a retirement.
- Root cause found, and it is much wider than one row: `tldextract` (pinned 5.3.1;
  used by `src/source_discovery/rebrand_detection.py` via
  `tldextract.TLDExtract(suffix_list_urls=())`) is **missing from
  `MAIN_RUNTIME_COLLECT_DATA_PACKAGES` in `scripts/build_portable_exe.py`**, so its
  `.tld_set_snapshot` data file is never bundled into the frozen `_internal/`. Every
  `is_rebrand` call in the shipped app raises `[Errno 2]`, which is why the 10-12 report
  carries **491** `tld_set_snapshot` errors and **2,187** `no jobs extracted from source
  pages`. A dev checkout has the snapshot (so the bug is invisible locally).

Three separable remediations exist; the choice is a user decision (the plan already marks
F-3's approach as a slice-time decision):

| Option | Change | Blast radius |
|---|---|---|
| **A (recommended). Packaging fix** | add `tldextract` to `MAIN_RUNTIME_COLLECT_DATA_PACKAGES` (+ a build-contract test proving `.tld_set_snapshot` lands in `_internal/`) | fixes ~491 rows' failure mode for the next build; does not touch the live registry |
| **B. Classifier fix** | teach the redirect classifier / rebrand path to accept a same-registrable-domain cross-host move (`vividgames.com` → `jobs.vividgames.com`) as non-terminal | prevents a future false retirement of this exact shape; F-3's documented alternative |
| **C. Retire the row anyway** | mutate the live registry by hand | **rejected**: no in-report evidence, and the retain-tool gate refuses; would retire on a premise the evidence contradicts |

**Resolution (user, 2026-10-14): Option A.** The row is *not* retired. Instead the real
root cause is fixed — the frozen build never bundled the `tldextract` suffix snapshot, which
is what actually failed these static rows — and a build-contract test locks it in. See the
"Landed" note below.

**Landed.** `tldextract` was added to `MAIN_RUNTIME_COLLECT_DATA_PACKAGES` in
`scripts/build_portable_exe.py` (so `--collect-data tldextract` ships `.tld_set_snapshot`
into `_internal/tldextract/`). Because the build plan's `pyinstaller.collectData` list and
the cache fingerprint both read that tuple, the change also invalidates any stale cached
portable build — a rebuild is forced, which is what makes the fix actually reach a box.

Three tests in `tests/test_build_portable_exe.py` lock it in:
`test_portable_build_collects_tldextract_suffix_snapshot_data` (tuple membership),
`test_tldextract_bundled_snapshot_exists_in_this_environment` (the snapshot the entry is
meant to ship exists), and
`test_pyinstaller_collect_data_for_tldextract_ships_the_snapshot` — which asserts the
**real** `PyInstaller.utils.hooks.collect_data_files("tldextract")` output maps
`.tld_set_snapshot` to destination `tldextract`, i.e. the exact mechanism, not a restatement
of our own constant.

No registry mutation was made: the live `vividgames.com/jobs` row stays active and the
delivering teamtailor row (`kept 1` on 10-12) is untouched. Row-count delta is **0**, not
−1, and the "zero redirect errors" acceptance below no longer applies — the errors were
never redirect errors. F-2's guard independently makes the original 09-07-style retirement
impossible in the in-repo path even if the classification were accepted.

**Acceptance (revised).** F-3's original acceptance (row gone, delta −1) is **withdrawn**
because it rested on a premise the live evidence contradicts. The revised acceptance is:
the next frozen build ships `_internal/tldextract/.tld_set_snapshot` and the report's
`tld_set_snapshot` error count drops toward zero — verified on the box in Phase 6, not by a
local probe.

## Slice 4 — F-4: Dover platform coverage (Hatsu Labs opening)

**Gate (time-boxed recon, its own commit if it only produces evidence).** Determine the
Dover board's data surface:

**Recon RESULT (2026-10-14): Branch A confirmed — a stable public JSON API exists.**
`https://app.dover.com/jobs/hatsulabs` and the audited apply URL both answer 200 with a
~4.4KB SPA shell (Dover's "search-builder2" bundle, Cloudflare Turnstile present). The shell
loads `/static/search-builder2/static/js/openapi-BpRgtX5w.js`, which enumerates Dover's ATS
API and exposed the public careers-page endpoints. Verified live:

- `GET https://app.dover.com/api/v1/careers-page-slug/hatsulabs` → 200
  `{"id":"d306a454-f8d9-47b7-8d65-08914d7b5644","slug":"hatsulabs","name":"Hatsu Labs", …}`
  (slug → client_id).
- `GET https://app.dover.com/api/v1/careers-page/<client_id>/jobs` → 200
  `{"count":2,"results":[…]}` — **both Hatsu openings**, including
  `8dbe43b3-3dc5-4812-bf95-fbb306d08e59` / *"Technical Artist (Shaders & Performance)"* and
  `3a138bf4-…` / *"Character Lighting & Look Development Artist"* (both `workplace_type:
  REMOTE`, location *International*).

Trailing-slash variants (`…/jobs/`) route to the SPA HTML, so the adapter must use the
**no-trailing-slash** form. `?client=<slug>` on the global `/api/v1/job-board/jobs/` is
ignored (that endpoint returns all 483 boards), so the **two-step resolution is required**.
No Turnstile challenge was hit on these JSON endpoints (the shell's Turnstile is for the
apply flow), so Branch C is not needed.

Note the second opening is the interesting artefact: the audited URL is the *apply* URL,
whose id is the *job* id, while `careers-page-slug` returns the *client* id — the adapter
must not conflate them.

**Resolution (user, 2026-10-14): Option A — ship the `/api/` adapter.** The `robots.txt`
`Disallow: /api/` is recorded here and in the adapter's module docstring as a known caveat.
Rationale: the endpoints serve **published, unauthenticated careers-page content** (no auth,
no PII, `is_published` rows only) — the same data the public careers page renders — and
Baluffo already reads other providers' posting APIs on the same basis. The blanket
`/api/` rule also covers unrelated admin paths, so it is not a board-specific "do not read
our openings" signal. The finding is documented rather than dropped, so a future reviewer can
re-litigate it with the evidence in hand.

- Capture the JSON endpoint the `/jobs/<slug>` SPA calls (the static HTML is a shell;
  guessed paths `/api/*` all fall back to `index.html`; the public call lives in a
  code-split chunk — inspect chunk graph or sniff one Playwright session).
- **Branch A — stable JSON endpoint exists:** implement a `dover_sources` provider:
  loader in `DEFAULT_SOURCE_LOADER_NAMES` + `SOURCE_REPORT_META`
  (`src/jobs_fetcher_registry.py`), plugin under
  `src/jobs/adapters/plugins/provider_api/`, and **probe/fetch URL parity** (discovery
  probe must resolve the same URL the fetch reads — the Ashby lesson; see
  `src/ashby_board_urls.py` pattern).
- **Branch B — no clean endpoint:** register boards as static rows with browser
  escalation (`browserEscalationEnabled` / Playwright path already wired in
  `static_runtime.py`) and prove extraction on the rendered DOM before registering.
- **Branch C (fallback if Turnstile blocks both):** extract from the studio's own
  homepage link (`hatsulabs.com` → `app.dover.com/jobs/hatsulabs`), the way Bandit
  Island's own page already feeds Dover apply URLs — register the *studio* page, not
  the Dover SPA.

**Registration and discovery (whichever branch lands).**

- Add `https://app.dover.com/jobs/hatsulabs` via `src/curated_coverage_boards.json`
  (runtime discovery path `STATIC_DISCOVERY_CANDIDATES` at
  `src/source_discovery/config.py:602` — not a curator only its own `main()` calls).
- Fix the Backbone curated row (`src/curated_coverage_boards.json:4944`): bare
  `https://app.dover.com` → `https://app.dover.com/jobs/backbone`.
- Add a Dover homepage-link signature for discovery (`src/source_discovery/provider_patterns.py`)
  matching `app.dover.com/jobs/<slug>` links on studio sites.

**Acceptance.**

- Isolated targeted fetch keeps ≥1 Hatsu row whose `jobLink` contains
  `8dbe43b3-3dc5-4812-bf95-fbb306d08e59`, with the real role title (not a slug).
- Discovery probe on the registered board does **not** return `zero_jobs`
  (no repeat of the Ashby quarantine).
- Controls in the same run unchanged (a known-good board keeps its expected count).

**Landed.** Branch A. New leaf `src/dover_board_urls.py` (slug resolution, both API URLs,
`dover_source_slug_url`), new runner
`src/jobs/adapters/plugins/provider_api/dover_runner.py` (two-step slug → client_id → jobs;
an HTML-not-JSON body is a hard error, never a silent zero), parser
`parse_dover_jobs_payload` in `src/jobs/adapters/parsers/json_payloads.py`, registered as
adapter key `dover_sources` with its loader name and `SOURCE_REPORT_META` entry, curated row
`Hatsu Labs (dover)` added, the Backbone row's bare root fixed to
`https://app.dover.com/jobs/backbone`, and `dover` added at every peer-provider seam
(`PROVIDER_ADAPTERS` + host maps, `SUPPORTED_PROVIDERS`, dedup namespace, ship-bundle
runtime scripts, registry loader selection, source-state keying).

Two defects found and fixed during review, both regression-tested:

- The parser's inherited `looks_like_game_job` gate **dropped a real opening**
  ("Character Lighting & Look Development Artist" carries no game keyword). That filter
  strips back-office roles from *wide multi-studio feeds*; a Dover source is one named
  studio's own board, so the gate does not apply.
- The row's `api_url` is the **slug** endpoint, which returns no postings, so a probe aimed
  there would read every Dover board as zero. `probe.py` resolves the client id and counts
  the **jobs** endpoint instead, so probe and fetch read the same rows.

**Evidence.**

- Live runner acceptance (`_out/coverage/f4_live_acceptance.py`, real API): keeps **2** rows,
  including `dover:8dbe43b3-3dc5-4812-bf95-fbb306d08e59` /
  *"Technical Artist (Shaders & Performance)"*; every link is `…/apply/hatsulabs/<job id>`.
- Registry check: the curated Dover row loads and reaches `STATIC_DISCOVERY_CANDIDATES`;
  the plugin registry selects `dover_sources` with a runnable `run_fn`.
- `npm run test:py` → **6,856 passed**, 3 skipped, 198 deselected; 34 new Dover tests.

The "isolated targeted fetch" acceptance is met by the live runner above; a full-pipeline
fetch would require the board in a *runtime* registry, which is a scheduled-run check on the
box (Phase 6), not a local probe.

## Slice 5 — F-5: Studio Gloam coverage

**Spike result (2026-10-14):** Option B works without build-hash coupling. The ~8.5KB
Vite/Base44 shell has one anchor and no `#careers` fragment; its openings are hardcoded in
the hashed `/assets/*.js` entry bundle. Fetch `index.html`, resolve that script URL from the
HTML, then match objects by `{title,type,location,contract}` shape—not the minified array
name. Require literal `title:` as the first key: a reviewer caught a `name:"Company award"`
decoy with otherwise opening-shaped metadata, now covered by regression test. The approach
finds the two actual roles and ignores team-member bio objects. `robots.txt` allows `/`;
rendered-DOM extraction would add browser cost without surfacing different data.

**Per-role links.** There are no role URLs. Each row uses a synthesized query link:
`https://studiogloam.com/?careers=<slug>`. The earlier fragment proposal
(`/#careers-<slug>`) was incorrect: `clean_text.normalize_url` strips fragments before
URL dedup, collapsing the two roles. The query survives normalization, remains on-domain,
and resolves HTTP 200 for each role.

**Implemented extraction.** Fetch `index.html`, resolve its hashed `/assets/*.js` entry
script URL (never hardcode the build hash), then match opening objects by their
`title/type/location/contract` shape, not by a minified array name. This is deterministic,
site-specific, and not coupled to the generated bundle name.

**Registration.** Seed `https://studiogloam.com/` (bare origin, no `#careers` fragment)
in `src/curated_coverage_boards.json`; this slice does not add a separate directory entry.
The shared title filter is bypassed only for the `studiogloam` plugin, whose bundle
extractor identifies the site's actual craft openings. Every other static plugin remains
subject to the shared filter; the regression control confirms that a `Senior Lighting
Artist` row from an unrelated plugin is still dropped.

**Acceptance evidence (isolated run, 2026-10-14).**

- `python src/jobs_fetcher.py --only-sources static_source::studiogloam --ignore-circuit-breaker --force-refresh-all --output-dir _out/coverage/f5-live` → `status=ok fetched=2 kept=2`; inputCount 2, rawFetched 2, canonicalKept 2, finalOutput 2, successfulSources 1. A temporary isolated runtime source registry was used because Studio Gloam is not yet in the packaged runtime registry; committed curated registration is included for its intended runtime path.
- Both per-role query links returned HTTP 200. The two rows have the exact titles, `Remote · Europe` location and `Fixed Term Contract` contract above.
- `python -m pytest tests/test_studiogloam_plugin.py tests/source_discovery/test_curated_boards_landing.py -q` → **21 passed, 1 skipped**, including the non-opening `name:`-object decoy regression and the static kept-opening count update from 3,440 to 3,442.
- `npm run test:py` → **6,871 passed, 3 skipped, 198 deselected** (exit 0).
- `npm run lint:repo-guardrails` in the isolated F-5-only worktree → workflow, compatibility, routes, frontend, repo-root, test-shape, fixtures, line-budget, registry, bundle, duplication and dead-code groups passed. Existing unrelated blockers are missing markdown-link targets in the performance follow-up plan and the pre-existing container version gate on commits F-5 does not address.
- `python tools/repo_health/loc_budget.py --report` in the isolated worktree measured `src=184156`, `tests=241579`, with all other areas at their baselines. The exact F-5 allowances are recorded in `docs/notes/discovery-receipt-loc-allowance.md`; no concurrent frontend files or baseline drift are included.
- `git status data/` → clean; isolated acceptance artifacts remain under gitignored `_out/coverage/f5-live`.
- Phase 6 on-box verification is explicitly outside this slice.

## Sequencing

| Order | Slice | Why here |
|---|---|---|
| 1 | F-1 JSON-LD parse fix | Largest user-facing defect class (135 rows), independent, fixes the delivered Vivid row's title |
| 2 | F-2 delivery-evidence guard | Install the safety rule before any further registry surgery |
| 3 | F-3 retire duplicate row | Single-row mutation, safe only after the F-2 guard exists to prevent repeats |
| 4 | F-4 Dover | Needs recon gate; biggest unknown; independent of 1–3 |
| 5 | F-5 Studio Gloam | Needs spike; independent; smallest blast radius |

Each slice: one commit; before/after entries in `catalogue-coverage-gap-plan.md`
(Hatsu/Gloam are Bucket D; F-1 is extraction/quality; F-2/F-3 are registry-hygiene
standing sections) and a status-line update in this plan.

## End-to-end execution sequence (prepared 2026-10-14)

Original order: **Phase 0 (clean HEAD)** → **Phases 1–5** (the slices above) → **Phase 6
(release + on-box verification)** → closeout. The F-5 slice is closed on isolated local/live
fetch evidence; Phase 6 on-box verification remains explicitly out of scope for its commit.

### Phase 0 — reach a clean HEAD (DONE 2026-10-14, HEAD `3508b8f9`)

The tree that earlier sessions left dirty is now **committed and clean** — the 0.3.019
milestone (discovery transport/permit/lifecycle/receipt work, release-docs guardrail), the
plan docs, the evidence snapshots, and a follow-on storage fix all landed in the window
`cbc5f35a` → `e133eba7` → `3508b8f9`. The four gate repairs this phase originally planned
(rule-list `src/app_version.py` drift, discovery-performance plan word tripwire, three
line-budget overruns, LOC drift) were resolved in that same window — the guardrails now
report **line-budget passed / loc passed / registry / bundle / duplication / dead-code
passed** and `npm run test:py` is **6,812 passed**. The junk (`$log`, the `C`+U+F03A
redirect dir) is gone. `stash@{0}` (2026-10-10) is untouched.

Readiness re-verified 2026-10-14 at `3508b8f9`:

- Working tree clean (`git status --porcelain` empty).
- `npm run test:py` → 6,812 passed / 3 skipped / 195 deselected (exit 0).
- `npm run test:frontend:unit` → exit 0.
- `npm run lint:repo-guardrails` → every group passes **except** the container shipped-code
  version gate: it flags `3508b8f9` (storage fix) as a shipping commit that landed after the
  0.3.019 bump without a version bump or `Release-tag:` intent. That is a **release-routing
  item for 0.3.019, not a coverage blocker** — Phase 6, not Phases 1–5.
- `data/` hygiene clean; `data/sheet-directory-discovery-audit.json` and `_out/` are
  gitignored (benign generated/scratch).

No commit steps remain in Phase 0. The execution runbook below starts clean at
`3508b8f9`.

### Phase 1 — F-1 JSON-LD raw-parse fix (DONE 2026-10-14)

- **Root cause** confirmed against the saved Vivid detail page (`_out/coverage/vivid_detail.html`):
  the JSON-LD block's role title is entity-encoded (`&quot;`), so the old
  `unescape`-then-`json.loads` order broke with `JSONDecodeError`. The raw block is valid JSON
  and parses first.
- **Fix**: `html_parsers.py` now uses `_load_jsonld_block` — try `json.loads(raw)` first, fall
  back to the unescaped text only on `JSONDecodeError`, then `_deep_unescape` string values.
  Applied to both JSON-LD sites (`parse_jobpostings_from_html`, `_gamesindustry_jsonld_rows`).
  `teamtailor_runner.py` additionally `_strip_leading_slug_id` strips the leading `<digits> `
  off slug-derived fallback titles.
- **Verified**: real parser returns title *Senior Technical Artist (Remote, Hybrid or On-site)*
  (sourceJobId `8351271`) on the live evidence. New tests:
  `test_parse_jobpostings_from_html_hydrates_entity_encoded_jsonld_title`,
  `test_teamtailor_fallback_job_strips_leading_slug_id_from_title`,
  `test_teamtailor_fallback_job_keeps_title_when_slug_has_no_id_prefix` + fixture
  `teamtailor_vivid_entity_encoded.html`. `npm run test:py` → 6,815 passed. LOC bump (src +38,
  tests +42) to be ratified via `loc_budget.py --update` and the allowance note.

### Phases 1–5 — per-slice protocol

Common loop, one commit per slice: (1) precondition = Phase 0 ready and green; (2)
implement against the slice spec above; (3) focused `python -m pytest <path> -q`;
(4) full gates `npm run test:py` + `npm run lint:repo-guardrails` + `git status data/`;
(5) isolated live probe with `--output-dir _out/coverage/<slice>` (never without it);
(6) commit; (7) update this plan's Status header and the umbrella plan entry (before/after);
(8) record evidence (commands + counts) for Phase 6. Slice contingencies stay as written
per slice (F-1 title-backfill question; F-4 branch gate A/B/C with Turnstile fallback;
F-5 option A/B spike). Guardrail caution for F-1: the plan and snapshot word budgets are
tripwired — keep added docs under 600 lines / 5,000 words each.

### Phase 6 — release and on-box verification

- **0.3.019 is drafted but not cut** (APP_VERSION, CHANGELOG and release-notes all carry
  `[0.3.019]`; git tags stop at `v0.3.017`). The `container_shipped_code_version_gate` is
  currently red because the storage commit `3508b8f9` landed after the 0.3.019 bump with no
  bump or `Release-tag:` intent. Both choices are open and belong to the entity cutting
  0.3.019:
  - **Default: 0.3.019 cuts with the storage fix, and our slices ride 0.3.020.** Release
    work follows the `baluffo-release-ship` flow (preflight → tag → desktop/GHCR/Umbrel
    verification), which resolves the version gate on 0.3.019.
  - **Fold-in alternative: if slices 1–3 land before 0.3.019 cuts**, their fixes can ride
    the existing `[0.3.019]` section instead of 0.3.020 — still needs the 0.3.019 gate
    resolved the same way.
- **Batch A** (Phases 1–3 → release): verified on the next scheduled run in the portable
  data root (`C:\Users\Andrea\AppData\Roaming\Baluffo`, read-only evidence): Vivid row title
  reads *Senior Technical Artist (Remote, Hybrid or On-site)*; zero
  `Unsafe static redirect … vividgames` entries; active-registry row delta −1; teamtailor
  `kept ≥ 1`.
- **Batch B** (Phases 4–5 → same or following release): a kept row whose `jobLink` contains
  `8dbe43b3-3dc5-4812-bf95-fbb306d08e59`; two kept Gloam rows with distinct identities;
  discovery probe does not quarantine the hatsulabs board.
- Delivered = kept > 0 on the box — never registry presence, never a local probe.

### Closeout

Umbrella plan entries closed with counts; this plan's Status → done; `docs/INDEX.md`
status column updated; Basic Memory handoff (slices, commands, final counts, next
priority); `git status data/` clean.

### Decision points for the user

1. **Release routing (the only open one for Phases 1–5):** default 0.3.020 vs folding
   slices 1–3 into the still-uncut 0.3.019. Also resolves how the `3508b8f9` container
   version gate on 0.3.019 is settled (bump/`Release-tag:`). F-1's title-backfill question
   and F-4's recon branch are slice-time decisions, not Phase-0.
2. Phase 0's original decisions (land milestone, LOC/line-budget ratchet, rule-list repair)
   are all **moot — already resolved and landed** across `cbc5f35a`→`3508b8f9`.
   The `stash@{0}` snapshot remains untouched.

## Verification and isolation rules

- Unit/focus: `python -m pytest <focused paths>`; full: `npm run test:py`;
  guardrails: `npm run lint:repo-guardrails`. Never bypass hooks.
- Every fetch probe is isolated: `--output-dir _out/coverage/<slice>` (omitting it
  writes stub state into live `data/`); discovery runs set `BALUFFO_DATA_DIR`.
- The AppData portable root is **read-only evidence**: inspect its reports/feeds; never
  aim repo fetch tooling at it without isolation; live delivery of registry changes is
  proven by the *next scheduled run on the box* (kept > 0), not by local probes.
- `git status data/` must show no `gameprog-*`/`gamesmap-*`/`*-discovery-audit.json`
  artifacts after any test run.

## Overall acceptance (F-5 slice closeout; Phase 6 excluded)

The F-5 implementation and isolated live delivery criteria are complete in this slice. The
remaining on-box verification is Phase 6 and is not a blocker for this commit.

| Finding | Done means |
|---|---|
| Opening 2 (Vivid) | Feed title reads *Senior Technical Artist (Remote, Hybrid or On-site)*; no `8351271 …` slug titles remain in teamtailor rows |
| F-2 | Guard test proves the 09-07-style decision is blocked without delivery evidence |
| F-3 | **Revised (2026-10-14):** the planned retirement was withdrawn — the row's real failure was the unbundled `tldextract` snapshot, not a redirect. Done means the next frozen build ships `_internal/tldextract/.tld_set_snapshot` and the report's `tld_set_snapshot` error count drops toward zero; no registry delta |
| Opening 1 (Hatsu) | **Landed (adapter level):** the live Dover runner keeps a row with `sourceJobId` containing `8dbe43b3-…` and the real title, from the registered `Hatsu Labs (dover)` board. On-box confirmation rides the next scheduled run (Phase 6) |
| Opening 3 (Gloam) | Isolated live fetch keeps 2/2 rows: *Senior Lighting Artist* and *Senior Technical Artist*, each with its own HTTP 200 query link; see F-5 evidence above (Phase 6 on-box verification excluded) |
| Process | All plans/status updated; `npm run test:py` + lint green; `data/` clean; Basic Memory handoff with counts, commands, next priority |

## Out of scope

- The ~19 slug-id titles outside the teamtailor family (audit separately under job
  data quality once F-1 lands and the residue is measurable).
- Umbrel verification — the portable install answered the freshness question; Umbrel
  spot-check only if its data root later disagrees.
- Feishu/out-of-reach boards and the wider catalogue buckets (B2/C/D) — stay in the
  umbrella plan.
- Any new Python/Node dependency (forbidden without explicit approval; Playwright is
  already in-tree).

## Assumptions

- The portable AppData root is the authoritative live dataset for verification (repo
  `data/` is stale at 09-17 and must not be used for delivery claims).
- Lifecycle refreshes titles on refetch; if Slice 1's acceptance shows otherwise, a
  title-only backfill commit is added (public text contract: titles only).
- Dover recon fits in one time-boxed session; if neither endpoint nor render path is
  obtainable (Turnstile), Branch C keeps the slice deliverable.
- `studiogloam.com` keeps its current single-page structure during the work; bundle
  hash coupling is handled by resolving the script src from `index.html`, never
  hardcoding the filename.
