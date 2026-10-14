> - **Class:** coverage-gap
> - **Trigger:** three-opening coverage audit (Hatsu Labs/Dover apply URL, Vivid Games job `8351271`, Studio Gloam `#careers`) — only 1 of 3 openings is delivered, and the audit surfaced three further defects on the way
> - **Verified against:** 9b8901f8 (diagnosis); portable 0.3.017, fetch run `6138ba610943a2e9` completed 2026-10-12 16:11 UTC (live state)
> - **Status:** planned — five slices, none started; end-to-end runbook added 2026-10-14 (Phase 0 = land the staged 0.3.019 milestone behind four measured gate repairs, then commit this plan)

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
or a targeted fetch first.*

**Tests.** Unit fixtures: provider with jobsFound>0 but no kept record → blocked;
provider with `keptCount>0` → allowed; excluded/304 rows → blocked. Assert the blocker's
presence/absence, never the tool's current report output.

**Acceptance.** Replaying the Vivid 09-07 decision through the guarded code yields
"blocked", while the real 10-12 state (teamtailor kept 1) yields "eligible".

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

## Slice 4 — F-4: Dover platform coverage (Hatsu Labs opening)

**Gate (time-boxed recon, its own commit if it only produces evidence).** Determine the
Dover board's data surface:

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

## Slice 5 — F-5: Studio Gloam coverage

**Spike (time-boxed, evidence-only commit allowed).** The board has **no job anchors at
all** — roles are `<button>` accordions, applications are an in-page form — so neither
plain static nor plain browser-escalation anchor extraction can work. Choose one:

- **Option A (preferred): rendered-DOM extraction.** Under browser escalation, read the
  `#careers` role blocks from the rendered DOM (title/location/contract from the `QN`
  data) and synthesize per-role links `https://studiogloam.com/#careers-<slug>` —
  unique per title so url-based dedup keeps **two** rows (both resolve to the live
  page; the apply step is the on-page form / `mailto:hello@studiogloam.com`).
- **Option B (fallback): bundle-JSON extraction.** Fetch `index.html`, resolve the
  hashed `/assets/index-*.js` script src, parse the openings array. Deterministic but
  build-hash-coupled and site-specific — acceptable as a scoped special case only.

**Registration.** Seed `https://studiogloam.com/` (bare origin, **no `#careers`
fragment** — fragment identities get swept, cf. `steerstudios.com/careers#joblist`) into
`src/curated_coverage_boards.json`, plus a GameDevMap/sheet directory entry so machine
discovery can find the studio independently.

**Tests / acceptance.**

- Unit: two role blocks → two rows, distinct `sourceJobId`/`dedupKey`, titles match the
  bundle data (`Senior Lighting Artist`, `Senior Technical Artist`), locations
  `Remote · Europe`, contract `Fixed Term Contract`.
- Isolated targeted run against the live site keeps **2** rows; both links resolve
  HTTP 200.
- If the spike proves neither option robust, record Gloam as a known limitation in the
  catalogue plan (Bucket D) rather than shipping a brittle parser.

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

Order: **Phase 0 (clean HEAD)** → **Phases 1–5** (the slices above) → **Phase 6 (release
+ on-box verification)** → closeout. Every phase ends at a gate; a red gate stops the
sequence. Nothing starts on a dirty HEAD.

### Phase 0 — reach a clean HEAD (measured 2026-10-14, HEAD `9b8901f8`)

The tree carries the **0.3.019 milestone** prepared by earlier sessions — one unit per its
own allowance note (`docs/notes/discovery-receipt-loc-allowance.md`, "Full-milestone
0.3.019 re-baseline"): the discovery transport/permit/lifecycle/receipt work
(`discovery-probe-permit-allocation-plan.md`, items 1–4 landed) **plus** the release-docs
guardrail fix. Inventory:

| Bundle | Content | State |
|---|---|---|
| Milestone code+tests+docs | 89 staged files (+9,382/−339): `src/source_discovery/*`, `src/shared/{page_fetch_lifecycle,tls_https,http_batch}.py`, `src/bridge/*`, frontend admin ops-health + their tests, staged docs (CHANGELOG 0.3.019 section, DATA_CONTRACT, INDEX rows, admin-bridge-api, scraping-pipeline) | staged |
| Milestone docs, untracked | 3 discovery plan docs + 14 `docs/snapshots/discovery-*.md` + `docs/notes/discovery-receipt-loc-allowance.md` — all already referenced by the staged INDEX rows | untracked → add in C1 |
| Release-docs half | `docs/RELEASE.md`, `docs/DOCS_WORKFLOW.md`, CHANGELOG rule bullet, `release-notes.md` ([0.3.019]), `tools/repo_health/release_docs_policy.py`, `tests/test_release_docs.py`, LOC hunk | unstaged |
| This plan | this file + one `docs/INDEX.md` row | ours → C2 |
| Junk | `$log` (95 B cmd-error text) and dir `C`+U+F03A (botched redirect) | delete |
| Pre-existing stash | `stash@{0}` (2026-10-10) | untouched |

**Both gates are measured red — repairs first:**

| Gate | Finding | Repair (belongs to the milestone) |
|---|---|---|
| `npm run test:py` | 6 failed / 6,801 passed, all `tests/test_rule_list_guardrail.py`: the 0.3.019 CHANGELOG rule bullet writes backticked `src/app_version.py` beside the changelog rule list; `_CHANGELOG_ENFORCEMENT` (`tools/repo_health/rule_list_policy.py:112`) does not declare it (fixtures read the real doc, so one root cause) | drop the literal path from the bullet (phrase as "the app-version module"); alternative: declare it in `_CHANGELOG_ENFORCEMENT` — the audit's other named remedy. Verify: `python -m pytest tests/test_rule_list_guardrail.py tests/test_release_docs.py -q` |
| `npm run lint:repo-guardrails` [docs] | `discovery-performance-follow-up-plan.md`: 7,049 words > 5,000 tripwire; `PLAN_LIFECYCLE_ENFORCE = True` | split: move executed P0–P5 detail into `docs/snapshots/discovery-performance-execution-2026-10-14.md`; keep scope/inventory/Post-P1 inventory/P6/verification/orchestration in the plan (<5,000 words); keep both INDEX rows honest; re-run the tripwire |
| … [line-budget] | `admin-ops-diagnostics-render.test.mjs` 551>400; `test_web_search_directory_audit.py` 837>805; `test_shared_http_batch.py` 440>400 | set exactly those three `files` entries in `tools/repo_health/test_line_budget_baseline.json` to the measured values (exact, no headroom — the LOC note's ratchet discipline); alternative: split the files |
| … [loc] | working tree over baseline: root +1, src +17, tests +59 | after **all** milestone files are staged (the gate counts `git ls-files` only), run `python tools/repo_health/loc_budget.py --update` and append the delta entry to `docs/notes/discovery-receipt-loc-allowance.md` |

**Junk removal:** enumerate both paths at full length, read `$log` and list the `C…` dir
(expect redirect debris), then delete — untracked, so this is status-cleanliness only.

**Commit sequence** (hooks run on each; `--no-verify` never):

| # | Contents | Proposed message |
|---|---|---|
| C1 | staged milestone + untracked milestone docs + the four gate repairs + refreshed baselines | match repo style at implementation time, e.g. `release 0.3.019: probe transport/permit ordering, lifecycle receipts, release-docs guardrail` |
| C2 | this plan + the `docs/INDEX.md` row | `docs(plans): plan the three-opening coverage audit fixes` |

**Phase 0 gates (definition of ready):** `npm run test:py` green; `npm run lint:repo-guardrails`
green; `npm run test:frontend:unit` green (W1 touches frontend/admin); `git status data/`
clean of audit artifacts; `git status --porcelain` empty after C2; push to `origin/main`
passes the pre-push gate (`lint:precommit:ci` + knip; run `PRE_PUSH_FULL_CI=1 npm run
prepush:full` for CI parity). **A blocked push is surfaced to the user, never bypassed.**

Alternative on record (not chosen): `git stash push -u` per bundle — rejected because it
discards the milestone's staged split and parks docs its INDEX rows already reference.

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

- **0.3.019 ships the milestone only** (its CHANGELOG section is written;
  `APP_VERSION = "0.3.019"` already bumped). Our slices target **0.3.020** unless the user
  folds slices 1–3 into 0.3.019 before it cuts (decision below). Release work follows the
  `baluffo-release-ship` flow (preflight → tag → desktop/GHCR/Umbrel verification).
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

1. Phase 0 lands the 0.3.019 milestone as C1 (recommended) vs stashing the bundles.
2. The LOC/line-budget ratchet above (the milestone note records explicit allowance
   precedent; confirm this delta or shrink instead).
3. Rule-list repair: doc reword (recommended) vs registry declare.
4. Release routing: slices 1–3 in 0.3.020 (recommended) vs folding into 0.3.019.

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

## Overall acceptance

| Finding | Done means |
|---|---|
| Opening 2 (Vivid) | Feed title reads *Senior Technical Artist (Remote, Hybrid or On-site)*; no `8351271 …` slug titles remain in teamtailor rows |
| F-2 | Guard test proves the 09-07-style decision is blocked without delivery evidence |
| F-3 | `vividgames.com/jobs` row gone (delta −1); no vividgames redirect errors; teamtailor keeps ≥ 1 |
| Opening 1 (Hatsu) | A row with jobLink containing `8dbe43b3-…` exists in a real run, correct title, attributed to a registered Dover-readable source |
| Opening 3 (Gloam) | Two distinct Gloam rows (Lighting/Technical Artist) in a real run, links resolve |
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
