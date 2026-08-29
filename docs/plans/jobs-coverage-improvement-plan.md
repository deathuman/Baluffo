# Jobs Coverage Improvement Plan

> - **Status:** Active follow-up plan (draft for review)
> - **Use this when:** improving jobs feed coverage — recovering zero-kept static sources, closing provider coverage gaps, promoting staged providers, or reducing sheet dominance
> - **Canonical for:** coverage-improvement prioritization and evidence thresholds; not canonical for adapter internals or source-policy approval authority
> - **Then inspect:** `docs/source-policy-runbook.md`, `docs/adapter-plugin-inventory.md`, `docs/scraping-pipeline.md`, `docs/archive/provider-discovery-coverage-gap-plan.md`, `docs/archive/browser-fallback-pool-plan.md`
> - **Evidence basis:** 2026-07-17 full-run artifacts (`data/jobs-source-state.json.gz`, `data/jobs-fetch-report-summary.json`, `data/registry-conflicts-summary.json`, `_out/source-policy-soak-report.json`), audit snapshot `docs/snapshots/jobs-entry-validation-audit-2026-08-12.md`; refreshed 2026-08-29 against live-run artifacts (`_out/coverage-refresh-2026-08-28/` — see "Evidence refresh" section)
> - **Last updated:** 2026-08-29 (WP0 evidence refresh; WP1 validation passes, link-queue audit, D1 rejections applied to live container; WP2 sample classification + Outerdawn plugin + multi-hop static redirect fix — live-verified, ~50 jobs recovered; WP3 full triage of the 19 remaining sample rows — 3 leaf plugins (astrid/immersity/perfectgarbage) + 7 jobs live-verified locally, registry re-seeds/demotions applied to the live container; WP4 browser-fallback JS-shell classifier widened to catch jQuery-era shells — Konami Gaming recovered 45 jobs via the pool, full production pipeline measurement on the browser-fallback candidates recorded below; WP5 triage of the rendered-empty boards — upsurge + sandsoft plugins recover 6 + 10 roles, optillusion demoted as genuinely closed; WP6 full active-static-registry jQuery-era shell sweep — the widening is classification-only, over-flags ~57% server-rendered sources, and recovers 0 net-new jobs; WP7 Konami Gaming investigation — the "45-job browser-pool recovery" is a false positive (11 nav-link junk rows), the real jobs live on an external UKG Pro/UltiPro board that is currently empty, no promotion or adapter justified now)

## Coverage Baseline (2026-07-17 run, 40,586 rows)

| Lever | Count | Impact |
|---|---|---|
| Feed composition | sheets 78% / static 14% / provider 7.4% | provider+static recovery rebalances dependence on 3 sheets |
| Zero-kept static sources (last run) | **2,428 of 4,479** (54%) | biggest single recoverable surface |
| `site_changed` static + had jobs before | **269** | true parser regressions (page changed, adapter stale) |
| `site_changed` static + never had jobs | 733 | dead or never-parsed candidates |
| `needs_review` static + never had jobs | 807 | likely parser misses / unsupported layouts |
| anti-bot / JS-required static zero-kept | **109** (23 browser-eligible) | browser-fallback recovery (pool shipped 2026-08-11) |
| Provider adapters with zero yield | oracle 2/2, personio 5/7, ashby 6/11, bamboo 8/15, pinpoint 4/7, workday 2/9 | provider validation/triage targets |
| Provider coverage gaps (soak report) | **24** (9 fetched-not-validated, 15 validated-missing-migration-identity) | validation link work, no code |
| Pending registry rows | **163** (132 static, 31 provider: bamboo 13, workday 8, breezy 4…) | staged-but-never-promoted candidates |
| Browser fallback queue | 130 (timeout 91, anti-bot 20, blocked 14, parse_error 3, js 2) | matches anti-bot/js surface above |
| Static high-cost low-yield | e.g. lionhearts 80.9 s/0 kept, redemptiongames 72.6 s/0 | cleanup + budget wins |

## Principles

- **Recover before deleting.** A zero-kept source is a parser miss, a dead page, or an ATS migration — classify each with evidence before dropping (source-policy runbook authority).
- **Provider first.** Supported-provider adapters (bamboo/workday/ashby/personio/oracle…) already exist; validating + staging them yields structured rows that dedup cleanly and are 99.7% Game sector — better coverage per unit effort than static crawling.
- **No new dependencies, no Apify, no generic crawler** (per provider-discovery-coverage-gap-plan).
- **Evidence-tracked losses.** All drops flow through existing `canonicalDropReasons` / registry buckets; no silent removal.

## Work Tracks (priority order)

### Track 1 — Static zero-kept triage (evidence-first, source-policy runbook)

1. **Parser regression recovery (269 `site_changed` + had jobs before).**
   - Pull the 269 source list with old/new fingerprints and last error.
   - Sample 20–30 live pages; classify: adapter layout change (fix parser or plugin), ATS migration (reclassify to provider adapter), dead (drop via dead-source evidence).
   - Reuse `data/jobs-parser-regression-queue.json` (398 entries) and `scripts/source_audit_sweep.py`.
2. **Anti-bot / JS-required recovery (109 sources, 23 browser-eligible).**
   - Re-run the browser-eligible subset through the new browser fallback pool (`BALUFFO_BROWSER_POOL` default on; `browser_fallback_pool.py`).
   - Measure recovery rate vs the 130-entry fallback queue before scaling eligibility.
3. **Never-yield static cleanup (733 + 807 never-had-jobs).**
   - Batch evidence sweep like `docs/snapshots/jobs-dead-source-evidence-2026-04-29.md`; separate dead pages from unsupported layouts.
   - Unsupported layouts worth a plugin get a narrow static plugin (per adapter inventory); the rest are dead-source candidates.

### Track 2 — Provider coverage closure (no-code validation + staging)

1. **Soak report refresh first** (current report is stale: 2026-05-22).
   - `python scripts/provider_migration_staging_refresh.py --data-dir data --out-dir _out --apply-pending` then `python scripts/source_policy_soak_report.py --data-dir data --out-dir _out`.
   - Re-derive the 24 gap rows against current artifacts.
2. **Close the 15 validated-provider-missing-migration-identity rows** via the Admin migration-link workflow (one link at a time, runbook §Guardrails).
3. **Triage the 9 fetched-but-not-validated rows** (e.g. Wolcen bamboo) — probe/retry via `--include-pending-provider-migration` fetch, then validation.
4. **Promote healthy staged providers from pending** (31 provider rows) only with repeated validation evidence (`providerCoverageConsecutiveSuccesses`, `validated_provider` status).

### Track 3 — Provider adapter zero-yield triage (code, bounded)

- Oracle HCM 2/2 zero, personio 5/7 zero, ashby 6/11 zero: bounded live validation on the failing subset; classify as feed-shape change, auth wall, or genuinely empty.
- Each fix is a leaf-adapter change with focused tests + bounded live run (per data-quality skill §5).
- Workday/bamboo already healthy (7/9, 7/15) — lower priority.

### Track 4 — Sheet dominance rebalance (follows Track 2)

- Only after provider/static recovery: re-evaluate whether the default `google_sheets` 78% share is intentional (community sheet includes Tech rows — product call, ties into `non-game-employer-evidence-2026-08-12.md`).

## Suggested Verification

```bash
# Track 1 evidence sweep
python scripts/source_audit_sweep.py --help   # follow runbook usage
# Track 2 refresh
python scripts/provider_migration_staging_refresh.py --data-dir data --out-dir _out --apply-pending
python scripts/source_policy_soak_report.py --data-dir data --out-dir _out
# Bounded fetch of staged providers
python src/jobs_fetcher.py --only-sources <staged> --include-pending-provider-migration --ignore-circuit-breaker
# Gates after any code change
python scripts/precommit_gate.py --mode changed
```

## Open Questions for Review

1. Track 1 scale: sample 20–30 first, or run the full 269-regression sweep? (Recommend: sample first, then sweep.)
2. Dead-source batch: proceed like the 2026-04-29 physical deletion batch, or keep to registry demotion? (Recommend: registry demotion first.)
3. Browser-eligible recovery is capped at 23 now — expand after measuring the pool recovery rate?
4. Track 2 link apply is one-at-a-time by runbook; confirm the 15-link backlog is worth the manual pass.

## Applied 2026-08-12 (operator-approved, Track 2)

Evidence snapshot: `docs/snapshots/provider-coverage-closure-2026-08-12.md`.

- **Migration links applied (2, high-confidence 0.95):** Xsolla → `lever:account:xsolla`; CD PROJEKT RED → `smartrecruiters:company_id:cdprojektred`. `alreadyLinkedCount` 3 → 5; reviewCandidates now 0.
- **Provider rows approved active (9, repeated validation evidence):** bamboo `activategames`, `blazinggriffin`, `catface`, `flyingbark`, `relicentertainment`, `streamlinestudios`; breezy `fugo-games`, `flowplay-llc`, `warhorsestudios`. Active 2268 → 2277.
- **Dead provider rows rejected (3):** `bamboohr:lemonskystudios` (board redirects to BambooHR marketing page) provider + static rows, and `oracle_hcm:glass-egg` (DNS dead). Rejected 0 → 3.
- **Triage findings:** Beamdog / Eleventh Hour / Expression bamboo boards return `[]` (genuinely empty — correct behavior); IllFonic breezy `/json` returns `[]` (empty); reforged/wolcen bamboo have 1 job each (recovery on refresh); workday SSL failures (Aristocrat/Intel/SciPlay/Light & Wonder) are upstream expired certs — transient; Glass Egg static `glassegg.com/careers/` is live (kept — only the oracle host is dead).
- **Validated provider count** 20 → 15 after refresh (workday SSL + empty boards reclassified); next action `resolve_link_ambiguity` (Ubisoft, 6 static candidates, 0.65 confidence — blocked below the 0.75 apply threshold by design).

## Applied 2026-08-13 (operator-approved: Ubisoft link, bulkhead demote, fold-in official Track 3)

Evidence snapshot: `docs/snapshots/widget-board-recovery-2026-08-13.md`.

- **Ubisoft link ambiguity resolved:** `smartrecruiters:company_id:ubisoft2` → `static:listing_url:https://www.ubisoft.com/en-us/company/careers/` @ 0.8 (operator decision; canonical EN careers entry, board API verified 271 postings). Bogus `www.ubisoft.com/careers` row rejected (redirects to homepage). Links 5 → 6; next-action `resolve_link_ambiguity` should clear on refresh.
- **Widget-board recovery:** Coffee Stain teamtailor row added + approved (**2 jobs kept**); sandsoft URL fixed to `sandsoft.com/careers/`; bulkhead demoted+rejected (DNS dead).
- **Browser-fallback measurement (0/3):** konami/sandsoft classify `dead_listing_page` → browser escalation hard-disabled (jQuery-era JS shells missed by `detect_js_shell`); yodo1 fires the pool but is a teamtailor widget with a dead CDN. Do NOT scale browser eligibility; classifier gap noted.
- **Provider zero-yield triage (official Track 3):** ashby 8 boards promoted (k-ID slug fixed `kid`→`k-id`; 90 fetched/87 kept), 5 dead slugs + 5 `/jobs` duplicates rejected, 2 genuinely-empty kept pending; personio — Yager genuinely empty (kept), Welevel 429 transient, InnoGames/Travian pending re-probe after 429 clears; oracle already closed.

## Evidence refresh 2026-08-29 (WP0, read-only)

Re-derived from live 0.2.140 artifacts (run `fetch_bc784dfdf2`, finished 2026-08-29T02:51Z; registry
snapshots local 2026-08-21/22 vintage) using `source_policy_soak_report.py` + dry-run
`provider_migration_staging_refresh.py` against `_out/coverage-refresh-2026-08-28/`. No registry
mutation performed.

### Baseline deltas (2026-07-17 → 2026-08-29)

| Lever | 2026-07-17 | 2026-08-29 | Reading |
|---|---|---|---|
| Provider coverage gaps | 24 | **49** (15 staged-not-fetched, **0 fetched-not-validated**, 31 validated-missing-identity, 3 active-static-despite-provider) | fetched-not-validated cleared by 8/12–8/13 work; missing-identity backlog nearly doubled and is now the main Track 2 surface |
| Validated providers | 20 | **14** (+1 unstable-failed) | workday SSL + empty-board reclassifications; needs re-validation pass |
| Registry active / pending | 2,268 / 163 | 2,301 / 850 (799 static + 51 provider) | pending ballooned — provider subset (51) is the promotion target |
| Static zero-kept (last run) | 2,428 of 4,479 | **~285 genuinely zero-kept** of 2,101 fetched (1,488 more were `cache_within_freshness_window` skips, not failures) | freshness cache masks most of the old zero-kept surface; triage the ~285 real ones |
| Feed composition (kept share) | sheets 78% / static 14% / provider 7.4% | **sheets (csv) 84% / static ~10% / provider ~5%** | sheet dominance grew — Track 4 product call is more relevant, still gated behind Track 2 |
| Suppression readiness | — | 14 providers ready, 14 missing linked static | dynamic suppression has concrete candidates |
| Staging refresh (dry-run) | — | 2 new stageable provider candidates, 140 skipped | small; fold into next Track 2 pass |

### Revised work-track priorities

1. **Track 2 (WP1) — missing-migration-identity backlog (31 rows).** Includes teamtailor/jazzhr
   providers already keeping jobs (Tanglewood 2, Fatshark 5, Vivid 1, Lost Boys Interactive…) —
   link closure via the Admin workflow unlocks identity links and future suppression. Then
   suppression: 3 active statics with validated providers at 29–120 consecutive successes
   (CDPR, Ubisoft, Bandai Namco) are ready for the suppression-eligibility review.
2. **Track 3 (WP1b) — staged-not-fetched (15 bamboo/teamtailor boards).** Bounded
   `--only-sources` fetch with `--include-pending-provider-migration`; expect most to be the
   known-empty Beamdog/Eleventh Hour/Expression boards — that is a valid closure outcome.
3. **Track 1 (WP2) — real zero-kept triage (~285).** The 2,428 baseline was inflated by
   freshness-cache skips; sample the ~285 actually-fetched zero-kept statics
   (reusing `jobs-parser-regression-queue.json`) before any sweep.
4. **Track 4 (WP5) — sheet share now 84%.** Re-evaluate after Track 2 lands.

Refresh artifacts (evidence): `_out/coverage-refresh-2026-08-28/` (soak report json+md, staging
refresh audit, live `fetch-report-summary`/`fetcher-metrics`/`sync-status` payloads, staging
data dir). Registry remains unmutated; `--apply-pending` stays an explicit operator step.

## Applied 2026-08-29 (WP1: provider validation evidence + link-queue audit)

Runbook-sanctioned bounded fetches against local runtime (no registry mutation, no link applies):

- **Bounded validation fetches** (per the soak's `safeLocalCommands`, `--include-pending-provider-migration`):
  `bamboohr_sources,breezy_sources,workday_sources` × 2 passes, then
  `personio_sources,ashby_sources,pinpoint_sources,smartrecruiters_sources` × 1 pass. All clean
  (0–1 failed sources per run; personio 429 cohort from 8/13 **cleared** — InnoGames/Travian/Welevel
  now have current evidence with 0 fetch failures).
- **Staged-not-fetched: 15 → 0.** `missingDetailEvidence` 15 → 0. Every pending provider-migration
  candidate now has row-level fetch evidence (two real passes for the bamboo/breezy/workday cohort).
- **Link-backfill queue audited: no applicable work.** 0 review candidates; 8 candidate links are
  all `provider_shaped_self_link` (blocked by design, `apiEligible=false`); 15 identities already
  linked; 6 disambiguation blockers are `source_state_not_ok`. The 8/12–8/13 link-closure line is
  **done** — remaining "missing identity" rows lack the evidence threshold for reviewable
  candidates, which is a valid closed state, not an action item.
- **Provider validation statusCounts** (soak-computed): `validated_provider` 15, `unstable` 8,
  `needs_review` 5, `failed` 2. Gap composition after the passes: 32 validated-missing-identity,
  14 fetched-but-not-validated (empty boards), 3 active-static-despite-provider.

### Operator decisions pending (explicit Admin actions, one at a time)

1. **14 empty-board pending providers** (bamboo/teamtailor; kept 0 across two clean passes —
   Beamdog, Dino Polo Club, Eleventh Hour, Expression, Reforged, …). Same class as the 8/12
   `lemonskystudios` rejection. Recommend: reject the confirmed-dead ones via Admin, one at a time;
   keep the ones with a plausible refresh story (e.g. Reforged/Wolcen had 1 job each in 8/12 triage).
2. **Promotion review** of pending providers with jobs-keeping evidence among
   `unstable`/`needs_review` (registry pending provider subset ~51 rows).
3. **Suppression-eligibility review** for active statics with validated providers
   (CDPR/Ubisoft/Bandai Namco class; requires the linked-static validation fetch passes when chosen).

Caveat: local runtime registry is the 2026-08-21/22 snapshot; live container state is newer. Before
applying any future link/rejection through Admin, reconcile local registry state with the sync
source so an apply does not clobber newer live-side changes. `migrationSourceIdentity` metadata
applied locally reaches the container through source-sync (normal registry metadata per runbook).

## Applied 2026-08-29 (WP2: zero-kept static sample classification)

### Reframed zero-kept surface

The 2,428 zero-kept baseline was inflated by `cache_within_freshness_window` skips. In run
`fetch_bc784dfdf2` only **285 statics were genuinely fetched and kept 0**:

| Bucket | Count | Reading |
|---|---|---|
| `site_changed` | 239 | treated as parser/redirect/dead candidates |
| `needs_review` | 39 | unsupported layouts / new candidates |
| `js_required` | 5 | browser-eligible (pool available) |
| `seed_invalid` | 2 | seed/identity issues |

All 239 `site_changed` rows have `lastSuccessAt` empty and zero-job streak 0 in the fresh
artifacts — no confirmed former-yield history available, so they cannot be called regressions by
saved evidence alone; live classification is required (done below).

### Stratified sample + live probe (28 rows, generated `wp2-zero-kept-sample.{json,csv}` + `wp2-sample-probe.json`)

Proportionate sample: 20 `site_changed`, 4 `needs_review`, 2 `js_required`, 2 `seed_invalid`.
Live HTTP probe (bounded, read-only) result:

- **15 `jobs_page`** (live page with job content — but Baluffo static parser kept 0): Cryptyd,
  Astrid, Aden, EA Capital Games, Leia/immersity, Optillusion, Upsurge, Bandai Namco JP,
  Media Vision, Funovus, Brainium; `needs_review` Outerdawn, Devolver; `seed_invalid`
  bkomstudios (Zoho), playsimple (greenhouse) — **parser misses / layout changes, recoverable**.
- **7 `has_tokens`** (live, thin/partial job tokens): Ubisoft Toronto, Perfect Garbage, Strange
  Beat, Merge Games, Appsoleut, Nomada, Dynamic Next — likely parser misses too, needs per-page review.
- **3 `http_404`** (dead): Fatshark Games, ASBO Interactive, Brightline Interactive —
  **dead-source candidates** (demote via registry, evidence-first).
- **2 `err_URLError`** (probe failed): Almedia, Marvelous USA — reprobe/verify.
- **1 `empty_page`** (JS shell): Twitch — matches the `js_required`/`detect_js_shell` browser gap.

### Revised Track 1 guidance (evidence-based)

1. **Parser recovery is the primary lever, not deletion.** ~22/28 sampled rows have live
   job pages that the static parser misses. Recommend a narrow leaf-parser/plugin trial on the
   confirmed `jobs_page` subset (start with the 3 `needs_review`/`seed_invalid` ones that already
   expose known careers URLs), not a dead-source sweep.
2. **Dead-source demotion is small and bounded.** Only the confirmed 404s (3 sample + whatever a
   full sweep confirms) are dead-source candidates; demote via registry, not deletion.
3. **Browser eligibility stays paused** (Twitch JS shell confirms the classifier gap; 0/3 from
   8/13).
4. Sample artifacts are evidence in `_out/coverage-refresh-2026-08-28/`; they are not code changes.

### WP2 implementation — Outerdawn static plugin (first parser-recovery fix)

Picked **Outerdawn** (`www.outerdawn.com/careers`) as the first recovery target: it is in the
`needs_review` bucket, the page fetches clean (200, no JS/anti-bot), and all five live roles are
plain Webflow `careerrow w-dyn-item` blocks. The generic parser missed it because the link text is
always the generic "Read &amp; Apply" and the role title lives in a sibling `<h3
class="contentbox__heading">` (link-text-based title extraction fails).

Implemented `src/jobs/adapters/plugins/static/outerdawn.py`:
- `can_handle` for `www.outerdawn.com` / `outerdawn.com` (registered in
  `plugins/static/register.py`, priority 90).
- Parses each `careerrow` block: title from `contentbox__heading`, location from
  `contentbox__subheading` (normalized via `normalize_location_details`), link from the
  `/careers/<slug>` button href; dedupes by absolute URL.
- `parser_stale_hint="outerdawn_listing_present_but_plugin_empty"` so an empty parse still lands
  in the parser-stale classification rather than silently passing.

Verified:
- Standard plugin tests: `test_standard_plugins.py` `_PLUGIN_CASES` row for outerdawn (success
  tags rows + empty-listing stale meta + fetch-failure blocked meta).
- Live-data check against the captured page: **5/5 roles extracted** with correct titles
  (Data Analyst (Mid or Senior Level), Senior Unity Developer, Senior UI Artist, Senior Unity
  Programmer, QA Lead), Auckland/New Zealand locations, and absolute job links.
- `tests/jobs/adapters/plugins/static tests/jobs_static tests/test_jobs_fetcher.py`: 313 passed.
- Registry dispatch resolves `outerdawn` for both hosts at priority 90; unrelated hosts still
  raise `NoPluginFoundError`.
- `docs/adapter-plugin-inventory.md` static-plugins table updated.

Next candidates in the same leaf-plugin lane: Cryptyd (`/join-us/`, Webflow), Aden
(`join.aden.pt` external ATS link detection), and re-probing the `has_tokens` group. Sources
behind Cloudflare/Vercel walls (Devolver, Upsurge, Brainium) belong to the browser-fallback
track, not the static parser.

## Applied 2026-08-29 (WP1 operator decisions — D1 rejections applied to live container)

Operator-approved on 2026-08-29. Applied via `POST /registry/reject` on the live container
(`192.168.50.61:8877`) — the synced authority — after reconciling against the live registry table
(the local 8/21 snapshot was stale; actions were taken where the current state lives, so no
clobber of newer live-side rows).

Rejected 4 confirmed-empty pending providers (8/12 empty-board triage + two clean WP1 fetch
passes kept 0 each):

| id | Studio | Evidence |
|---|---|---|
| `bamboohr:listing_url:https://beamdog.bamboohr.com/careers` | Beamdog | 8/12 verified empty board; 2× WP1 passes kept 0 |
| `bamboohr:listing_url:https://eleventhhourgames.bamboohr.com/careers` | Eleventh Hour | 8/12 verified empty board; 2× WP1 passes kept 0 |
| `bamboohr:listing_url:https://expressiongames.bamboohr.com/careers` | Expression | 8/12 verified empty board; 2× WP1 passes kept 0 |
| `breezy:board_url:https://illfonic.breezy.hr/` | IllFonic | 8/12 verified empty (`/json` `[]`); 2× WP1 passes kept 0 |

Result: `rejected: 4`, pending 866 → **862**, rejected 0 → **4**, active unchanged 2,305.
Confirmed via `/registry/sources?view=table` read-back: all 4 now in the rejected bucket
(will propagate to containers/desktop via the next source-sync push).

Held pending (no action — needs fresh probe or adapter review, not rejection): Dino Polo Club,
Reforged, Wolcen (bamboo, prior success history), InnoGames/Travian (personio — re-probed clean,
kept 0 suggests feed parse review, not dead board), Lucky VR (breezy jobs=3, conflict-demoted),
~16 stale ashby slugs (per-slug review deferred). Promotions: none actionable today. Suppression:
deferred (needs linked-static validation evidence runs).

## Applied 2026-08-29 (WP2 follow-up: multi-hop static redirect fix — root-cause correction)

Deeper analysis of the fresh fetch-report errors **corrected the WP2 diagnosis**: most of the 285
genuinely-zero-kept statics never reached the parser at all. The static fetcher failed on
**redirect handling**, not layout parsing:

- **HTTP 301/307 treated as terminal**: Funovus (`funovus.com/careers` → 301 →
  `www.funovus.com:443/careers` → 301 → `/careers/`), Optillusion, Upsurge, Bandai Namco JP.
- **"Static redirect loop" false positives**: Cryptyd, Aden, Brainium, Media Vision, Voxel Agents,
  Bandai Namco Mobile — `_safe_redirect_url` compared the **normalized** target against the
  normalized source, and `normalize_url` strips trailing slashes, so a legitimate
  `/careers` → `/careers/` canonicalization was flagged as a loop.
- **"Unsafe static redirect"**: Leia, NAMCO BANDAI (cross-host, correctly rejected).
- **HTTP 429**: Devolver Digital (transient). **Network error**: Outerdawn (transient; plugin now
  in place for when the fetch succeeds).

The single-hop redirect follow could not complete the common two-hop
apex→www-with-port→trailing-slash chain that CDN-fronted studio sites emit.

### Fix (bounded multi-hop redirect following)

- `static_runtime_support.py` `fetch_html_cached`: bounded loop (`_MAX_STATIC_REDIRECT_HOPS = 4`)
  with cross-hop raw-URL `visited` set, per-hop cache reuse, and cache fill for every visited hop.
- `_safe_redirect_url`: resolves the **raw** redirect target (preserving trailing-slash
  distinctions) instead of the normalized one; loop detection now compares raw URLs, so a
  trailing-slash canonicalization is followed rather than falsely rejected. Scheme/credential/
  cross-host/https-downgrade safety checks unchanged.
- `static_listing_runner.py`: sync `_fetch_listing_html_sync` and the async listing fetch path
  converted from single-hop to the same bounded loop (first hop keeps source retries; subsequent
  hops retry-free).

### Verified

- `test_static_redirect_fetch.py`: 10 passed — 3 new tests (two-hop www/port/trailing-slash chain
  with per-hop cache fill; hop-cap exhaustion after 4; cross-hop loop detection).
- Full battery `tests/jobs_static + plugins/static + test_jobs_fetcher + finalize harness`:
  **322 passed**.
- Expected recovery set (pending pipeline confirmation): Funovus, Optillusion, Upsurge, Bandai
  Namco JP, Cryptyd, Aden, Brainium, Media Vision, Voxel Agents, Bandai Namco Mobile — sources
  whose HTML was never reached by the parser. Cryptyd additionally verified as a contact-form page
  (no real listings — expect dead-listing classification, which is correct behavior).

### Live verification (2026-08-29, bounded fetch of the recovery set)

`python src/jobs_fetcher.py --only-sources <11 recovery ids> --ignore-circuit-breaker
--force-refresh-all` → **0 failed sources** (previously 11/11 error/site_changed). Per-source:

| Source | Before | After | Note |
|---|---|---|---|
| Bandai Namco JP (`bandainamcoent.co.jp/job/`) | 0 (301) | **35 kept** | biggest single recovery |
| Outerdawn | 0 (network error) | **5 kept** | plugin extracts exactly the 5 probed roles |
| Funovus | 0 (301 chain) | **5 kept** | 3 listing anchors + detail traversal |
| The Voxel Agents | 0 (redirect loop) | **2 kept** | |
| Aden Interactive | 0 (redirect loop) | **1 kept** | |
| Brainium | 0 (redirect loop) | **1 kept** | Dayforce HCM apply link |
| Media Vision | 0 (redirect loop) | **1 kept** | |
| Bandai Namco Mobile | 0 | 0, `needs_review` | reaches parser now; genuinely thin page |
| Cryptyd | 0 | 0, `needs_review` | contact-form page (no listings) — correct classification |
| Optillusion | 0 | 0, `needs_review` | jobs behind JS modal — browser-fallback track |
| Upsurge | 0 | 0, `needs_review` | JS challenge — browser-fallback track |

**Net: ~50 jobs recovered** from 11 sources; all 11 now complete with `status=ok` instead of
redirect errors. The 4 still-zero sources are correctly classified (`needs_review`), with the
JS-shell ones belonging to the browser-fallback track.

## Applied 2026-08-29 (WP3: static zero-kept triage — full pass)

### Scope

Carried all 19 WP2-sample rows not dispositioned by the WP2 fix to a final outcome. Ground
work: `_out/coverage-refresh-2026-08-28/wp3-probe-results.json` + `wp3-probes/` (HTML captures,
read-only bounded probes), `wp3-sample-dispositions.json` (per-row disposition artifact),
local bounded fetcher verification (`verify-wp3/jobs-fetch-report.json`), and live-container
registry actions (`192.168.50.61:8877`, operator-approved per plan §3).

### Dispositions (19 rows)

| Row | Probe | Disposition | Evidence / action |
|---|---|---|---|
| Fatshark Games (`fatsharkgames.com/career`) | 404 | **Demoted** (stale alias) | Careers live at `jobs.fatsharkgames.com`, already covered by active provider row `teamtailor:listing_url:https://jobs.fatsharkgames.com`; live check-source found 5 jobs via alternate |
| ASBO Interactive | 404 | **Demoted** (dead) | Two independent 404 probes + live check-source `not_found`, no working alternate |
| Brightline Interactive | 404 (`careers.html`) | **Re-seeded** (page moved) | Live listing is `/careers` (2 BambooHR postings inline); row demoted, `/careers` appended as page variant, re-approved. Staged bamboo candidate `brightline.bamboohr.com/careers/65` is a detail URL, not the board — Track 2 item |
| Almedia | TLS cert expired | `needs_review` (keep) | Site live (verify-disabled 200); page says apply via email; upstream expired cert is transient, not dead |
| Marvelous USA | TLS cert expired | `needs_review` (identity issue) | Row points at homepage; careers content links to XSEED BambooHR board `xseedgames.bamboohr.com` (no provider row yet) — Track 2 provider-staging candidate |
| Astrid Entertainment | jobs_page | **Recovered** (plugin `astrid`) | 4 server-rendered Workable links missed by generic parser (`/j/<id>` path); local fetch 4 kept |
| EA Capital Games | jobs_page | `needs_review` | Careers link out to `jobs.ea.com` (SuccessFactors CSB, Vue SPA, no adapter); no on-page listings |
| Leia (immersity) | jobs_page (cross-host) | **Re-seeded + recovered** (plugin `immersity`) | `leiainc.com/careers` cross-host redirect rejected; re-seeded to `immersity.ai/careers` + approved; 1 Webflow role; local fetch 1 kept |
| Ubisoft Toronto | has_tokens | `needs_review` | JS shell; jobs live on already-linked Ubisoft SmartRecruiters board |
| Perfect Garbage | has_tokens (cross-host) | **Re-seeded + recovered** (plugin `perfectgarbage`) | `perfectgarbagestudios.com` cross-host redirect rejected; re-seeded to `perfectgarbage.com/careers` + approved; 2 Work With Indies postings; local fetch 2 kept |
| Strange Beat Games | has_tokens | `needs_review` (genuinely empty) | Explicit "no open positions" marker — correct zero-kept behavior |
| Merge Games | has_tokens (redirect) | **Demoted** (absorbed) | Redirects to Silver Lining Interactive info page, no listings; already pending live-side |
| Appsoleut Games | has_tokens | `needs_review` | `#open_positions` section is marketing text in served HTML; listings not server-rendered |
| Nomada Studio | has_tokens | `needs_review` (genuinely empty) | Explicit "no open positions" marker |
| Devolver Digital | jobs_page | `needs_review` (genuinely empty) — **removed from browser-fallback list** | Earlier 429 misread as Cloudflare wall; fresh probe 200 with explicit "No Open Positions" |
| Twitch | empty_page | **Browser-fallback track (WP4)** | JS shell; jobs.twitch.tv redirects into a JS app; classifier-gap blocker stands |
| Dynamic Next | has_tokens | `needs_review` | Careers pitch page; no structured listings in HTML |
| bkomstudios (Zoho Recruit) | jobs_page | `needs_review` (unsupported ATS) | Zoho Lyte SPA shell, no embedded JSON; no Zoho adapter exists |
| playsimple (Zoho Recruit) | jobs_page | `needs_review` (unsupported ATS) | Same Zoho SPA shell; no static recovery without an adapter |

### WP3 implementation — three leaf static plugins

All three follow the `outerdawn.py` shape (`can_handle` by host, priority 90 in
`plugins/static/register.py`, `parser_stale_hint` on empty parses):

- **`astrid.py`** (`astridentertainment.com`): WordPress `job-listing` blocks with `job-title`
anchor → `apply.workable.com/j/<id>` + `job-location` cell. Generic parser rejected the Workable
paths (no detail-path token). Recover **4** roles (Senior Gameplay Engineer ×3, Senior UI/UX Designer).
- **`immersity.py`** (`immersity.ai`): Webflow `careers_cms_item` blocks; title/location from
`u-text-style-h4` / `u-color-faded`, link from `/company-careers/<slug>`. Recover **1** role
(IT Operations Specialist, Nashua NH).
- **`perfectgarbage.py`** (`perfectgarbage.com`): Squarespace anchors to
`workwithindies.com/careers/perfect-garbage-<slug>` with `Hiring: <title>` labels. Recover **2**
roles (Senior Programmer, Technical Sound Designer).

Tests: `_PLUGIN_CASES` rows in `test_standard_plugins.py` (success / fetch-fail / empty-listing
meta) + focused `test_wp3_leaf_plugins.py` (multi-row extraction, location pass-through, host
dispatch). Full static battery **337 passed** (baseline 322). `docs/adapter-plugin-inventory.md`
static-plugins table updated.

### Live verification (local bounded fetch, re-seeded identities)

`python src/jobs_fetcher.py --only-sources <3 ids> --force-refresh-all --ignore-circuit-breaker`
→ **0 failed sources, 7 kept**:

| Source | Kept | Note |
|---|---|---|
| Astrid (`astridentertainment.com/careers`) | **4** | plugin extracts the 4 Workable roles |
| Immersity (`immersity.ai/careers`) | **1** | plugin extracts the Webflow role |
| Perfect Garbage (`perfectgarbage.com/careers`) | **2** | plugin extracts the 2 Work With Indies postings |

(The local registry snapshot was temporarily re-seeded for the verification run and restored;
the authoritative re-seeds live on the container registry.)

### Registry actions applied to the live container (2026-08-29, operator-approved)

- **Demoted to pending (5):** ASBO, Fatshark static alias, Brightline `careers.html`, Leia
`leiainc.com/careers`, Perfect Garbage `perfectgarbagestudios.com/careers`. Merge Games was
already pending live-side (absorbed).
- **Added + approved (3):** `static:listing_url:https://immersity.ai/careers`,
`static:listing_url:https://perfectgarbage.com/careers`, and the Brightline row (with
`brightlineinteractive.com/careers` appended as a page variant). Summary after: active 2303 /
pending 867 / rejected 4. Changes propagate via the normal source-sync push.
- **Evidence recorded on live rows** via `POST /discovery/check-source` per source
(`jobsFound` / `lastProbeError` / `lastProbedAt` updated, `source_check_updated` reasons).

### WP4 handoff (browser-fallback track, no code in this pass)

- **Candidates:** Twitch (`m.twitch.tv/careers`, JS shell). Devolver was removed (explicit no
openings — correct classification, not a wall). Optillusion and Upsurge stay `needs_review`
from WP2 (JS modal / JS challenge).
- **Standing blocker:** `detect_js_shell` classifier gap (jQuery-era JS shells never reach the
browser pool; 0/3 pool measurement from 8/13). WP4 stays gated.

### WP4 implementation — widened jQuery-era JS-shell classifier + pool measurement

**Change:** `detect_js_shell` (`_heuristics.py`) now also recognizes jQuery-era / legacy-hydration
shells (Ember, AngularJS, Backbone, jQuery SPA) that emit no modern React/Next/Angular-2 boot
tokens. New corroborated signals — handlebars/ember/knockout template markers, client-hydrated
`data-href` placeholders, a legacy-SPA boot (≥2 of ng-app/ng-controller/ng-view/ng-repeat/
backbone/requirejs), or jQuery rehydrating an explicit job-listing container — only fire when a
careers/job context is present. This keeps plain server-rendered pages that merely bundle
jQuery/handlebars negative (validated negative: astrid, leia/immersity, appsoleut, dynamicnext,
nomada, strangebeat, mergegames). Tests: `test_wp4_js_shell_detection.py` (10 cases); full static
battery 387 passed; precommit gate green.

**Effect on empty-parse classification** (verified against live captures):

| Capture | Before | After |
|---|---|---|
| sandsoft | `needs_review` (browser-rec False) | `blocked_or_challenge`, browser-fallback rec **True** |
| twitch | `blocked_or_challenge` (SPA already caught) | unchanged `blocked_or_challenge`, rec True |
| konami main `/jobs/` | `dead_listing_page` | `empty_confirmed` (page has explicit "no open positions") — correct |
| astrid / leia / appsoleut | `needs_review` | `needs_review` (server-rendered, not flagged) |

**Pool recovery measurement** (bounded pipeline run, `--only-sources` on the 6 candidates,
`--force-refresh-all`, browser fallback on; before/after reports in `_out/coverage-refresh-2026-08-28/wp4-{baseline,after}/`):

| Source | Before | After | Note |
|---|---|---|---|
| **Konami Gaming** (`konamigaming.com/careers`) | 0 (needs_review) | **45 kept (junk — see WP7)** | 45 raw anchors are nav links; real jobs on external UltiPro board (currently empty) |
| konami main `/jobs/` | 0 dead_listing | 0 `empty_confirmed` | page explicitly says "no open positions" — correct |
| sandsoft | 0 needs_review | 0, now `blocked_or_challenge` + browser-rec | empty board, classified as shell |
| twitch | 0 site_changed | 0 site_changed | JS board behind the careers route; lexical `react` hits pre-existing |
| upsurge / optillusion | 0 needs_review | 0 needs_review | jobs behind JS modal / challenge — remain browser-fallback candidates |

**Outcome:** browser-fallback recovery is positive (45 jobs from Konami Gaming via the pool), and
the general `detect_js_shell` classifier gap for jQuery-era shells is closed: plugin empty-parse
now flags these as `blocked_or_challenge` + browser-recommended instead of silently `needs_review`,
so they enter the browser fallback queue / stay escalation-eligible rather than being dropped as dead
listings. Remaining WP4 candidates (upsurge, optillusion, twitch) are jobs genuinely behind JS
modal/challenge routes, not classifier misses; they stay `needs_review`/browser-eligible by design.

### WP4 full production pipeline measurement (2026-08-29, after the classifier change)

`python -m src.jobs.pipeline --output-dir _out/coverage-refresh-2026-08-28/wp4-scale
--only-sources <6 candidates> --force-refresh-all --ignore-circuit-breaker
--no-seed-existing-output --no-preserve-previous-on-empty --timeout 14
--browser-fallback-max-workers 3 --quiet` → output report
`wp4-scale/jobs-fetch-report.json`. Per-source (kept / pool-fallback acquisitions /
`browserEscalationEligible` / classification), compared across `wp4-{baseline,after,scale}`:

| Source | kept | pool fbs | elig | classification |
|---|---|---|---|---|
| **Konami Gaming** (`konamigaming.com/careers`) | **45** (all 3 runs, junk — see WP7) | 1 | — | ok |
| konami main `/jobs/` | 0 | 1–2 | False | `empty_confirmed` (explicit "no open positions") |
| sandsoft | 0 | 1–2 | False | `dead_listing_page` (rendered board empty) |
| upsurge | 0 | 2 | False | `dead_listing_page` |
| optillusion | 0 | 1 | False | `dead_listing_page` |
| twitch | 0 | 2 | False | `site_changed` |

**Honest finding — the four "newly-unblocked" shells yield no *new* recoveries.** All four
sources already reached the browser pool in the *baseline* (pool-fallback acquisitions 1–2),
because the inline `empty_page`/`jobs_path` triggers fire on careers URLs independently of
`detect_js_shell`. The widened classifier closes the jQuery-era *classification* gap (plugin
empty-parse now labels them `blocked_or_challenge` + browser-recommended instead of silently
`needs_review`/dead) and future-proofs non-careers-URL jQuery shells, but for these specific rows
the pool was already being reached. They keep 0 because the rendered boards genuinely expose no
job rows (empty boards / JS modal / challenge), so forcing `browserEscalationEligible=True` here
would merely re-run an empty board — not a recovery. **Konami Gaming's 45 "jobs" were the only
apparent pool win — and WP7 showed they are nav-link junk, not jobs (the browser-pool win count
is 0). Do not scale browser eligibility beyond it on this evidence.**

Remaining path to more browser recovery: re-classify the genuinely-empty dead boards
(sandsoft, upsurge, optillusion) via registry demotion/review rather than browser escalation, and
keep twitch's JS board on the browser-eligible review list (its roles are not server-rendered).

### WP5 triage (2026-08-29) — rendered-empty WP4 boards: Upsurge / Sandsoft plugin recovery, Optillusion demotion

Follow-up to the WP4 measurement, which showed the three shells (sandsoft, upsurge, optillusion)
yielding **0 recovery** via the browser pool. Triage targeted each toward either plugin recovery
or registry demotion based on what the rendered boards actually expose:

| Source | Live probe | Reading | Disposition | Action |
|---|---|---|---|---|
| **upsurgestudios.com/careers/** | 200, server-rendered | 6 `CareerSummary` roles (title + Job Description / Requirements inline), **no per-role links** | **Plugin recovery** | New leaf plugin `upsurge` (6 fragment-anchored rows) |
| **sandsoft.com/careers/** | 200, jQuery-era shell; **`/careers/feed/` RSS** | **10 postings** in the server-rendered feed (each `<item>` = title + detail link); live check-source `jobsFound: 2, weakSignal: true` | **Plugin recovery** | New leaf plugin `sandsoft` (fetches `/careers/feed/`, parses 10 postings) |
| **optillusion.games/job** | 200, server-rendered | Explicit "We are not actively hiring" / "Job Openings Currently Closed"; `/jobs` and `/careers` both 404; zero open-role headings domain-wide | **Genuinely empty → demote** | `/registry/demote-active` on live container — active 2303 → **2302**, pending 867 → **868** |

**Implementation** (both follow the established leaf-plugin shape, priority 90,
`parser_stale_hint` on empty parses):

- **`upsurge.py`** (`upsurgestudios.com`): splits on `class=CareerSummary` blocks, title from
  `CareerSummary__Title`. The page emits no per-role links, so each row is anchored to the
  careers page with a title-derived `#<slug>` fragment to keep `sourceJobId`s distinct and
  on-domain. Extracts **6** live roles.
- **`sandsoft.py`** (`sandsoft.com`): custom `run` that derives the listing's server-rendered
  RSS feed URL (`/careers/feed/`) `_feed_url` and parses `<item>` title+link. The listing page
  itself is the jQuery-era shell the WP4 classifier now flags, so the feed is the reliable,
  non-browser source of truth. Extracts **10** postings (feed verified live).

Verified: `test_wp5_leaf_plugins.py` (12 cases: multi-row extraction, fragment-link uniqueness,
`_feed_url` slash forms, empty-feed, host dispatch) + static plugins battery **57 passed**
+ `tests/jobs_static` **282 passed**; precommit gate green. `docs/adapter-plugin-inventory.md`
static-plugins table and CHANGELOG updated.

Live registry action was evidence-backed: sandsoft kept active (feed liveness + check-source
corroboration), upsurge kept active (6 server-rendered roles — a plugin miss, not a dead board),
and optillusion demoted (genuinely closed domain-wide, no alternate page or provider coverage).
Changes propagate via the normal source-sync push; the new plugins recover the roles on the next
default pipeline run without any browser escalation.

### WP6 measurement (2026-08-29) — full active-static-registry jQuery-era shell sweep

Question: among all **2,110 active static** listing URLs, which do the WP4-widened `detect_js_shell`
now flag as jQuery-era shells, and do any actually hold recoverable jobs? Answer: the widening is
classification-only — it flags hundreds of already-working server-rendered pages as shells and
surfaces **0 net-new recoverable jobs**.

**Sweep:** bounded concurrent fetch (16 workers, 2/host, 7 s timeout, 3 MiB body cap) of every
unique active static URL → 1,963 fetched / 147 fetch-error. `detect_js_shell` flagged **942** total;
split by detection tier (re-derivation against the modern-SPA tier vs the WP4 legacy/jQuery tier):

| Tier | Sources |
|---|---|
| Modern SPA already caught pre-WP4 (`react`/`<div id=root>`/`window.__` …) | 346 |
| **Legacy / jQuery-era widening only** (newly caught by WP4) | **347** |
| Both tiers | 247 |
| **Widening-addressed total** (legacy-only + both) | **594** |

The loose `jquery>=2+listing` rule is by far the most common latest-sig, driving most of the 594.

**Production pipeline pass** (`--only-sources` on the 347 legacy-only ids, `--force-refresh-all`,
`--no-seed-existing-output`, browser fallback on, 3 browser workers) → 346 sources, **2,646 jobs kept**:

| Outcome | Count | Reading |
|---|---|---|
| **Already keeps jobs** via the generic parser (no browser) | **196 / 346 (~57%)** | reaktor 140, immersivetouch 126, obsidian 106, wildbrain 630, valve 31+31, kojima 27, hrmos gamefreak 42, … — **false-positive shell flags**: server-rendered pages that merely bundle jQuery + a `job-listing`/`opening` token |
| Keeps 0 | 150 | 128 `needs_review`, 22 `broken_extraction`; 19 also `status=error` |
| Recovered by the browser pool | **0** | 40 pool acquisitions across the run, **0 net recovery** |

**Conclusion (honest, matches WP4/WP5):** the jQuery-era widening does not surface recoverable jobs.
The newly-flagged shells that *do* hold jobs (reaktor, obsidian, valve, immersivetouch, …) are
server-rendered and already kept by the existing generic parser — their shell flag is a false
positive that would trigger an unnecessary browser render. The genuinely-zero ones keep 0 even
through browser (genuinely empty/dead or unsupported layouts). **Do not scale browser escalation on
this set.** The widening's value is classification honesty (real jQuery shells label
`blocked_or_challenge` instead of silently `needs_review`), not recovery. If browser-escalation
volume matters, tighten the loose `jquery>=2+listing` corroboration (drives nearly all the false
positives) as a future item.

Working artifacts: `_out/coverage-refresh-2026-08-28/wp6-{shell-sweep,run,measurement}*/`
(evidence; not committed). Registry unchanged — no demote/promote on this pass.

### WP7 investigation (2026-08-29) — Konami Gaming "45-job browser-pool recovery" is a false positive

**Question:** is the WP4 headline browser-pool recovery (Konami Gaming, 45 kept) stable, and is it
worth promoting or converting to a dedicated adapter? **Answer: the 45 is stable but it is not
jobs — it is navigation-link noise; do not promote it, and a dedicated adapter is technically
feasible but inert today (the real board is empty).**

**The 45 is deterministic junk, stable across 4 runs.** Source-level `keptCount` is 45 in all three
WP4 runs (`wp4-{baseline,after,scale}`) and a fresh 2026-08-29 run (`kg-run`); `loss` shows
`dedupMerged 34 → finalOutput 11` in every run. The 11 unique rows are all menu/footer/nav anchors
from the browser-rendered Sitefinity careers page — "Systems", "Sign Out", "Request Account",
"My Account", "Log-In to Account", "Leadership Team", "JTEST-AI…", "HIRING THE BEST & BRIGHTEST",
"DIVERSITY & INCLUSION", "OUR RECRUITMENT PHILOSOPHY", "Architecture" — **zero actual job
postings**. The generic parser extracted the rendered page's anchors as "jobs"; the
`staticNonJobUrlRejected` gate dropped 66 others but these 11 passed through. The earlier WP4/WP6
claims that Konami Gaming's 45 is "the whole browser-pool win" are therefore **incorrect** — the
browser-pool win count is effectively 0, and the WP4 `do not scale browser eligibility` guidance
was right for the wrong reason (it was the only *apparent* win).

**The real jobs live on an external UKG Pro (UltiPro) board, currently empty.** The careers page
links out ("Click Here") to `https://recruiting.ultipro.com/KON1000/JobBoard/c70fc266-51c5-5296-2005-ff4f122ccc1c`
and a Paycom ATS page. The UltiPro board is a React app whose XHR endpoint
`/KON1000/JobBoard/<guid>/JobBoardView/LoadSearchResults` returns clean structured JSON
(`{opportunities, totalCount, locations}`) with **no browser required** — verified `totalCount: 0`
across four payload variants (the board has zero open positions right now). Paycom's page is a
JS-only "Loading…" shell. Live `check-source` agrees: `jobsFound: 6, weakSignal: true`,
browser attempted but not used.

**Decision: no promotion, no adapter now.** Promoting would publish 11 junk nav rows as jobs.
`recruiting.ultipro.com` is already a recognized ATS host in page-gating `_JOB_LISTING_HREF_HINTS`,
but the repo has no UltiPro provider adapter and no other registry row references UltiPro/Paycom,
so a full adapter has no surface. The low-cost path when the board starts posting: stage an
UltiPro provider row (Track 2 style) or a leaf plugin hitting `LoadSearchResults` — the API is
standard across UltiPro boards and needs no browser. Until then the correct disposition is to treat
`konamigaming.com/careers` as an ATS-redirect careers page (external board, currently empty),
not a listing with 45 jobs. Evidence in `_out/coverage-refresh-2026-08-28/kg-*` + `kg-run/`
(not committed). Registry unchanged.

### Track 2 / follow-up notes

- **Zoho Recruit** (bkomstudios, playsimple) and **SuccessFactors** (jobs.ea.com) have no
adapter; a future provider-adapter decision (new adapter or acceptance of `needs_review`) is a
separate scope item.
- **XSEED/Marvelous USA** careers live on `xseedgames.bamboohr.com` — bamboo provider-staging
candidate for the next Track 2 pass.
- **Brightline bamboo candidate** was staged with a detail URL (`careers/65`) — correct to
`brightline.bamboohr.com/careers` (board root) before any promotion.
- **Reconciliation caveat:** the local `data/` registry snapshot is stale (8/21 vintage); all
WP3 mutations were applied where current state lives (the live container) per the 8/29 caveat.

## Out of Scope

- Apify / external crawlers; new Python/Node deps; broad `google_sheets` removal; parser rewrites beyond leaf fixes; any auto-promote/suppress/delete behavior.
