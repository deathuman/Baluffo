# Jobs Coverage Improvement Plan

> - **Status:** Active follow-up plan (draft for review)
> - **Use this when:** improving jobs feed coverage — recovering zero-kept static sources, closing provider coverage gaps, promoting staged providers, or reducing sheet dominance
> - **Canonical for:** coverage-improvement prioritization and evidence thresholds; not canonical for adapter internals or source-policy approval authority
> - **Then inspect:** `docs/source-policy-runbook.md`, `docs/adapter-plugin-inventory.md`, `docs/scraping-pipeline.md`, `docs/archive/provider-discovery-coverage-gap-plan.md`, `docs/archive/browser-fallback-pool-plan.md`
> - **Evidence basis:** 2026-07-17 full-run artifacts (`data/jobs-source-state.json.gz`, `data/jobs-fetch-report-summary.json`, `data/registry-conflicts-summary.json`, `_out/source-policy-soak-report.json`), audit snapshot `docs/snapshots/jobs-entry-validation-audit-2026-08-12.md`; refreshed 2026-08-29 against live-run artifacts (`_out/coverage-refresh-2026-08-28/` — see "Evidence refresh" section)
> - **Last updated:** 2026-08-29 (WP0 evidence refresh; WP1 validation passes, link-queue audit, D1 rejections applied to live container; WP2 sample classification + Outerdawn plugin + multi-hop static redirect fix — live-verified, ~50 jobs recovered)

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

## Out of Scope

- Apify / external crawlers; new Python/Node deps; broad `google_sheets` removal; parser rewrites beyond leaf fixes; any auto-promote/suppress/delete behavior.
