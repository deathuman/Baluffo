# Jobs Coverage Improvement Plan

> - **Status:** Active follow-up plan (draft for review)
> - **Use this when:** improving jobs feed coverage — recovering zero-kept static sources, closing provider coverage gaps, promoting staged providers, or reducing sheet dominance
> - **Canonical for:** coverage-improvement prioritization and evidence thresholds; not canonical for adapter internals or source-policy approval authority
> - **Then inspect:** `docs/source-policy-runbook.md`, `docs/adapter-plugin-inventory.md`, `docs/scraping-pipeline.md`, `docs/archive/provider-discovery-coverage-gap-plan.md`, `docs/archive/browser-fallback-pool-plan.md`
> - **Evidence basis:** 2026-07-17 full-run artifacts (`data/jobs-source-state.json.gz`, `data/jobs-fetch-report-summary.json`, `data/registry-conflicts-summary.json`, `_out/source-policy-soak-report.json`), audit snapshot `docs/snapshots/jobs-entry-validation-audit-2026-08-12.md`; refreshed 2026-08-29 against live-run artifacts (`_out/coverage-refresh-2026-08-28/` — see "Evidence refresh" section)
> - **Last updated:** 2026-08-29 (WP0 evidence refresh; WP1 validation passes, link-queue audit, D1 rejections applied to live container; WP2 sample classification + Outerdawn plugin + multi-hop static redirect fix — live-verified, ~50 jobs recovered; WP3 full triage of the 19 remaining sample rows — 3 leaf plugins (astrid/immersity/perfectgarbage) + 7 jobs live-verified locally, registry re-seeds/demotions applied to the live container; WP4 browser-fallback JS-shell classifier widened to catch jQuery-era shells — Konami Gaming recovered 45 jobs via the pool, full production pipeline measurement on the browser-fallback candidates recorded below; WP5 triage of the rendered-empty boards — upsurge + sandsoft plugins recover 6 + 10 roles, optillusion demoted as genuinely closed; WP6 full active-static-registry jQuery-era shell sweep — the widening is classification-only, over-flags ~57% server-rendered sources, and recovers 0 net-new jobs; WP7 Konami Gaming investigation — the "45-job browser-pool recovery" is a false positive (11 nav-link junk rows), the real jobs live on an external UKG Pro/UltiPro board that is currently empty, no promotion or adapter justified now; WP8 feed audit of the zero-kept jQuery-era shells — no Sandsoft-class dedicated jobs feeds exist, only 3 blog-feed job postings (arsanesia, petprojectgames, thegoodevil), not worth fragile feed-filter plugins; WP9 WP5-plugin pipeline measurement — upsurge 6/6 + sandsoft 10/10 recovered end-to-end (16 output jobs) after switching the list-only anchor from #-fragments (which normalize_url strips at the repair-dedup, canonicalize, and fingerprint stages) to ?static-role= query params; WP10 generic block-title list-only fallback in the static runner — heading-based, query-anchored rows recover list-only boards with no per-host plugin (fires only on otherwise-empty sources: zero parsed rows, detail links, or dead-listing evidence); WP11 list-only board sweep of the zero-kept set — a4vr (3 roles), amrita (4), animvs (5) converted to static_list_only_job_rows plugins, 10 jobs recovered end-to-end (animvs currently blocked by an expired TLS cert; recovers when renewed); WP11 de-dup — duplicate www.a4vr.com active row demoted to pending on the live container (kept the seeded a4vr.com row), feed now carries 3 a4vr jobs instead of 6; WP12 full-active-registry list-only sweep (all 2,110 static URLs, not just zero-kept) — playstack (21 roles), twirlbound (4), tatem (9) converted to static_list_only_job_rows plugins, 34 jobs recovered end-to-end; shared list-only helper now unescapes HTML entities so entity-variant duplicate titles ("PC &amp; Console" vs "PC and Console") collapse to one row; WP13 follow-up to WP8 — conservative feed-filter leaf plugins for arsanesia + petprojectgames (role keyword + hiring signal + negative-news gate), 2 jobs recovered end-to-end (1 each) from mixed site news feeds; WP14 ATS-backed shell triage (King/Blizzard/Microsoft/Netflix/Activision) — all five run the proprietary phApp/vscdn careers platform (no repo adapter); Activision is the only one with an existing-adapter path (delegates to a Workday board `xboxgaming.wd1.myworkdayjobs.com/CentralTech`, CXS returns 3 live "Central Technology" jobs → provider-staging candidate), Microsoft resolves to SAP SuccessFactors (no adapter), King/Blizzard/Netflix expose no standard board (future phApp-adapter decision); WP15 full-registry phApp scan — 13 active static rows host the phApp/vscdn platform directly (lower bound, 1,597 WP12 captures scanned); 5 already expose a Workday board recoverable today via the existing workday_sources adapter (Activision, Beenox, High Moon, Infinity Ward, Warner Bros. Games), 8 are widget-only (Blizzard, King, Raven, Sledgehammer, Treyarch, Scopely/Genjoy, Scopely/Omnidrone, TT Games) — one shared phApp/Workday adapter lever is the single largest zero-kept platform surface; WP16 sub-studio scan (Undead Labs, inXile, Compulsion, Smoking Gun, Next Games, Night School, Boss Fight) — none currently yields real recoverable jobs: 3 are on ATS boards the existing adapters can parse but are empty today (Undead Labs Greenhouse undeadlabsllc = general-interest only; inXile/Compulsion BambooHR /careers/list = 0/scam-warning only) → adapter-ready, wait-for-openings; 2 (Next Games, Night School) route to the Netflix custom platform (WP15 phApp decision); 2 (Smoking Gun, Boss Fight) expose no board; WP17 shared phApp adapter — reverse-engineered the open recovery path (per-locale sitemap of /job/{jobCode}/{slug} URLs + server-rendered <title> extractor for both the Blizzard "job in | jobs at" and King "in | at" title shapes), registered for the widget-only rows with the dedicated blizzard/activision plugins now falling back to it, and recovered 103 jobs end-to-end on a bounded live pass (Activision 50, Blizzard 37, King 14, Treyarch 2); WP18 Workday rows for the 5 phApp families — measured what workday_sources recovers today: the boards are live (xboxgaming/External=67 for Beenox/HighMoon/InfinityWard/Sledgehammer, xboxgaming/CentralTech=3 for Activision, warnerbros/global=356 company-wide) but the adapter recovers 0 today because its CXS path uses verified Python TLS that rejects these hosts (cert valid per system store; unverified works) → documented ready-to-stage rows + TLS-gating blocker, no registry mutation; WP19 duplicate-Scopely reconciliation — the two GameDevMap join-us rows (Genjoy apex + Omnidrone www, same phApp board, both jobsFound 19) are now one canonical registration in the tracked seeds: kept static:listing_url:https://scopely.com/en/join-us (apex, per the a4vr precedent), demoted the www twin to the pending seed via transition_registry_to_pending (active seed 2016→2015, pending 47→48); live container still needs the equivalent /registry/demote-active runtime step (active 2301→2300)

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
| **upsurgestudios.com/careers/** | 200, server-rendered | 6 `CareerSummary` roles (title + Job Description / Requirements inline), **no per-role links** | **Plugin recovery** | New leaf plugin `upsurge` (6 query-anchored rows; see WP9 for the fragment→query anchor fix) |
| **sandsoft.com/careers/** | 200, jQuery-era shell; **`/careers/feed/` RSS** | **10 postings** in the server-rendered feed (each `<item>` = title + detail link); live check-source `jobsFound: 2, weakSignal: true` | **Plugin recovery** | New leaf plugin `sandsoft` (fetches `/careers/feed/`, parses 10 postings) |
| **optillusion.games/job** | 200, server-rendered | Explicit "We are not actively hiring" / "Job Openings Currently Closed"; `/jobs` and `/careers` both 404; zero open-role headings domain-wide | **Genuinely empty → demote** | `/registry/demote-active` on live container — active 2303 → **2302**, pending 867 → **868** |

**Implementation** (both follow the established leaf-plugin shape, priority 90,
`parser_stale_hint` on empty parses):

- **`upsurge.py`** (`upsurgestudios.com`): splits on `class=CareerSummary` blocks, title from
  `CareerSummary__Title`. The page emits no per-role links, so each row is anchored to the
  careers page with a title-derived `?static-role=<slug>` query parameter to keep
  `sourceJobId`s distinct and on-domain (query params survive pipeline URL normalization,
  unlike `#`-fragments — see WP9). Extracts **6** live roles.
- **`sandsoft.py`** (`sandsoft.com`): custom `run` that derives the listing's server-rendered
  RSS feed URL (`/careers/feed/`) `_feed_url` and parses `<item>` title+link. The listing page
  itself is the jQuery-era shell the WP4 classifier now flags, so the feed is the reliable,
  non-browser source of truth. Extracts **10** postings (feed verified live).

Verified: `test_wp5_leaf_plugins.py` (12 cases: multi-row extraction, anchor-link uniqueness,
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

### WP8 feed audit (2026-08-29) — server-rendered feed recoveries among the zero-kept jQuery-era shells

Question: which of the 150 zero-kept jQuery-era shells expose a server-rendered RSS/feed URL
recoverable without the browser pool, like Sandsoft's `/careers/feed/` (10 postings)? **Answer:
none at Sandsoft scale. Only three genuine feed-hosted job postings exist across all 150, each a
single blog post mixed into a site feed — not worth fragile feed-filter plugins today.**

Method (all evidence in `_out/coverage-refresh-2026-08-28/feed-audit-*`):

1. **Advertised feeds** — scanned every capture for `<link rel=alternate type=application/rss+xml>`
   and feed-ish hrefs: 84/150 advertise a feed. Nearly all are WordPress site/news feeds (dev logs,
   press releases, trailers); the `/jobs/feed/` variants are WordPress **comments** feeds
   ("Comments on: Jobs"), i.e. the jobs pages are static pages, not post-type archives — empty.
2. **WordPress probe** — the 48 WP zero-kept sources with no advertised feed were probed at
   `/feed/`, `/jobs/feed/`, `/career/feed/`, `?feed=rss2`: all blog feeds, no job content.
3. **Ghost/Tumblr `/rss/`** — aggrocrrab/throwback/thegoodevil expose `/rss/`; thegoodevil's is a
   Tumblr feed with 1–2 job posts, the rest are studio news.

Verified feed-hosted job postings (live detail pages fetched):

| Source | Feed | Posting | Yield |
|---|---|---|---|
| arsanesia.com (career page is a WP page) | site `/feed/` | "Game Programmer: Full-Time & Intern" | **1** |
| petprojectgames.com (careers page JS) | site `/feed/` | "Pet Project Games Is Looking for a 3D Animator" | **1** |
| thegoodevil.com (Tumblr jobs page) | `/rss/` | "Pflichtpraktikum Game-Design od. Programmierung" + jobs roundup | **1–2** |

**Decision: no plugins now.** Unlike Sandsoft (dedicated jobs feed, 10 structured postings with
per-posting links), these are single blog posts inside news feeds; a leaf plugin would have to
role-keyword-filter mixed feeds, which will false-positive on studio news ("Business Development
Director" announcements, "Developer Blog" posts, awards posts) and risks publishing non-jobs.
Recovery value is 1 job per board. Recommend: keep these boards `needs_review`; revisit only if a
studio starts posting jobs regularly (the feed URLs above are documented for a future leaf plugin
using the sandsoft feed pattern + a conservative title filter). No registry or code changes on
this pass.

### WP13 implementation (2026-08-29) — conservative feed-filter plugins for arsanesia + petprojectgames

Follow-up to the WP8 feed audit (which documented these two feed URLs and explicitly
recommended a future leaf plugin *with a conservative title filter* but made no code
change): build that now. Both studios expose their only recoverable job posting as a
**single blog post mixed into the site WordPress news feed** — so unlike the dedicated
sandsoft jobs feed, every item must pass a conservative role-keyword + negative-news gate.

**Shared filter** (`src/jobs/adapters/plugins/static/_feed_postings.py`):
`looks_like_feed_role_posting(title)` keeps an item only when its title: (1) carries a
concrete **role keyword** (word-boundary role nouns like `programmer`, `animator`, `designer`, …,
so `Developer` never matches inside `Development Log`), (2) contains a **hiring-context signal**
(`looking for`, `hiring`, `full-time`, `intern`, `wanted`, `position available`, `apply`, …),
and (3) has **no negative news term** (dev log, trailer, release, teaser, launch, introducing,
blog, top list, facts, movies, awards, …). Requiring the hiring signal is the extra guard that
keeps team-profile/news posts like "Spotlight on our 3D Artist" or "Meet the Art Director" from
becoming false rows — false negatives are preferred over publishing a non-job.

Two leaf plugins (`arsanesia.py`, `petprojectgames.py`, registered priority 90) reuse the shared
`run_website_feed_postings` runner (sandsoft-style fetch/parse/fallback wiring, but on the site
`/feed/`). Verified against the **live feeds**:

| Source | Feed | Job posting recovered | Non-job items filtered |
|---|---|---|---|
| arsanesia.com | `/feed/` | "Game Programmer: Full-Time & Intern" | 9 dev logs (`Development Log: …`) |
| petprojectgames.com | `/feed/` | "Pet Project Games Is Looking for a 3D Animator" | trailer/teaser/introducing/blog/top-list posts |

**Bounded pipeline pass** (`--only-sources` on the 2 registry rows): **2 output jobs, 0 failed**
— arsanesia 1, petprojectgames 1, both before kept 0 (WP6 zero-kept). Rows link to the real post
detail pages (`…/career/game-programmer-full-time-intern/`, `…/looking-for-a-3d-animator`).

Tests: `tests/jobs/adapters/plugins/static/test_feed_posting_plugins.py` (31 cases — the exact
live feed titles, conservative guards, per-plugin extraction, empty/non-job feeds, host dispatch).
Static battery **419 passed**, precommit gate green. `docs/adapter-plugin-inventory.md` +
CHANGELOG updated.

### WP14 triage (2026-08-29) — ATS-backed zero-kept shells (King, Blizzard, Microsoft, Netflix, Activision)

Feed recovery (WP8/WP13) does not apply to these five — they are large ATS-backed boards, not
WordPress feeds. Triage question: does an **existing provider adapter** cover the underlying
board, or should the board be a **provider-staging candidate** (vs. kept `needs_review`)?

**Finding: all five run the same proprietary careers platform** — a `phApp` / `phw-unified`
client-side shell with a `widgetApiEndpoint` and `static.vscdn.net` asset CDN (Microsoft fka
"Phenix People"). It is not any repo-supported ATS; jobs are fetched client-side. Live probes
(site HTML + Workday CXS POST attempts) per shell:

| Shell | careersite platform | Underlying ATS | Existing adapter? | Disposition |
|---|---|---|---|---|
| **Activision** (`careers.activision.com/`) | `phApp` shell | **Workday** (`xboxgaming.wd1.myworkdayjobs.com/CentralTech` + `activision.wd1…`, linked from the page) | ✅ `workday_sources` (CXS) | **Provider-staging candidate** — CXS verified live: `wday/cxs/xboxgaming/CentralTech/jobs` → 200, currently **3 jobs** (all "Central Technology": Expert Engineer Security, Sr TPM-Central Tech (Temporary), Sr TPM-Tech Strategy-Central Tech). Small but real, already covered by the existing adapter. |
| **Microsoft** (`jobs.careers.microsoft.com`) | `phApp`/vscdn SPA (`/api/search` returns the SPA shell, not JSON) | **SAP SuccessFactors** (search/facet payload: `position.ats_data` gated on `position.system_id:successfactors`) | ❌ no adapter | **Not a staging candidate** — needs a new SuccessFactors adapter (already flagged in Track-2 notes re: jobs.ea.com). Keep `needs_review`. |
| **King** (`careers.king.com/us/en`) | `phApp` shell, `widgetApiEndpoint=/widgets` | not exposed in HTML; guessed `king.wd*.myworkdayjobs.com` CXS → 422 | ❌ no adapter | **Not a staging candidate** — proprietary client API; future adapter decision. Keep `needs_review`. |
| **Blizzard** (`careers.blizzard.com/global/en`) | `phApp` shell, `widgetApiEndpoint=/widgets` | not exposed in HTML; guessed `blizzard.wd*.myworkdayjobs.com` CXS → 422 | ❌ no adapter | **Not a staging candidate** — proprietary client API; future adapter decision. Keep `needs_review`. |
| **Netflix** (`explore.jobs.netflix.net/careers`) | `phApp`/vscdn shell (jobs.netflix.com), `wday` CXS → 401 | custom, no standard board | ❌ no adapter | **Not a staging candidate** — proprietary client API; future adapter decision. Keep `needs_review`. |

**Recommendation:** the only actionable item with an existing adapter is **Activision** — stage a
`workday` provider row for `https://xboxgaming.wd1.myworkdayjobs.com/CentralTech` (the repo's
`workday_sources` CXS adapter derives the correct tenant/site for that URL and returns real jobs;
current yield ~3 — Microsoft Gaming's "Central Technology" external openings that
`careers.activision.com` links to). This is a **modest** surface, so the staging is low-risk
recovery rather than a fleet-level win. Microsoft needs a SuccessFactors adapter decision;
King/Blizzard/Netflix share a proprietary `phApp`/vscdn platform that would need one new
adapter to cover all three (plus Microsoft) — the biggest single platform lever, but a
reverse-engineering effort, out of scope for "existing provider adapters or provider staging".
No registry mutation on this pass (provider staging is operator-approved); the Activision
staging candidate is documented for a future `workday` row if approved.

### WP15 scan (2026-08-29) — every studio running the phApp/vscdn careers platform

Follow-up to WP14 (which established that the named shells run a proprietary
`phApp`/`vscdn.net` careers platform): scan the **whole active static registry** for
**every** source whose careers page hosts that platform directly, and tally what one
shared `phApp` adapter would recover.

**Method:** content-scan of the 1,597 active static careers pages with WP12 sweep
captures (deterministic filename-from-URL reused; ~303 of 1,901 static rows had no
capture and are unmeasured, so the count below is a lower bound — the WP12 captures
are today's). phApp shell markers: `var phApp = phApp`, `phw-unified`,
`widgetApiEndpoint`, `ph-page-element`, `phGlobalDefOptions` (with `vscdn.net`
corroboration).

**Result — 13 active-static rows host the phApp platform directly:**

**5 also expose a Workday board in the page HTML** → ALREADY recoverable today via the
existing `workday_sources` CXS adapter (best near-term value, no new adapter):

- **Activision** (`careers.activision.com`) → `xboxgaming.wd1.myworkdayjobs.com/CentralTech` (+`activision.wd1…`)
- **Beenox** (`careers.beenox.com`) → `xboxgaming.wd1…/…Beenox…` posting links
- **High Moon** (`careers.highmoonstudios.com`) → `xboxgaming.wd1…` posting links
- **Infinity Ward** (`careers.infinityward.com`) → `xboxgaming.wd1…` posting links
- **Warner Bros. Games** (`careers.wbd.com/global/en/wb-games-jobs`) → **`warnerbros.wd5.myworkdayjobs.com`** — a large WB board, with real posting links in the page (Rocksteady, WB Games Montreal, WB Games Boston, etc.)

**8 are widget-only** (jobs served from the phApp widget API, no standard board in
HTML) → covered only by a new `phApp` adapter:

- **Blizzard** (`careers.blizzard.com`), **King** (`careers.king.com`), **Raven Software**, **Sledgehammer Games**, **Treyarch** (the 5 named sisters, all widget-only except Activision)
- **Scopely / Genjoy** (`scopely.com/en/join-us`) + **Scopely / Omnidrone** (`www.scopely.com/en/join-us`) — Scopely runs the same platform (two registry rows, likely duplicate joinings to reconcile)
- **TT Games** (`careers.wbd.com/tt-games-jobs`) — widget-only in its own capture (same careers.wbd.com host/platform as the Warner board above, but no Workday posting links in that page)

**Tally / recovery potential of one shared `phApp` adapter:** covers **13 rows across
~11 studios** that serve jobs only through the phApp widget API (Blizzard, King, Raven,
Sledgehammer, Treyarch, Scopely/Genjoy+Omnidrone, TT Games, Warner, plus the 5
Workday-backed boards would also stop depending on the browser pool). That is the
single largest zero-kept platform lever in the registry — but job *counts* per source
are not measurable until the widget-API payload format is reverse-engineered (WP14
noted the API is client-side; `widgetApiEndpoint` is captured per row in the WP15
artifact for future reverse-engineering).

**Distinct from this set:** the standalone sub-studio sites (inXile, Undead Labs,
Compulsion, Smoking Gun, Next Games, Night School, Boss Fight, Demonware, Activision
Austin, Elsewhere…) do **not** host the phApp shell themselves — they are separate
registrations that typically point at the parent board — so one phApp adapter does not
cover those unless they also link a Workday/wb board (follow-up). No code/registry
change on this pass; evidence in `_out/coverage-refresh-2026-08-28/wp15-phapp-platform.json`.

### WP16 scan (2026-08-29) — standalone sub-studio pages: which link a recoverable parent board

WP15 covered the studios that **host** the `phApp` shell. The question posed here: the standalone
sub-studio sites (inXile, Undead Labs, Compulsion, Smoking Gun, Next Games, Night School, Boss
Fight) rarely host `phApp` themselves — do they link a **recoverable** parent/ATS board?

**Method:** WP12 captures + live probes; where a real ATS board was found, the repo's own provider
parser was run against it (definitive — the adapter's endpoints, not a guess).

| Sub-studio (parent) | careers page | References | Recoverable today? | Disposition |
|---|---|---|---|---|
| **Undead Labs** (Microsoft) | undeadlabs.com/careers | Greenhouse embed (`boards.greenhouse.io/embed/...?for=undeadlabsllc`) | ❌ Greenhouse board `undeadlabsllc` exists but holds **only a "General Interest Application" catch-all** (0 real jobs) | Not a staging candidate now; document slug `undeadlabsllc` for staging when roles open |
| **inXile** (Microsoft) | inxile-entertainment.com/careers | BambooHR widget (`data-domain=inxile.bamboohr.com`) | ❌ BambooHR `/careers/list` (repo adapter) returns **only a "Be Careful of Hiring Scams" warning** (0 real jobs) | Not a staging candidate now; document `inxile.bamboohr.com` |
| **Compulsion** (Microsoft) | compulsiongames.com | BambooHR (`compulsiongames.bamboohr.com/jobs/`) | ❌ BambooHR `/careers/list` (repo adapter) returns **0 postings** | Not a staging candidate now; document `compulsiongames.bamboohr.com` |
| **Next Games** (Netflix) | nextgames.com/careers | links `explore.jobs.netflix.net/careers?Teams=Next Games` (Netflix custom platform) | ❌ no adapter | Covered by the WP15 `phApp`/custom-platform adapter decision |
| **Night School** (Netflix) | nightschoolstudio.com/work-with-us/ | links `jobs.netflix.com/teams/night-school-studio` (Netflix custom platform) | ❌ no adapter | Covered by the WP15 platform decision |
| **Smoking Gun** | smokingguninc.com/careers/ | **no ATS board at all** (97K page, no ATS markers; no WP12 capture) | ❌ | Dead/static-only; keep `needs_review` |
| **Boss Fight** (Netflix) | bossfightentertainment.com/careers | **no board** (tiny 30K page, only a careers banner image) | ❌ | Dead/static-only; keep `needs_review` |

**Takeaway — none of the standalone sub-studios currently yields real recoverable jobs.** The honest
disposition: 3 (Undead Labs, inXile, Compulsion) are on **ATS boards the repo's existing provider
adapters can already parse** (Greenhouse via `undeadlabsllc`; BambooHR via `inxile.bamboohr.com`
/ `compulsiongames.bamboohr.com`) but those boards are **empty today** — so they are
**adapter-ready, wait-for-openings** candidates, not active recoveries. 2 (Next Games, Night School)
route to the **Netflix custom platform** and are covered only by the WP15 `phApp` adapter decision.
2 (Smoking Gun, Boss Fight) expose no board at all. No provider staging is worthwhile this pass;
the three ATS-capable boards are documented (above) for staging the moment an opening appears.

### WP17 (2026-08-29) — shared phApp adapter: reverse-engineered sitemap recovery path

WP15 identified **13 active static rows** on the Phenom People "CareerConnect"/`phApp` platform
(the widget-only lever). WP15/14 correctly noted the widget-API JSON is tenant+CSRF gated — but this
pass found the **fully open recovery path**: every phApp jobsite publishes a **per-locale sitemap**
(`<scheme>://<host>/<locale>/sitemap.xml`) listing every job as a `/job/{jobCode}/{slug-title}` URL,
and each detail page is **server-rendered** with a stable `<title>`:

- **Blizzard family:** `{Title} | {InnerLoc} job in {Loc}, {Country} | {Disc} jobs at {Co}`
- **King family:** `{Title} in {Loc}, {Country} | {Disc} at {Co}`

**The shared adapter** (`src/jobs/adapters/plugins/static/phapp.py`):
- `sitemap_candidates` — derives the locale prefix from the page URL and probes the two canonical
  sitemap roots (no extra HTTP for the common case; the sitemap document doubles as the existence
  check).
- `collect_job_urls` — extracts `/job/{code}/{slug}` URLs including CDATA-wrapped `<loc>` entries,
  dedupes, and resolves relative joins; a sitemap *index* is tolerated (no crash, no false jobs).
- `extract_phapp_job_meta` — prefers the `<title>` canonical shape (both family variants via a
  `job in`/`in` anchor that also strips the word "job"), falls back to the URL slug, and only
  returns `None` when nothing usable is present.
- `run` — fetches the first candidate containing a sitemap, caps at 80 jobs/source, maps each
  detail page to a stamped `RawJob` (`city`/`country` via `parse_generic_location_fields`, company
  from the `<title>`/source row, `sourceJobId=phapp:<jobCode>`), and returns `[]` on empty feeds.
- `can_handle` — covers the `PHA_HOSTS` set (Activision, Blizzard, King, Raven, Sledgehammer,
  Treyarch, WBD, Beenox, High Moon, Infinity Ward, Scopely).

**Registration model:** the registry selects *one* plugin per source, so the shared adapter is
registered at priority 90 for the **widget-only rows** (King, Treyarch, Raven, Sledgehammer, WBD,
Scopely, …). Blizzard/Activision keep their dedicated per-host plugins, which now **fall back to
the shared phApp path** when their server-rendered-card parse yields nothing — exactly what happens
in production (those jobsites are JS shells). The dedicated plugins' unit behavior is preserved and
still covered by their existing integration tests.

**Bounded end-to-end recovery** (`run_static_studio_pages_source`, real fetches, live sites) — the
four rows with a live sitemap yield across the full pipeline (quality filters + dedup applied):

| Source | kept before | kept after | sample titles |
|---|---|---|---|
| **Activision** (careers.activision.com) | 0 | **50** | Social Media Manager; 2026 Fall Co-Ops – Software Dev (Demonware Shanghai) |
| **Blizzard Entertainment** (careers.blizzard.com/global/en) | 0 | **37** | Senior Combat Designer, Systems – WoW; Design Director, Systems – Diablo |
| **King** (careers.king.com) | 0 | **14** | Senior Principal AI/ML Engineer; QA Lead (Minecraft Blast) |
| **Treyarch** (careers.treyarch.com) | 0 | **2** | Expert Engine Engineer – Treyarch (LA / Vancouver) |

**Total: 103 output jobs, 0 failed sources.** Activision's surface is much larger than WP14's
3-Job Workday "Central Technology" estimate — the careers site's *own* sitemap serves 50 live jobs
(so the old WP14 provider-staging-of-the-Workday-board recommendation is superseded for the careers
site itself; the Workday board covers a different subset). The other widget-only rows (Raven,
Sledgehammer, WBD, Scopely, Beenox, High Moon, Infinity Ward) share the adapter but had no jobs in
their live sitemaps at measurement time — they return `[]` cleanly rather than erroring, and will
recover automatically whenever their sitemap lists jobs.

Tests: `tests/jobs/adapters/plugins/static/test_wp17_phapp_adapter.py` (12 cases — sitemap URL
collection, CDATA/dedupe/index handling, both `<title>` shapes, slug fallback, empty-signal None,host gating, end-to-end row mapping, and the registry selection for widget-only hosts). Batteries:
`tests/jobs_static` + `tests/jobs/adapters` **857 passed**; precommit gate green; tree clean.

### WP18 (2026-08-29) — Workday provider rows for the phApp families: measurement + TLS blocker

WP15 named 5 phApp families as "already recoverable via the existing `workday_sources` adapter"
because their careers pages link to a Workday board. This pass staged-and-measured that claim by
running the adapter's own collection logic against each board and sizing the CXS API directly.

**Extracted boards (from the live careers pages + WP12 captures):**

| Family | Board | CXS site (tenant) | CXS total jobs | Adapter yield today |
|---|---|---|---|---|
| Activision (central tech) | `xboxgaming.wd1.myworkdayjobs.com/CentralTech` | xboxgaming / CentralTech | **3** | ❌ error |
| Activision (alt) | `activision.wd1.myworkdayjobs.com/CentralTech` | — | 422 (wrong site) | — |
| **Beenox** | `xboxgaming.wd1.myworkdayjobs.com/External` | xboxgaming / External | **67** | ❌ error |
| **High Moon** | `xboxgaming.wd1…/External` (same board) | — | (in the 67) | ❌ error |
| **Infinity Ward** | `xboxgaming.wd1…/External` (same board) | — | (in the 67) | ❌ error |
| **Warner Bros. Games** | `warnerbros.wd5.myworkdayjobs.com/global` | warnerbros / global | **356** | ❌ error |

(Sledgehammer also posts on the `xboxgaming/External` board, so that single board covers four of the
five families; `warnerbros/global` is the **company-wide** WBD board — its 356 jobs include CNN and
non-game WB roles, so it is out of scope for a games feed without a games-site filter.)

**The blocker is precise and reproducible:** the `workday_sources` CXS sub-path calls stdlib
`urlopen` with a **verified** TLS context (`provider_structured_listing._fetch_workday_cxs_page`),
and Python's chain validation rejects these MyWorkDay hosts (`certificate verify failed: … expired`)
— even though the certificate is **not expired** (`openssl s_client`: notAfter Oct 5 2026, today is
Aug 29) and verified `curl` gets HTTP 200 + jobs from the same CXS endpoints. Over the unverified
path (WP14's workaround) the CXS returns the real totals above. Because the repo's production fetch
is verified Python TLS everywhere (no unverified path in production code), **this would hit the
Linux container identically.** So today `workday_sources` recovers **0** from all five boards — they
are *erroring*, not zero-kept.

**Disposition — no registry mutation this pass.** Staging these rows to active would add five
hard-erroring sources and the feed gains nothing; staging to pending by hand risks corrupting the
delta-encoded registry (pending rows are produced by the discovery/migration reconciliation, and
capture-analyst ops consume them there). Ready-to-stage row set (in-scope boards only):

- `xboxgaming.wd1.myworkdayjobs.com/CentralTech` → Activision (3)
- `xboxgaming.wd1.myworkdayjobs.com/External` → Beenox, High Moon, Infinity Ward (+Sledgehammer) (67)

**Gating recommendation:** before any staging, align the `workday_sources` CXS transport with the
pipeline's TLS behavior (or add a per-host exception) so these boards stop erroring — that is the
single change that turns this into the largest remaining provider lever (70 in-scope jobs across the
named studios). The `warnerbros/global` board (356) needs a games-scope filter before it is eligible.
Ready-to-stage row definitions + live totals are in `_out/coverage-refresh-2026-08-28/wp18-*.json`.

### WP19 (2026-08-29) — reconcile duplicate Scopely phApp rows (Genjoy / Omnidrone)

WP15 flagged "Scopely/Genjoy + Scopely/Omnidrone — two rows, likely duplicate joinings to
reconcile." Confirmed: two active static rows point at the **same** phApp join-us board
(`scopely.com/en/join-us` vs its `www` twin) — both GameDevMap-derived, both `jobsFound: 19`
with identical evidence (`evidenceScore 40`, same reasons), differing only in studio label
(Genjoy vs Omnidrone) and URL form. With the WP17 phApp adapter registered for both hosts,
these two rows would emit the same Scopely jobs twice once the board's sitemap has jobs
(Sledgehammer-class duplication, exactly the a4vr/WP11 failure mode).

**Reconciliation (this commit):** the tracked canonical seeds now carry **one** join-us row.
- **Kept (active seed):** `static:listing_url:https://scopely.com/en/join-us` ("Genjoy (Scopely)") —
  the apex form, matching the WP11 a4vr precedent (keep the non-www identity).
- **Demoted (active → pending seed):** `static:listing_url:https://www.scopely.com/en/join-us`
  ("Omnidrone (Scopely)") — moved with the repo's own `transition_registry_to_pending`
  (`pendingReason`: duplicate of the apex row, same board via www; `candidateState: validated`,
  `enabledByDefault: false`, `lastDemotedAt` stamped), so the shape matches a
  `/registry/demote-active` transition exactly.

Active seed 2016 → 2015 rows; pending seed 47 → 48. The `phapp` plugin's `can_handle` still gates
`scopely.com` (kept row), so recovery is unchanged — only the duplicate row is gone. **The live
container still needs the equivalent runtime demotion** (`POST /registry/demote-active` with
`ids: ["static:listing_url:https://www.scopely.com/en/join-us"]`, active 2301 → 2300, pending
→ +1) because the runtime registry grew beyond the seed; reversible via `/registry/approve`.

Verification: `tests/test_source_registry_storage_and_seed.py`,
`tests/test_source_registry_seed_runtime.py`, `tests/test_build_ship_bundle_registry_seeds.py`,
`tests/test_source_policy_soak_report.py`, `tests/bridge` **710 passed**; precommit gate green.


### WP9 measurement (2026-08-29) — WP5 plugins end-to-end pipeline recovery (before/after)

Question: what do the two WP5 plugins actually keep on a bounded production pipeline pass, and
does the measured recovery match the plugin unit tests? Answer: **yes, after one fix** — the
list-only anchor scheme had to move from `#`-fragments to `?static-role=` query parameters.

**Before (WP4/WP5 baseline):** both sources rendered empty through the browser pool — `kept 0`
each (`dead_listing_page` / `needs_review`); the WP5 dispositions artifact recorded
`kept_before_wp5: 0` for both.

**After — bounded pipeline pass** (`--only-sources` on the two registry rows,
`--force-refresh-all --no-seed-existing-output`, browser fallback on):

| Source | kept before | plugin unit parse | pipeline run #1 | pipeline run #2 (post-fix) |
|---|---|---|---|---|
| upsurge (`www.upsurgestudios.com/careers/`) | 0 | 6 | **1** | **6** |
| sandsoft (`sandsoft.com/careers/`) | 0 | 10 | **10** | **10** |

Run #1 exposed a real bug: upsurge kept **1 of 6**. Root cause: the WP5 list-only helper anchored
rows with `#<slug>` **fragments**, but pipeline URL normalization strips fragments at three
stages — the plugin repair-row dedup (`_append_repaired_plugin_rows` in
`static_listing_plugin.py`, keyed on `normalize_url(jobLink)`), canonicalization
(`_resolve_job_link` in `canonicalize_locations.py`), and the finalize dedup fingerprint
(`canonical_url_fingerprint_seed`). Every role on the page collapsed into the first surviving row.

**Fix:** `static_fragment_link` → `static_listing_anchor_link` (shared helper in `_runner.py`);
rows are now anchored with a `?static-role=<slug>` **query parameter**, which survives
`normalize_url` (query params are preserved and sorted; fragments are dropped). No changes to the
shared canonicalize/dedup code — the anchor simply no longer collides with the repo's URL
semantics. `upsurge.py` and the helper/WP5 tests were updated (fragment expectations → query).

**Verified:** run #2 keeps **upsurge 6/6** (all six `CareerSummary` roles distinct end-to-end,
links `…/careers/?static-role=<slug>`) and **sandsoft 10/10** unchanged (real detail links) —
**16 output jobs** vs 11 before the fix. The list-only pattern now works for future boards
without per-role links.

### WP10 implementation (2026-08-29) — generic block-title list-only fallback (no per-host plugin)

Follow-up to WP9: since the query-anchor pattern now survives the pipeline end-to-end, the same
recovery was generalized into the **generic static listing runner**, so list-only boards recover
without a per-host plugin at all.

**Behavior:** when a listing page yields **zero** rows from every existing generic path (JSON-LD
parse, rendered-card scan, detail-link collection) and has **no dead-listing evidence**, the
runner scans the HTML for block-structured headings (`<h2>`–`<h4>`, script/style stripped). A
heading becomes a row only when it is a distinct, job-title-looking title
(`looks_like_job_title_candidate`, not `looks_like_static_parser_noise_title`) and is **not** a
section-header phrase ("Open Roles", "We're Hiring", "Join Our Team", …). At least **2** distinct
job-like headings are required (a single heading is treated as a page/hero header). Each title
becomes a query-anchored row via `static_listing_anchor_link` (`?static-role=<slug>`) with a
distinct `sourceJobId`, and the source classifies `ok_with_jobs` with
`extractorHint=block_title_fallback`.

**Safeguards:** the fallback can only *add* recovery to otherwise-empty sources — it never runs
when any parsed row, provisional row, detail link, or dead-listing rejection exists, so it
cannot change behavior for sources the pipeline already handles.

Implemented in `static_listing_rows.py` (`_job_like_heading_titles` +
`_append_block_title_fallback_rows`, hooked at the end of `_extract_listing_candidates`), reusing
the WP9 shared helpers from `_runner.py`. Tests: `tests/jobs_static/test_static_listing_block_title_fallback.py`
(8 cases — heading filtering incl. section-header exclusions and script/style stripping,
end-to-end recovery via `run_static_studio_pages_source`, single-heading no-fire, detail-link
wins over fallback, query anchors survive `normalize_url`). Static battery **357 passed**,
precommit gate green.

### WP11 implementation (2026-08-29) — list-only boards converted to `static_list_only_job_rows` plugins

Follow-up to WP10: sweep the 150 zero-kept WP6 sources for **list-only boards** (roles with no
per-role detail URLs) whose titles sit in non-heading blocks (which the generic fallback misses)
or need precision the fallback can't give, and convert them to leaf plugins using the shared
`static_list_only_job_rows` helper.

**Sweep:** scanned the WP6 sweep captures of all 150 zero-kept sources for repeated role-token
titles with ≤1 detail link → 55 candidates, then live-probed the structured ones (reused the
WP9/WP10 title filters to separate real role lists from prose noise). Three boards were genuine
list-only recoveries:

| Source | Structure | Roles | Notes |
|---|---|---|---|
| **a4vr.com/jobs** (+www row) | Squarespace `<h2><strong>POSITION: …</strong></h2>` blocks | **3** (TECHNICAL ARTIST, SENIOR 3D ARTIST, JUNIOR QA ENGINEER) | Trailing `INITIATIVBEWERBUNG - TALENTE FÜR VR/AR` is a speculative "send CV" block — excluded (never a split point + defensive filter). `POSITION: ` label stripped from titles. |
| **amrita.studio/career** | SP Page Builder accordion `<span class="sppb-panel-title" aria-label="…">` | **4** (Middle/Senior Unity Developer, Golang Developer, Game Designer, QA Engineer) | Titles in non-heading spans → WP10 fallback misses; `aria-label` is the clean title source. |
| **animvs.com/work-with-us/** | Elementor tabs `<div class="elementor-tab-title elementor-tab-desktop-title">` | **5** (ARTISTA 3D, GAME DEVELOPER, LEVEL DESIGNER, BACK-END DEV, BLOCKCHAIN DEVELOPER) | Non-heading → fallback misses. Desktop tabs only (mobile duplicates); page's own "work with us" tab excluded. |

**Bounded pipeline pass** (`--only-sources` on the 4 registry rows, browser fallback on):
**10 output jobs** — a4vr 3 + www.a4vr.com 3 + amrita 4; animvs kept 0 today because
`https://animvs.com` currently serves an **expired TLS certificate** (pipeline transport
`SSL: CERTIFICATE_VERIFY_FAILED`, classified `timeout` + `browserFallbackRecommended`). The
animvs plugin is registered and capture-verified (5 roles); it recovers as soon as the cert is
renewed (or via browser fallback). Before (WP6): all four rows kept 0.Note: `a4vr.com` and `www.a4vr.com` were two active registry rows for the same studio, so the
unified feed carried the same 3 roles twice (6 rows). **Resolved — WP11 de-dup (2026-08-29):**
kept the seeded row `static:listing_url:https://a4vr.com/jobs` (the registry's established
identity, present in `data/defaults/source-registry-active.seed.json`) and demoted the duplicate
intake `static:listing_url:https://www.a4vr.com/jobs` to pending via `/registry/demote-active` on
the live container (active 2302 → **2301**, pending 868 → **869**, `demoted: 1`). Evidence: both
rows served the identical 3 roles in the WP11 bounded pass; the site's own `<link rel="canonical">`
names `www.a4vr.com/jobs`, but the non-www row is the seeded registry identity and both hosts serve
the page (no redirect between them), so keeping the seeded row preserves registry history and the
feed now carries exactly **3 a4vr jobs**. Re-verified: a bounded pass on the kept row keeps 3 rows
with `a4vr.com` links and no www duplicates. The demotion is reversible via `/registry/approve` if
the www host is ever needed separately.

Plugins: `a4vr.py`, `amrita.py`, `animvs.py` (all priority 90, `parser_stale_hint` on empty
parses, registered in `register.py`). Tests: `tests/jobs/adapters/plugins/static/test_wp11_leaf_plugins.py`
(15 cases — host dispatch, POSITION extraction + speculative exclusion, accordion aria-label
titles, desktop-only tabs + nav-tab exclusion, empty pages). Static battery **372 passed**,
precommit gate green. `docs/adapter-plugin-inventory.md` table + CHANGELOG updated.

### WP12 implementation (2026-08-29) — full-active-registry list-only sweep (playstack / twirlbound / tatem)

Follow-up to WP11: extend the list-only sweep from the 150 zero-kept sources to **all 2,110 active
static URLs** to find more non-heading list-only boards worth plugins (the WP11-style patterns the
WP10 generic heading fallback can't see).

**Sweep:** reused the WP6 sweep machinery (unverified SSL, resumable, host-limited) over the full
active static registry → **2,110 URLs, 1,961 captured, 149 failed** (7.5 min). The scanner required
repeated structure (≥2 role-token titles in the same element type) to keep prose noise out → 304
candidates; live-probed the structurally promising ones. Rejected: team-profile pages
(curvature, punyastronaut), employee-testimonial lists (softgames, hypersonic), 403/404 walls
(massiveblack, pixelmafia), transient-403 retry successes, and boards with real per-role detail
links (heliogames). Three genuine list-only recoveries:

| Source | Structure | Roles | Notes |
|---|---|---|---|
| **playstack.com/careers/** | Astro card grid `<span id="dynamic-title">Role</span>` | **21** | Page hero shares the `dynamic-title` markup ("Join Our Team") — post-filtered with the shared job-title / section-header checks. HTML-entity variants ("PC &amp; Console…" vs "PC and Console…") collapse to one row via the anchor slug after the entity-unescape fix (below). |
| **twirlbound.com/jobs/** | WordPress ub-content-toggle accordions `<p class="wp-block-ub-content-toggle-accordion-title…"><strong>Role</strong></p>` | **4** | Class carries a random uuid suffix per accordion — matched on the stable `wp-block-ub-content-toggle-accordion-title` prefix. Details inline, no per-role link. |
| **tatem.games/tatemjobs** | Tilda cards `<div class="t-card__title …" field="li_title__…">Role</div>` | **9** | Details inline; the only anchor is the page's own `/tatemjobs` link. |

**Entity-unescape fix in the shared helper:** `static_list_only_job_rows` (and the WP10 heading
fallback in `static_listing_rows.py`) now run titles through `html.unescape` before slugging, so
`&amp;` / `&#8211;` render as real characters in titles and entity-variant duplicates collapse via
the anchor slug (playstack's "PC &amp; Console Games Marketing Manager" and "PC and Console Games
Marketing Manager" are the same role — previously two rows, now one). Benefit applies to every
list-only plugin (upsurge, sandsoft, a4vr, amrita, animvs, playstack, twirlbound, tatem) and the
generic fallback.

**Bounded pipeline pass** (`--only-sources` on the 3 registry rows, browser fallback on):
**34 output jobs, 0 failed** — playstack 21 + twirlbound 4 + tatem 9, all rows query-anchored
(`?static-role=<slug>`) and distinct. Before (WP6): all three rows kept 0.

Plugins: `playstack.py`, `twirlbound.py`, `tatem.py` (all priority 90, `parser_stale_hint` on
empty parses, registered in `register.py`). Tests:
`tests/jobs/adapters/plugins/static/test_wp12_leaf_plugins.py` (16 cases — host dispatch, card /
accordion / Tilda extraction, hero-heading filter, entity-variant collapse, empty pages). Static
battery **388 passed**, precommit gate green. `docs/adapter-plugin-inventory.md` table +
CHANGELOG updated.

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
