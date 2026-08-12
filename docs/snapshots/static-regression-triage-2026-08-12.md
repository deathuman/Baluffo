# Static Regression Triage — 2026-08-12 (Track 1, site_changed)

> - **Status:** Evidence report; **19 dead/absorbed sources demoted to pending on 2026-08-12 via the admin bridge** (operator-approved); ATS-migrated staging still pending
> - **Basis:** 2026-07-17 `data/jobs-source-state.json.gz`; live HTTP probes on 2026-08-12; bounded pipeline runs (`_out`-style temp dirs) on the same day
> - **Canonical for:** classification of the 53 `site_changed` static regressions; not canonical for registry mutations
> - **Then inspect:** `docs/plans/jobs-coverage-improvement-plan.md`, `docs/source-policy-runbook.md`, `docs/snapshots/jobs-entry-validation-audit-2026-08-12.md`

## Definition

Sources matching: `static_source::static:listing_url:*` with `lastFailureBucket == site_changed`, `lastNonEmptyAt` set (had jobs before), and `lastJobsFound == 0` (current run empty). **53 sources.**

## Method

- Extracted all 53 listing URLs from source-state.
- Live-probed 54 URLs (all unique) on 2026-08-12: HTTP status, redirect target, ATS signature sniffing (teamtailor/workday/greenhouse/personio/lever/ashby/breezy/jobvite/icims/cornerstone/oracle/dayforce…), `<title>` capture.
- Re-ran the real pipeline bounded (`--only-sources` of 14 highest-value targets) to capture actual parser errors.
- Classified each source into: ATS-migrated / dead-404 / down-521 / live-layout-change / redirect-non-careers / redirect-careers-alias.

## Classification (53 sources)

| Class | Count | Meaning |
|---|---|---|
| **ATS-migrated** | **8** | Careers moved to an ATS the provider adapter already supports (or a near equivalent); static parser cannot extract |
| **Dead 404** | **19** | Listing URL returns 404/410 — page removed |
| **Down 521** | **1** | Origin down at probe time (Cloudflare 521) |
| **Live layout change** | **6** | Page live, real jobs likely present, static parser misses (link selection / redirect-loop / JS) |
| **Redirect to non-careers** | **6** | Listing redirects to a non-careers page (rebrand/absorbed) |
| **Redirect careers alias** | **15** | Trailing-slash / www↔bare-domain normalization aliases (parser should already handle or the redirect is transient) |
| (subtotal) | 53 | 54 probes − 1 double-listed URL |

## ATS-migrated (8) — reclassify to provider adapter

| Source | Redirect/probe evidence | ATS |
|---|---|---|
| `coffeestain.com/careers/` | → `jobs.coffeestain.com/#jobs` | teamtailor |
| `bulkhead.com/careers` | teamtailor signature on page | teamtailor |
| `www.liongamelion.com/careers.html` | teamtailor signature | teamtailor |
| `www.yodo1.com/careers` | "HTML contains teamtailor signature" (pipeline error) | teamtailor |
| `careers.ccpgames.com/` | → `careers.fenriscreations.com/` | workday |
| `sandsoft.com/careers-at-sandsoft/` | workday signature; detail 404 on `career.sandsoft.com/jobs/…` | workday |
| `careers.nintendo.com/jobs` | → greenhouse-shaped careers site | greenhouse |
| `welevel.jobs.personio.com/` | personio signature (redirect to personio.com) | personio |

Registry note: `welevel.jobs.personio.com/` is **active static** (should be personio provider); `coffeestain.com/careers/` and `sandsoft.com/careers-at-sandsoft/` are already **pending** with reasons `site_changed_static_source` / `stale_or_dead_static_source`.

## Dead 404 (19) — dead-source demotion candidates

andarion-games, reply.com/careers, hasbro, darkartssoftware, dedalord, frogdice, htxlabs, penrosestudios, poncle1.homerun.co, athenaworlds, aurorapunks, bamtang, blastworksinc, expertia.ai/flyingcaps, gsc-game, masongames, probablymonsters, shortgun, tigerrollstudios.

Notes: `jobs.andarion-games.com` → `timestableshunt.com/jobs` (domain pivoted); `reply.com/careers` → 404 (Reply uses `careers.reply.com` — the *active* source already exists); `htxlabs` → `www.htxlabs.com/careers` 404 (page gone); `poncle1.homerun.co` → Homerun 404 (Poncle already pending `unsupported_static_source`).

## Live layout change (6) — parser-leaf fixes (bounded)

| Source | Bounded-run root cause |
|---|---|
| `www.konami.com/games/us/en/jobs/` | Parser follows nav links (`sns_account`, `<%= official_site %>` template URL → HTTP 400); jobs are JS-rendered (no job anchors in raw HTML) |
| `www.yodo1.com/careers` | teamtailor (reclassified above) |
| `invergestudios.com/jobs/` | Detail extraction follows 404 detail link (`/jobs/concept-artist`); listing live |
| `astrum-entertainment.ru/en/careers` | **Static redirect loop** (source itself loops) |
| `www.archetype-entertainment.com/careers` | **Static redirect loop** on `en-US/` |
| `www.letiarts.com/careers/` | **Recovered** in bounded run (3 kept) — transient, not a regression |

## Redirect non-careers (6) — dead/absorbed candidates

hangar13games → `2k.com/studios/hangar-13/` (absorbed into 2K careers), larvagamestudios → `radientgamestudio.com/` (rebrand), saigondragonstudios → `/about-us/`, tripleogames → homepage, winnipeg.ubisoft.com → Ubisoft global about-us, everyweargames → `metacoregames.com/` (rebrand).

## Redirect careers alias (15) — low priority, likely normalization-only

ahoiii, strangebeat, bossfight, miniclip, digitalbros (×2), reflector→dayforcehcm, funnytales, urbangames, bigant, movingstonedigital, paxiegames, trace-studio, triangle-factory, upsurge. Most are trailing-slash/www-bare aliases; one is a real migration (`emplois.reflectorentertainment.com` → `jobs.dayforcehcm.com` — Dayforce unsupported, keep static or demote per policy).

## Recommended actions (operator decision required)

1. **ATS-migrated (8)**: promote/validate provider equivalents per runbook (teamtailor/workday/personio adapters exist). Where a provider row is missing (e.g. `jobs.coffeestain.com`, `careers.fenriscreations.com`), run discovery/provider inference to stage them. Update the static listing URL to the ATS-hosted listing or demote the static row.
2. **Dead 404 (19)**: demote active→pending (reversible, `POST /registry/demote-active`) with reason `dead_listing_page` — no physical deletion. Already-pending rows (coffeestain, sandsoft, poncle) need no action.
3. **Redirect non-careers (6)**: demote as dead/absorbed unless a valid replacement careers URL is staged.
4. **Live layout change (5, minus recovered letiarts)**: three parser-leaf fixes worth implementing:
   - redirect-loop sources (astrum, archetype): investigate loop cause (source-side) — likely site issue, monitor; not a code regression.
   - konami: JS-rendered jobs — browser-fallback candidate, not a static-parser fix.
   - inverge: detail-link validation (skip 404 detail candidates) — candidate for a small leaf fix + regression test.
5. **Redirect careers alias (15)**: re-run pipeline once after other fixes; reclassify survivors.
6. **Down 521 (1)**: pixowl — re-probe later; no action now.

## Applied 2026-08-12 (operator-approved demotions)

19 active static sources were demoted to pending via `POST /registry/demote-active` on the local admin bridge (127.0.0.1:8877), each verified dead/absorbed by live probe on the same day:

- Dead-404 (14): hasbro, darkartssoftware, dedalord, frogdice, htxlabs, penrosestudios, aurorapunks, bamtang, blastworksinc, expertia.ai/flyingcaps, gsc-game, masongames, probablymonsters, tigerrollstudios.
- Redirect-to-non-careers (5): larvagamestudios → radientgamestudio (rebrand), saigondragonstudios → about-us, tripleogames → homepage, winnipeg.ubisoft.com → Ubisoft global, everyweargames.com/careers → metacoregames (already covered by 2 active Metacore rows).

Result: bridge reported `{"demoted": 19}`, registry summary active 2268 / pending 897 / rejected 0; all 19 present in `data/source-registry-pending.json.gz` with `pendingReason: fetch_failure_demote`, zero remaining in the active export. Source-sync unaffected (pull-only, no push, no conflicts). Registry exports are gitignored runtime artifacts; no repo files changed by the demotion.

Not demoted: 5 already-pending rows (andarion, reply.com/careers, poncle, shortgun, hangar13 — no action needed) and the 2 rows missing from the registry (athenaworlds, everyweargames root — the everyweargames.com/careers row was demoted instead).

## Verification

- Bounded runs used: `python -m src.jobs.pipeline --output-dir <tmp> --only-sources <urls> --no-seed-existing-output --no-preserve-previous-on-empty --force-refresh-all --ignore-circuit-breaker --timeout 12`.
- Probe script (temp): `C:\Users\Andrea\AppData\Local\Temp\opencode\probe_regressions.py`; results `probe_sample_25.json`, `probe_sample_29.json`.
- Full probe data in this snapshot's evidence sources; no registry or code mutations were made.
