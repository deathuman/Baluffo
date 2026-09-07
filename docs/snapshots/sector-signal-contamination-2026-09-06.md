# Sector Signal Contamination Sweep — 2026-09-06

> - **Status:** Read-only evidence sweep (T13 follow-up). No registry mutations. **Signal fixes executed 2026-09-06:** the `role_token` branch measured below is removed from `has_positive_game_evidence` (and the frontend mirror); the `keyword_anywhere` branch is scoped to **company/title only** (URLs no longer carry keyword evidence); the keyword scoping's employer residual is recovered by curated **employer-name evidence** (`GAME_EMPLOYER_NAME_HINTS`, company field only — disposition 5). Post-fix recount chain on this exact dataset: Game **16,177 → 8,503** (role_token removal; reclassifications led by Apple 284 / Marvell 153 / NVIDIA 140 / Cadence 116 / CyberArk 97 / Qualcomm 94 / Broadcom 84 / NXP 80 / Thales 79 / Lockheed Martin 77 — the noise inventory below, verbatim) **→ 8,500** (multi-board provenance scoping) **→ 8,042** (keyword scoping: −458 URL-keyword rows, led by Disney 64 / WBD 50 / Sglottery 34 / CNN 21 / Sonyglobal 13; zero dropped rows carry a company-name keyword) **→ 8,382** (+340 employer-name recovery, zero rows lost). Game-company rows keep keyword (title/company), provenance, and employer-name evidence. See the execution notes in the coverage plan.
> - **Basis:** `data/jobs-unified.json.gz` (42,181 rows, latest full-run output); branch-exact reproduction of `has_positive_game_evidence` (`src/jobs/game_detection.py`) in first-firing production order; validated against stored `sector` with 88/16,177 (0.5%) mismatches (snapshot-vs-storage timing drift). Sweep artifacts: `_out/sector-contamination-sweep-20260906/`.
> - **Canonical for:** the 2026-09-06 contamination measurements and mechanism attribution below; not canonical for any fix design (T13's unscheduled filter sketch is superseded on mechanism detail by this snapshot).
> - **Then inspect:** `docs/plans/jobs-coverage-improvement-plan.md` (T13), `docs/snapshots/non-game-employer-evidence-2026-08-12.md`, `src/jobs/game_detection.py`, `src/jobs/normalizers.py` (`normalize_sector`), `src/jobs/pipeline_finalize.py` (`_apply_sector_gate`).

## Summary

Of 16,177 `sector=Game` rows (38.4% of the 42,181-row feed), attribution by the production signal's first-firing branch:

| Branch (production order) | Rows | Share | What it means |
|---|---|---|---|
| `role_token` | **7,730** | **47.8%** | Generic role word in title + company name appearing in its own job URL. **No game keyword anywhere.** (**Branch removed 2026-09-06** — see status.) |
| `keyword_anywhere` | 5,069 | 31.3% | A `GAME_KEYWORDS` substring in company/title/source/link combined text (**scoped to company/title 2026-09-06** — the source/link share, 458 rows, reclassified Tech; see disposition 4) |
| `provenance` | 3,362 | 20.8% | Source-family hints (gracklehq, gamejobs, epic_games_careers, workwithindies…) or sourceBundle studio+adapter |
| `not_game` | 16 | 0.1% | Stored Game without reproduced signal (timing drift) |

**The dominant Game classifier is a tautology.** The `role_token` branch fires when the title contains any `GAME_ROLE_KEYWORDS` role word (`engineer`, `designer`, `artist`, `developer`, `programmer`, `animator`, …) **and** the company name appears inside `source` or `job_link` — but ATS job URLs embed the company slug for *every* company (`job-boards.greenhouse.io/<company>/…`, `jobs.lever.co/<company>/…`, `apply.workable.com/<company>/…`). The corroboration therefore carries ~zero employer-specific information for ATS-hosted rows.

## role_token: the noise inventory

7,730 rows across **1,049 distinct companies**, classified Game with zero game-keyword evidence. Top carriers (sample titles are real, verbatim):

| Company | Game rows via role_token | Sample titles |
|---|---|---|
| Apple | 284 | IS&T Retail Field Leader, Worldwide Supply Demand Product Planner, Wireless SoC Design Engineer |
| Marvell | 153 | (semiconductor roles) |
| NVIDIA | 140 | (GPU/compute roles) |
| Cadence | 116 | (EDA roles) |
| CyberArk | 97 | (cybersecurity) |
| Qualcomm | 94 | (wireless SoC) |
| Broadcom | 84 | (semiconductor) |
| Nxp | 80 | (semiconductor) |
| Thales | 79 | (defense/aerospace) |
| Lockheed Martin | 77 | (defense) |
| Paypal / Ebay / Salesforce / Anthropic | 67 / 62 / 51 / 56 | (fintech, commerce, SaaS, AI) |
| Activision / Ubisoft2 | 62 / 57 | (these two are correct-sector employers — carried by the wrong branch) |

Source composition: the community `google_sheets` sheet (e.g. all 384 Apple rows) **and** the provider board adapters. The provider effect is stark: `greenhouse_boards` ships **1,064 Game / 1 Tech** (the lone Tech row survives only via the `manufacturing` industry-hint veto in `NON_GAME_INDUSTRY_HINTS`), `workable_sources` **290/0**, `lever_sources` **264/0**, `ashby_sources` **78/0**, `bamboohr_sources` **62/0**. Provider boards appear to be ~100% Game because the provenance and role_token branches absorb everything.

**Product impact:** `_apply_sector_gate` (`BALUFFO_STRICT_GAME_ONLY`) keeps exactly `sector == "Game"` — so under the strict gate, Apple/NVIDIA/Lockheed/Bosch/Salesforce-class roles *are the feed*. The 2026-08-12 non-game-employer snapshot covered the opposite class (1,364 **Tech**-sector sheet rows that the strict gate would drop); the role_token class sails through it.

## Static rows with material URL/provenance noise (all ACTIVE registry rows)

| Active static row | Risky Game rows | Composition | Mechanism |
|---|---|---|---|
| `static:listing_url:https://careers.wbd.com/global/en/wb-games-jobs` | 80 | Warner Bros. Discovery 50, **CNN 21**, HBO Max 4, Avalanche 5 | WBD's whole-company careers site sectioned under a `wb-games-jobs` URL; the URL keyword + provenance classify CNN/HBO rows Game. Site also ships 2 correctly-Tech rows. |
| `static:listing_url:https://www.disneycareers.com/en/search-jobs/game/391/1` | 66 | Disney 64, Disney Experiences 2 | The `/search-jobs/game/391/1` **search-URL** keyword carries every row (site total: 78 Game / 82 Tech — same-site classification is a coin flip driven by other signals). |
| `static:listing_url:https://www.lightheart.games` | 15 | Lightheart Entertainment 15 | Nav/junk titles ("Website") carried by the `.games` domain keyword. |
| `static:listing_url:https://itch.io/jobs` | 14 | "Teo Chhim" 14 | **itch.io category-navigation junk** (`itch.io/games/input-webcam` links, titles like "With Webcam support"). Parser-pollution class, not employer misattribution. |
| `static:listing_url:https://koeitecmo.vn` | 14 | Koei Tecmo Vietnam 14 | VNHR/partner-page junk rows carried by provenance. |

Smaller same-pattern rows: `square-enix-games.com` (6), `ll-games.com` (6), `metacoregames.com` (4+2), `careers.ea.com` (4, incl. an `/games/library/mobile` nav link), `goodshepherd.games` (3), `keenswh.com` (3, via `spaceengineersgame.com` link), plus ~80 more active static rows with 1–3 risky rows each (full list in the sweep artifact).

## What is NOT noise

- **Company-name hits (3,037 rows):** Riot Games, Epic Games, Gameloft, Wargaming, Moonton Games, Cygames, Schell Games, thatgamecompany, Roof Games, Activate Games… For game-industry employers the name *is* the employer-scope signal — correct by design (same rationale the Xboxgaming/"Lnw" naming rule encodes).
- **Title hits (2,809 rows):** the intended signal.
- **Provenance for dedicated boards:** `epic_games_careers`, `gamejobs`, `gracklehq`, `workwithindies` rows are games-industry by construction; the mechanism only misfires when a bundle row mixes a multi-board static (WBD/Disney class).

## Corrections to the T13 record

T13's contamination note attributed Intel's false-Game rows to the `?q=game` URL query. Branch-exact reproduction corrects this:

- **Intel (29 Game rows): all carried by `role_token`** ("intel" appears in every `intel.wd1…` ATS link + generic role words in titles). The URL query is not present in stored per-row links at all.
- **SciGames (34 Game rows): carried by `keyword_anywhere` via the board URL** ("scientific**games**" in the myworkdayjobs links), not the company field — the row's company value is "Sglottery", which contains no game keyword.

The prune decision and its evidence basis are unaffected; the mechanism note above supersedes T13's parenthetical.

## Disposition

No registry or signal changes made by this sweep. The three workday rows T13 pruned are already rejected; their static sheet twins remain pending (flagged in T13). Follow-up candidates, in impact order:

1. **Signal fix — EXECUTED 2026-09-06:** the `role_token` branch is removed outright from `has_positive_game_evidence` (`src/jobs/game_detection.py`) and from the frontend mirror (`frontend/jobs/domain/feed.js`). Measured on this snapshot's baseline dataset: Game 16,177 → 8,503 (−7,746), reclassifications matching the noise inventory above (Apple 284 … Lockheed Martin 77); strict subset of the old signal, so no row gained Game. Regression tests: `tests/test_game_detection_company_url_evidence.py`, `tests/frontend/unit/jobs-sector-evidence.test.mjs`.
2. **Provenance scoping — EXECUTED 2026-09-06:** `has_game_source_provenance` now takes the row's company and, when a bundle **mixes** provider-adapter items with static-adapter items (the multi-board-static signature), counts provider provenance only if every provider item's studio is employer-consistent with the row company (substring either way or non-generic-token overlap). Pure provider bundles and legacy adapterless items are unchanged. Correction to the WBD/Disney premise: their rows in this dataset are **keyword-carried** (row sources contain `wb-games-job` / `search-jobs/game`; their bundles are all-static, which the rule already excluded), so this scoping does not move them — that class is the keyword-branch fix. Measured effect on this dataset: Game 8,503 → 8,500 — exactly the 3 keyword-free mixed-bundle rows whose provider studio disagreed with the row employer (Polyphony Digital ×2 via a `Sony Computer Entertainment` greenhouse item, Coldwood Interactive ×1 via `New Moon Production` on teamtailor); all 140 employer-consistent mixed bundles (PlayStation Global class, 188 rows) keep Game. Regression tests: `tests/test_game_detection_company_url_evidence.py`, `tests/frontend/unit/jobs-sector-evidence.test.mjs`.
3. **Parser pollution — itch.io class EXECUTED 2026-09-06:** `_itch_noise` (`src/jobs/page_gating.py`) now enforces the board's structural rule — individual postings live only at `itch.io/j/<numeric-id>/<slug>`; everything else the static parser picks up on itch.io hosts (jobs-board filter pages, `/games/` directory rows, near-* location filters, site pages, `<studio>.itch.io` game/devlog pages) is gated, with the pre-existing /j/ title-vs-slug disagreement check preserved. Measured on this dataset: **48 of the 53 shipped itch.io rows gate** (the 5 real `/j/` postings stay), zero non-itch rows newly gated. The koeitecmo.vn sibling is a different mechanism (third-party careerviet.vn aggregator links, not category navigation) and stays out of scope. Tests: `tests/jobs_static/test_source_specific_noise_rows.py`.
5. **Employer-name evidence recovery — EXECUTED 2026-09-06:** the keyword scoping's residual (the URL-keyword-carried true game employers) is recovered by a curated `GAME_EMPLOYER_NAME_HINTS` table in `has_positive_game_evidence` (`src/jobs/game_detection.py`, mirrored in `frontend/jobs/domain/feed.js`): substring match against the **company field only**, never titles/URLs. Per-source provenance staging was measured impossible for this class — the rows ride the shared `google_sheets` aggregator (which also serves every other studio, so it cannot be whitelisted per-employer), adapterless lever rows, and static boards whose `static` adapter is excluded from provider provenance by design; their `studio` fields already equal the company. The table (`playrix`, `daybreak`, `metacore`, `avalanche`, `square enix`, `lightbulb crew`, `electronic arts`, `ea sports`, `ea create`) was precision-gated against every distinct company containing each candidate hint in the 42,181-row baseline: all matches are genuine games employers (including the EA sub-org variants `Electronic Arts (EA)`, `DICE (Electronic Arts)`, `BioWare Edmonton (Electronic Arts)`), and short-name trap companies (`EACH1`, `Eacproductdevelopmentsolutions`, `Eataly`) match nothing — EA is deliberately multi-token so a bare `ea` hint cannot capture them. Measured delta (old vs new signal, identical data): **8,042 → 8,382 (+340), zero rows lost** — per-company gains exactly equal the keyword-scoping drops: Electronic Arts +206, Electronic Arts (EA) +38, EA Sports +22, Avalanchestudios +17, Metacore +11, playrix/Playrix +11, Lightbulb Crew +9, Square Enix +7, Avalanche Software +5, Avalanche +5, Daybreak +4, EA Create +2, DICE +2, BioWare Edmonton +1. Junk nav/news rows on these employers' boards (Avalanche's `ZH_HK`/`ES_MX`/`Benefits`, Square Enix's `Japan`/`SQUARE ENIX LONDON`, Lightbulb Crew's Steam-news rows) also recover — they are employer-identity-true rows; junk-title gating for that class is disposition 3's separate concern. Tests: `tests/test_game_detection_company_url_evidence.py`, `tests/frontend/unit/jobs-sector-evidence.test.mjs`.
6. **Keyword-branch scoping — EXECUTED 2026-09-06:** the `GAME_KEYWORDS` branch of `has_positive_game_evidence` now matches **company/title only**; source/job_link text no longer contributes keyword evidence (`src/jobs/game_detection.py`, mirrored in `frontend/jobs/domain/feed.js`; `classify_company_type`'s employer-token corroboration scoped identically so `sector` and `companyType` cannot disagree). This is the fix for the keyword-carried WBD/Disney/CNN class items 1–2 explicitly left open. Measured (branch-exact recount, `_out/keyword-scope-recount-20260906/`): 458 rows dropped — 224 source-carried, 234 link-carried, zero split-field — led by Disney 64 / WBD 50 / Sglottery 34 / CNN 21 / Sonyglobal 13; **zero dropped rows carry a keyword in the company name**, so no game employer loses name-based evidence; 4 genuine game-titled WBD/Disney rows survive. Dataset Game count 8,500 → **8,042 (−458)**, exactly the pre-measured class. Residual (recorded honestly): a tail of true game companies whose rows were URL-keyword-carried only (Playrix, Daybreak, Metacore 6, Avalanche 5, EA 4, Square Enix 6, Lightbulb Crew 7) reclassify Tech until promoted with proper provenance or name evidence. Tests: `tests/test_game_detection_company_url_evidence.py`, `tests/frontend/unit/jobs-sector-evidence.test.mjs`.
5. **Product note:** with (1) and (4) executed the strict-gate poison classes are gone; `BALUFFO_STRICT_GAME_ONLY` no longer ships the Apple/NVIDIA class or the WBD/Disney/CNN/Sonyglobal board-URL class. Remaining same-site keyword influence is limited to rows whose *titles* legitimately contain board-adjacent keywords.
7. **gamejobs listing-parser artifacts — EXECUTED 2026-09-06:** the residual from the full pipeline pass (the "Apple 50"/"Wargaming 51" class riding legitimate `gamejobs` provenance) is fixed at the parser: `parse_gamejobs_html` (`src/jobs/adapters/community/__init__.py`) now extracts only real `<div class="job">` cards (class-`title` job anchor, class-`c` company, class-`w` location), so the board's directory/facet `/search` links and site nav — which the old whole-page anchor-chain regex swept into 105 of 109 stored rows as company/title fields ("Digipen 47", "Design 510", the homepage mega-blob) — can never become rows. Verified live via a targeted `--only-sources gamejobs` pass: 86 real postings shipped (Bungie, teamLFG, Smile-Break, NetEase class), zero artifact-shaped rows; stored artifacts are carried rows no source re-emits and age out via the 14-day missing-row archive policy. Tests: `tests/test_jobs_fetcher_providers_niche_boards.py`, `tests/fixtures/gamejobs.html`.
