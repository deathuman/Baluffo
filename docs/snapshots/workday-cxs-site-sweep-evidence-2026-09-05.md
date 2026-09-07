# Workday CXS Site-Id Sweep Evidence — 2026-09-05 (five tenants)

> - **Status:** Evidence snapshot; read-only probes only — **no registry mutations performed by the sweeps**. All five tenants probed 2026-09-05 through `scripts/workday_cxs_site_sweep.py` (bounded, certifi-anchored TLS, no-redirect POST).
> - **Basis:** live HTTP probes against the adapter's real CXS endpoint shape, `POST /wday/cxs/{tenant}/{site}/jobs` (see `_workday_cxs_config` in `src/jobs/adapters/provider_structured_listing.py`), plus per-board sample-title capture (`extract_sample_titles`) and og:title page probes; raw artifacts in `_out/timi-workday-site-sweep-2026-09-05/`, `_out/workday-games-site-sweep-2026-09-05/{intel,lnw,sglottery}/`, `_out/xboxgaming-maintenance-probe-20260905.json`
> - **Canonical for:** the consolidated probe-evidence record for the 2026-09-05 Workday site-id sweeps across `tencent.wd1`, `intel.wd1`, `lnw.wd5`, `sglottery.wd5`, and `xboxgaming.wd1`; not canonical for adapter behavior or registry policy
> - **Then inspect:** `docs/notes/t3-workday-promotion-2026-09-05.md` (cohort dispositions), `docs/notes/timi-workday-board-identity-discovery.md` (TiMi resolution), `docs/notes/xboxgaming-workday-investigation-results.md` (xboxgaming staging/attribution)

## Why these sweeps exist

The T3 workday cohort's open questions all reduced to one probe shape: **which site ids actually exist and are populated on each Workday tenant, and are any of them games-facing?** The prior session's probe set tried `/ccs/...`, `/ccx/...`, and listing paths but never the adapter's actual `/wday/cxs/{tenant}/{site}/jobs` POST shape, so "empty board" vs "wrong slug" was unresolved. `scripts/workday_cxs_site_sweep.py` (stdlib-only leaf tool; `REQUEST_GAP_S = 1.0` between probes, `--timeout` default 20 s) closes that gap; every sweep ran with a known-populated positive control and a nonsense-slug 404 control so the classification method is validated inside each artifact.

Classification taxonomy (per probe): `populated` (CXS `total > 0`), `configured_empty` (HTTP 200, `total = 0`), `not_found` (404), `blocked_or_challenge`, `redirect`, `http_error`, `error`. Workday site ids proved case-insensitive on these tenants (`TiMi_Careers` ≡ `timi_careers`).

## Cross-tenant summary

| Tenant | Site ids found (classification · CXS total · sample titles) | Games-facing alternative? | Disposition impact |
|---|---|---|---|
| `tencent.wd1.myworkdayjobs.com` | `lightspeed` (populated · 43), `Tencent_Careers` (populated · 299), `timi_careers` (**configured_empty · 0**), `timi_montreal_careers` (configured_empty · 0), `TiMi_Careers` ≡ `timi_careers`, 11 `timi*` variants (not_found) | No populated TiMi-specific board | TiMi pending row is **genuinely empty** — keep pending, do not promote, no blind retry (resolution of `docs/notes/timi-workday-board-identity-discovery.md`) |
| `intel.wd1.myworkdayjobs.com` | `External` (populated · 600 · DBA PL/SQL, Senior Middleware Dev, Manufacturing Tech) only; 7 games-slug variants (not_found) | **No.** `searchText=game` at CXS level → total 83, titles still CPU-debug/thin-films/industrial — keyword noise, not games roles | Intel `?q=game` row **HOLD is final**: the query cannot be tightened into a games filter |
| `lnw.wd5.myworkdayjobs.com` | `LightWonderExternalCareers` (populated · 108 · mixed QA/Java/field-service), **`GroverGamingExternalCareerSite`** (populated · 6 · Analyst Gaming & Performance, SW Engineer), control `sciplayexternalcareerssite` (populated · 15 · Technical Artist, 2D Game Artist); 7 alternatives (not_found) | No L&W games-only board; Grover Gaming is a separate company with its own site id | L&W row **HOLD stands**; new stageable lead: Grover Gaming as its own workday source (staged 2026-09-05 — see the T3 note addendum) |
| `sglottery.wd5.myworkdayjobs.com` | `ScientificGamesExternalCareers` (populated · 123 · Field Service Tech ×2, Training Manager) only; 7 alternatives (not_found) | No | SciGames row **HOLD stands** — no cleaner board to re-decide on |
| `xboxgaming.wd1.myworkdayjobs.com` | `CentralTech` (populated · 2 · Expert Engineer Security–Central Tech, Sr Technical PM), `External` (populated · 40 · Senior Expert Gameplay Engineer–Raven, Senior UI Engineer–Sledgehammer) | Yes — this whole tenant is games-facing (Activision family studios) | Confirms T5-staged container rows recovered post-maintenance; attribution fix (host-derived "Xboxgaming" → per-studio) remains a promotion-time T3 review item |

## Per-tenant evidence

### tencent.wd1 (TiMi board-identity resolution)

Probes 2026-09-05T20:03–20:06Z; artifacts `sweep-evidence.json` + `variants/sweep-evidence.json` (raw 12-variant pass). Endpoint shape `/wday/cxs/{tenant}/{site}/jobs (POST)`; zero blocked/redirect/error outcomes.

| Site id | Classification | total | Note |
|---|---|---|---|
| `lightspeed` (positive control) | populated | 43 | Matches the T3 promotion note's 43 exactly — method validated |
| `nonexistent_control_slug` (negative control) | not_found | — | 404s are detectable, not silent |
| **`timi_careers` (the pending row's site id)** | **configured_empty** | **0** | HTTP 200; og:title `Timi Studio Group Careers` |
| `TiMi_Careers` (case variant) | configured_empty | 0 | Site ids case-insensitive here |
| `timi_montreal_careers` | configured_empty | 0 | Matches its public "0 JOBS FOUND" page |
| `Tencent_Careers` | populated | 299 | Tenant serves CXS fine |
| `timi`, `TiMi`, `timi_studios`, `TiMi_Studios`, `timi_career`, `timi_jobs`, `timi_external`, `timi_global`, `TiMiStudioGroup`, `timi_studio_group` | not_found | — | No populated TiMi-specific board exists |

Corroboration beyond the sweep (details in the TiMi note): a Google-indexed TiMi requisition returns `postingAvailable: false` server-side (closed postings/crawl lag, not live-board evidence); TiMi's canonical hiring surface is `timistudios.com/careers`, already registered as the static row. Per the note's pre-registered decision tree: **no populated TiMi Workday site/slug found — record as genuinely empty; do not promote; do not retry blindly** (re-probe only on external signal). Registry untouched.

### intel.wd1 (held row: `external?q=game`)

Probe 2026-09-05T20:15:51Z; 8 candidates + controls, zero blocked/redirect/error. Only `External` exists (populated, 600; sample titles: Database Administrator PL/SQL Developer, Senior Middleware Development Engineer, Experienced/Early Careers Manufacturing Technician). All games-slug variants (`IntelGames`, `intel_games`, `Intel_Games`, `IntelGaming`, `intel_gaming`, `GamesExternal`, `GamingExternal`) are not_found.

Decisive extra measurement: the row's `?q=game` flows into CXS as `searchText`; a direct `searchText=game` POST returned total 83 but sample titles remained Senior Post-Silicon CPU Debug / Thin Films / Industrial Engineering — **the query matches keyword noise, not games roles**. This confirms the T3 fetch evidence (0 explicit game-title hits in 38 output rows). There is no games-facing board to migrate to; the row is a broad non-games portfolio and a candidate for pruning only as an operator decision.

### lnw.wd5 (held row: `lightwonderexternalcareers`)

Probe 2026-09-05T20:16:29Z; 8 candidates + controls, zero blocked/redirect/error.

| Site id | Classification | total | Sample titles |
|---|---|---|---|
| `sciplayexternalcareerssite` (control, promoted row) | populated | 15 | Technical Artist, 2D Game Artist, Director of Monetization — **matches its promoted `jobsFound` of 15 exactly** |
| `LightWonderExternalCareers` (the row's board) | populated | 108 | Senior Software Engineer (Java), Software QA Engineer, Partner Enablement Manager, Field Service Technician |
| `GroverGamingExternalCareerSite` | populated | 6 | Analyst Gaming & Performance, Software Engineer, Supervisor Field Services |
| `GroverGaming`, `JackpocketExternalCareers`, `Jackpocket`, `LightWonderGames`, `LnWGames`, `GamesExternal` | not_found | — | — |

No games-only L&W board exists; the HOLD stands. **New positive lead:** `GroverGamingExternalCareerSite` is L&W's charitable-gaming division (Grover Gaming) with its own site id and 6 live jobs — stageable as its own workday source with clean per-studio attribution, not a fix for the L&W row.

### sglottery.wd5 (held row: `scientificgamesexternalcareers`)

Probe 2026-09-05T20:17:07Z; 8 candidates + controls, zero blocked/redirect/error. Only `ScientificGamesExternalCareers` exists (populated, 123; sample titles: Field Service Tech Topeka, Field Service Technician Coshocton Region, Manager of Training and Knowledge Management, DevOps Specialist). All alternatives (`ScientificGames`, `SGExternalCareers`, `SGLotteryExternalCareers`, `SciGamesExternalCareers`, `SGInteractiveExternalCareers`, `SGInteractive`, `GamesExternal`) are not_found. The board is a lottery/operating-company board; HOLD stands — no cleaner board exists to re-decide on.

### xboxgaming.wd1 (T5-staged container rows, maintenance recovery)

Probed through the fixed `workday_sources` transport (certifi-anchored TLS from the T5 fix) on 2026-09-05, after the Workday platform maintenance window cleared; artifact `_out/xboxgaming-maintenance-probe-20260905.json`. These rows live on the live container (staged 2026-09-05 via `/sources/manual`), not in this checkout's local registry.

| Board | Studio attribution | kept today | Sample titles |
|---|---|---|---|
| `CentralTech` | Activision (Central Technology) | 2 | Expert Engineer Security – Central Technology; Senior Technical Program Manager (Temporary) |
| `External` | Beenox / High Moon / Infinity Ward / Sledgehammer | 40 | Senior Expert Gameplay Engineer – Raven Software; Senior UI Engineer – Sledgehammer Games Melbourne |

In-band comparison: WP18 8/28 measured CentralTech ~3 / External ~67 before the TLS fix blocked the adapter; today's 2/40 is the same ballpark (point-in-time CXS totals, not a regression) and confirms recoverability. This tenant is the counter-example that proves the sweep's discrimination: it is genuinely games-facing, while the three held T3 tenants are not.

## Sweep method (reproducible)

```bash
# Example: the TiMi tenant sweep (bounded, read-only, ~1 request/second)
python scripts/workday_cxs_site_sweep.py \
  --host tencent.wd1.myworkdayjobs.com \
  --sites timi_careers TiMi_Careers timi_montreal_careers Tencent_Careers \
  --control-sites lightspeed nonexistent_control_slug \
  --probe-page-titles \
  --out-dir _out/timi-workday-site-sweep-2026-09-05
```

- Mirrors `_workday_cxs_config` in `src/jobs/adapters/provider_structured_listing.py`: POST to `/wday/cxs/{tenant}/{site}/jobs`, certifi-anchored verified TLS for `*.myworkdayjobs.com`, redirects disabled (redirect ⇒ classified, not followed).
- Classifies per site id: populated / configured_empty / not_found / blocked_or_challenge / redirect / http_error / error; captures `og:title` page probes with `--probe-page-titles` and sample job titles from CXS payloads.
- Controls are mandatory practice: a known-populated board validates the method, a nonsense slug validates 404 detection. Every artifact above ran both.
- Test coverage: `tests/test_workday_cxs_site_sweep.py` (14 tests, no network) covers endpoint-shape mirroring, classification, POST payload/headers, og:title extraction, sample-title capture, and the TLS host gate. Precommit gate green on the tool + tests.

## Consequences for the T3 workday cohort

- **Promoted (3):** SciPlay 15, LIGHTSPEED 43, Avalanche 7 — validated fresh yield 2026-09-05 (see `docs/notes/t3-workday-promotion-2026-09-05.md`).
- **Genuinely empty (1):** TiMi — keep pending, do not promote, do not retry blindly.
- **Holds final (3):** Intel / Light & Wonder / Scientific Games — **no games-facing Workday site id exists on any of their tenants**; the holds are structural, not query-fixable. Remaining paths are product-level: prune the rows, or promote-with-filter once a games-site/source filter exists.
- **Actionable new lead (1):** stage Grover Gaming as its own workday source (`GroverGamingExternalCareerSite` on `lnw.wd5`, 6 jobs, own identity) — **done 2026-09-05**: staged through the manual-add path with correct per-studio attribution and fresh in-band CXS revalidation; disposition and write verification recorded in the T3 note's "Grover Gaming staged" addendum.
- **Container-side follow-up:** xboxgaming rows recovered (CentralTech 2 / External 40); their host-derived "Xboxgaming" attribution needs correction to Activision / per-studio at T3 promotion review.
