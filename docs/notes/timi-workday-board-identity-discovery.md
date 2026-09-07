# TiMi Workday board-identity discovery — handoff

**Status:** RESOLVED 2026-09-05 — the empty result is **correct**; see "Resolution" below.
**Tenant under study:** `tencent.wd1.myworkdayjobs.com`
**Pending row in this checkout:** `workday:listing_url:https://tencent.wd1.myworkdayjobs.com/timi_careers`

## What we know already

- `timi_careers` returns a live, well-formed shell page (200, `text/html`, canonical `https://tencent.wd1.myworkdayjobs.com/timi_careers`, `og:title="Timi Studio Group Careers"`, JS config reports `siteId: "timi_careers"`, `tenant: "tencent"`, `isExternal: true`, `appName: "cxs"`), but `postingAvailable` is `null` and the page has **0** server-rendered job anchors and **0** CXS listing-service endpoints reachable at the obvious paths (`/ccs/service/tencent/timi_careers/Search`, `/ccx/service/tencent/timi_careers/Job_Requisitions`, `/Open_Jobs`, `/Openings`, `/List_Jobs`, `/Job_Postings` all 404).
- This is the **empty-configured-board signal**, not a transient failure and not a generic adapter path mismatch. Ruling out: dead host, DNS/tenant problem, temporary outage, and "adapter picked the wrong CXS path and got a 404 silently."
- The honest classification today is: **genuinely empty or wrongly configured board/slug on the Tencent Workday tenant**. Either TiMi does not actually have a populated CXS site/slug on this tenant, or the pending row is pointing at the wrong one.

## Why it is still unresolved

The session ran into repeated `uv_spawn bash.exe` spawning failures on this host when trying to run a fresh HTTP probe sweep against many candidate `timi_*` path variants. That means the candidate-site-path sweep was **interrupted before it could classify the empty-board signal as "genuinely empty" vs "wrong slug / wrong site id"**. We can't close the question from this checkout alone with the probes we successfully ran.

## Resolution (2026-09-05, bounded CXS sweep)

**The pending row's site id is real and the empty result is correct: `timi_careers` is a configured-but-genuinely-empty CXS site.** The empty-board signal is *correct*, not *incorrect* — this closes the open classification question from the section above.

The blocker was the probe path, not the environment: the earlier sweep tried `/ccs/...`, `/ccx/...`, and listing-path variants but never the adapter's actual CXS endpoint shape, `POST /wday/cxs/{tenant}/{site}/jobs` (see `_workday_cxs_config` in `src/jobs/adapters/provider_structured_listing.py`). A new bounded read-only sweep tool (`scripts/workday_cxs_site_sweep.py`, stdlib-only, certifi-anchored TLS mirroring the T5 fix, no-redirect POST handling) probed that exact shape with both method controls:

| Site id | Classification | total | HTTP |
|---|---|---|---|
| `lightspeed` (positive control, promoted row) | populated | 43 | 200 — matches the T3 promotion note's 43 exactly |
| `nonexistent_control_slug` (negative control) | not_found | — | 404 |
| **`timi_careers` (the pending row's site id)** | **configured_empty** | **0** | **200** |
| `TiMi_Careers` (case variant; Workday site ids are case-insensitive here) | configured_empty | 0 | 200 |
| `timi_montreal_careers` (sibling TiMi board) | configured_empty | 0 | 200 |
| `Tencent_Careers` (broad company board) | populated | 299 | 200 |
| 11 other `timi*` variants (`timi`, `TiMi`, `timi_studios`, `TiMi_Studios`, `timi_career`, `timi_jobs`, `timi_external`, `timi_global`, `TiMiStudioGroup`, `timi_studio_group`, …) | not_found | — | 404 |

All 16 probes are in `_out/timi-workday-site-sweep-2026-09-05/sweep-evidence.json` (+ `variants/sweep-evidence.json` for the raw 12-variant pass), run 2026-09-05T20:03–20:06Z.

Corroboration beyond the sweep:

- **Google-indexed TiMi requisitions are closed postings, not crawl-evidence of a live board.** A directly fetched indexed requisition (`/en-US/timi_careers/job/3A---_R106634/...`) returns 200 with `postingAvailable: false` server-side — crawl lag over closed roles, consistent with an empty CXS total.
- **`timi_montreal_careers` matches its public page.** Google's snippet shows "There are no job openings at this time … 0 JOBS FOUND" and our CXS probe agrees (configured_empty, total=0).
- **TiMi's canonical hiring surface is `timistudios.com/careers`** (LinkedIn, NEXARDA, ContactOut all point there), which is already registered in this repo as the TiMi static row in `data/defaults/source-registry-active.seed.json`. The Workday board is a secondary, currently-empty surface.
- **No populated TiMi-specific site id exists on this tenant.** The sweep covered every plausible slug variant; the only populated boards on `tencent.wd1` are company-level (`Tencent_Careers` 299) and the already-promoted `lightspeed` (43).

## Disposition

Per the pre-registered decision tree in "Recommended next actions": **no populated TiMi Workday site/slug was found; record the row as a genuinely empty Workday candidate — do not promote; do not retry blindly.**

- The pending row stays **untouched** in this checkout (no registry mutation was performed; rejection on the live container would be a separate operator decision through the normal Admin flow, like the 8/29 D1 empty-board rejections).
- This is an existence question with a negative answer, categorically different from the Intel/L&W/Scientific Games scope question (those boards return real yield; theirs is a *filtering* problem).
- Re-probe only on an external signal: a fresh role appearing on the board (Google re-index of a `postingAvailable: true` page), a public announcement of TiMi Workday hiring, or a TiMi careers-page change. A one-off natural re-probe on a future scheduled pass is acceptable; a dedicated retry sweep is not.

**Test coverage:** `tests/test_workday_cxs_site_sweep.py` (12 tests, no network) covers the endpoint shape mirroring, the populated/configured-empty/not-found classification, the POST payload/headers, og:title extraction, and the TLS host gate.

## Repo context that constrains the decision

- In this repo, `timi_careers` only appears as a **pending registry row path seed** (in `data/defaults/source-registry-active.seed.json`) and as a **test-input URL shape** in the `source_discovery` test suite (`test_provider_migration_advisory.py`, `test_provider_inference.py`, `test_web_search_candidates.py`). It is not a registered active Workday source in this checkout.
- The Workday candidate builder (`_workday_candidate` in `src/source_discovery/provider_inference.py`) only needs `host ~ myworkdayjobs.com` plus a non-empty listing path; it does **not** validate that the site id actually exposes a populated CXS listing service. That's why an empty shell can be a valid-seeming Workday candidate row even when the board has no postings.
- The provider-migration advisory path (`provider_staging_decision_for_advisory` / `enrich_provider_migration_metadata` in `src/source_discovery/provider_migration_advisory.py`) is the right review surface if we end up wanting to replace this row's URL with a different Workday listing URL: that is where the "replace URL / replace site id" decision would be staged. (Not applicable now — no populated replacement exists.)

## What would actually resolve this

1. **Discover the correct TiMi Workday site identity on the Tencent tenant.** That means probing whether TiMi has its own real CXS site/slug, or whether this row should point elsewhere (a different site id, a different reporting endpoint, a different company slug on the same `tencent.wd1.myworkdayjobs.com` tenant). — **DONE 2026-09-05: no populated TiMi-specific site id exists; `timi_careers` is real but empty.**
2. **Classify the empty result properly.** The probe evidence we do have already says "empty configured board signal, not an adapter blind spot." The missing piece is whether that empty signal is *correct* (TiMi has no active Workday board here) or *incorrect* (this row should be a different slug/site id). — **DONE 2026-09-05: the empty signal is correct.**

## Recommended next actions

1. ~~**Do a proper candidate-site-id sweep against `tencent.wd1.myworkdayjobs.com` from an environment that can actually run the HTTP probes.**~~ **DONE 2026-09-05** — the sweep ran via the new `scripts/workday_cxs_site_sweep.py` (the earlier `uv_spawn bash.exe` blocker did not reproduce; the real gap was that the prior probe set never tried the adapter's `/wday/cxs/{tenant}/{site}/jobs` POST shape).
2. ~~**If a populated TiMi Workday site/slug is found, replace the pending row's `listing_url` with that verified URL**~~ — **N/A**: no populated TiMi site id exists on this tenant.
3. ~~**If no populated TiMi Workday board exists on this tenant, record that as a "genuinely empty or misconfigured Workday candidate; do not promote; do not retry blindly" disposition.**~~ **DONE** — see "Disposition" above. (Unlike Intel/L&W/Scientific Games, which have a scope question over real yield, TiMi's question is an existence question, and the answer is negative.)
4. ~~**Do not conflate this with the Intel/L&W/Scientific Games scope decision.**~~ — honored throughout; the disposition above stands alone.

## Related artifacts already in this session

- `docs/notes/t3-workday-promotion-2026-09-05.md` — TiMi "keep pending / do not promote" disposition with the empty-configured-board reasoning already recorded there (now updated to point at this resolution).
- `docs/notes/xboxgaming-workday-investigation-results.md` — the same Tencent-tenant Workday investigation style used for xboxgaming; the probe method used here (bounded CXS POSTs through the fixed adapter TLS path) mirrors that precedent.
- `scripts/workday_cxs_site_sweep.py` + `_out/timi-workday-site-sweep-2026-09-05/` — the sweep tool and raw evidence; reusable for the Intel/L&W/SciGames "find each company's actual games-facing Workday site/slug" discovery passes recommended by the T3 note.
- Source surfaces referenced by the (now moot) URL-replacement path: `src/source_discovery/provider_migration_advisory.py` (advisory enrichment + staging decision), `src/source_discovery/provider_inference.py` (Workday candidate builder), and the `source_discovery` tests for the URL shapes already in the repo.
