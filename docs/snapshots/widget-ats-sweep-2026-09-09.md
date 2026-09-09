# Embedded-ATS Widget Signature Sweep — 2026-09-09

> - **Status:** Read-only evidence sweep + executed 5-repair staging wave. The sweep fetched static listing pages only (no registry mutations); the follow-on wave staged 5 pending provider rows through the sanctioned lane (runtime pending registry only, seed untouched).
> - **Basis:** all 1,972 active static registry rows (listing pages, bounded 14s, CERT_NONE, 6 workers, resume-safe) against 20 provider-widget signature families; join basis run5 slot `jobs-fetch-report-run-20260908-155153`. Raw artifacts: `tmp/widget-sweep-20260909/` (`results.jsonl`, `findings.json`); wave artifacts: `tmp/widget-wave-20260909/` (`stage_five.py`, `EXECUTION-ADDENDUM.md`, evidence slot `jobs-fetch-report-run-20260909-093844`).
> - **Canonical for:** the 2026-09-09 embedded-widget audit measurements, Tier-1 adjudication, and the executed 5-repair staging wave with its corrections.
> - **Then inspect:** `docs/CHANGELOG.md` ([Unreleased] Fixed entries), `scripts/provider_migration_staging_refresh.py` (staging conventions), `docs/source-policy-runbook.md` (`--include-pending-provider-migration` validation path).

## Headline numbers

- **232 rows carry at least one signature**; **115 are embed-shaped** (≥3 hits).
- **149 of the 232 already yield jobs** (run5 kept>0) — the static flow + adapter reclassification is working; the sweep *confirms* coverage for the phApp family (Treyarch/Raven/Sledgehammer/King/Blizzard/Beenox/Activision), WBD rows, Ubisoft Toronto, PlayStation, Larian, Jagex, InnoGames, etc.
- **81 rows are failing-or-zero-kept WITH a signature** → after removing already-covered hosts and weak (1–2 hit) noise: **18 Tier-1 rows**.
- Fetch health: 1,860 HTTP 200, 57×403, 5×404, 4×500, 6×429, 2×999, 36 connection errors, 8 provider-host rows skipped by design.

## Tier-1 adjudication (18 rows)

### Recovery candidates — provider row staging via EXISTING adapters (5, executed same day)

| Static row | Widget evidence | Tenant | Staged row |
|---|---|---|---|
| `grand.gs/careers` (run5 **error**) | lever ×29 | `jobs.lever.co/grand` live, 9 postings (Istanbul) | `lever:account:grand` |
| `www.riftgaming.gg/careers` (ok/0) | tt api-key | `jobs.riftgaming.gg` custom-domain Teamtailor tenant | `teamtailor:listing_url:https://jobs.riftgaming.gg/jobs` |
| `www.wetaworkshop.com/about-us/careers` (ok/0) | tt api-key + tenant ref | `wetaworkshop.teamtailor.com/jobs` live, 4 postings | `teamtailor:listing_url:https://wetaworkshop.teamtailor.com/jobs` |
| `tornbanner.com/careers/` (ok/0) | bamboohr ×3 | `tornbanner.bamboohr.com/careers` live (JS shell) | `bamboohr:listing_url:https://tornbanner.bamboohr.com/careers` |
| `voldex.com/careers/#jobs` (ok/0) | ashby ×4 | `jobs.ashbyhq.com/voldex` live (SPA) | `ashby:board_url:https://jobs.ashbyhq.com/voldex` |

### Adapter-gap — live embeds, no matching adapter (3 rows, 2 families)

- **careers.amd.com** ×2 → iCIMS SPA board — no icims adapter (iCIMS exists only as a discovery-advisory detection family; games-adjacent employer, sector-gate question first).
- **playstudios.com/careers** → Dayforce embed — no dayforce adapter (Reflector hold precedent).
- **Axes in Motion** was flagged as a possible sixth repair after the sweep because the repo **does** have a workable adapter (`workable` in `PROVIDER_REGISTRY_ADAPTERS`, `account` field → `apply.workable.com/api/v1/widget/accounts/{value}?details=true`). Re-adjudicated 2026-09-09 **against staging**: the `axesinmotion` account is live (200) but the board holds exactly **1 job — "Speculative Application"** (talent-pool pseudo-entry, the GSC "Submit your CV" / a4vr INITIATIVBEWERBUNG class), and the page's second workable link (`/j/3E383EBEC0`) is a stale posting absent from the API. Staging would add pseudo-entry contamination, not coverage; the correct disposition is record-only. The audit's adapter-gap *outcome* was right, its *reason* ("no workable adapter") was wrong.

### Dead/stale embeds — no rescue possible (4)

`liongamelion` (already adjudicated dead), `studio-hermitage` + `wayfinderstudios` (tt widget API **401** — revoked keys), `impulsegear` (lever tenant 404; static row still yields 1).

### Already-covered / different mechanism (5)

`frontier.co.uk` + `unknownworlds.com` (browser-fallback working), `socialpoint.es` (weak greenhouse refs, take-two ecosystem), `emplois.reflectorentertainment.com` + `playstudios` dayforce (holds above), AMD counted once per family.

## Method notes / caveats

- Embed vs. incidental: signature hits ≥3 treated as embed-shaped; 1–2 hits are footer/carousel links (57 rows kept as Tier-2 in `findings.json`).
- `registry-covered` greps can false-positive on the studio's own static row — tenant-level registry checks were done per candidate.
- Tier-2 (57 weak rows) is bulk noise; only worth revisiting after Tier-1 lands.
- The 2 ads/aggregate rows (`lazyapply`, `brainpop`) are content-parsing coincidences, not studio boards.

## Executed wave — 2026-09-09

All 5 recovery candidates staged through the sanctioned pending provider-migration lane (pending 848 → 853, `migrationSourceIdentity` → superseded static row, `candidateState="staged_provider_candidate"`, seed untouched) and validated with a targeted `--include-pending-provider-migration` pass over the four family loaders: exit 0, 0 failed sources, all 5 rows `status ok` keeping jobs on the first fetch — **Grand 9/9, RiftGaming 2/2, Weta 4/4, TornBanner 4/4, Voldex 4/4 = 23 jobs** into the feed (output 41,841; slot `jobs-fetch-report-run-20260909-093844`). Promotion through the normal approval flow (Fanatee/inXile precedent) is the separate next step.

Corrections vs. the audit's projections:

- **RiftGaming tenant family corrected: Teamtailor, not Lever.** The lever-account sweep (`api.lever.co/v0/postings/{riftgaming,rift,rift-gaming}`) is 404 across the board; the custom-domain board `jobs.riftgaming.gg/jobs` is a Teamtailor tenant with 2 server-rendered job links (yodo1 precedent).
- **Weta's widget API key** is no longer re-derivable from the rewritten marketing page; the direct tenant board itself is live, which is the registration target anyway.
- **TornBanner** fetched 4 (bamboo pagination) → 2 unique after dedup; net feed +2.
- **Axes in Motion re-adjudicated: not a sixth repair.** Workable adapter exists and the `axesinmotion` account is live, but the board's only posting is a "Speculative Application" talent-pool pseudo-entry (plus one stale page link not in the API) — staging would trade zero-yield for pseudo-entry contamination. Record-only.
