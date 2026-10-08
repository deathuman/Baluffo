> - **Class:** shipped-defect
> - **Trigger:** Voodoo's careers ATS moved from Lever to Ashby; `lever:account:voodoo` now 404s, so job `13968523-e0f2-4cdb-81a1-4ac338bd5e0a` (published 2026-10-01) is absent from the feed
> - **Verified against:** be26a0f0 (diagnosis); 64fd27b2 (still missing on v0.3.014, root cause found)
> - **Status:** items 1-4 implemented but **not effective**; the probe/fetch reader defect is fixed 2026-10-08, release + live verification deferred

# Voodoo Ashby board migration

## Diagnosis (verified 2026-10-02)

Voodoo migrated its ATS from Lever to Ashby. Baluffo's only registered Voodoo board is the dead
Lever row, and no Ashby row exists.

| Probe | Result |
|---|---|
| `api.lever.co/v0/postings/voodoo?mode=json` | 404 |
| `jobs.lever.co/voodoo` | 404 |
| `api.lever.co/v0/postings/jagex?mode=json` (control) | `[]` — endpoint shape correct |
| `jobs.ashbyhq.com/voodoo` (board root) | 200 |
| Ashby posting-api `voodoo` | 120 live jobs |

Live Umbrel `192.168.50.61:8877` (v0.3.006, fetch run 2026-10-02 19:03) `/ops/fetch-report`:

```json
{"name":"Voodoo (Lever)","status":"error","adapter":"lever","studio":"Voodoo",
 "fetchedCount":0,"keptCount":0,
 "error":"HTTP 404 for https://api.lever.co/v0/postings/voodoo?mode=json"}
```

Registry: `lever:account:voodoo` active. No `ashby:board_url:https://jobs.ashbyhq.com/voodoo`
row in active, pending, rejected, or tombstones. The Ashby adapter is healthy — 9 boards
registered, 84 fetched / 81 kept.

Adapter proof (production `run_ashby_sources_source`, injected row, `thatgamecompany` as
control): 120 Voodoo rows including
`sourceJobId: ashby:13968523-e0f2-4cdb-81a1-4ac338bd5e0a`, city Paris / FR, Remote,
Full Time. No quality-gate risk — `normalize_contract_type` matches `"full time"` before
`"freelance"`.

## Why it was still missing on v0.3.014 (found 2026-10-08)

Item 1 landed as *code*, which is not the same as a registered board — and the job
stayed absent through every release since. Verified on live Umbrel v0.3.014
(`appVersion 0.3.014`, pipeline `pipeline_d799a3b921`):

| Check | Result |
|---|---|
| Ashby posting-api `voodoo` (control) | **122 jobs**, target job present |
| `ashby_sources` per-board detail | 48 children, **Voodoo not among them** |
| Live registry, all three buckets | no `ashby:…/voodoo` row; `lever:account:voodoo` still active and 404ing |
| Discovery report | `{"name":"Voodoo (Ashby)","stage":"probe_quarantined","dropReason":"zero_jobs"}` — consecutive=3 |

So item 1 never ran on the box: `refresh_active_ashby_registry()` is reachable only
from `src/ashby_registry_refresh.py`'s own `main()`, and nothing in the runtime,
pipeline, or bridge calls it. A curated row in a test is not a board in a registry.

The deeper cause is a **reader defect, not a registration defect**. Ashby serves
`jobs.ashbyhq.com/<slug>` client-rendered with zero `/job/` anchors, and both readers
counted anchors:

- discovery's probe read `zero_jobs` three times and quarantined a live board;
- the fetch adapter parsed the same page, so registered Ashby boards such as
  supercell kept 0 against a board holding 32.

Measured 2026-10-08 — detail links vs posting-API jobs: voodoo **0 / 122**,
thatgamecompany **0 / 40**, supercell **0 / 32**.

Fixed: probe and fetch both resolve `api.ashbyhq.com/posting-api/job-board/<slug>`
through `src/ashby_board_urls.py`, and Ashby moved from the HTML-board plugin to
`JSON_FEED_SPECS`. Verified against the live board through the production fetch path:
53 game rows kept, target collected as `ashby:13968523-…`, Paris / FR, Full Time —
identical to what the HTML reader produced, so nothing was traded away.

**Live verification is still owed.** This is delivery, and delivery is only proven by a
fetch run keeping the job — on the box, after a release.

## Open items

| # | Item | Where it is handled | Pinned by | Status |
|---|---|---|---|---|
| 1 | Register Voodoo's Ashby board | `src/ashby_registry_refresh.py` `CURATED_ASHBY_ROWS` | `tests/test_ashby_registry_refresh.py` | **landed `37e79f19`, but ineffective** — the curator runs only from its own `main()`, and the probe quarantined the candidate anyway |
| 2 | Repoint Voodoo discovery seed | `src/discovery_seed_catalog.json:32`, `src/source_discovery/config.py:424` | `tests/source_discovery/test_voodoo_ashby_seed.py` | implemented |
| 3 | Treat explicit `likelyProviders` as a prior, not a ceiling | `src/source_discovery/provider_patterns.py:37` `likely_providers_for_seed` | `tests/source_discovery/test_provider_prior_not_ceiling.py` | implemented |
| 4 | Surface provider-family child 404s in source health | `src/jobs/common/contracts_source_health.py:190` `derive_source_health`, `_provider_child_rows` | `tests/test_provider_child_failures.py` | implemented |
| 5 | Automatic dead provider-board retirement | no implementation exists today | — | deferred, see below |

### Defects item 1 exposed and also fixed in `37e79f19`

Verifying item 1 against a copy of the live registry surfaced two real bugs in
`refresh_active_ashby_registry`, both fixed in the same commit:

- **Data loss.** The registry persists a slim row projection, so a row read back
  carries its board URL only inside `id: ashby:board_url:<url>`. The probe and the
  candidate key read `board_url`/`careersUrl` only, so all 9 existing Ashby boards
  validated as `invalid`/`missing board_url` and were dropped — thatgamecompany
  (40 jobs), k-ID, Sleeper and six more. Resolved from the source id now, scoped to
  `src/ashby_registry_refresh.py`; `provider_fields_from_source_id` also feeds
  conflict adjudication, so extending its map would have widened the change.
- **Duplicate rows.** `source_identity` lowercases values when building `id`, so a
  curated `.../Joyteractive` and a persisted `.../joyteractive` were two candidates
  that both wrote the same lowercase `id` — two rows for one board, double-counting
  its jobs. Candidate keys are now case-insensitive; the fetched URL still preserves
  the caller's casing.

Post-fix measurement on the same registry copy: 9 boards -> 17, **no board lost, no
duplicate ids**, Voodoo at 120 postings, 278 across all boards. The single removal is
Day[9]'s Game Studio, whose board probes `empty` with its org name resolving while
thatgamecompany as a control parses 40 — a genuinely empty board.

`refresh_active_ashby_registry` is only reachable from its own `main()`, so the
pipeline never ran this; the exposure was operator-triggered.

## Findings that constrain the work

### Repeated 404s do not tombstone anything today

Item 5 is **not** an existing behaviour that needs enabling. Verified absent:

- `get_quarantined_sources` (`src/jobs/common/health.py:88`) reads `source_states`, which
  tracks the provider **family** as one unit. Live `lever_sources` shows
  `consecutiveFailures: 0` while three children 404.
- Family children carry no failure counter; their payload is only
  `name, status, adapter, studio, fetchedCount, keptCount, lowConfidenceDropped, error`.
- Every `auto_demote_*` path is alias/conflict-based
  (`registry_conflict_safe_auto_demote`, `auto_demote_provider_static_weaker_source`),
  not failure-based.
- No provider plugin retires a dead board.

The one automatic dead-board retirement that does exist is **Ashby-only**:
`src/ashby_registry_refresh.py:320-328` drops any Ashby row whose board probe does not
return `ok_with_jobs`. Item 4 therefore delivers the **visibility** that item 5 would need;
item 5 itself stays deferred and is not in this plan's scope.

### Provider-child 404s are currently invisible

`derive_source_health` walked only top-level rows. Family children sit in `row["details"]`
and were never visited, so their `failureBucket` was never counted and they never reached
`sourcesNeedingAttention`. Live Admin reports `status: "healthy"`, `alerts: []`.

Currently hidden: 12 dead ATS boards — Lever x3 (Voodoo, Big Time Studios, Rolocule Games),
Greenhouse x5 (ArenaNet, Guerrilla Games, Magic Leap, Mythical Games, Trailer Park Group),
Workable (Soulbound), Recruitee (Scorewarrior), Pinpoint (CCP Games), Teamtailor
(Lionbridge Games).

Item 4 surfaces them additively rather than by folding them into
`sourcesNeedingAttention`. Measured on the live report: **58 failing child boards**, of
which exactly the 12 above are HTTP 404 dead boards. The rest are pre-existing per-board
config and network errors the same blind spot was hiding.

Two deliberate choices, both because this is a public report payload:

- **New keys, not new entries.** `providerChildFailures` / `providerChildFailureCount` are
  additive. `sourcesNeedingAttention` is the family-level triage list Admin presents, and a
  child row there would misreport a healthy family as failing. Family totals
  (`totalSources`, `okSources`, `failedSources`, `sourcesNeedingAttention`) are unchanged —
  verified against the live report.
- **Adapter-gated.** Only multi-board provider adapters (`_MULTI_BOARD_PROVIDER_ADAPTERS`)
  fan out this way. A `static_source` row's `details` is one payload, not a board list;
  including it restated the parent's own failure as a child across all 287 such rows.
  Failure **buckets** do include children, so a dead board's bucket reaches
  `topFailureBuckets`.

### The systemic discovery fix reaches all 34 seeds

All 34 seeds carry an explicit `likelyProviders`, so item 3 affects every one. Distribution:
`static` 15, `greenhouse` 4, `greenhouse+static` 3, `smartrecruiters` 3, `lever` 2,
`personio` 2, `teamtailor` 2, `workable` 2, `ashby` 1.

Unconditionally appending the non-`nlPriority` default set would newly probe
lever/smartrecruiters/ashby/recruitee/pinpoint for the 15 `static`-pinned studios. With 859
pending approvals already queued, item 3 is **host-gated**: a provider the seed's own
`careersUrl` already points at is added, and nothing else is.

Measured on landing: **0 of 34 shipped seeds change.** They are all already
host-consistent, so the gate only does work for a seed that drifts later. A seed
consistent with its pinned vendor, and a seed with no `careersUrl`, both come back
unchanged; pinned providers keep their declared order and are never dropped.

Item 2 also turned up a **second dead Lever row** in a surface the plan had not
accounted for: `STATIC_DISCOVERY_CANDIDATES` in `src/source_discovery/config.py`
is a curated-candidate list separate from the studio catalog, and it carried its
own `Voodoo (Lever)` row that `stage_curated_seed_candidates` kept re-staging every
run even with the catalog fixed. Both are fixed.

## Verification record

| Step | Result |
|---|---|
| 1. Item 1 — `refresh_active_ashby_registry` vs an isolated copy of the live registry | **done.** 9 boards -> 17; 0 lost, 0 duplicate ids; Voodoo 120 postings; 278 across all boards. Sole removal Day[9]'s probes `empty` with org name resolving, thatgamecompany control parses 40. |
| 2. Item 1 — scoped `ashby_sources` fetch | **done.** Production `run_ashby_sources_source` over the real network with `thatgamecompany` as control: 120 Voodoo rows including the target `13968523-e0f2-4cdb-81a1-4ac338bd5e0a`, Paris/FR, Remote, Full Time. |
| 3. Items 2-3 — discovery derives Ashby for Voodoo | **done.** `likely_providers_for_seed` -> `['ashby']`; ashby reinforcement 0 -> 18, lever 18 -> 0; curated candidate stages as `ashby:board_url:https://jobs.ashbyhq.com/voodoo`; 0 of 34 shipped seeds change. |
| 4. Item 4 — child failures surface | **done.** 58 failing children on the live report, 12 of them the HTTP 404 dead boards. Family totals byte-identical. |
| 5. Full Python lane + `data/` hygiene | **done.** 5566 passed, 1 skipped. No `gameprog-*` / `gamesmap-*` / `*-discovery-audit.json` in `data/`. |
| 6. Umbrel pipeline run | **deferred** — the box runs v0.3.006; verifying there needs the release shipped first. |

## Two premises from this plan that measurement disproved

Recorded because both were acted on before being checked, and both would have
shipped a change with no benefit.

**"The static extractor returns 0 despite visible jobs."** Not true. The one
production row measured as `ok` + `0/0` (Mundfish) failed with
`time_budget_exceeded`, a timeout. Holonautic and Donkey Crew reported
`excluded` / `cache_within_freshness_window`, which per repo guardrails means *not
fetched* — no conclusion available. Fetching all three directly gave Holonautic
3 jobs, Donkey Crew 5 and Gamucatex 3 both before and after a fix to
`detect_js_shell`, and a real A/B of the old and new functions across twelve
boards showed **no measurable output change anywhere**. The `detect_js_shell`
false positives are real (Mundfish is `data-ssr="true"` yet flagged, and six
boards including Nintendo, Larian and Supercell are misclassified) but they sit
in a path that does not currently suppress output. The change was reverted.

**"8,273 rows carry a country name instead of a code."** The names were never
rejected: `sanitize_country_text("Poland")` already returned `('Poland', '')` and
`looks_like_country_token` was already `True`. The real defect was 10 contract
labels — Anguilla, Bermuda, European Union, Gibraltar, Greenland, Hong Kong, Isle
of Man, Macau, Montserrat, Puerto Rico — present only in `countryNameByCode`,
which the loader never read. All ten affected **zero** live rows. The bulk
name-to-code rewrite would have been a user-visible regression (`Poland` -> `PL`,
5 location tests failing) for no correctness gain, so it was dropped in favour of
`ba17bac7`.

Also corrected: `personio_sources` fetching 2 and keeping 0 is **not** a bug. A
live read-only probe returned one speculative title and one Remote-location QA
role, both correctly dropped.

## Added after the coverage check

`tools/coverage_audit.py` (`22fec319`) classifies GJI-vs-feed misses into
actionable buckets. It is the tool that should drive board registration, including
the four verified-live boards held back from this plan:

| Studio | Board | Live | TA roles |
|---|---|---|---|
| 2K Czech | greenhouse `2kczech` | 7 | 1 |
| Hangar 13 | greenhouse `hangar13` | 9 | 2 (same role as 2K Czech) |
| Companion Group | recruitee | 12 | 1 |
| Yggdrasil | smartrecruiters `YggdrasilSandbox` | 4 | 1 |

Voodoo is additionally fixed in unreleased 0.3.007 and needs no registration.

## Out of scope by decision

- `lever:account:voodoo` is **not** retired. It stays active and keeps 404ing harmlessly;
  item 4 makes it visible.
