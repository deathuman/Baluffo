> - **Class:** shipped-defect
> - **Trigger:** Voodoo's careers ATS moved from Lever to Ashby; `lever:account:voodoo` now 404s, so job `13968523-e0f2-4cdb-81a1-4ac338bd5e0a` (published 2026-10-01) is absent from the feed
> - **Verified against:** be26a0f0
> - **Status:** active — 0 of 5 commits landed

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

## Open items

| # | Item | Where it is handled | Pinned by | Status |
|---|---|---|---|---|
| 1 | Register Voodoo's Ashby board | `src/ashby_registry_refresh.py` `CURATED_ASHBY_ROWS` | to add | open |
| 2 | Repoint Voodoo discovery seed | `src/discovery_seed_catalog.json:32`, `src/source_discovery/config.py:425` | to add | open |
| 3 | Treat explicit `likelyProviders` as a prior, not a ceiling | `src/source_discovery/provider_patterns.py:37` `likely_providers_for_seed` | to add | open |
| 4 | Surface provider-family child 404s in source health | `src/jobs/common/contracts_source_health.py:150` `derive_source_health` | to add | open |
| 5 | Automatic dead provider-board retirement | no implementation exists today | — | deferred, see below |

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

`derive_source_health` walks only top-level rows. Family children sit in `row["details"]`
and are never visited, so their `failureBucket` is never counted and they never reach
`sourcesNeedingAttention`. Live Admin reports `status: "healthy"`, `alerts: []`.

Currently hidden: 12 dead ATS boards — Lever x3 (Voodoo, Big Time Studios, Rolocule Games),
Greenhouse x5 (ArenaNet, Guerrilla Games, Magic Leap, Mythical Games, Trailer Park Group),
Workable (Soulbound), Recruitee (Scorewarrior), Pinpoint (CCP Games), Teamtailor
(Lionbridge Games).

### The systemic discovery fix reaches all 34 seeds

All 34 seeds carry an explicit `likelyProviders`, so item 3 affects every one. Distribution:
`static` 15, `greenhouse` 4, `greenhouse+static` 3, `smartrecruiters` 3, `lever` 2,
`personio` 2, `teamtailor` 2, `workable` 2, `ashby` 1.

Unconditionally appending the non-`nlPriority` default set would newly probe
lever/smartrecruiters/ashby/recruitee/pinpoint for the 15 `static`-pinned studios. With 859
pending approvals already queued, item 3 is **evidence-gated**: reinforce only when the
pinned providers are failing.

## Verification order

1. Item 1 — run `refresh_active_ashby_registry` against an isolated `ACTIVE_PATH`; assert
   the count delta is exactly +1 and the new row reads back.
2. Item 1 — scoped fetch:
   `python src/jobs_fetcher.py --only-sources ashby_sources --ignore-circuit-breaker --force-refresh-all --output-dir <temp>`.
   `--output-dir` is mandatory; omitting it writes stub state into live `data/`.
3. Items 2-3 — run discovery scoped to Voodoo; confirm the Ashby candidate is generated.
4. Item 4 — confirm the 12 child 404s reach `topFailureBuckets` / `sourcesNeedingAttention`.
5. `npm run test:py`, then `git status` to confirm no `gameprog-*` / `gamesmap-*` /
   `*-discovery-audit.json` artifacts leaked into `data/`.
6. Ship, then run `/tasks/run-discovery` + `/tasks/run-jobs-pipeline` on the Umbrel host.

## Out of scope by decision

- `lever:account:voodoo` is **not** retired. It stays active and keeps 404ing harmlessly;
  item 4 makes it visible.
