# DG Role Gate — Evidence Snapshot — 2026-10-06

> - **Status:** Shipped with the role gate (`coverage_audit` / `coverage_boards` /
>   `coverage_verify`). Read-only measurement; no registry mutation. The parse-path
>   asymmetry at the end is **measured, not fixed** — it is a product decision.
> - **Basis:** the saved Games Jobs Index snapshot (`_out/coverage/step1-dg-2026-10-06/gji-jobs.json`,
>   `run_at 2026-10-06`, 15,394 records, 934 studios), the pre-registration registry
>   snapshot (`registry-live.json`), the on-disk feeds (generation 2026-09-17), and a
>   live re-fetch of the greenhouse and ashby candidates on 2026-10-06 ~16:50 UTC.
> - **Canonical for:** the role-gate replay numbers, the per-board game-role yields, and
>   the parse-path asymmetry measurement below. Not canonical for fix design (the plan
>   owns that) or for the boards' current live counts (they move; re-fetch to re-measure).
> - **Then inspect:** `docs/plans/catalogue-coverage-gap-plan.md` (§ DG), `tools/coverage_audit.py`,
>   `tools/coverage_boards.py`, `tools/coverage_verify.py`, `src/jobs/game_detection.py`
>   (`looks_like_game_job`), `src/jobs/adapters/parsers/json_payloads.py`,
>   `src/jobs/adapters/parsers/provider_html.py`.

## What the gate is

A raw miss count is not recoverable coverage. The gate intersects "what Baluffo lacks"
with "would Baluffo keep these roles" where the numbers are produced, using the
production row filter (`looks_like_game_job`) as the predicate on three bases:

| Tool | Basis | What it reports |
|---|---|---|
| `coverage_audit` | GJI record title + company | `gameRole` per miss; `gjiMissingGameRoles` / `gjiMissingNonGameRoles` |
| `coverage_boards` | per-board grouping of misses | `gameRoleCount` / `nonGameRoleCount`; `openingsRecoverableGameRoles` / `openingsRecoverableNonGameRoles` |
| `coverage_verify` | the board's own fetched rows | `gameRoles` beside `rows`, per board |

Two properties are load-bearing. First, the audit's tag is **tri-state**: a report
written before the gate carries no tag, and an untagged miss counts in the total and in
neither split — the raw count stays the authority, the split is additive. Second, GJI
publishes no tags, and the production filter also consults tags, so the audit's game-role
count is a **floor** (everything it calls a game role is a role the pipeline keeps; the
reverse does not hold). The verify basis has no such floor: it reads the titles the board
actually serves.

## Replay against the saved snapshot

Chain: `coverage_audit` → `coverage_boards` → `coverage_verify`, EU region, the
pre-registration registry snapshot. Artifacts: `_out/coverage/step2-role-gate-eu/`.

| Measurement | Result |
|---|---|
| GJI considered / matched / missing | 2,265 / 577 / 1,688 |
| Misses, game-role split | **728 game / 960 non-game** |
| `unregistered_board` bucket | **94 openings** (reproduces the morning's 94 exactly) |
| Unregistered bucket, title basis | **20 of 94 (21%) game roles** |
| New candidates (stale feed) | 72 boards, 367 openings, **115 game roles (31%)** |
| Live re-fetch, greenhouse + ashby candidates | **488 listings, 132 game roles (27%)** |

The unregistered bucket's zero-yield boards, verbatim from the replay: Corsair
(14 openings, 0 game — peripherals), Kambi (5, 0 — its betting business: Compliance
Manager, Head of Tax), i3D.net (5, 0 — infrastructure: Data Center Lead, Lead Security
Analyst), GG Gute Gesellschaft (7, 0). The game-role-heavy boards: Volka Games
(10 openings, 10 game), 2K Madrid (9, 1), 2K Czech (6, 3).

The matched count (577 vs the morning's 1,150) is **feed staleness**: the on-disk feeds
are generation 2026-09-17 while the morning's run used a fresher feed. The unregistered
bucket — the one the gate sizes — is stable across both generations because those studios
are absent from every feed generation. This is the exact hazard the plan's correction on
record warns about (`data/` artifacts are not current).

## Per-board game-role yields: morning vs live re-fetch

The morning's measurement (12:54 UTC) and the live re-fetch (16:50 UTC) of the same
boards. Boards where the count is identical are stable; where it moved, the board moved —
these are live, self-refreshing listings, not a predicate change:

| Board | Morning (rows / game) | Live (rows / game) |
|---|---:|---:|
| greenhouse `2k` | 125 / 41 | 125 / 41 |
| ashby `hyperhug` | 13 / 7 | 13 / 7 |
| ashby `voodoo` | 122 / 71 | 121 / 55 |
| ashby `playson` | 20 / 8 | 20 / 6 |
| greenhouse `sportygroup` | 35 / — | 35 / 1 |
| greenhouse `applovin` | 43 / — | 43 / 0 |
| greenhouse `2kmadrid` | 10 / — | 10 / 1 |
| ashby `reaktor` | 22 / — | 22 / 1 |

Two independent bases — GJI titles on a stale feed, and fetched listing titles live —
both land in the high-20s to low-30s percent. That convergence is the finding: **the raw
miss count overstates recoverable coverage by roughly 3x, and the gate says so before
anyone sizes a target off it.**

Reproduction:

```bash
python tools/coverage_audit.py --feed data/jobs-unified-light.json.gz \
  --gji _out/coverage/step1-dg-2026-10-06/gji-jobs.json \
  --registry _out/coverage/step1-dg-2026-10-06/registry-live.json \
  --region EU --out _out/coverage/step2-role-gate-eu
python tools/coverage_boards.py --report _out/coverage/step2-role-gate-eu/report.json \
  --registry _out/coverage/step1-dg-2026-10-06/registry-live.json \
  --vendor greenhouse --out _out/coverage/step2-role-gate-eu/boards-greenhouse.json
python tools/coverage_verify.py --boards _out/coverage/step2-role-gate-eu/boards-greenhouse.json \
  --adapter greenhouse --out _out/coverage/step2-role-gate-eu/verified-greenhouse.json
# (repeat the last two with --vendor ashby / --adapter ashby)
```

## The parse-path asymmetry the gate exposed

`coverage_verify` reports `rows` and `gameRoles` side by side, and on the greenhouse and
ashby boards the two numbers differ by design — because **the row filter is not applied on
every parse path**. `looks_like_game_job` runs on:

- the JSON-feed providers: lever, smartrecruiters, recruitee (`parsers/json_payloads.py`),
  personio (`parsers/personio.py`), remotive and the generic JSON payload (`common/parsing.py`);
- the social parser (`social_parser/signals.py`);
- the static **detail-page** parse (`html_parsers.py`).

It does **not** run on:

- `parse_greenhouse_jobs_payload` (filters only general-application titles);
- the HTML-board providers — ashby, breezy, jazzhr (`parsers/provider_html.py`);
- the community Google Sheets path (`adapters/community/` filters only its remote-ok and
  remotive payloads);
- the static listing-extraction lanes (`static_listing_rows.py`).

Measured on the public feed (`jobs-unified-light.json`, 2026-09-17 generation, 40,247 rows):

| Host family | Rows in public feed | Pass the row filter |
|---|---:|---:|
| greenhouse-hosted | 4,734 | 341 |
| ashby-hosted | 1,212 | 74 |

The pollution is overwhelmingly **sheet-sourced**: 4,664 of the 4,734 greenhouse rows and
1,190 of the 1,212 ashby rows come from community Google Sheets, which aggregate
non-game employers' boards (Zscaler, JetBrains, Adyen, Chainguard — "Staff Site
Reliability Engineer", "VP, Self-Serve", "Trading Recruiter"). Only 22 ashby-hosted rows
come from provider/static rows, 21 of them non-game — all from the `inworld.ai/careers`
static row ("Product Marketing Manager - USA", "Lead Technical Recruiter - USA").

**Consequence for v0.3.011.** The 27 boards registered in that release sit on exactly the
unfiltered paths (14 greenhouse, 13 ashby), so the next fetch delivers ~488 rows of which
~132 are game roles; the other ~356 are business roles at game studios (Kambi's Compliance
Manager, voodoo's Account Executive). Whether the public feed should carry those is a
product decision with a ~4,700-row blast radius (the sheet path dominates), and the sheet
half is as much a *source-selection* question as a parser one — which community sheets are
ingested, and what they are allowed to aggregate. Both numbers are now measured per board by
the gate, so the cost of either answer is visible before it is chosen.
