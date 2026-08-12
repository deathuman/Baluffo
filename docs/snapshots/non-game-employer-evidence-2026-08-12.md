# Non-Game Employer Evidence — Google Sheets rows (2026-08-12)

> - **Status:** Evidence report for operator decision. **No drops applied.** Per repo guardrails, broad employer/domain drops need explicit product approval.
> - **Basis:** `data/jobs-unified.json` from the 2026-07-17 full run (40,586 rows).
> - **Track:** B of the jobs entry validation plan (snapshot: `docs/snapshots/jobs-entry-validation-audit-2026-08-12.md`).
> - **Then inspect:** `docs/source-policy-runbook.md`, `docs/DATA_CONTRACT.md`, `src/jobs/common/exact_category_titles.py`, `src/jobs/game_detection.py`.

## Summary

**1,364 rows (3.4% of feed)** come from 19 clearly non-game employers, all via the default `google_sheets` source. These are real ATS-hosted jobs (smartrecruiters/greenhouse/lever/jobvite) but are not game-industry roles. They ship because the strict-sector gate (`BALUFFO_STRICT_GAME_ONLY`) is opt-in and the default feed intentionally includes Tech-sector sheet rows.

## Per-employer breakdown

| Employer | Rows | Primary host | Sample titles |
|---|---|---|---|
| Dominos | 255 | jobs.smartrecruiters.com | Assistant Manager (20), Customer Service Rep, General Manager, Team Member |
| Boschgroup | 189 | jobs.smartrecruiters.com | Senior Site Reliability Engineer (Data Platform) |
| Nationalvision1 | 169 | jobs.smartrecruiters.com | UT Lab Bench - Overnight - Lab Tech 1 (optometry labs) |
| Accorhotel | 161 | jobs.smartrecruiters.com | Breakfast Employee, Waiter/Waitress apprenticeships |
| Turnertownsend | 112 | jobs.smartrecruiters.com | Senior Cost Manager / Quantity Surveyor (construction consultancy) |
| Jsheldllc | 90 | jobs.smartrecruiters.com | Traffic Engineer (PE License) (engineering consultancy) |
| Abercrombieandfitchco | 79 | jobs.smartrecruiters.com | Assistant Manager, Hollister Co. |
| Varonis | 55 | jobs.jobvite.com | Technical Account Manager (cybersecurity) |
| Fliff | 39 | jobs.lever.co | Senior Python Engineer (Contract) — fantasy sports app |
| Pilotcompany | 33 | jobs.smartrecruiters.com | (Pilot Flying J truck stops) |
| Oportun | 29 | job-boards.greenhouse.io | Sr. Manager Capital Markets & Treasury (fintech) |
| Xplor | 27 | jobs.smartrecruiters.com | (SaaS field-service software) |
| Publicstorage | 25 | jobs.smartrecruiters.com | Customer Service - Self Storage Manager |
| Deangelocontractingservices | 25 | jobs.smartrecruiters.com | Snow Supervisor (On Call Winter Work) (industrial services) |
| Relaischateaux | 21 | jobs.smartrecruiters.com | (luxury hotels) |
| AjnaLens | 20 | bebee.com | UI/UX Design Intern (AR hardware, mirror site) |
| Endeavourgroupcareers | 14 | jobs.smartrecruiters.com | (Australian retail/hospitality group) |
| Securitas | 13 | jobs.smartrecruiters.com | Security Guard |
| Barriere | 8 | jobs.smartrecruiters.com | Waiter/Waitress apprenticeship (casino group) |

Host totals: smartrecruiters 1,221, jobvite 55, lever 39, greenhouse 29, bebee 20.

## Why this happens

- Default `google_sheets` is a community spreadsheet containing game + broader tech rows; `companyType` for sheet rows splits Tech 21,319 / Game 10,502 (of 31,821).
- The sector gate (`BALUFFO_STRICT_GAME_ONLY=1` → `sector_gate_filtered` loss reason) exists but is opt-in.
- These employers have no plausible game subsidiary evidence at the row level (title + company + host all non-game).

## Options for decision

1. **Keep shipping (status quo).** No code change; rows remain. Trade-off: non-game jobs visible on a game-focused board.
2. **Strict gate by default (product policy change).** Enable/strengthen sector gating so `companyType=Tech` sheet rows drop unless game evidence exists. Largest behavioral change; affects ~21k Tech sheet rows, not just these 19 employers. Needs its own impact analysis.
3. **Employer evidence blocklist (targeted).** Extend the P2.0 employer-frozenset machinery (`src/jobs/common/exact_category_titles.py` / `game_detection.py`) with these 19 employer keys, dropping ~1,364 rows via the existing sector-gate loss path. Targeted, reversible, evidence-backed.
4. **Source-policy demotion.** Remove or demote the offending sheet rows upstream per `docs/source-policy-runbook.md` (migration-link validation, not a code drop).

## Recommended next step

Decision required. If approved, **option 3** is the smallest safe change consistent with the existing frozenset precedent (P2.0 extended 59→99 terms) and keeps the drop evidence-tracked. It must be validated with focused tests + a bounded live run before a full feed refresh.

## Caveats

- The 1,364 count is from July artifacts; the live 0.2.130 feed (~48k rows) may differ.
- Fliff (fantasy sports) and Oportun/Xplor (fintech) are not "game" but are tech-adjacent; product call, not parser call.
- No `Kforce` rows exist in the current artifact (the audit's 255-bundle Kforce row was from a different company-key case; staffing-agency rows remain a separate dedup-evidence topic).
