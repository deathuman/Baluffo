# Remaining Work Implementation Plan

> - **Status:** Planned — every item below is scoped against measured evidence, none has been started
> - **Use this when:** picking up any of the five open items, or re-deciding whether one is still worth doing
> - **Canonical for:** the scope, sequencing, and stop conditions of the open work
> - **Not canonical for:** the completed 2026-09-26/27 registry repairs (see [`registry-hygiene-followup-plan.md`](registry-hygiene-followup-plan.md)) or the test-reduction rules (see [`test-reduction-triage.md`](test-reduction-triage.md))
> - **Then inspect:** [`../testing.md`](../testing.md), the candidate registry row, and the owning source doc
> - **Last updated:** 2026-09-27

Every scope figure below was measured, not estimated. Where a number came from a delegated
measurement rather than a local one, that is called out — the single largest lesson of the work this
follows is that a plausible measurement is not a verified one.

## Why this exists rather than why it is worth doing

The prior programme closed with a clean preflight, so nothing here is a regression. These are the
items that ran out of session, plus two the earlier scoping got wrong. In order of leverage:

| # | Item | Size | Risk | Verdict |
|---|---|---|---|---|
| 1 | 23 `root_alive_404_path` boards | 23 rows, needs a per-root scan | low | **do it** — real coverage repair |
| 2 | Duplicate board rows | 2 groups, ~4 rows | low | **do it** — small and precise |
| 3 | `tmp/` retention | 752 MB, 89 dirs | none | **do it** — largest disk win here |
| 4 | 2 retained stashes | 84 KB, unrecoverable | high if applied | **leave archived** |
| 5 | 2 undiagnosed flakes | 0 | none | **watch** — rule already documented |

---

## 1. Repoint the 23 moved boards

**What is known.** A 404/410 filter over the September sweep produced 28 candidates. A control-first
root-and-path probe (supercell and careers.sega.co.uk both 200 first) reduced that to 4 genuine
deaths — retired in `408ce3d9` — and **23 `root_alive_404_path`**, where the studio is up and the
registered board path moved or is UA-gated. Affected studios include Nintendo, EA, Square Enix,
Pocket Gems, Ludia, Amplitude Studios, Arte, Reply.com and WBD.

**What is not known.** Where each of the 23 moved *to*. That is the work.

**Method, per board.**
1. Fetch the board root. Locate the careers link in the returned HTML — a `href` matching
   `career|job|vacanc|position`, following only same-host links first.
2. If no careers link is in the root HTML, try the two conventional fallbacks
   (`<root>/careers`, `<root>/jobs`) and record which resolved.
3. Only if none resolve, record `no-board-found` and leave the row alone. Do **not** retire — the
   studio is demonstrably alive.
4. Re-fetch the candidate URL and confirm it is a listings page, not a marketing page. A 200 that
   renders no postings is not a repair.

**Stop conditions.** Abort the sweep if the control fails. Do not mutate any row whose replacement
URL was not itself fetched and confirmed. If a board has no findable replacement, that is a finding
to record, not a retirement.

**Verification.** Per repaired row: fetch the new URL, confirm postings, then repoint
`listing_url`/`pages` and re-run `tools/repo_health/source_registry_preflight.py`. Expect
`uncoveredCollisions` to stay 0 — if a repointed row collides with an existing row for the same host,
that is a duplicate (item 2), not a repair.

---

## 2. Collapse the duplicate board rows

**What is known.** Grouping active rows by *(host, path)* yields **2 groups / ~4 redundant rows**:

- `romerogames.com` — two registrations of one board:
  `https://romerogames.com/careers` and `https://www.romerogames.com/home/#careers`.
  Same studio, same board. This is the duplicate-URL class the collision guard should have caught.
- `jobs.ea.com` — three rows for one board on three paths
  (`/en_us/careers`, `/en_us/careers/home/?…`, `/en_us/careers/searchjobs/?…`).
  One is the board; the other two are search-filter URLs.

**Needs adjudication first, not action.** A third group — `careers.microsoft.com` /
`jobs.careers.microsoft.com` — surfaced under the same key but is *not* obviously a duplicate:
Halo Studios, Microsoft and Xbox Game Studios are different employers with different search URLs.
Judge it on employer identity, not on path shape.

**Method.** Confirm both sides resolve to the same postings (compare a posting count or a stable
title), then demote the redundant rows via `transition_registry_to_pending` with the
`duplicate_family_weaker_variant` reason so the loser records `duplicateOfSourceId`. Pick the
survivor by rank, not by URL aesthetics.

**Stop condition.** If the two URLs do not demonstrably serve the same postings, they are two boards
and both stay. This is the trap that produced the P3 mass-demotion near-miss; the per-row evidence
requirement is the whole point.

**Verification.** Active count falls by exactly the number demoted; no other row's bytes change;
`preflight.py` reports 0 uncovered collisions and does **not** report the affected board as emptied
by conflict demote (that would mean the survivor was not actually retained).

---

## 3. `tmp/` retention policy

**What is known.** `tmp/` is **752 MB across 89 scratch directories** plus ~100 loose logs, all
dated September 2026. It is ignored via `.gitignore:34`, so it never reaches git.

**Why it is in scope.** This is the largest disk win available and carries no code risk at all.
`tmp/head-check/` (66 MB) was already removed as a confirmed stale snapshot; the rest was left
because it was not what was asked for.

**Method.**
1. Classify each directory by age and name. Date-stamped work dirs (`*-20260906` … `*-20260916`) are
   one-shot run artifacts.
2. Preserve anything that is not obviously regenerable — in particular anything that is not
   reproducible by re-running the pipeline. If in doubt, keep it and say so.
3. Propose a cutoff (everything older than N days) and get it approved before deleting.
4. Add a `.gitignore` comment recording the convention so the directory does not silently regrow.

**Stop condition.** No deletion of a directory whose contents are not understood. `probe-zero-audit`
data referenced by item 1 may still be needed; move it under `_out/` if so rather than leaving it
in `tmp/`.

---

## 4. The two retained stashes — leave archived

**Do not apply either.** Both fail a clean *and* a 3-way apply against `main`, which means they are
mechanically unrecoverable: porting either would be a hand-merge against code refactored five times
since.

- `stash@{0}` (42 KB) — abandoned unmerged work, not superseded. `linkedCandidates`,
  `recommendedAction` and the function it removed are all absent from `main`, so the feature it
  targeted never shipped. It is a real product decision, not a recovery: if the "linked migration
  identity review with admin-only clear" feature is still wanted, re-implement it against current
  code. Do not resurrect the patch.
- `stash@{1}` (42 KB) — rewrites `.githooks/pre-commit`, `admin_bridge.py`, `registry_service.py`
  and `source_sync.py`. A five-month-stale hand-port of a pre-commit hook is how a gate starts
  failing in ways nobody traces back. Leave it.

**Action:** none beyond this note. If a stash is wanted later, read the patch and re-implement
deliberately. Record the decision here so the next session does not re-triage them.

---

## 5. The two undiagnosed flakes — watch only

`test_transient_get_error_retries_with_backoff` and
`test_bridge_profile_summary_records_external_sample_failure` each failed once in ~8 suite runs.
Both are `tmp_path` users in files never touched by this work; both pass 8/8 in isolation; three
subsequent full runs were green.

**Not proven.** The traceback was truncated before capture, so "concurrent pytest temp-root race"
is inference, not diagnosis. The rule that follows from the inference is already in
`docs/testing.md`: never start a second pytest process while a suite is in flight.

**Action if either recurs:** capture the **full** output — no `Select-Object -Last`, no truncation.
`git stash`/`git apply` on a 3-way basis is not needed; the failure is self-contained in the test.

---

## Sequencing

1. **Item 3** first. No dependencies, no code risk, largest disk win, and it may contain data item 1
   still needs — so do it before the registry work to avoid deleting a live input.
2. **Item 2** next. Smallest registry change, and it de-risks item 1 by establishing the
   repointing discipline.
3. **Item 1** last and in batches. It is the only item that touches live coverage, so it gets the
   most attention and the smallest batches.
4. **Items 4 and 5** need no scheduling beyond this note.

## Gates for every item

- Focused tests for the touched surface, then `npm run test:py:extended` and `npm run test:py:linux`.
- `npm run lint:repo-guardrails`, `npm run typecheck:py`,
  `python -m vulture --min-confidence=60 src whitelist.py` clean.
- Any registry mutation: backup, read-back verification, an asserted count delta, and a check that
  no unrelated row's bytes changed.
- `python tools/repo_health/source_registry_preflight.py` after any registry change, read for what
  it *says*, not just whether it is green.
- **Re-ratchet `loc_baseline.py --update` only after staging a change** — the ratchet compares the
  index against the baseline, so updating first records the pre-change count and the pre-commit gate
  fails on a stale baseline.
