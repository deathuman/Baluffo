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
| 2 | Duplicate board rows | 2 groups, ~4 rows | low | **DONE** `6822e3b8` — 1 collapsed, 3 kept |
| 3 | `tmp/` retention | 752 MB claimed | none | **DONE** `50c28c1f` — 139 MB, not 752 |
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

## 2. Collapse the duplicate board rows — DONE `6822e3b8`

**The scoping below was half wrong and is preserved so the error is not repeated.** It called the
`jobs.ea.com` rows "three rows for one board on three paths… one is the board; the other two are
search-filter URLs", implying two were redundant. Measured against the 2026-09-15 unified feed,
they are not:

| Row | Links | Also in unfiltered | Not in unfiltered |
|---|---:|---:|---:|
| `/en_US/careers` (Electronic Arts) | 149 | — | — |
| `?4538=8369` (Criterion) | 76 | 49 | **27** |
| `?4538=[8354]` (Glu Mobile) | 73 | 38 | **35** |

The extras are real postings — `jobdetail/lead-narrative-designer-tft/214170`,
`jobdetail/lead-product-manager/211550`. Workday's supervisory-organization filter returns detail
links the unfiltered board's pagination never reaches, so the three rows are **complementary, not
duplicate**, and all three canonicalize distinctly. Collapsing the filtered pair would have deleted
62 job links for nothing. **No action taken on EA.**

The genuine twin was `romerogames.com`. `canonicalize_careers_url` returns `romerogames.com/home`
for both active rows — the repo's own twin-detection canonicalizer, whose docstring states it drops
the fragment precisely to collapse URLs that "differ only by scheme/www/slash/fragment". Demoted
`…/romerogames.com/careers` to pending with the existing `duplicate_family_weaker_variant` reason
(33 rows already use it). It is the weaker of the pair: its id says `/careers` while its
listing_url says `/home`, and row identity **is** the url here. The third registration,
`/work-with-us`, is a distinct path with `jobsFound 0` and stays untouched.

**The better source of truth was already in the repo.** `data/defaults/
source-registry-known-url-collisions.json` records same-URL groups that are known and
intentional. It already listed `romerogames.com/home` — so the twins were a *tracked* condition,
not an unnoticed defect — and it also settles the two groups this plan proposed to adjudicate from
scratch: `careers.microsoft.com` is a "shared parent board: Halo Studios, Microsoft, Xbox Game
Studios", and `ea.com/careers` is a "shared parent board: EA Chillingo + EA Bucharest". Judging
either on URL path shape would have been wrong. **Read that baseline before adjudicating any
same-URL group.**

The guard then correctly refused the commit while the baseline still carried the resolved entry,
and pruning it in the same change took `stale baseline` back to 0.

---

## 3. `tmp/` retention — DONE `50c28c1f`

**The premise was wrong.** This item was scoped as "752 MB, the largest disk win available".
Measured, only **~139 MB was deletable**, because **65% of the volume (222 MB across 47
directories) was cited provenance** — 44 of those directories are referenced from
`docs/CHANGELOG.md`, plus `docs/plans/hold-tail-repair-plan.md`,
`docs/adapter-plugin-inventory.md` and six snapshots. A bulk purge would have deleted the evidence
trail of shipped work.

The real accumulation is elsewhere and was never in scope: **`_out/perf-runs` is 10.5 GB** of
September perf-measurement output with **zero tracked citations**. Left untouched pending a
separate decision — its 23 SQLite seed DBs (7.1 GB) have *differing* hashes, so they are per-run
end states, not redundant copies, and deleting them discards measurement results.

**What was done.** All 47 cited directories moved to `_out/evidence/<name>/` with all 95 citations
rewritten across 11 files; 40 unreferenced directories (118.2 MB) and 66 unreferenced log files
(20.5 MB) purged. Two larger keepers went to `_out/preserved-20260927/` rather than being deleted
— `dead-board-sweep-20260906` holds the 27-entry probe list **item 1 still needs**, and
`prefullpass-backup-20260906` is a 2026-09-06 pre-mutation registry backup, a snapshot of a past
state that no re-run reproduces.

**Method, for next time.** Decide by *reference*, not by name or age: a directory no tracked file
mentions is deletable, one a doc points at is evidence-of-record. Name patterns are a poor proxy —
`dayforce-wave` and `konami-s7-pass` have no date stamp but are cited, while 40 date-stamped dirs
are orphans.

**The rewrite must be per-artifact-name, never a prefix swap.** `/tmp/` also holds operational paths
the application itself resolves — `tmp/baluffo-ship` (52 refs), `tmp/desktop.lock` (25),
`tmp/pytest` (13), `tmp/packaged-desktop-smoke` (35). `tmp/` → `_out/` across the board would point
the desktop app at a data root that does not exist. All five are now named in `.gitignore` so a
future pass does not "fix" them.

**Write bytes, not text.** The first rewrite used `read_text`/`write_text`, which on Windows
translates `\n` → `\r\n` and silently converted 11 files; the pre-commit `ruff-format` gate blocked
the commit, correctly. `git checkout --` was *not* enough to recover, because the index already held
the damaged blob — both index and worktree had to be reset to HEAD. The working tree is LF despite
`core.autocrlf=true` (a `.gitattributes` rule wins), and git's "CRLF will be replaced by LF" warning
is about normalisation on commit, not a signal that the working tree is acceptable.

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
