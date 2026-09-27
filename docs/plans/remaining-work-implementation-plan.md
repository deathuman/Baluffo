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
| 1 | 23 `root_alive_404_path` boards | 23 rows, needs a per-root scan | **higher than stated** | **BLOCKED** — 0 repointed, 22 need the rendered path |
| 2 | Duplicate board rows | 2 groups, ~4 rows | low | **DONE** `6822e3b8` — 1 collapsed, 3 kept |
| 3 | `tmp/` retention | 752 MB claimed | none | **DONE** `50c28c1f` — 139 MB, not 752 |
| 4 | 15 stashes | all resolved | — | **CLOSED** — 15 dropped, 0 remaining |
| 5 | 2 undiagnosed flakes | 0 | none | **watch** — rule already documented |

---

## 1. Repoint the 23 moved boards — BLOCKED, and the method in this plan is unsafe

**Status: no repoint was made. Do not run the method below.** The classification is sound; the
*candidate discovery* is not, and it produces confidently wrong answers.

### What is established

The control-first root-and-path probe reproduces exactly: **23 `root_alive_404_path`**, 4
`root_dead`, 1 `junk_row`. And all **4 `root_dead` rows are the boards already retired in
`408ce3d9`** — so that retirement was complete, and this sweep contributes no new deaths.

### Why the method fails

The control gate did its job by failing quietly. Both known-good controls returned
**`joblinks=0`**:

```
[OK ] 200  77086b joblinks=0  https://www.supercell.com/en/careers
[OK ] 200  60188b joblinks=0  https://careers.sega.co.uk/vacancies
```

A detector that sees zero job links on Supercell and Sega cannot be trusted to say a candidate
"is not a board". Both verdicts are unreliable, so `confirmed` is not evidence and `not_a_board`
is not evidence either. The gates above only asserted `status == 200 and len > 1000`, which is why
the control printed `OK` while its own job-link signal was dead.

Of 10 proposals, **4 would cause real damage** — all verified, not suspected:

| Row | Proposal | Why it is not a repair |
|---|---|---|
| EA ×3 | all three → `jobs.ea.com/en_US/careers/Home` | **1 distinct target for 3 rows.** Collapses rows the feed measured as carrying 27 and 35 unique job detail pages, losing 62 links, and re-creates the twin condition just cleaned up in `6822e3b8` |
| Impact Reality | → `flat2vrstudios.com/careers` | **Different registrable domain.** The Neon Play → `iscoolentertainment.com` precedent says refuse: a sister brand is not a rename, and this ships another company's jobs under the studio's name |
| WBD | → `careers.wbd.com/global/en/home` | Registered URL is a **single job detail page** (`/job/r000087820/lead-environment-artist`, `jobsFound=1`). Repointing to the board home silently turns one job into the whole board |
| Square Enix | → `square-enix-games.com/en_EU/careers` | A **locale switch** (`/en_us/` → `/en_eu/`), not a board move. Changes which jobs are visible — a public job-data contract concern |

The remaining 6 proposals are unverified, and **13 of the 23 boards yielded no candidate at all**
(their root pages do not link a careers board in raw HTML). Per the rule below those 13 are a
legitimate recorded outcome, not a failure: the studio is demonstrably alive, so the row stays.

### What is actually required

**Two findings from a second attempt (2026-09-28). No repoint was made; the registry is unchanged.**

**1. Use the repo's detector, not a hand-rolled one.** `static_probe_evidence`
(`src/source_discovery/probe.py:400`) returns **42 postings on Supercell and 23 on Sega**, where
the hand-rolled regex used above returned **0 on both**. This is the R3/R4 detector already covered
by the repo's tests. Stop re-implementing it.

**2. It is a positive-only signal, and that asymmetry decides the whole item.** Scored against 18
active rows with `jobsFound >= 10` — boards known to actually collect jobs — the distribution is:

```
0, 0, 0, 0, 0, 0, 0, 0, 7, 7, 8, 9, 13, 18, 31, 50, 88
```

**Eight of seventeen genuinely-yielding boards score zero.** They are JS-rendered and reach their
postings through the browser-fallback lane, not raw HTML. So a high score is real evidence and a
zero means *nothing at all*. **Never conclude "no board found" from a zero.**

Applied honestly, that yields: of the 23 boards, **one** candidate (Nintendo, 51 postings) cleared
the observed non-zero floor of 7. The other 22 scored 0–3 — the noise band — and are *inconclusive*,
not absent. They need the pipeline's own rendered path:

```
python src/jobs_fetcher.py --only-sources static_source::<id> \
  --ignore-circuit-breaker --force-refresh-all --output-dir <dir>
```

`--output-dir` is mandatory; omitting it writes stub state into live `data/`.

### The near-miss, and the check that would have caught it

The one confirmed repoint was applied and then reverted. `careers.nintendo.com/job-openings/` had a
stale id and had drifted to the bare host (`jobsFound=3`, nav links), and `/jobs/` was a real board
with 51 postings — but **a healthy row for that board was already registered** at
`static:listing_url:https://careers.nintendo.com/jobs`, no trailing slash, **`jobsFound=59`**,
stamped `discovery_auto_approve`. Canonicalization strips the trailing slash, so the repoint made a
twin. The preflight reported it immediately as `uncovered duplicate-URL groups (live): 1` — the
exact regression `6822e3b8` removed.

The guard that should have stopped it compared **`NEW_ID` for exact equality** and never compared
**canonical forms**. Every id-collision check in this programme has made that exact-vs-canonical
slip in some form. **Before repointing a row onto a URL, check whether any active row already
canonicalizes to it** — and note that the row being repaired may be a redundant registration of a
board another row already covers, in which case the disposition is a duplicate adjudication, not
a repoint.

A related control-design error: the first version of the control list included two URLs *from the
candidate set* (`careers.nintendo.com/job-openings/`, `badrobotgames.com/job-openings`), both of
which are 404 by definition. Calling those controls tested nothing and made a working detector look
blind. **Controls must be verified healthy independently of the set under test.**

### The rule that must hold

**A studio that is provably alive is never retired.** If no replacement board can be found, that
is a finding to record, not a retirement. All 23 rows remain active and untouched.

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

## 4. The stashes — CLOSED, all 15 resolved

**All stashes are now resolved. The list is empty.** Thirteen were dropped earlier in the programme;
these two were adjudicated on 2026-09-28 and both dropped, SHA-verified immediately before each
drop.

**`stash@{1}` (`424348ba`, "codex-temp-before-branch-switch") — DROPPED.** It bundled two unrelated
things under a branch-switch temp label: a `.githooks/pre-commit` rewrite adding `set -eu` plus
`npm run lint:precommit:changed`, and ~583 lines of registry merge logic in `source_sync.py` /
`post_routes.py` (`_canonicalize_snapshot_rows`, `_row_transition_score`, `_row_bucket_rank`,
`_row_merge_key`, `_choose_more_recent_row`). The `set -eu` makes any non-zero exit anywhere in the
chain abort the commit, and `_choose_more_recent_row` reads as last-write-wins between two registry
rows — a semantics decision from the era that produced the lean-registry corruption bug. Confirmed
with the operator: that merge behaviour was **not** wanted.

**`stash@{0}` (`40e3e6b4`) — DROPPED as fully superseded. Do not re-implement.**

An earlier version of this section claimed the feature "never shipped" because `linkedCandidates`
and `recommendedAction` were "absent from `main`". **That was wrong**, and wrong in the expensive
direction: it would have had someone build a feature that already exists. Checked against the tree:

| Stash | `main` today |
|---|---|
| `linkedCandidates` in the source-policy response, inline in `get_routes.py` | `src/bridge/source_policy_link_backfill.py:81`, a dedicated 381-line module |
| +123 lines on `source-policy-review.js` | **723 lines** |
| `clear` allowed only for admin-owned links | `adminOwnedLink` gate at `source-policy-review.js:181-189`, plus an `apply_migration_identity_link` action the stash never had |
| 5 tests | **All 5 present**, plus 6 further test files; 6 + 5 cases in the two matching files |

All five of the stash's test intents resolve to live tests today, including the two that carry its
actual policy: `..._marks_non_admin_owned_links_not_clearable` and `...action visibility allows only
admin-owned clear`. The route logic was also extracted out of `get_routes.py` into
`get_source_policy.py` / `source_policy_link_backfill.py`, which is why the original symbols no
longer appear where the stash put them — the feature moved, it did not vanish. Confirmed with the
operator that today's build covers the intent, including the apply side the stash lacked.

**The lesson is the same one as the EA rows and the Nintendo repoint:** a claim of absence was
inherited from an earlier summary and never checked against the tree. **Absence of a symbol is not
absence of a feature** — check the behaviour, not the identifier.

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
