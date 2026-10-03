> - **Class:** coverage-gap
> - **Trigger:** catalogue sweep shows Baluffo carries 31.7% of the index's openings; 10,465 openings are absent and most are on boards it could read
> - **Verified against:** 79ef50b9
> - **Status:** phase 0 landed; phase 1 next; phase 2 diagnosis not started

# Closing the catalogue coverage gap

## Why this plan exists

The catalogue sweep says Baluffo holds 4,867 of the index's 15,332 openings
(**31.7%**). The premise for the plan is that no opening should be left behind,
because a job board's whole purpose is the openings it carries. Three earlier
estimates in this effort were wrong before measurement, so every number below is
quoted with how it was obtained.

## Honest baseline

Matched on **posting URL** first, which is the only definitive signal: both boards
publish the same URL for the same requisition.

| | Count |
|---|---|
| GJI openings considered | 15,332 |
| Matched | 4,867 (31.7%) |
| — by URL, definitive | 3,368 |
| — by title only (over-claims) | 1,499 |
| **Missing** | **10,465** |

Missing, decomposed against the live registry:

| Cause | Count |
|---|---|
| Board not registered, studio not in registry | 2,080 |
| Board not registered, studio has other boards | 2,102 |
| Board not registered, no studio relationship | 748 |
| Board registered but the opening is not collected | 4,589 |

**4,930 of 10,465 (47%) are unreachable only because the board was never
registered.** That is mechanical work, not a research problem.

### Provenance of what Baluffo *does* carry

Recomputed on the live feed, not the stale local snapshot:

| Source | Share |
|---|---|
| community Google Sheet | 81.9% |
| static career page (scraped) | 9.4% |
| direct ATS API | 5.7% |
| other aggregators | 2.9% |

An earlier figure of "0.3% direct ATS" was wrong twice over: it came from a
2026-09-17 local generation *and* miscounted. The structural point stands and is
stronger than first stated — the feed leans on one community spreadsheet.

## Phase 0 — make the measurement trustworthy (done, `79ef50b9`)

- URL-first matching, keeping the query string. Stripping it collapses up to 218
  distinct openings onto one base URL, because Ashby's job id is `?ashby_jid=`.
- Match basis reported separately so proven coverage is never confused with
  similar-looking coverage.
- Two looser matchers were tried and **rejected on evidence**: token-set equality
  recovered 139 rows and erased the qualifier that distinguishes two openings
  (`(West)` vs `(East)`); fuzzy ratio wanted to merge `Staff Platform Engineer`
  with `Data Platform Engineer`. Both are pinned by tests so they do not return.
- **Game-relevance scoping was measured and deliberately not used.** 62% of misses
  lack game evidence on title+company alone, but that detector is vocabulary-bound:
  `Senior Scene Editor (NARAKA: BLADEPOINT)` is plainly a game role and scores
  false. Pre-filtering boards by it would drop real openings, so the pipeline's own
  per-role gate decides.

Still open in phase 0: `registered_no_role` is inflated because a registered but
dead row masks a live unregistered board (2K Czech), and the registry persists no
URL fields at all, so board identity has to be recovered by parsing `id`.

## Phase 1 — register the missing boards

Target: the 4,930 openings behind unregistered boards.

1. **Board candidate extraction.** Resolve host + tenant/slug per missed opening.
   The top hosts are adapters Baluffo already supports: `jobs.smartrecruiters.com`
   (272), `apply.workable.com` (258), `job-boards.greenhouse.io` (209),
   `jobs.ashbyhq.com` (173), `myworkdayjobs.com` (296 across tenants),
   `jobs.lever.co` (153). The tail runs to 30 vendors including `feishu`, `hrmos`,
   `oracle-hcm`, `bamboohr`, `huntflow`, `csod`.
2. **Probe before proposing.** Reuse `tools/coverage_probe.py`, including its
   known-good control and retry discipline, so no board is registered on a
   single-endpoint guess.
3. **Emit registry-ready rows** for boards that verifiably serve openings, with
   no volume threshold: a single live opening justifies the row.
4. **Apply under the repo's mutation guardrails** — match rows by host rather than
   studio label, print the plan, assert the size, require an explicit apply flag,
   back up to `_out/`, read back after writing.

Sequencing note: `workday` alone is 296 openings and `feishu` 148, so per-vendor
adapter support is the rate limiter, not the row count.

## Phase 2 — fix "registered but not collecting" (4,589)

Not started. This needs diagnosis before code; each candidate cause below must be
measured, not assumed:

- quality gates dropping legitimate openings — the same weakness that lets
  Gamucatex's nav text through may also be dropping real roles
- pagination caps (SmartRecruiters `limit=100`, Lever, Greenhouse)
- per-source detail-fetch budgets — a Bandai note records "38 sibling detail
  verifications burning the source budget"
- freshness and cadence skips
- boards registered against a stale tenant slug after a rebrand

## Phase 3 — the spreadsheet dependency

81.9% of the feed is one community Google Sheet. That is simultaneously the ceiling
on coverage and a single point of failure. Separate plan; it should not be mixed
into phases 1-2, because it changes the shape of the system rather than filling it.

## Carried over from the Voodoo/Ashby plan

Four boards were queued for registration from that audit. Three are now registered
(`5b7ccc45`): `2kczech`, `hangar13`, `YggdrasilSandbox`. **Companion Group turned
out to be already registered and active** — it was missing from the plan only
because that plan was built from the stale 2026-09-17 snapshot. Its remaining gap
is collection, not registration, so it belongs to phase 2.

## Verification for every phase

- Provider adapters exercised against a registered control board whose expected
  count is known, never a raw endpoint alone.
- `npm run test:py` plus `npm run lint:repo-guardrails` before each commit.
- `git status` on `data/` to confirm no discovery-audit artefacts leaked.
- Coverage re-measured after each phase, with URL-matched and title-only counts
  reported separately.
