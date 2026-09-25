# Registry Hygiene Follow-Up Plan

> - **Status:** Code work complete (P0, P1, P5, P6); data reconciliation (P2, P3, P4) delivered as an adjudication worksheet, awaiting human dispositions. Continuity handoff recorded in Basic Memory (`baluffo/registry-hygiene-program-closeout-2026-09-25`).
> - **Use this when:** recording or applying a registry repair decision, reconciling live-registry drift from the committed seed, or extending the advisory/guard surfaces
> - **Canonical for:** the human-approval gate's boundaries, the live-vs-seed authority question, and the ordered follow-up work
> - **Not canonical for:** the advisory report shape (see `docs/DATA_CONTRACT.md`) or registry transition mechanics (see `src/source_registry_state.py`)
> - **Last updated:** 2026-09-25

## What landed

| Commit | Surface |
|---|---|
| `2e8fc780` | Advisory `registryHygieneAudit` report (asset pages, duplicate candidates, host drift, unreachable). |
| `180c3036` | Two hard commit-time guards: duplicate registry ids, malformed page refs. |
| `9c9e5cd1` | `repair_candidate` — dead-domain signal under a repeated-evidence rule. |
| `eec27403` | `data/registry-repair-review.json` — the human-approval gate. |
| `12dbe58a` | `POST /registry/repair-review-action` — the gate's writer (P0+P1), plus the two `typecheck:py` fixes (P6). |
| `fec4e443` | `tools/repo_health/source_registry_preflight.py` — the live-registry surface (P5). |
| `6f564a6b` | `--worksheet` — the per-row adjudication inventory. |

## The authority question: answered

**The live registry is authoritative; the committed seed is stale.** This was the fork gating P3/P4
and it is now settled by evidence rather than assumption:

- The changelog records the invariant directly. Sanctioned mutations perform a **dual-path seed
  swap** (`save_registry_state_atomic`), e.g. "runtime 2,167 → 2,168, seed 1,892 → 1,893" and "active
  seed 1,890 → 1,882 with the High-5 promotion added". The seed is meant to *track* live, not to lag
  it.
- 426 of the 517 live-only rows are `static:` — the class the seed is dominated by and is supposed to
  carry.
- 21 of the 31 uncovered duplicate groups are **partly seeded**: one twin in the seed, its twin live
  only. That is the signature of a row added live without the seed swap, not of a design where the
  seed holds a subset.
- Zero duplicate groups exist only in the seed.

So drift is a real defect and the seed is behind. What that does *not* settle is how to close it, and
that remains a policy decision — see P2/P3/P4 below.

## Delivered: the adjudication inventory

`python tools/repo_health/source_registry_preflight.py --worksheet <path>` writes 717 individually
adjudicable items on the current tree:

| Kind | Count | Why it is a human decision |
|---|---|---|
| `live_only_row` | 517 | Should the seed adopt it (a fresh install lacks it), or is it live-only pollution? |
| `seed_only_row` | 165 | Prune (a seed restore would resurrect a retired source) or restore to live? |
| `uncovered_duplicate_url` | 31 | Baseline the twin or retire one? Most are `www`/apex pairs of one board. |
| `stale_baseline_entry` | 3 | Prune the entry or restore the twin? |
| `definitionless_static_row` | 1 | The Miniclip row (P2). |

Every item carries its evidence, suggested dispositions, and an explicit
`disposition: "undecided"`. The tool does not decide any of them.

## Also fixed en route

`test_batch_scoped_recovery_cache_preserves_fixture_outcomes` failed intermittently in the
source-discovery lane — on `HEAD` as well as on this branch, roughly two runs in three, and it
aborted `npm run test:py:linux` twice. Root cause found by diffing the two results field by field
rather than reading the truncated pytest output: the parity helper stripped timestamp fields but not
`probeDurationMs`, a wall-clock measurement that jitters by a millisecond or two between otherwise
identical runs, and it compared the candidate lists positionally while discovery emits them in
nondeterministic probe-completion order. Both are test defects, not product defects; the stripper now
removes `*DurationMs`/`*ElapsedMs`/`*Ms` alongside the timestamps, sorts lists canonically, and still
reports a genuine content change. Ten consecutive runs green afterwards.

## Completed

### P0 — Push blocker: two dead functions in `src/` — **done**

`vulture --min-confidence=60 src whitelist.py` returned exactly two findings repo-wide, both from
`eec27403`. Both are now resolved: `apply_registry_repair_review_action` is live via the new route,
and `find_registry_repair_review_row` was **deleted** rather than whitelisted, because
`registry_repair_review_status` already owns the fingerprint match and returns a superset. Vulture is
back to **0** findings.

Correction on my own earlier reporting: the `dead-code` guardrail group in
`tools/repo_health/repo_guardrails.py` covers dead *branches* and leaked loop bindings. It does not
run vulture and says nothing about unused functions. I previously treated "repo guardrails: dead-code
passed" as if it covered this class; it does not.

### P1 — Admin write route — **done**

`POST /registry/repair-review-action`, following the dedup pair review precedent. It records and never
applies: there is no branch that writes, demotes, deletes, or repoints a row, the response carries
`"applied": false`, and a test asserts the active registry file is byte-identical across a recorded
`retire` approval. `REGISTRY_REPAIR_REVIEW_PATH` is threaded through `api.py`, `bootstrap.py`, the
entrypoint api/runtime rebind, and `admin_bridge` — required rather than incidental, because the
`api.py` class default is a relative path and the packaged app would otherwise read a stray file from
the CWD.

### P5 — Live-registry coverage gap — **done**

`tools/repo_health/source_registry_preflight.py` re-runs the existing, already-tested predicates
against the live rows and adds a live-versus-seed drift comparison, so the two sides cannot disagree
about what counts as a defect. Advisory by default with `--strict` to fail, because a gate that fails
on every invocation until 31 collisions are adjudicated gets ignored.

Two runtime behaviors it surfaces that nothing else did:

- A **missing** live registry resolves to the committed seed (correct for a fresh install).
- A **corrupt** live registry *also* silently resolves to the seed. Right at runtime, wrong to hide:
  the operator's live rows vanish while every count still looks plausible.

### P6 — `typecheck:py` — **done**

Green for the first time on this branch: 1381 source files, 0 errors. Both were pre-existing on
`origin/main`. `npm run lint:repo-guardrails` 15/15 and `vulture` 0.

## Awaiting human disposition

These three are the same decision at three scales. Each is a registry-policy call about rows that are
live and being fetched today, so none is mechanical and none is made unilaterally here.

### P2 — The Miniclip row (1 item, strongest evidence of the three)

`static:listing_url:https://www.miniclip.com/careers/vacancies` is active with no `listing_url` and no
`pages`, so it fetches nothing while reporting ok. It is **not** a missing definition — it is a
superseded intermediate:

- `data/source-registry-tombstones.json.gz` records `corporate.miniclip.com/careers` as
  `superseded_by_promotion: ... the canonical www.miniclip.com/careers/vacancies registration was
  promoted from pending (live-verified board)`
- the live active Miniclip row is now
  `static:listing_url:https://careers.miniclip.com/go/miniclip-all-jobs/9013655/` (healthy, one page)

So the supersession chain was applied once too many times and the intermediate row was never retired
— the missing tombstone *is* the defect. Record `retire` through the route, then apply through
`transition_registry_to_pending` plus `add_tombstone`/`save_tombstones`.

**Deliberately not applied here.** The write path has a documented corruption mode (a raw-gzip or
non-`load_json_array` read makes the sanctioned save rebuild the metadata map from lean rows —
"metadata 3,038 → 2 entries"), and the marginal gain from retiring one inert row does not justify
paying that risk on its own. Apply it batched with P3/P4, once, with a read-back check.

### P3 — The 31 uncovered duplicate groups

Almost all are `www`/apex twins of one board — exactly what
`data/defaults/source-registry-known-url-collisions.json` exists for. But they are heterogeneous (some
are the same path, some are genuinely different windows on one host), so a blanket baseline would
grandfather real duplication. Decide per group: baseline the twin, or retire one.

Retiring is not free: these rows are being fetched today, so each decision trades duplicate output
against losing a board.

### P4 — The 3 stale baseline entries and the 165 seed-only rows

Backed by only one live row each, though the seed still has two:
`amazongamestudios.com/en-us/careers`, `careers.nintendo.com`, `waterproofstudios.com/careers`.

The 165 seed-only rows are the larger half of this: 2 are tombstoned (definitely prune) and **163 have
unknown provenance**, including `greenhouse:slug:examplestudio` — a literal placeholder row that
should never have been seeded. They need per-row investigation, not a bulk prune; pruning blindly
resurrects retired sources, which the changelog explicitly warns about ("removed ... so seed restores
cannot resurrect them").

Note for whoever does this: a bulk seed sync is *not* the fix. Regenerating the seed from live would
carry the 31 collisions into the seed and trip the commit-time guard, and it would change what a fresh
install receives.

## Open decisions for the operator

1. **Release.** 44 commits ahead of `origin/main`, intentionally unpushed. `vulture` is 0 and every
   lane is green, so nothing technical blocks a push; the constraint is that nobody has asked for a
   release. A push would also trip the container published-code gate, which is already satisfied for
   this window by the existing `Release-tag: v0.2.153` intent, so no new declaration is needed.
2. **The 717 worksheet dispositions**, then one batched verified apply through the sanctioned
   transition paths.
3. **Whether to schedule the 31 duplicate groups as policy.** They are mostly `www`/apex twins of a
   single board, which is exactly what the reviewed-collision baseline exists for, but they are
   heterogeneous enough that a blanket baseline would grandfather real duplication.

## Non-goals

- **No automatic URL or host mutation.** The gate records decisions; applying one is a human action
  through the existing sanctioned transition paths. P1 added a way to *record*, not a way to *apply*.
- **No blanket suppression.** Every finding stays reportable; review state annotates a finding rather
  than hiding it, and changed evidence reappears as `stale`.
- **No bulk registry reconciliation without per-row evidence.**
- **No new dependency.**

## Verification gates

Each item: focused tests for the touched module, then `npm run test:py:extended` and
`npm run test:py:linux`, plus `npm run lint:repo-guardrails`, `npm run typecheck:py`, and
`python -m vulture --min-confidence=60 src whitelist.py` returning zero findings.
