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

## Resolved by operator decision

### P2 — The Miniclip row — **repaired, and the original reading was wrong**

`static:listing_url:https://www.miniclip.com/careers/vacancies` was active with no `listing_url` and no
`pages`, so it fetched nothing while reporting ok. The first reading called it a superseded
intermediate left unretired. That was wrong. The tombstone for `corporate.miniclip.com/careers`
(`deletedBy repair_batch4_20260907`) records that batch 4 **promoted** this row from pending as the
live-verified replacement, and the row's own `lastPromotedAt`/`stateChangedBy` carry the same batch-4
stamp. So the promotion is what corrupted it: the store copy had 48 of 58 fields stripped, leaving a
row with no definition. The seed held the complete definition and was used as the graft source.
Repaired with the backup-and-verify procedure; the preflight now reports the seed/store split
divergence rather than a definitionless row.

The lesson generalises: the write path has a documented corruption mode (a raw-gzip or
non-`load_json_array` read makes the sanctioned save rebuild the metadata map from lean rows —
"metadata 3,038 → 2 entries"). Every store write since must be read back, not assumed.

### The 13 `seed_row_lost_from_store` rows — **resolved, 1 deliberately held**

Thirteen seed rows were absent from the store with no tombstone. Reported to the operator, who asked
for the live boards to be checked rather than the seed metadata to be trusted. Probing the boards
through the same endpoints the pipeline's own adapters use — validated against two working controls
(`activategames` 38 jobs, `streamlinestudios` 16) — changed the answer completely:

| Group | Count | Probe | Disposition |
|---|---:|---|---|
| Six BambooHR tenants + IllFonic (Breezy) | 7 | HTTP 200, structured zero | Restored |
| TiMi Studio Group (Workday) | 1 | HTTP 200 **via certifi** | Restored |
| Lucky VR (Breezy) | 1 | **HTTP 404 at the board root** (seed `jobsFound: 3`) | Retired |
| Vivid Games (Teamtailor) | 1 | board root HTTP 200, ~93 KB | Restored |
| RTL Enterprises (Phenom) | 1 | 200, but renders client-side | Held |
| inXile (static) | 1 | already tombstoned 2026-09-07 | Seed pruned only |
| `greenhouse:slug:examplestudio` | 1 | literal placeholder | Seed pruned only |

**Restored (9).** Their absence was a *bug*, not a decision — the same store write that stripped
Miniclip dropped them. The 2026-09-09 bamboo/breezy/workday wave promoted and deliberately kept these
rows even when live-empty, and an empty board that answers with a clean structured zero is a
legitimate row under the existing Steer precedent. Seed rows were grafted verbatim so the promotion
metadata stays the truthful history. Active 2,246 → 2,255, metadata 3,086 → 3,095 (+9 each), zero
unrelated rows changed. Vivid Games needed its store row grafted too, after the probe correction
below — its seed row alone would have left it flagged as still lost.

**Retired (1).** `/registry/delete` only tombstones rows it finds in a bucket, and Lucky VR is not in
the store, so it would have been a no-op. The tombstone was written from the seed row — the only
surviving definition — with `reason: dead_board_http_404`. The tombstone is what stops a later
discovery pass resurrecting the board; the seed prune stops a fresh install shipping a dead row.

### Three probe traps, each of which nearly caused a wrong retirement

Every one of these produced a confident wrong answer first. This is the most important lesson from
the pass:

1. **Bare `urllib` vs certifi.** TiMi returned `CERTIFICATE_VERIFY_FAILED`, which reads exactly like
   a dead board. It is alive, and the documented certifi-anchored fix for `*.myworkdayjobs.com` makes
   it return 200. It was historically the group's largest contributor (197 feed rows), so retiring it
   on that one probe would have been the most expensive available error.
2. **The wrong endpoint 404s on healthy rows.** BambooHR's `/api/listed_jobs` 404s on *known-good*
   tenants; the adapter's real endpoint is `/careers/list`. The Teamtailor `embed` endpoint 404s on a
   live board, and `teamtailor_runner` never uses it — it parses listing links out of the board HTML.
   **Vivid Games was retired on exactly this error** and had to be walked back: its board root
   answers HTTP 200. The seed row was re-inserted at its original index and its tombstone removed,
   leaving only the three genuinely-stale prunes. It is listed as "Restored" above.
3. **An empty board is not a failure.** A 200 with a clean structured zero is a legitimate row under
   the Steer precedent, not grounds for retirement.

The rule: **probe the board root as well as the adapter endpoint, and always run a known-good
control before believing a 404.** The control is what exposed trap 2 immediately.

`seed_row_lost_from_store` is now **1** (RTL, held pending a rendered probe).

### Two golden-seed tests had to be re-pinned

`tests/test_source_registry_p1_operational_noise.py` uses the *committed* seed as its fixture, so
retiring Lucky VR broke two tests that named it. Both were re-pinned to rows that are live today
(`greenhouse:slug:bungie` for the static-residual pair, and the four remaining `antiBotBrowserRetry`
rows), which is the correct fix: the tests were asserting seed history, not behaviour. Standing
fragility — any future legitimate retirement of a pinned row will break them again.

## Awaiting human disposition

Both remaining items are registry-policy calls about rows that are live and being fetched today, so
neither is mechanical and neither is made unilaterally here.

### P3 — The 31 uncovered duplicate groups

Almost all are `www`/apex twins of one board — exactly what
`data/defaults/source-registry-known-url-collisions.json` exists for. But they are heterogeneous (some
are the same path, some are genuinely different windows on one host), so a blanket baseline would
grandfather real duplication. Decide per group: baseline the twin, or retire one.

Retiring is not free: these rows are being fetched today, so each decision trades duplicate output
against losing a board.

### P4 — The 3 stale baseline entries and the 153 seed-only rows

Backed by only one live row each, though the seed still has two:
`amazongamestudios.com/en-us/careers`, `careers.nintendo.com`, `waterproofstudios.com/careers`.

The 153 seed-only rows are the larger half of this. Their provenance is now fully decomposed:
**152 `seed_row_demoted_in_store`** (the store demoted them, so the seed copy is stale) and **1
`seed_row_lost_from_store`** (RTL, held above). The earlier "163 unknown provenance" figure is
resolved — it was these two buckets plus the lost rows, not a third mystery class. The
`greenhouse:slug:examplestudio` placeholder and the tombstoned inXile row are pruned.

Pruning the 152 still needs per-row care rather than a blind bulk delete, because a wrong prune
resurrects retired sources — the changelog explicitly warns about this ("removed ... so seed restores
cannot resurrect them"). A tombstone check per row is the minimum bar.

Note for whoever does this: a bulk seed sync is *not* the fix. Regenerating the seed from live would
carry the 31 collisions into the seed and trip the commit-time guard (seed collisions would go
7 → 62), and it would change what a fresh install receives.

## Open decisions for the operator

1. **The 31 duplicate groups (P3).** They are mostly `www`/apex twins of a single board, which is
   exactly what the reviewed-collision baseline exists for, but they are heterogeneous enough that a
   blanket baseline would grandfather real duplication. The classification into 9 `redundant_twin`,
   12 `label_variant`, 9 `true_shared_board`, and 1 `distinct_windows` is in
   `_out/registry-repair-20260925/duplicate-classification.json` with yield evidence per group. No row
   in any group kept a job in the last run, so there is no live double-emission to stop — the decision
   is purely about future coverage. All 62 rows are acknowledged in `data/registry-repair-review.json`.
2. **The 152 demoted seed rows (P4).** Needs a per-row tombstone check, not a blind bulk prune.
3. **RTL Enterprises (Phenom).** Held. The host answers 200 but renders client-side, so no job count
   can be established from here, and it is the only restored-candidate with no `approvedBy` at all.
   Needs a browser-rendered probe before it is restored or retired.

### Release state — no gate work needed

The commit carrying the store/seed repair touches `data/defaults/` and `data/source-registry-tombstones.json.gz`,
which are shipped paths, so it republishes the container. That does **not** need a new `Release-tag`
line: the window since anchor `f704cdae` (v0.2.152, 53 commits) already carries `Release-tag: v0.2.153`
intent on `fa573231` and `13a11206`, and the gate passes if *any* commit in the window declares valid
intent. A decorative tag on a data repair would misdeclare a release, so none was added.


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
