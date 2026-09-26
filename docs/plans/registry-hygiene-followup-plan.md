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

**Resolved 2026-09-26: all 31 are the same board, and all 31 were reconciled by demotion.**

Live evidence, with three working controls and certifi-anchored TLS:

- **31/31 groups resolve to one board.** Compared on the registry's *own*
  `canonicalize_careers_url` applied to the post-redirect final URL, not on raw URL strings. Raw
  comparison invents differences that are not there — `http` vs `https`, an explicit `:443`, a
  trailing slash and `www` vs apex are exactly what the canonicalizer collapses for the twin rule.
- A first pass using a **content hash** called the scopely pair "different boards". Wrong: the page
  embeds dynamic content. Both URLs redirect to the identical `careers.scopely.com/us/en` with the
  same declared canonical. The fingerprint was the unreliable signal, not the boards.
- The last 3 apparent differences (krafton ×2, rollicgames) were confirmed to be self-referential
  links: each page links to its own alternate URL form and locale variants.

Survivor authority, in order: **the row the committed seed already keeps** (19 groups — the shipped
default is the deliberate shape, which is why the seed has zero collisions), else the row the other
one redirects to (5), else the row matching the page's declared canonical (3), else the apex
spelling (3). Every demoted row moves to pending, hidden, carrying `duplicateOfSourceId` back to an
active survivor — not tombstoned, so the board stays covered and the change is reversible.

Result: active 2,255 → 2,224, pending 840 → 871, `uncovered collisions 31 → 0`,
`seed_row_demoted_in_store` unchanged at 152 (confirming no loser was a seed row).

**Why not `demote_duplicate_active_variants` and why not the baseline file:**

- The sanctioned bulk function sweeps the whole registry — **193 conflict cards** against 62 rows
  under review — and its family grouping does not match the verified collision boundaries. For
  `infinityward` and `mindstormstudios` it marks *both* twins as losers, because the family winner
  is a third row outside the reviewed set; running it would have stripped those studios of their
  board entirely. The per-row primitive it uses internally (`_demote_duplicate_variant`) was applied
  to exactly the 31 verified losers instead.
- Baselining was **not available**: the commit-time stale check runs against the *seed*
  (`check_active_seed_stale_baseline`), so an entry needs 2+ active seed rows. The seed
  deliberately keeps one row per board, so every one of these 31 URLs has ≤1 seed row. Baselining
  them would have failed the guard immediately. Reconciling to one active row is the resolution the
  seed's own shape implies.

### P4 — The 3 stale baseline entries and the 153 seed-only rows

Backed by only one live row each, though the seed still has two:
`amazongamestudios.com/en-us/careers`, `careers.nintendo.com`, `waterproofstudios.com/careers`.

The 153 seed-only rows are the larger half of this. Their provenance is now fully decomposed:
**152 `seed_row_demoted_in_store`** (the store demoted them, so the seed copy is stale) and **1
`seed_row_lost_from_store`** (RTL, resolved below). The earlier "163 unknown provenance" figure is
resolved — it was these two buckets plus the lost rows, not a third mystery class. The
`greenhouse:slug:examplestudio` placeholder and the tombstoned inXile row are pruned.

**Resolved 2026-09-26: 102 pruned, 50 deliberately kept.** The 152 were classified by *why* the
store demoted them, because the store's reason decides whether the seed copy is stale or the store
is merely retrying:

| Bucket | Count | Disposition |
|---|---:|---|
| `registry_conflict_*_auto_demote` (parked losers) | 127 | prune where a live replacement is verified |
| `sheet_directory` / `seed` / `provider_migration_candidate` (superseded) | 8 | prune |
| `fetch_failure_demote` (retry state) | 13 | **keep** — a failed fetch is not a retirement |
| `hidden:repeated_zero_jobs` (retry state) | 4 | **keep** — parked, deliberately retryable |
| tombstoned | 0 | — |

Of the 135 prune candidates, only **102** had a verified live replacement: 31 superseded by an active
row on the same canonical board, 3 with an explicit `duplicateOfSourceId` winner, and 68 superseded
by a provider-backed row for the same studio under a different adapter (whose canonical URL is the
provider endpoint, not the careers page — which is why a same-board check alone undercounts).

**The other 33 must not be pruned, and this is the important finding of P4.** They have no active
row on any adapter: `registry_conflict_*_auto_demote` demoted their *entire studio family*, leaving
the studio with zero coverage. Several are whole clusters of one board's URL variants
(Studio IGGYMOB ×5, Rogue Duck Interactive ×5, Proton Studio ×5, Clay Token ×5 — all `?l=thai` /
`?ckattempt=1` variants of a single page). Affected studios include k-ID, Unknown Worlds
Entertainment, Vertigo Games (PLAION), 4A Games, Frogwares, Double Eleven, Ever Curious
Entertainment, Magnopus, Behaviour Interactive, Black Beach Studio, GigXR.

This is the same pathology as the `infinityward`/`mindstormstudios` family, at scale: a conflict
resolution that demotes every member of a family leaves the board uncovered rather than resolving
the conflict. **Pruning those 33 seed rows would have silently retired 33 real boards** — the exact
"prune blindly and you lose coverage" failure the plan warned about, and the reason a per-row
tombstone check was the minimum bar. The seed rows stay, so a fresh install still activates them.

Also pruned in the same change: the 3 stale collision-baseline entries. A baseline entry no longer
backed by 2+ active rows would otherwise mask the URL permanently.

Result: seed 1,890 → 1,788, collision baseline 7 → 4, every non-pruned row byte-identical and in
order.

### RTL Enterprises — not restored; the row is misconfigured

`phenom:listing_url:https://jobsearch.createyourowncareer.com/RTL/` is alive but is **not a Phenom
board**. A real browser render (not a plain fetch) shows a jobs2web (`j2w`) platform: search lives at
`/RTL/content/search/?currentPage=1&pageSize=12`, and every same-host link is a
`/RTL/content/<category>/` nav entry (`job-world-tech`, `entry-level`, …) with **zero job-posting
links** on any category page.

The phenom parser matches `^/[^/]+/job/[^/]+/\d+/?$` and paginates on `?startrow=`. Neither matches
j2w's shape, so this row **provably cannot extract jobs** as configured — restoring it would add a
row guaranteed to yield nothing. It is also the only candidate with no `approvedBy`; it was promoted
by `phenom_adapter_migration` with no human approval.

So it stays out of the store, and the follow-up is provider-coverage work, not a registry restore:
either add a j2w adapter or re-point the row at a real Phenom tenant. `seed_row_lost_from_store` is
now **0**.

## Open items for the operator

1. **The 33 uncovered studios** (P4). Real coverage gap created by whole-family conflict demotes.
   The fix belongs in the conflict-resolution policy, not the registry: it should demote the losers
   and keep one winner active, not demote every member. Needs a decision because changing it alters
   runtime demotion behaviour.
2. **RTL / jobs2web** needs an adapter or a re-point before the row can be restored.
3. **A static-parser gap found while probing**: `about-fun.com/jobs` serves 14 real postings
   (`/jobs-<role>` slugs, read directly from the hrefs) that the pipeline's own
   `static_probe_evidence` counts as **0**. Several of these boards are server-rendered and do have
   openings, so `lastKeptCount: 0` on a duplicate row is not evidence of an empty board. That is a
   parser-coverage task in its own right.
4. **Optional D:** a frontend surface for `POST /registry/repair-review-action` (no UI exists).


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
