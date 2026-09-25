# Registry Hygiene Follow-Up Plan

> - **Status:** Phase 1 landed (4 commits); follow-up not started
> - **Use this when:** recording or applying a registry repair decision, reconciling live-registry drift from the committed seed, or extending the advisory/guard surfaces
> - **Canonical for:** the human-approval gate's boundaries, the live-vs-seed authority question, and the ordered follow-up work
> - **Not canonical for:** the advisory report shape (see `docs/DATA_CONTRACT.md`) or registry transition mechanics (see `src/source_registry_state.py`)
> - **Last updated:** 2026-09-25

## What already landed

Four commits, in order, all read-only with respect to the registry:

| Commit | Surface |
|---|---|
| `2e8fc780` | Advisory `registryHygieneAudit` report (asset pages, duplicate candidates, host drift, unreachable). |
| `180c3036` | Two hard commit-time guards: duplicate registry ids, malformed page refs. |
| `9c9e5cd1` | `repair_candidate` — dead-domain signal under a repeated-evidence rule. |
| `eec27403` | `data/registry-repair-review.json` — the human-approval gate. |

Live state at time of writing: 2,245 active rows, 206 flagged, 31 uncovered duplicate groups, 0 repair
candidates (every dead source in state belongs to a row earlier repointing/pruning waves already removed).

## The decision that gates most of this work

**Is the live registry authoritative and the committed seed stale, or is the live registry polluted
and the seed correct?**

The live registry has drifted from `data/defaults/source-registry-active.seed.json`:

- live 2,245 rows vs seed 1,893
- 517 rows exist only live; 165 exist only in the seed
- 31 duplicate-URL groups exist only in the live view; **0** uncovered groups in the seed

Every structural guardrail reads the *seed*, so the commit-time gate is green while the registry the
pipeline actually fetches carries 31 unbaselined collisions. Almost all are `www`/bare-domain twins.

This fork is not resolvable from code. The answer decides whether the work is "regenerate the seed from
live" (a mechanical export) or "reconcile 62 live rows against the seed" (a per-row adjudication). It
also decides whether the 3 stale baseline entries below are a pruning task or a restore task.

**Nothing below should be actioned before this is answered.**

## Ordered follow-up

### P0 — Unblock the push: two dead functions in `src/`

`vulture --min-confidence=60 src whitelist.py` returns exactly two findings repo-wide, and both are
from `eec27403`:

```
contracts_registry_repair_review.py:219: unused function 'find_registry_repair_review_row' (60%)
contracts_registry_repair_review.py:265: unused function 'apply_registry_repair_review_action' (60%)
```

Vulture is a pre-push hook, so the branch cannot ship until this is resolved. Do **not** whitelist
these: the finding is correct, and the fix is P1, which makes both live.

Correction on my own earlier reporting: the `dead-code` guardrail group in
`tools/repo_health/repo_guardrails.py` covers dead *branches* and leaked loop bindings. It does not
run vulture and says nothing about unused functions. I previously treated "repo guardrails: dead-code
passed" as if it covered this class; it does not.

### P1 — Add the admin write route for recording a repair decision

The gate is readable but **not writable through the product**: `apply_registry_repair_review_action`
has no production caller. A recorded decision currently cannot be created except by hand-editing the
artifact, which defeats the provenance the fingerprint depends on.

Precedent to follow exactly — the dedup pair review, where `apply_source_policy_review_action` is
reached from `src/bridge/routes/post_routes_admin.py`:

- POST route alongside the existing admin review route
- validate the decision through the existing bounded vocabulary; reject unknown values at the boundary
- carry the evidence fingerprint from the report the operator was looking at, so the decision binds to
  the defect on screen rather than a recomputed one
- write the artifact atomically

Out of scope for P1: applying the repair. That stays a separate operator action.

### P2 — Retire the superseded Miniclip row

`static:listing_url:https://www.miniclip.com/careers/vacancies` is active with no `listing_url` and no
`pages`, so it fetches nothing while reporting ok. It is **not** a missing definition — it is a
tombstone casualty:

- `data/source-registry-tombstones.json.gz` records `corporate.miniclip.com/careers` as
  `superseded_by_promotion: ... the canonical www.miniclip.com/careers/vacancies registration was
  promoted from pending (live-verified board)`
- the live active Miniclip row is now `static:listing_url:https://careers.miniclip.com/go/miniclip-all-jobs/9013655/`
  (healthy, one page)

So the supersession chain was applied once too many times and the intermediate row was never retired.
Correct action is `retire` via the P1 gate, then apply through the sanctioned transition. Record the
decision; do not perform the transition unprompted.

### P3 — Reconcile the 31 uncovered duplicate groups

Blocked on the authority fork above. Also decide the grandfathering policy: these are `www`/apex
twins of the same board, which is precisely what `data/defaults/source-registry-known-url-collisions.json`
exists for. Whether to add entries or dedupe the rows is a policy choice, not a mechanical one.

### P4 — Resolve the 3 stale baseline entries

Backed by only one live row each, though the seed still has two:

- `amazongamestudios.com/en-us/careers`
- `careers.nintendo.com`
- `waterproofstudios.com/careers`

Prune the entry or restore the twin. Which one is correct depends on P3's answer. The commit-time
stale-baseline guard cannot see this because it reads the seed.

### P5 — Close the live-registry coverage gap in CI

The systemic cause of P3 and P4 recurring. Structural guardrails read the committed seed; the advisory
monitor reads live but only when a pipeline run happens, so nothing gates a live-registry defect at
commit time. Options, in rough order of cost:

1. A pre-flight check over the live registry reusing the existing `list_*` predicates, runnable locally
   and in CI where a live registry exists.
2. An ops-health surface flag so drift is visible without opening a report.
3. Nothing, and accept that drift is found by running the pipeline.

Option 1 is the highest value per line: the predicates already exist and are already tested; they only
need a live-registry entrypoint.

### P6 — Clear the two pre-existing `typecheck:py` errors

`npm run typecheck:py` has been red before this program started, on `origin/main`:

- `tests/bridge/test_task_process_registry.py:119` — `append` used as a value
- `tests/test_jobs_fetcher_google_sheets_redirect_stats.py:31` — unannotated `detail_rows`

Small, independent of everything above, and worth doing so a green typecheck is a usable signal for
the rest of the work. Confirmed pre-existing on a stashed tree at every step of this program.

### P7 — Closeout

- Refresh the Basic Memory handoff for the performance/reliability program; it still describes the
  advisory report as the recommended next task, which is three commits stale.
- Release decision on the branch (38+ commits ahead, intentionally unpushed).

## Non-goals

- **No automatic URL or host mutation.** The gate records decisions; applying one is a human action
  through the existing sanctioned transition paths. P1 adds a way to *record*, not a way to *apply*.
- **No blanket suppression.** Every finding stays reportable; review state annotates a finding rather
  than hiding it, and a changed defect reappears as `stale`.
- **No new dependency.**

## Verification gates

Each item: focused tests for the touched module, then `npm run test:py:extended` and
`npm run test:py:linux`, plus `npm run lint:repo-guardrails` and `npm run typecheck:py`. P0 additionally
requires `vulture --min-confidence=60 src whitelist.py` to return zero findings.
