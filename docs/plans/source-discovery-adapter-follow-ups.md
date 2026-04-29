# Source Discovery Evidence-Backed Next Steps

> - **Status:** Active next-step tracker
> - **Use this when:** choosing the next source-discovery behavior change after the deletion-first adapter cleanup
> - **Canonical for:** completed source-discovery reset status, protected surfaces, evidence-backed next steps, and behavior-change gates
> - **Not canonical for:** saved-job/local-user data contracts, bridge endpoint contracts, persisted payload schemas, or fetcher adapter inventory
> - **Then inspect:** [`source-discovery-directory-web-evidence-2026-04-29.md`](../snapshots/source-discovery-directory-web-evidence-2026-04-29.md), [`source-discovery-fresh-audit-evidence-2026-04-29.md`](../snapshots/source-discovery-fresh-audit-evidence-2026-04-29.md), [`scraping-pipeline.md`](../scraping-pipeline.md), and [`DATA_CONTRACT.md`](../DATA_CONTRACT.md)
> - **Last updated:** 2026-04-29

The deletion-first adapter cleanup is complete. Keep this page active until the remaining source-discovery behavior work is either shipped or explicitly deferred. Do not restart broad compatibility-preserving helper extraction from this tracker.
Related fetch-adapter refactoring now lives in [jobs-adapter-mass-refactoring-plan.md](jobs-adapter-mass-refactoring-plan.md). Keep this source-discovery tracker focused on discovery evidence and behavior choices.


## Current Status

- Gameprog, Gamesmap, Sheet-directory, Web-derived discovery, and GameDevMap public runtime paths use audit-artifact rows.
- Legacy `activeAuditEnabled`, adapter-owned `cachePath`, legacy `cacheTtlMinutes`, web direct scanner exports, and the unused generic direct-scan helper are removed from source-discovery runtime/tests.
- Current `src/source_discovery` C901 offenders are cleared in [`scripts/complexity_baseline.json`](../../scripts/complexity_baseline.json).
- The first evidence-backed P2 behavior change landed: unrecovered Sheet-directory static homepage fallbacks are no longer carried forward after HTTP recovery fails to find a usable provider or jobs page.
- The local after-change rerun under `_out/source-discovery-directory-web-evidence-20260429-sheet-static-after-nobom` showed the main zero-job pressure improved: validated candidates `166 -> 89`, zero-job candidates `143 -> 67`, Sheet zero-job `125 -> 49`, and static zero-job `111 -> 19`.

## Protected Surfaces

- Saved jobs and local user data.
- Current frontend/local storage behavior for saved/local user sections.
- Current UI/runtime invocation paths that start discovery and fetch flows.
- Bridge/API contracts needed by the current UI/runtime.
- Queue, pending review, tombstone, static suppression, and admin auto-approval behavior when candidates enter the current product flow.

## Remaining Work

| Priority | Slice | Goal | Acceptance signal |
| --- | --- | --- | --- |
| 1 | Capture tracked Sheet/static after-change evidence | Turn the local `_out` rerun into a concise checked-in snapshot or update the existing directory/web snapshot with before/after counts. | Docs clearly show the Sheet/static quality win and the command/root used; no runtime behavior changes. |
| 2 | Run bounded GameDevMap evidence | Replace the timed-out uncapped GameDevMap attempt with bounded or longer uninterrupted evidence. | Snapshot ranks GameDevMap active/static/provider yield, zero-job probes, recovery failures, browser-recovery candidates, timeout/429/fetch buckets, and cache reuse. |
| 3 | Choose the next P2 behavior change from evidence | Decide whether to tune remaining static fallback quality, queue/pending thresholds, browser-recovery eligibility, or provider inference. | A specific behavior slice has before/after metrics and tests around queue, pending, tombstone, suppression, auto-approval, and saved/local boundaries. |
| 4 | Archive this tracker | Retire this page once source-discovery cleanup and evidence-backed behavior selection no longer need an active tracker. | Move only a short closure note to `docs/archive/README.md` or rely on git history if no active follow-up remains. |

## Hard Gates

- No new helper unless the same slice deletes or substantially thins adapter-owned code.
- Each source-discovery refactor should be net LOC-negative unless it adds new source coverage.
- Do not restore legacy direct discovery or legacy config compatibility paths.
- No adapter should own fetch, recovery, probe, dedupe, report, or audit lifecycle after migration.
- No new source-discovery C901 offenders.
- Behavior changes are allowed inside discovery/fetch internals only when protected surfaces remain tested.
- Browser-recovery expansion must wait for evidence showing meaningful recovered yield.

## Validation Standard

Documentation-only evidence slices:

```powershell
cmd /c npm run lint:precommit
```

Source-discovery behavior slices:

```powershell
python -m pytest -q tests/source_discovery
cmd /c npm run lint:precommit
```

Add targeted adapter or queue tests before the full source-discovery suite when changing candidate promotion, probing, pending movement, tombstone handling, suppression, or auto-approval.

## Decision Rules

- If a path does not improve current active-source/job discovery and is not a protected surface, prefer deletion or deferral.
- If preserving old behavior blocks simplification, preserve only current product behavior and test that boundary.
- If evidence points to operational noise rather than product yield, fix the noise narrowly and return to evidence-backed behavior work.
- If a behavior change cannot show before/after impact, capture evidence first instead of changing runtime policy.
