# Source Discovery Yield Evidence Snapshot - 2026-04-29

> - **Status:** Active evidence snapshot
> - **Use this when:** choosing the next behavior-changing source-discovery improvement after the deletion-first adapter cleanup
> - **Canonical for:** current local artifact availability, registry/backlog pressure, and evidence gaps before P2 tuning
> - **Not canonical for:** saved-job/local-user data contracts, bridge contracts, adapter API contracts, or historical refactor provenance
> - **Then inspect:** [`source-discovery-adapter-follow-ups-closeout.md`](../archive/source-discovery-adapter-follow-ups-closeout.md), [`scraping-pipeline.md`](../scraping-pipeline.md), and [`DATA_CONTRACT.md`](../DATA_CONTRACT.md)
> - **Last updated:** 2026-04-29

This snapshot is intentionally evidence-only. It does not change discovery behavior, queue policy, saved jobs, local user data, bridge routes, frontend storage, artifact schemas, or source registry contracts.

## Summary

The deletion-first adapter cleanup is closed. The next source-discovery work should be behavior-changing only when backed by yield evidence.

Current local artifacts are useful for availability and shape checks, but they are not a representative live-yield run. The 2026-04-29 directory audit artifacts are small smoke-style artifacts with zero or near-zero source rows for Gameprog, Sheet-directory, and Web-derived discovery. The strongest yield evidence remains the prior HTTP-recovery snapshot from 2026-04-27, where Sheet-directory recovery produced substantial recovered candidates and Web-derived recovery stayed low-yield but clean.

## Current Local Artifact Baseline

| Artifact | Updated | Candidates | Failures | Recovery/browser evidence | Interpretation |
| --- | --- | ---: | ---: | --- | --- |
| `data/gameprog-discovery-audit.json` | 2026-04-29 09:19 UTC | 0 provider, 0 static | 1 | 0 recovery attempts, 0 browser candidates | Smoke artifact only; teams JSON parsed zero entries. |
| `data/gamesmap-discovery-audit.json` | 2026-04-29 09:19 UTC | 1 provider, 0 static | 2 | 0 recovery attempts, 0 browser candidates | Small bounded artifact; useful for shape, not yield ranking. |
| `data/sheet-directory-discovery-audit.json` | 2026-04-29 09:19 UTC | 0 provider, 0 static | 1 | no recovery summary fields present in this artifact | Smoke artifact only; CSV fetch failed. |
| `data/web-search-discovery-audit.json` | 2026-04-29 09:19 UTC | 0 provider, 0 static | 0 | 0 browser candidates, 0 browser-recovered active candidates | Smoke artifact only; seed catalog count is 0 and web search is disabled. |
| `data/gamedevmap-active-source-dry-run.json` | unavailable | unavailable | unavailable | unavailable | No current local GameDevMap audit artifact was present. |

## Registry And Candidate Pressure

| File | Count | Notes |
| --- | ---: | --- |
| `data/source-registry-active.json` | 2,021 active rows | Top discovery methods: GameDevMap 1,461, Sheet-directory 502, Seed careers 11, Gameprog 4, Gamesmap 3. |
| `data/source-registry-pending.json` | 57 pending rows | Top discovery methods: Sheet-directory 42, Gamesmap 3, Gameprog 3, Seed careers 1. |
| `data/source-discovery-candidates.json` | 142 validated candidates | 137 Sheet-directory, 3 Seed careers, 2 pattern candidates. |

The candidate file is older than the current audit artifacts and should not be treated as a fresh discovery run. It still shows useful pressure: 141 of 142 rows have `jobsFound == 0`, mostly Sheet-directory rows. That makes zero-job/static validation quality a better next investigation target than more adapter refactoring.

## Prior Recovery Evidence Still In Force

The 2026-04-27 HTTP recovery evidence remains the strongest live-yield signal:

| Adapter | Evidence | Direction |
| --- | --- | --- |
| Sheet-directory | Recovery produced 106 provider and about 366-368 static recovered candidates in the sampled runs, with fresh-cache reruns dropping runtime from about 201s to about 25s. | Keep recovery enabled; investigate zero-job quality and static cap behavior before widening more recovery. |
| Web-derived discovery | Recovery made only 7 attempts, fetched 2 pages, and recovered 0 additional candidates in the sampled run. | Do not widen web/browser coverage without a fresher JS-shell or browser-recoverable miss sample. |
| Gameprog/Gamesmap | Current local artifacts are too small to rank yield. | Gather a representative uncached audit before tuning limits or fallback rules. |
| GameDevMap | No current local active-audit artifact was available. | Capture or run an explicit active-audit artifact before changing browser recovery, queue override, or static cap behavior. |

## Failure And Recovery Pressure

| Area | Current observed pressure | Recommended next evidence |
| --- | --- | --- |
| No-candidate misses | Not measurable from current smoke artifacts. | Run a representative source-discovery audit and rank successful pages with no provider/static result. |
| JS-shell/browser-recoverable rows | 0 in current local web and directory artifacts. | Use a fresh web-derived audit with non-empty seed/web-search inputs before expanding browser eligibility. |
| HTTP recovery failures | Strong prior Sheet-directory volume; current smoke artifacts do not exercise recovery. | Compare Sheet recovered candidates against `jobsFound > 0` and pending/active movement. |
| Timeout/429/fetch failures | Current smoke artifacts show fetch/parse failures, but not live external-access pressure. | Reuse the external-access residual snapshot for fetcher failures; do not infer discovery policy from smoke artifacts. |
| Zero-job probes | 141 of 142 older discovery candidates have `jobsFound == 0`. | Rank by source directory and adapter to decide whether validation, scoring, or static caps should change. |
| Cache/artifact reuse | Directory artifacts exist and preserve shape; current files are too small for runtime conclusions. | Measure uncached and cached runs in the same data root when evaluating any P2 change. |

## Decision

Do not start another behavior-preserving cleanup slice. The next source-discovery work should be one of these evidence-backed choices:

1. **Zero-job/static quality investigation**: rank Sheet-directory and static candidates by `jobsFound == 0`, evidence type, source directory, and pending/active movement before changing caps or static fallback behavior.
2. **Representative discovery audit run**: produce a fresh uncached and cached source-discovery run with Gameprog, Gamesmap, Sheet-directory, Web-derived discovery, and GameDevMap artifacts present.
3. **Browser-recovery eligibility study**: only after a fresh audit shows JS-shell or browser-recoverable misses with plausible recovered yield.

Any P2 behavior change should preserve saved jobs, local user data, bridge contracts, frontend storage, queue/pending/tombstone/suppression behavior, and admin auto-approval boundaries.
