# Source Discovery HTTP Recovery Evidence - 2026-04-27

## Summary

Controlled live discovery smoke runs were executed with generated data backed up and restored afterward. The runs used in-memory config overrides, `preset="uncapped"`, `top_n=0`, and default adapter settings except for explicit HTTP recovery enablement.

Both sheet-directory and web-derived audit artifacts reused fresh cache on the second run. Sheet-directory recovery produced substantial recovered candidate evidence. Web-derived recovery executed cleanly and stayed diagnostic-only in this sample because it found no additional candidates.

## Runs

| Scenario | Run | Duration | Sheet cache | Web cache | Queued total |
|---|---:|---:|---|---|---:|
| Sheet recovery, no web search | 1 | 200.677s | miss | miss | 709 |
| Sheet recovery, no web search | 2 | 25.061s | hit | hit | 522 |
| Sheet + web recovery | 1 | 113.607s | miss | miss | 691 |
| Sheet + web recovery | 2 | 25.057s | hit | hit | 499 |

## Recovery Counts

| Scenario | Adapter | Attempts | Pages fetched | Recovered provider | Recovered static | Recovery failures | Normal failures |
|---|---|---:|---:|---:|---:|---:|---:|
| Sheet recovery, no web search | sheet_directory | 2132 | 731 | 106 | 368 | 1429 | 0 |
| Sheet + web recovery | sheet_directory | 2136 | 741 | 106 | 366 | 1423 | 0 |
| Sheet + web recovery | web_search | 7 | 2 | 0 | 0 | 5 | 2 |

## Candidate And Registry Evidence

- Sheet-directory audit output after recovery: `providerCandidates=118`, `staticCandidates=565`, `failures=0`.
- Web-derived audit output with recovery enabled: `providerCandidates=14`, `staticCandidates=15`, `failures=2`, `browserRecoveryCandidates=2`.
- Generated stage totals were 2728 candidates in the sheet/no-web pair and 2729 candidates in the sheet+web pair.
- The restored smoke would have moved registries before restore:
  - Sheet/no-web pair: active `2021 -> 2260`, pending `57 -> 457`, rejected unchanged at `0`.
  - Sheet+web pair: active `2021 -> 2261`, pending `57 -> 451`, rejected unchanged at `0`.

## Decision

The evidence supports default-enabling both HTTP recovery lanes:

- Sheet-directory recovery has high yield and fresh-artifact reuse reduces subsequent run cost from about 201s to about 25s.
- Web-derived recovery has low incremental yield in this sample, but only made 7 recovery attempts, preserved normal failure behavior, and reused cache on the second run.
- Recovery failures remained audit diagnostics and did not become normal adapter failures.
