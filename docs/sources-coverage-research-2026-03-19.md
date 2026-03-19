# Sources Coverage Backlog (2026-03-19)

This document is the working backlog for source-coverage expansion. It reflects the current repo state rather than the earlier research snapshot.

## Current Baseline

- Active source registry rows: `140`
- Enabled-by-default rows: `139`
- Registry-backed provider families already active:
  - `greenhouse` (`10`)
  - `teamtailor` (`7`)
  - `lever` (`11`)
  - `smartrecruiters` (`6`)
  - `workable` (`11`)
  - `recruitee` (`1`)
  - `pinpoint` (`1`)
  - `ashby` (`7`)
  - `personio` (`2`)
- Community / board loaders active in `DEFAULT_SOURCE_LOADER_NAMES`:
  - `google_sheets`
  - `remote_ok`
  - `gamesindustry`
  - `gamejobs`
  - `workwithindies`
  - `8bitplay`
  - `gracklehq`
  - `epic_games_careers`

Corrections from the original research note:
- `ashby` was already supported before this expansion wave and is not a new adapter.
- The source catalog was not `25` studios; the live active registry is already much larger.
- `breezy` and `jazzhr` remain supported provider families, but they are not currently represented in the checked-in active registry snapshot.

Current checked-in fetch-report snapshot for the main community boards:
- `gamejobs`: healthy baseline at `93 fetched / 93 kept`
- `workwithindies`: latest checked-in report still shows `0 / 0`, but the parser/runner was updated in this recovery wave to match the live `/careers/` cards and needs a fresh live rerun to refresh the report
- `8bitplay`: latest checked-in report still shows `0 / 0`, but the parser/runner now follows the live `post__similar-job` cards and `job-board-paged` pagination
- `gracklehq`: latest checked-in report still shows `0 / 0`, but the parser/runner now follows the live paginated `.joblisting` rows and `pageidx` next links

## Ready Next

| Source family | Expected coverage impact | Implementation pattern | Verification path | Status |
|---|---|---|---|---|
| `Live rerun for recovered community boards` | Immediate posting increase if the live boards match the repaired parsers | rerun `workwithindies`, `8bitplay`, and `gracklehq` through the normal fetcher path and confirm real kept jobs | `py -3 -m pytest tests/test_jobs_fetcher.py -q` plus a fresh fetch run | `ready_now` |
| `Recruitee seed expansion` | More provider coverage from the already-live family | add more verified public game-studio boards to the provider registry | `py -3 -m pytest tests/test_jobs_fetcher.py tests/test_provider_api_plugins.py -q` | `planned` |
| `Pinpoint seed expansion` | More provider coverage from the already-live family | add more verified public game-studio boards to the provider registry | `py -3 -m pytest tests/test_jobs_fetcher.py tests/test_provider_api_plugins.py -q` | `planned` |

## Later Planned

| Source family | Expected coverage impact | Implementation pattern | Verification path | Status |
|---|---|---|---|---|
| Japan provider-first expansion | Large geographic coverage gain | prefer provider seeds; add static only when provider absent | targeted provider/static tests per batch | `planned` |
| South Korea provider-first expansion | Large geographic coverage gain | prefer provider seeds; add static only when provider absent | targeted provider/static tests per batch | `planned` |
| China provider-first expansion | Large geographic coverage gain | prefer provider seeds; add static only when provider absent | targeted provider/static tests per batch | `planned` |

## Deferred

| Source family | Expected coverage impact | Implementation pattern | Verification path | Status |
|---|---|---|---|---|
| Discord / auth-gated communities | Unknown until ingestion policy is defined | new ingestion policy first, then source design | policy + prototype, not routine fetch tests | `deferred` |
| Social expansion beyond current Reddit/X/Mastodon lanes | Incremental only if it fits existing social architecture | extend `social` family, not community-board loaders | targeted social parser tests | `deferred` |

## Wave 1 Shipped In This Pass

- Added `gamejobs` community loader and parser.
- Added `workwithindies` community loader and parser.
- Added `breezy_sources` provider family with seeded `YallaPlay` board coverage.
- Added `jazzhr_sources` provider family with seeded `Lost Boys Interactive` and `Next Level Games` boards.
- Added `recruitee_sources` provider family with seeded `CrazyGames` coverage via the public `jobs.crazygames.com` API.
- Added `pinpoint_sources` provider family with seeded `Gameplay Galaxy` coverage via the public Pinpoint postings feed.
- Added `8bitplay` as a first-class community board loader.
- Added `gracklehq` as a first-class community board loader while preserving the existing `gracklehq.com` redirect normalization path.
- Repaired `workwithindies` to parse the live `/careers/` card markup instead of the older sentence-shaped link labels.
- Repaired `8bitplay` to parse the live `post__similar-job` cards and follow `job-board-paged` pagination.
- Repaired `gracklehq` to parse live `.joblisting` rows and follow `pageidx` pagination.
- Extended compatibility exports and fetcher wrappers so the new source families can run through normal fetch/report flow.
- Added parser fixtures, fetcher/plugin tests, and source-discovery coverage for the new families.

## Next Recommended Batches

1. Run a fresh live fetch so the recovered `workwithindies`, `8bitplay`, and `gracklehq` loaders can replace the stale `0 / 0` snapshot with real counts.
2. Expand `recruitee` with more game-first public boards beyond `CrazyGames`.
3. Expand `pinpoint` with more game-first public boards beyond `Gameplay Galaxy`.
4. Start a provider-first geographic batch for Japan, then South Korea, then China.

## Current Blockers / Open Research

- `Recruitee`: the adapter is live, but the next batch still needs more verified public game-studio boards with stable unauthenticated feeds.
- `Pinpoint`: `Gameplay Galaxy` proves the family shape, but more gaming-relevant public boards still need verification before the next seed batch.
- `Work With Indies`, `8Bit Play`, and `Grackle HQ`: parser/runner recovery is implemented and test-covered, but the checked-in fetch report is stale until the next live run lands.

## Rules For Future Expansion

- Prefer provider-backed sources over static scraping whenever a careers site exposes a supported ATS.
- Do not add dormant adapters; every new provider family should ship with real registry rows.
- Do not broaden `src.jobs.common` for source expansion.
- Keep new community boards in the community loader family rather than mixing them into social or static code.
- Update this document when source families change so it stays the single backlog for coverage work.
