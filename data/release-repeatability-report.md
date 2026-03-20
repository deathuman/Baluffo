# Release Repeatability Report

Generated: 2026-03-20T18:55:24.149248+00:00

## Executive summary
- Runs analyzed: 3
- Output range: 33230 to 34081
- Output swing: 851
- Failed source range: 0 to 19
- Release floor: 34131
- Passes release floor on every run: False
- Passes zero top-level failures on every run: False

## Runs
- `jobs-fetch-report`: output=33230, failed=19, wallClock=649454 ms, social=True
- `release-repeatability-run2-jobs-fetch-report`: output=34081, failed=0, wallClock=138770 ms, social=True
- `release-repeatability-run3-jobs-fetch-report`: output=33230, failed=19, wallClock=649454 ms, social=True

## Volatile sources
- `gracklehq` (html): kept swing=1200, min/max=0/1200, errorRuns=2, zeroFlip=True
- `gamejobs` (html): kept swing=12, min/max=1093/1105, errorRuns=0, zeroFlip=False
- `google_sheets_1er2oaxo` (csv): kept swing=11, min/max=2289/2300, errorRuns=0, zeroFlip=False
- `social_reddit` (social): kept swing=3, min/max=10/13, errorRuns=0, zeroFlip=False
- `static_source::static:listing_url:https://careers.animocabrands.com/jobs` (static): kept swing=2, min/max=18/20, errorRuns=0, zeroFlip=False
- `workwithindies` (html): kept swing=2, min/max=68/70, errorRuns=0, zeroFlip=False
- `greenhouse_boards` (greenhouse): kept swing=1, min/max=771/772, errorRuns=0, zeroFlip=False
- `lever_sources` (lever): kept swing=1, min/max=300/301, errorRuns=0, zeroFlip=False
- `static_source::static:listing_url:https://jobs.ea.com/en_us/careers` (static): kept swing=1, min/max=19/20, errorRuns=0, zeroFlip=False
- `static_source::static:listing_url:https://www.riotgames.com/en/work-with-us/jobs` (static): kept swing=1, min/max=168/169, errorRuns=0, zeroFlip=False

## Recommendations
- Do not use a single high-water run as the release gate; the minimum repeated output (33230) is below the release floor (34131).
- Treat top-level source failures as a repeatability blocker until the maximum repeated run failure count returns to zero.
- Stabilize `gracklehq` first; it has the largest kept-job swing (1200) across repeated full-refresh runs.
- Add a preflight or monitored-volatility rule for `gracklehq` so one transient network miss does not dominate release acceptance.
- Use the repeatability report together with the single-run audit: one shows correctness of a run, the other shows whether the baseline is stable enough to release.
