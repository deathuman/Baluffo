# Pipeline Audit Report

Generated: 2026-03-20T17:29:29.247385+00:00

## Executive summary
- Discovery duration: 58946 ms
- Fetch duration: 328026 ms
- Fetch wall-clock: 97110 ms
- Total jobs: 34080
- Top-level failed fetch sources: 0
- Top-level high-cost/low-yield sources: 0
- Issue inventory hard failures: 25
- Issue inventory soft failures: 25
- Issue inventory high-cost/low-yield: 0
- Issue inventory coverage risks: 25

## Slowest areas
- Discovery stage `probe`: 33740 ms
- Discovery stage `seedCareersScan`: 20286 ms
- Discovery stage `webSearch`: 4222 ms
- Discovery stage `sheetDirectory`: 671 ms
- Discovery stage `queueBalancing`: 5 ms
- Fetch adapter `static`: 150629 ms across 47 source(s)
- Fetch adapter `csv`: 80727 ms across 3 source(s)
- Fetch adapter `html`: 37430 ms across 6 source(s)
- Fetch adapter `lever`: 14998 ms across 1 source(s)
- Fetch adapter `api`: 11108 ms across 2 source(s)
- Loader `google_sheets` (csv): 68903 ms, kept 29433
- Loader `static_source::static:listing_url:https://jobs.ea.com/en_us/careers` (static): 31798 ms, kept 19
- Loader `gracklehq` (html): 18749 ms, kept 1200
- Loader `lever_sources` (lever): 14998 ms, kept 300
- Loader `static_source::static:listing_url:https://www.disneycareers.com/en/search-jobs/game/391/1` (static): 12256 ms, kept 10
- Source entry `Electronic Arts (EA) (Sheet)` via `static_source::static:listing_url:https://jobs.ea.com/en_us/careers`: 247140 ms, kept 19/1
- Source entry `google_sheets` via `google_sheets`: 61562 ms, kept 29434/29434
- Source entry `Disney (Manual Website)` via `static_source::static:listing_url:https://www.disneycareers.com/en/search-jobs/game/391/1`: 40323 ms, kept 10/7
- Source entry `Criterion (Sheet)` via `static_source::static:listing_url:https://jobs.ea.com/en_us/careers/home/?4538=8369&4538_format=3021&listfiltermode=1`: 25900 ms, kept 7/1
- Source entry `Animoca Brands (Sheet)` via `static_source::static:listing_url:https://careers.animocabrands.com/jobs`: 24133 ms, kept 20/1

## Productive but expensive sources
- `google_sheets` (csv): 68903 ms, kept 29433/29434
- `static_source::static:listing_url:https://jobs.ea.com/en_us/careers` (static): 31798 ms, kept 19/19

## Detail-fetch dominated sources
- `static_source::static:listing_url:https://jobs.ea.com/en_us/careers` (static): detailFetch=245392 ms, total=31798 ms, kept 19
- `static_source::static:listing_url:https://www.disneycareers.com/en/search-jobs/game/391/1` (static): detailFetch=33566 ms, total=12256 ms, kept 10
- `static_source::static:listing_url:https://jobs.ea.com/en_us/careers/home/?4538=8369&4538_format=3021&listfiltermode=1` (static): detailFetch=24512 ms, total=8055 ms, kept 7
- `static_source::static:listing_url:https://careers.animocabrands.com/jobs` (static): detailFetch=23392 ms, total=3864 ms, kept 20
- `static_source::static:listing_url:https://www.stillfront.com/en/career/join-the-team/` (static): detailFetch=20858 ms, total=3253 ms, kept 56

## Failed or not properly acting sources
### Hard Failures
- `Bonfire Studios (Sheet)` (static) | duration=0 ms | kept=0/1 | category=fetch_detail_error | error=no jobs extracted from source pages
- `Blizzard Entertainment (Sheet)` (static) | duration=0 ms | kept=0/1 | category=fetch_detail_error | error=no jobs extracted from source pages
- `Cool Games (Sheet)` (static) | duration=0 ms | kept=0/1 | category=fetch_detail_error | error=no jobs extracted from source pages
- `4J Studios (Sheet)` (static) | duration=0 ms | kept=0/1 | category=fetch_detail_error | error=no jobs extracted from source pages
- `Black Snow Games (Sheet)` (static) | duration=0 ms | kept=0/1 | category=fetch_detail_error | error=no jobs extracted from source pages
- `k-ID (Ashby)` (ashby) | duration=0 ms | kept=0/0 | category=fetch_detail_error | error=no jobs extracted from ashby board html
- `Area 35 East (Sheet)` (static) | duration=0 ms | kept=0/1 | category=fetch_detail_error | error=no jobs extracted from source pages
- `Aspyr (Sheet)` (static) | duration=0 ms | kept=0/1 | category=fetch_detail_error | error=no jobs extracted from source pages
- `24 Bit Games (Sheet)` (static) | duration=0 ms | kept=0/1 | category=fetch_detail_error | error=no jobs extracted from source pages
- `Gismart (Manual Website)` (static) | duration=0 ms | kept=0/7 | category=fetch_detail_error | error=no jobs extracted from source pages
### Soft Failures
- `Bonfire Studios (Sheet)` (static) | duration=0 ms | kept=0/1 | category=fetch_ok_extract_zero | error=no jobs extracted from source pages
- `Blizzard Entertainment (Sheet)` (static) | duration=0 ms | kept=0/1 | category=fetch_ok_extract_zero | error=no jobs extracted from source pages
- `Cool Games (Sheet)` (static) | duration=0 ms | kept=0/1 | category=fetch_ok_extract_zero | error=no jobs extracted from source pages
- `4J Studios (Sheet)` (static) | duration=0 ms | kept=0/1 | category=fetch_ok_extract_zero | error=no jobs extracted from source pages
- `Black Snow Games (Sheet)` (static) | duration=0 ms | kept=0/1 | category=fetch_ok_extract_zero | error=no jobs extracted from source pages
- `Area 35 East (Sheet)` (static) | duration=0 ms | kept=0/1 | category=fetch_ok_extract_zero | error=no jobs extracted from source pages
- `Aspyr (Sheet)` (static) | duration=0 ms | kept=0/1 | category=fetch_ok_extract_zero | error=no jobs extracted from source pages
- `24 Bit Games (Sheet)` (static) | duration=0 ms | kept=0/1 | category=fetch_ok_extract_zero | error=no jobs extracted from source pages
- `Gismart (Manual Website)` (static) | duration=0 ms | kept=0/7 | category=fetch_ok_extract_zero | error=no jobs extracted from source pages
- `Chubbypixel (Manual Website)` (static) | duration=0 ms | kept=0/1 | category=fetch_ok_extract_zero | error=no jobs extracted from source pages
### High Cost Low Yield
- None
### Coverage Risks
- `personio_sources` (personio) | duration=2159 ms | kept=0/28 | category=low_yield | error=
- `Paradox Interactive (Teamtailor)` (teamtailor) | duration=0 ms | kept=0/20 | category=low_yield | error=
- `Sandbox VR (Lever)` (lever) | duration=0 ms | kept=0/32 | category=low_yield | error=
- `Unknown Worlds Entertainment (Sheet)` (static) | duration=0 ms | kept=0/0 | category=low_evidence_skipped | error=evidence score 18 below probe threshold 22
- `Wushu Studios (Sheet)` (static) | duration=0 ms | kept=0/0 | category=low_evidence_skipped | error=evidence score 18 below probe threshold 22
- `Karizma Game Studio (Sheet)` (static) | duration=0 ms | kept=0/0 | category=low_evidence_skipped | error=evidence score 16 below probe threshold 22
- `Aither Entertainment (Sheet)` (static) | duration=0 ms | kept=0/0 | category=low_evidence_skipped | error=evidence score 12 below probe threshold 22
- `Aurory (Sheet)` (static) | duration=0 ms | kept=0/0 | category=low_evidence_skipped | error=evidence score 12 below probe threshold 22
- `BebopBee, Inc. (Sheet)` (static) | duration=0 ms | kept=0/0 | category=low_evidence_skipped | error=evidence score 12 below probe threshold 22
- `Failbetter Games (Sheet)` (static) | duration=0 ms | kept=0/0 | category=low_evidence_skipped | error=evidence score 12 below probe threshold 22

## Recommendations
- Investigate static first for fetch-time wins; it is the slowest adapter family in the baseline.
- Review discovery deferred and queue-filtered candidates next; they are immediate coverage expansion opportunities.
- Triage hard failures before adding new sources so the baseline reliability does not regress further.
