# Tools

This directory contains standalone analysis and measurement tools that are separate from:
- Build/orchestration scripts in `scripts/`
- Product code in `src/`
- Frontend code in `frontend/`

## Directory Structure

```
tools/
  mcp/               # Required Serena and Basic Memory setup plus deprecated local MCP fallback
  repo_health/       # Repository readiness and refactorability analysis
  measurements/      # Pipeline, source and desktop performance measurement
  reports/           # Generated outputs (not tracked in git)
```

## Categories

### `repo_health/`
Tools that assess the repository itself:
- **bin/** - Executable analyzers
- **profiles/** - Criteria definitions and maturity levels

Run with: `python tools/repo_health/bin/analyze_repo.py` or `python tools/repo_health/bin/analyze_refactorability.py`

### `measurements/`
Tools that run Baluffo or its pipeline and measure behavior:
- **desktop/** - Packaged desktop startup, measured from process spawn
- **discovery/** - Discovery-lane measurements (e.g., audit lane-count outcome parity)
- **pipeline/** - Pipeline-level measurements (e.g., job discovery gains)
- **sources/** - Source performance monitoring and optimization

Fetch-lane CPU attribution also lives in `pipeline/`:
- **thread_cpu.py** - samples a running bench container's fetch child per thread from the host; writes `thread-cpu.ndjson` and `thread-cpu-summary.json` into the run directory
- **cpu_attribution.py** - folds a fetch report, the in-process samples, and that thread summary into `CPU-ATTRIBUTION.md`

Together they answer "which thread, and which function, burns the CPU". They are dev-only and
stay here on purpose: a `scripts/` edit is shipped container code and trips the release gate.
Start the sampler before the pipeline request, because the fetch child does not exist until the
run starts.

```bash
python tools/measurements/pipeline/thread_cpu.py --container <name> --run-dir <run-dir>
python tools/measurements/pipeline/cpu_attribution.py --run-dir <run-dir> --wall-clock-seconds <s>
```

Revalidation is the one fetch saving a replay cannot show: a 304 removes a download, and
replay has no wire. `revalidation_study.py` therefore drives **live** arms, but pins everything
else -- one seeded registry/state, one data dir per arm, a named code tree per arm, and a paired
verdict that refuses when the arms did not produce the same work. `live_run.py` is the arm
driver it launches; it deliberately does **not** set `force_refresh_all`, because a forced run
can never answer "did the policy skip this board?".

```bash
# freeze the population (all static boards, every clock moved past its window)
python tools/measurements/pipeline/revalidation_study.py seed --study .tmp/study \
    --state <data-dir>/jobs-source-state.json.gz \
    --registry <data-dir>/source-registry-active.json --all-static --age-minutes 900

# one arm per code tree (a worktree is a before arm); arms are immutable once run
python tools/measurements/pipeline/revalidation_study.py arm --study .tmp/study --name a --code .
python tools/measurements/pipeline/revalidation_study.py arm --study .tmp/study --name b \
    --code ../before-tree --strip-validators

# what the arm's state holds now, and what the tagless zero-kept boards answer
python tools/measurements/pipeline/revalidation_study.py census --study .tmp/study --name a
python tools/measurements/pipeline/revalidation_study.py probe --study .tmp/study --name a

# paired deltas + verdict (inconclusive when the arms' kept/output counts diverge)
python tools/measurements/pipeline/revalidation_study.py report --study .tmp/study --compare a:b
```

`census` prices a cohort from the run's own `durationMs`. Never use `lastDurationMs` for this:
it reads 0 for a row whose last run was a skip, so a skipped cohort would be priced at zero.

`probe` asks each page of the tagless cohort three questions, and the third is what makes the answer
readable: `plain` is a HEAD with no condition (it can only ever be a request), `since` carries the
board's own read clock as `If-Modified-Since`, and `old` carries a 2015 date. A 200 in `since` is
only evidence about the header if `old` also answered 200 — otherwise the host is honouring
`If-Modified-Since` and `since` was simply too recent. Compare `since` against `plain` to price the
fallback inside a sweep that already probes, and against the cohort's read cost to decide whether
the board should be probed at all.

### `measurements/discovery/`
The GameDevMap active audit's homepage phase is volume-bound, so a wider
`activeAuditHomepageFetchConcurrency` makes it faster. `lane_parity.py` asks the next question:
whether the wider arm also changes *which boards* the audit validates. Live hosts make that
unanswerable in one run, so the tool pins what it can and measures the rest -- every arm audits
the same first N rows (proven by a population digest and the CSV's SHA-256, not by the
arguments), only the lane count differs, each arm gets its own data dir, and a pair's order is
counterbalanced. `arm` proves its lane count from the artifact's own `phaseRuntime` receipt and
refuses to be compared if the receipt does not show it; `report` judges the pairs against the
study's **same-lane churn** rather than a threshold carried over from an earlier study, and
attributes every differing board to its record in the other arm -- a host 429 or a probe rejection
matched by candidate identity and endpoint, not by studio label, and never a sibling candidate's
zero-job row. A 429 names the host that failed; it is not proof the lane count caused it, because
the same host also throttles at one lane count and not at another.

```bash
# the whole measurement: N counterbalanced pairs, low then high, high then low, then a verdict
python tools/measurements/discovery/lane_parity.py study \
    --study _out/perf-runs/discovery/lane-parity --pairs 2

# or run arms separately (immutable once written; --force re-measures a whole study)
python tools/measurements/discovery/lane_parity.py arm \
    --study _out/perf-runs/discovery/lane-parity --name p1-64 --lanes 64
python tools/measurements/discovery/lane_parity.py report \
    --study _out/perf-runs/discovery/lane-parity --pairs p1:p1-32,p1-64 p2:p2-32,p2-64

# per-host request and 429 windows for a traced pair (tracing is on by default)
python tools/measurements/discovery/lane_parity.py trace \
    --study _out/perf-runs/discovery/lane-parity --pairs p1:p1-32,p1-64
```

Every arm records a `request-trace.jsonl` beside its artifact -- phase, host, status and
start/end for each request -- and `trace` prints both arms' request counts and 429 rates by
phase. Each phase also reports how many hosts it asked, how many were **fully** throttled (every
request 429) versus **partially**, and the window its 429s occupied as a share of the phase plus
the ten time bins that held one. A rate difference is a count spread across many hosts and bins;
an episode is a short window holding most of the 429s. Per-host rows are limited to hosts asked
more than once (top six, full list in `--json`), because a one-request homepage host's 429 is
already counted in the phase's fully-throttled total. The page phases come from each job's own
`failureStage` and the probe phase from the retry-called fetcher, so a retry is its own row. The
`episode`/`spread` label is a heuristic (429 window at most 25% or at least 50% of the phase's or
host's request span) and is printed beside the raw window; `--no-trace` skips recording.

`report --screen-429-free` (the same flag on `study`) adds a second read beside the raw one, over
only the boards whose hosts never answered 429 in either arm's trace: it drops every board whose
host matches a throttled one after `www.` is stripped and through parent/subdomain in either
direction, so it over-excludes rather than under-excludes. A difference that survives the screen
is not a 429; a pair with no clean boards on either side returns `429-free-population-empty`
instead of claiming parity. The flag requires traced arms and does not change the raw verdict.

Read failure counts from the artifact's whole-run `failureCounts`/`failureErrorCounts`, never
its `failures` list: that sample is capped at 200 rows and understated one pair's homepage
change by 7x. The studies that answered P6's R4 — the recorded arms, the end-to-end re-run
through `study`, the traced pairs and the 429-free screen study — are in
`docs/snapshots/discovery-64-lane-parity-2026-10-26.md`.

### `measurements/corpus/`
The hermetic fetch lane: record one real run's HTTP into a content-addressed corpus, then replay
it as many times as you like with no network at all. This is what turns "is this change faster?"
into a measurable question, because replay removes the board drift that made every earlier answer
inconclusive.

```bash
# 1. record one real run (bounded, labelled; writes _out/perf-runs/corpus-<scope>-<stamp>/)
python tools/measurements/corpus/run_fetch.py --mode record --scope subset \
    --label "subset epoch" --sources 4 --families google_sheets --max-workers 1

# 1b. record a *reference* epoch: every registered loader, not a stride sample. The flag is
#     mutually exclusive with --only-sources and supersedes --families/--sources, which are
#     then inert (the effective count is coverage-report.sourceCount). A record runs on the
#     pipeline's worker pool, so the store and ledger are lock-guarded for --max-workers > 1.
#     Every knob that bounds a phase's *work set* has to be set high enough that it cannot bind
#     in either mode, and passed identically everywhere -- a clock-bounded work set is not one a
#     replay can reproduce. Two such knobs, both recorded in runConfig:
#       BALUFFO_STATIC_SOURCE_TIME_BUDGET_S (runConfig.staticSourceTimeBudgetS) -- a source
#         truncated live keeps exploring in replay until the run fails with a corpus miss;
#       --google-sheets-redirect-concurrency (runConfig.googleSheetsRedirectConcurrency) -- the
#         resolver abandons a failing host after a bounded number of response-less failures, so
#         a small pool records only a *prefix* of that host's redirect candidates and the replay#     asks for the rest (measured: 199 of 465 at the default 16, 465 of 465 at 64).
#     This is enforced, not just advised: a record whose audit finds either bound to have cut work
#     refuses the delta-referee certification (exit 3) and names the knob; --accept-truncated-referee
#     records the epoch anyway, marked accepted-but-truncated in EPOCH.json.
#     A record that *does* certify the epoch tests that claim before the manifest is final: one
#     fresh replay of the epoch just recorded (the same check replay_check.py runs), attached to
#     EPOCH.json as replayVerification with the receipt under <epoch>/referee-verification/. A
#     birth replay that does not verify refuses the record like the audit refusal does: the epoch
#     is written, the record exits with the check's own code (1 falsified, 2 unjudgeable), and the
#     epoch is moved to a sibling rejected/ directory, where no listing scans and it supersedes
#     nothing; --accept-unverified-referee keeps it in place instead, labelled
#     accepted-but-unverified in EPOCH.json, and a study still re-runs the check before pinning.
#     --no-referee-verification skips it. An epoch whose certification the audit already refuses
#     is not checked at birth: there is no certification to test.
#     A record also declares the one gap a replay cannot close: request keys the run answered more
#     than once with *different* responses. The store writes each replacement to
#     <epoch>/replacements.jsonl as it overwrites the entry, and conflicts.py joins those lines to
#     the bodies no entry points at, reporting both per host. The count travels in EPOCH.json as
#     conflicts and as a key_conflict note in deltaReferee.notes -- declared, never a refusal:
#     the differing responses are what the boards served, and no ordering or store policy removes
#     them (keep-first is as wrong as keep-last when four callers saw four renders).
#     A drift that declaration explains is *bound* rather than refused. binding.py reads the
#     record's own rows (through EPOCH.json.recordedRunDir) and the replay's, attributes every
#     differing row to the host of the source that carried it, and replay_check answers
#     `parity-bound` -- a labelled pass, never `verified` -- only when every one of those hosts is
#     one the record declared. One differing source outside the set refuses it, as do a differing
#     row that names no source host, a record whose rows are gone, and an epoch that declares
#     nothing. A bound birth replay keeps the epoch in the parent and supersedes nothing, and the
#     same bind is applied to the A/B's arms, so a pair whose arms both drift inside the declared
#     class *and agree with each other* still yields a verdict.
#     The full-source epoch's own replay bound, and the probes behind those numbers, are in
#     docs/snapshots/fetch-pipeline-full-source-epoch-2026-10-09.md.
python tools/measurements/corpus/run_fetch.py --mode record --scope full --all-sources \
    --label "full-source reference epoch" --max-workers 12 \
    --google-sheets-redirect-concurrency 64

# 2. replay it (hermetic: any outbound socket raises, and the run records how many tried)
python tools/measurements/corpus/run_fetch.py --mode replay --scope subset \
    --corpus-dir _out/perf-runs/corpus-subset-<stamp>

# 3. prove replay is deterministic, and measure the two instrumentation ceilings (D1)
python tools/measurements/corpus/determinism.py --epoch <epoch-dir> --samples 3 --ceiling-runs 2

# 4. compare two configurations, or two code states, in one command
#    (--resume reuses pairs that already finished under the same study directory: an interrupted
#    study rebuilds each reused pair from its own report window instead of re-running it)
python tools/measurements/corpus/ab.py run --epoch <epoch-dir> --pairs 6 \
    --arm-a-concurrency 32 --arm-b-concurrency 48 [--resume]

# 5. rank bottlenecks from any past run token
python tools/measurements/corpus/explain.py --run <token-or-dir> --top 8

# 6. adjudicate the shipped pool claim on this harness (S0)
python tools/measurements/corpus/adjudicate.py --epoch <epoch-dir> --pairs 6

# 7. ask which recorded epoch can referee a delta, and what would certify the rest
python tools/measurements/corpus/referees.py [--require-eligible] [--json <path>]

# 8. verify the listing's claim instead of trusting it: every epoch the listing calls eligible
#    must actually replay, end to end, without a corpus miss -- one fresh process per epoch, each
#    under that epoch's own recorded configuration
python tools/measurements/corpus/replay_check.py [--samples 2] [<epoch-dir> ...]

# 9. price a candidate change *before* committing to it: an ablation payload rides one arm
#    through PYTHONPATH, so src/ is untouched and the arms differ by one variable. Both arms get
#    the PYTHONPATH; only the candidate sets the ablation. The probe writes a marker per process,
#    so a payload that never applied cannot read as a measured zero.
PROBE_DIR="$PWD/tools/measurements/corpus/probes/row_dict"
python tools/measurements/corpus/ab.py run --epoch <epoch-dir> --pairs 4 \
    --arm-a-label control --arm-b-label candidate \
    --arm-a-env "PYTHONPATH=$PROBE_DIR" \
    --arm-b-env "PYTHONPATH=$PROBE_DIR" --arm-b-env "BALUFFO_ROW_DICT_ABLATION=cached" \
    --arm-b-env "BALUFFO_PRICE_PROBE_MARKER=<study-dir>/markers"
```

What the artifacts mean:
- **`run_fetch.py`** writes the run's own `jobs-unified.json.gz`, `jobs-fetch-report.json` and a
  `replay-parity.json` (four parity units: row count, job identities, a run-independent row
  projection, and per-source status counts), plus the epoch's `EPOCH.json`, `coverage-report.json`,
  `size-report.json` and `truncation-report.json`.
- **`truncation-report.json`** is the record-time check for the one gap a corpus cannot show about
  itself: work a bounded phase *dropped*. It has two families. **Redirect:** every kept row's
  supported-redirect link is a candidate the resolver was handed, so a candidate the corpus holds
  no request for is a request the run decided not to make — invisible to replay until it misses.
  **Static boards:** a per-source clock or a detail cap drops page and detail work the same way.
  There the run's own report is the declaration — a refusal naming the URL the clock cut, or
  counters for detail pages a cap skipped — and the corpus verifies the refused URLs it holds no
  request for. A clock-stopped source is the sharper case: replay removes the wall clock, so it
  re-runs *past* the recorded stop and asks for exactly those URLs (measured: the 90s-clock epoch's
  replay died on the URL its report had named). A record is still written when either family flags
  (a truncated phase is still a coverage record), but it refuses to certify the epoch as a **delta
  referee**: those two findings are bounds on failure pacing that replay removes, so the record
  exits 3 and the refusal names the knob to raise. A declared cap does not refuse it — both cap
  paths are count-driven, so replay reproduces the drop — and travels as a note in `EPOCH.json`'s
  `deltaReferee` block beside the blockers. Re-record with the knob raised, or pass
  `run_fetch.py --accept-truncated-referee` to record the epoch anyway, marked
  accepted-but-truncated. **Which epoch may referee is a query, not a reading exercise**:
  `referees.py` lists every recorded epoch with its verdict, states where each verdict came from
  (`EPOCH.json`'s block, the epoch's own stored `truncation-report.json`, or the audit re-run over
  its artifacts now -- the last of which is what makes epochs recorded before the check answerable
  at all), and names the knob that would certify a refused one. An epoch nobody can judge is listed
  as such rather than dropped, and `--require-eligible` exits 3 when nothing can referee, so a
  study can gate on the answer instead of parsing it. **That verdict is a prediction until a
  replay tests it**: `replay_check.py` runs one fresh replay per eligible epoch — each under the
  epoch's own `recordedConfig` where it has one, so a stride sample cannot silently move under a
  changed registry — and calls it `verified` only when the run completed, the run's own ledger
  shows `requestsMissed == 0` and `unattributedCount == 0`, and today's code reproduced the
  record's four parity units (the precondition `ab.py` refuses a study over). A miss is read from
  the ledger rather than the exit code, because a source that swallowed the `CorpusMiss` still
  shows up there. Falsified epochs (`corpus-miss`, `unanswered`, `pipeline-error`,
  `parity-diverged`) exit 1 and name the URLs; an epoch that could not be judged
  (`egress-blocked`, `harness-error`) exits 2 and is never reported as a finding about the corpus;
  an empty eligible set exits 3, the same refusal a record makes about itself. An epoch recorded
  before `recordedConfig` existed is replayed on the driver defaults and its row says
  `configSource: defaults` — a weaker statement, labelled rather than blurred. **The certification is
  a precondition, not advice**: `ab.py run` (and `adjudicate.py`) verify their pinned epoch before
  the first pair — one fresh replay through the same check, refusing with the check's own codes
  (1 falsified, 2 unjudgeable, 3 the listing refuses the epoch) and leaving the check's report plus
  a receipt under the study's `referee-verification/` — so no pair is paid for on an epoch the
  replay did not substantiate. A record runs the same check on itself before finalizing, so a
  certified epoch is born with its verdict attached (`EPOCH.json.replayVerification`, receipt under
  the epoch's `referee-verification/`) and the listing shows it in a `birth replay` column. A birth
  replay that does not verify refuses the record the way the audit refusal does — the epoch is
  written, the record exits with the check's own code (1 falsified, 2 unjudgeable) — and the epoch
  is moved, whole, to a sibling `rejected/` directory with a `quarantine` block saying why: no
  listing scans it and it supersedes nothing, so a parent's certified epochs survive a failed
  re-record. The move retargets every path the epoch carries -- the receipt, the audit reports and
  the sample run's own artifacts -- to where the evidence now lives, because a pointer into the
  directory the move removed is worse than no pointer; the `quarantine` block's own
  `originalDir`/`movedTo` remain the record of the move. `--accept-unverified-referee` keeps it in place instead, labelled
  accepted-but-unverified: a receipt for the operator, never a permission a study trusts, since the
  study re-runs the check. **`parity-bound` is the second pass, and it is not `verified`**: the
  replay did not reproduce the record, and every row it differs by belongs to a host the record's
  own conflict audit declared as a key conflict — `binding.py` reads the residue from the record's
  rows and the replay's, attributes each differing row to the host of the source that carried it
  (its `source`, and every `sourceBundle` entry that also published it, falling back to the job
  link), and refuses the bind on a single differing source outside the declared set, on a differing
  row that names no source host, when the record's rows are no longer on disk, and when the epoch
  declares nothing. The verdict is host-level, so a divergence inside a declared host for another
  reason is bound too — which is why the block counts the fields that moved. The receipt keeps the
  per-host evidence, the manifest keeps the compact form, and the study applies the same bind to
  its arms: a pair whose arms both drift inside the declared class *and agree with each other* is
  still a comparison of the same work, and a unit the arms disagree on is refused whatever either
  arm's own residue looks like. Both are floors,
  not full audits: the redirect family's candidates come from kept rows and the resolver's own
  rule, while the static family can only see
  drops the report names or counts — a cap that drops candidates silently leaves no trace to read. `coverage-report.json` states what the corpus does *not* cover —
  discovery, browser fallback (D9) and the async httpx transport — so "we replayed the fetch lane"
  cannot be misread as "we replayed all traffic".  `recordedConfig.allSources` records whether the
  epoch covers the whole registry or a sample; `ab.py run --all-sources` replays it that way, and
  `--resume` refuses to reuse an arm recorded with the other selection. `runConfig` states the
  work-bounding knobs in force (`staticSourceTimeBudgetS`, `googleSheetsRedirectConcurrency`) so
  an arm that does not share them is visible before it misses.
- **Egress bar.** Replay arms a socket-level guard: every outbound connect raises and is counted.
  `blockedRemote == 0` is the bar; loopback attempts are counted separately, and
  `socket.socketpair()` (asyncio's event-loop self-pipe) is allowed and reported as
  `socketpairAllowed`, because blocking it killed every `asyncio.run` in the lane.
- **Not production.** Record and replay both use `force_refresh_all`, so this is the full-fetch
  workload; replay also removes network time, which means it can refute a wall-clock claim but
  never confirm one. Every report says so.
- **Epochs are captures, not caches.** One recorded day, local-only under `_out/perf-runs/`, never
  committed, and a re-record is a *different* corpus — keep the one a disputed claim refers to.
- **Repeated-fetch exposure**, reported beside every verdict, is the share of summed source time
  spent in sources whose `httpCounters.repeatFetches` is non-zero — a retry or an intentional
  re-fetch of the same URL, which the seam cannot tell apart. It is a proxy for the backoff that
  still sleeps in replay; it is not a retry count.
- **Priced ablations.** `probes/` holds dev-only payloads that measure a *candidate* change without
  shipping it: an ablation is injected into one arm through `PYTHONPATH` (`sitecustomize` is
  imported before the driver runs), the control arm gets the same path with nothing armed, and the
  two arms differ by exactly one variable. The payload's docstring is the contract; the two
  properties that matter are that the arms' parity line is also the end-to-end equivalence check
  for the ablation, and that every payload writes a per-process marker (calls, builds,
  mismatches) because an instrument that silently failed to apply reports a confident zero — the
  failure mode S1 was corrected for. `probes/row_dict/` carries the row-payload ablation, priced
  at **−25.7%** engine wall clock on the reference subset epoch with parity intact; the decision
  and its limits are in
  [the snapshot](../docs/snapshots/fetch-pipeline-next-optimisation-price-2026-10-10.md).

Instrumentation flags: `BALUFFO_FETCH_COUNTERS=1` enables the per-source HTTP counters (off by
default, aggregated per source, added to the report as `httpCounters`); `BALUFFO_PROFILE=1` is the
opt-in profiler. Both are measured against their D1 ceilings by `determinism.py`, and the driver
sets `BALUFFO_BROWSER_POOL=0` for both modes because Playwright is out of corpus scope.

### Live provider revalidation smoke
`tools/provider_revalidation_smoke.py` proves one registered provider feed earns a validator from
its own full read, and then revalidates with it. It runs two phases against an isolated state file:
a forced full read must leave `lastHttpEtag` / `lastHttpLastModified` in
`jobs-source-state.json`, and a second run with that state aged into the provider's stable window
must be decided `revalidate_only`, fetch no listing body, and be recorded as
`cache_not_modified_304`.

```bash
python tools/provider_revalidation_smoke.py --adapter greenhouse --source atariinc
python tools/provider_revalidation_smoke.py --adapter lever --list
```

Output goes under `_out/provider-revalidation-smoke/<adapter>-<board>/`; the tool refuses a path
inside `data/` because that is live state. It drives the one provider loader, the shipped transport
and the real state writer, but skips dedup, availability and output assembly. A feed that publishes
no `ETag` / `Last-Modified` fails on that cause alone. Exit 0 means the tag was stored, the decision
was revalidation, and the second run answered 304 without fetching the listing.

`--all` sweeps every feed the adapter registers instead of one. Each feed gets its own isolated
state directory under `_out/provider-revalidation-smoke/<adapter>-sweep/`, and is classified by what
its own conditional read proved: `honours` (304, no body), `ignores` (a validator is published but
`If-None-Match` is answered with a body), `probe_refused` (the probe was rejected, e.g. HEAD 405),
`no_probe` (the runner never sends `If-None-Match`), `no_validator`, `read_failed`, or
`inconclusive` (the policy did not reach `revalidate_only`). Per-feed summaries and `sweep.json`
are written beside the state. Exit 1 means some feed publishes a validator and then ignores it; a
refused probe is reported, never failed, because a rejected HEAD says nothing about the tag.

```bash
python tools/provider_revalidation_smoke.py --adapter lever --all
```

### `mcp/`
Repo AI tooling guidance plus a deprecated local MCP fallback for editor-assisted workflows.
Use `tools/mcp/INDEX.md` as the MCP tooling entrypoint.
Use `tools/mcp/SERENA.md` for the required Serena setup used by Codex CLI and OpenCode.
Use `tools/mcp/BASIC_MEMORY.md` for the required Basic Memory continuity setup used by Codex CLI and OpenCode.
Use Codex's built-in in-app Browser and Developer mode for Codex browser inspection; do not add Chrome DevTools MCP or `@playwright/mcp` for Baluffo. The local Playwright server entrypoint is retained only as a deprecated non-Codex fallback at `tools/mcp/playwright-server.cjs`, with setup notes in `tools/mcp/PLAYWRIGHT.md`.

### `reports/`
Generated outputs (JSON reports, measurement results). This directory
is typically gitignored - do not commit generated files here.

## Quick Reference

| Task | Tool Location |
|------|---------------|
| Browse MCP tooling docs | `tools/mcp/INDEX.md` |
| Set up required Serena MCP tooling | `tools/mcp/SERENA.md` |
| Set up required Basic Memory continuity | `tools/mcp/BASIC_MEMORY.md` |
| Use Codex browser inspection | `docs/AI_ASSISTANT_GUIDE.md` / Codex in-app Browser Developer mode |
| Analyze repo maturity | `tools/repo_health/bin/analyze_repo.py` |
| Analyze refactorability | `tools/repo_health/bin/analyze_refactorability.py` |
| Run the deprecated non-Codex Playwright MCP fallback | `tools/mcp/PLAYWRIGHT.md` / `node tools/mcp/playwright-server.cjs` |
| Summarize latest discovery/fetch run | `python tools/measurements/pipeline/latest_run_report.py` |
| Measure pipeline discovery gains | `tools/measurements/pipeline/job_discovery_increment_measurement.py` |
| Sample fetch-lane CPU per thread | `python tools/measurements/pipeline/thread_cpu.py --container <name> --run-dir <run-dir>` |
| Aggregate a fetch-lane CPU attribution report | `python tools/measurements/pipeline/cpu_attribution.py --run-dir <run-dir>` |
| Record/replay the fetch lane hermetically | `python tools/measurements/corpus/run_fetch.py --mode record|replay ...` |
| Prove replay determinism + D1 ceilings | `python tools/measurements/corpus/determinism.py --epoch <epoch-dir>` |
| A/B two configurations over replay (verifies the referee first) | `python tools/measurements/corpus/ab.py run --epoch <epoch-dir> --pairs 6 [--resume]` |
| Rank bottlenecks from a past run | `python tools/measurements/corpus/explain.py --run <token-or-dir>` |
| Find which epoch can referee a delta | `python tools/measurements/corpus/referees.py [--require-eligible]` |
| Verify the eligible epochs really replay | `python tools/measurements/corpus/replay_check.py [--samples 2]` |
| Adjudicate the shipped pool claim | `python tools/measurements/corpus/adjudicate.py --epoch <epoch-dir>` |
| Price a candidate change before committing it | `tools/measurements/corpus/probes/row_dict/sitecustomize.py` via `ab.py run --arm-b-env` |
| Test whether a wider audit lane count changes which boards are validated | `python tools/measurements/discovery/lane_parity.py study --study <dir>` |
| Live-check provider ETag capture + 304 revalidation | `python tools/provider_revalidation_smoke.py --adapter <adapter> --source <board>` |
| Sweep an adapter for feeds that ignore If-None-Match | `python tools/provider_revalidation_smoke.py --adapter <adapter> --all` |
| Monitor social sources | `tools/measurements/sources/social_sources_monitoring.py` |
| Optimize social sources config | `tools/measurements/sources/social_sources_optimization.py` |
