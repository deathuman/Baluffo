> - **Class:** performance-tooling
> - **Trigger:** the shipped pool-tuning claim (`61c43fab`, "−11.3% wall") rests on a replay that is **not hermetic** — the frozen seed carries state and rows but no HTTP responses, so live board drift sits inside the variance. The 2026-09-25 paired benchmark reached the same conclusion and left the fix open: *"a larger controlled/replayed timing study is required."*
> - **Verified against:** `61c43fab`
> - **Status:** **Executed 2026-10-08 (S1–S7); S0 adjudicated on the harness.** Plan: design and acceptance criteria. Receipts: [corpus evidence for S2–S7 and S0](../snapshots/fetch-pipeline-corpus-evidence-2026-10-08.md), [S1 attribution](../snapshots/fetch-pipeline-cpu-attribution-2026-10-08.md). S0's verdict is **inconclusive by harness**, so `61c43fab` stands unverified rather than disproven.

# Fetch pipeline performance and debug instrumentation plan

Canonical plan for making Baluffo fetch-pipeline performance work **measurable, cheap to
inspect, and cheap to A/B**. It proposes no speedup of its own: its output is trustworthy
evidence, which the current tooling does not produce.

See [`INDEX.md`](../INDEX.md).

---

## Decisions taken

Settled 2026-10-08. These are constraints on the implementation, not preferences.

| # | Decision | Consequence |
|---|---|---|
| D1 | **Runtime overhead: always-on ≤0.5%, opt-in ≤2%.** | Two ceilings, both *demonstrated* in S6. A slice missing its ceiling ships counters disabled. |
| D2 | **If −11.3% is refuted: release note + Basic Memory correction.** No history rewrite. | `61c43fab` is pushed. No force-push, tag move, or amend. |
| D3 | **S0 deferred until replay exists.** | A non-hermetic study cannot produce a trustworthy positive, so it cannot precede the tooling that would settle it. |
| D4 | **First optimisation target chosen after S1.** | Nothing is pre-selected; the choice follows evidence, not the current GIL inference. |
| D5 | **Record and replay both use `force_refresh_all`.** | Removes clock dependence and makes the corpus cover every registered source. An existing parameter (`state_incremental.py:178`), not a new seam. See [The clock gap](#the-clock-gap-the-one-that-would-have-silently-broken-replay). |
| D6 | **Corpus scope is the pipeline fetch lane only.** | Discovery, Admin registry API, maintenance scripts and browser fallback are out — and must be *reported* as uncovered. |
| D7 | **Replay A/B does not join per-PR CI.** | A gate already exists; replay needs an irreplaceable artifact. Runs on schedule or dispatch. |
| D8 | **"Every source runs every run" remains a live requirement.** | Met but unenforced. S2 publishes the histogram; a no-skip assertion is proposed. See [The no-skipping requirement](#the-no-skipping-requirement-met-in-practice-unverified-in-policy). |
| D9 | **Browser fallback and discovery are out of scope.** | Confirmed: a Node driver outside the seam, and a separate transport with its own harness. |
| D10 | **Corpus epochs follow the `_out/perf-runs/` token convention.** | Derived from precedent, not invented — see [Retention](#retention-a-corpus-is-not-reproducible). |

D3 and D4 together mean the first deliverable is **knowledge, not speed**: S1 decides what to
optimise, S6 decides whether any result can be believed.

## Why this plan exists

Three facts, in order of how much they cost us.

**1. A performance claim shipped without a reproducible experiment.** `61c43fab` carries
"−11.3% wall clock" from four Docker arms. The seed at `%TEMP%\blf-fullseed` is
`jobs-source-state.json`, `.pipeline-fetched-rows.jsonl`, lifecycle state and registry files —
**no recorded HTTP responses anywhere**. So every arm still hit live career sites. Row counts
across those arms moved 41,098 → 41,506 on their own. An 11% effect is not separable from that
drift. The number may be real; it is currently undefendable, and it is in a released commit
message.

**2. We optimise against an unproven premise.** The run is assumed GIL-bound: CPU sits at
~1.06 of 2.0 cores, 12→20 workers moved CPU by 0.1%, 4 async lanes was *worse*, and all HTTP
funnels through one event loop in one thread (`src/jobs/transport.py:413-414`). That is a
coherent story, but **no profile has ever attributed that core to specific code.** Every
optimisation decision so far has been made on that inference.

**3. The measurement loop is slow and lossy.** A single full-path arm is ~37 min. Four arms is
2.5 hours and still inconclusive. That is why we shipped an unverified number: the cost of
doing it properly exceeded the cost of guessing.

## What already exists

Verified by reading the code, not by assumption. **Do not rebuild any of this.**

| Layer | Entry point | Scope | Hermetic? |
|---|---|---|---|
| Real-path container bench | `scripts/perf_pipeline_stages.py` | `POST /tasks/run-jobs-pipeline`, stage ledger, host CPU/RSS sampling, `pi4-tight` / `pi4-roomy` / `pi4-roomy-3g` | seeded volume, **live HTTP** |
| Broad suite coordinator | `npm run perf:complete` → `scripts/perf_complete.py` + 9 leaf modules | consolidated report; storage / bridge / process / fetch-timing / profile sub-benches; `--record-trend`, `--record-baseline` | live |
| CI gate | `npm run perf:ci` → `scripts/perf_ci.py` (`--runs 3`) | median-of-N against a stored baseline | live; medians blunt drift only |
| Compare / baseline / trend | `perf_compare.py`, `perf_baseline.py`, `perf_trend.py` | A/B compare, baseline records, trend reporting | live |
| Isolated fetch bench | `npm run perf:fetch:*` → `src/fetch_incremental_sanity_benchmark.py` | two-pass incremental; `sourceDecisionMatrix`, `nextOptimizationTargets`, `timeoutDiagnostics`, `slow_productive_static` | live, curated source groups |
| Isolated discovery bench | `src/discovery_sanity_benchmark.py` | presets incl. `capped-gamedevmap` | live, **separate subsystem** |
| cgroup memory profile | `tools/measurements/pipeline/memory_profile.py` | cgroup peaks, `samplerRestarted`, max sample gap | live |
| Policy replay | `tools/coverage_freshness_replay.py` | drives the real cache-decision function over a simulated cadence | **hermetic** |
| Narrow fixture replays | e.g. malformed-pagination parity (`c9a28f94`) | outcome parity for one subsystem | hermetic, narrow |
| Profiling hooks | `src/shared/profile_utils.py`, `BALUFFO_PROFILE=1` | cProfile / tracemalloc capture, sanitised naming, `_out/perf-profiles/` | opt-in |
| Stage timing | `lastStageTimingsMs` per source, `stageLedger` on the lifecycle row | per-source stage attribution | always on, additive |

**The gap, stated precisely.** Hermeticity exists for *policy functions* and for *single
subsystems*. It does not exist for the *pipeline HTTP path*. Confirmed absent repo-wide: no
`cassette` / `vcr` / `respx` / `MockTransport` / `httpmock`, and no `--offline` / `--replay` /
`--fixture` / `--http-cache` flag in `scripts/`.

## The diagnosis being optimised against

Measured, with confidence stated — because this session produced several confident wrong
answers (host contention, sequential sources, "truncated" diagnostics) and the plan is stronger
for admitting which parts are load-bearing.

| Finding | Evidence | Confidence |
|---|---|---|
| All HTTP runs in **one** event loop, one thread | `transport.py:413-414`, `:475-478` | **High** — read in code |
| ~1.06 of 2.0 cores used; workers idle ~39% of capacity | 305 of 498 worker-min; `docker stats` CPU median | **High** |
| Extra threads buy nothing | 12→20 workers: CPU 106.4% → 106.4% | **High** — controlled arm |
| Per-domain politeness gate is never contended | 1,663 distinct hosts for 1,739 active sources; whole registry forces 14 extra waves past a 3-permit gate | **High** — measured |
| Cost is extremely long-tailed | top 10 sources = 16.6% of worker-time; 9 sources over the nominal 90 s budget = 16% | **Medium** — from a 2026-04-10 state slice, predates current instrumentation |
| Zero-kept sources dominate cost | prior program: 989 sources / 48.1% of rows / **37.71%** of summed duration; this session: 33% / 29.7% on a single-run slice | **High** — two independent measurements agree |
| **36%** of boards carry a validator | 2 508-source run: **1 789 of 4 910** state rows; 959 tagless zero-kept rows have no clock | **High** — [snapshot](../snapshots/fetch-pipeline-revalidation-population-2026-10-25.md) |
| ~~Conditional GET unwired for static~~ **Resolved 2026-10-23; zero-kept lane and tagless probe measured 2026-10-25** | Kept boards past 180 min and zero-kept past 12 h ask via HEAD; a 304 skips parse. The zero-kept confirming read (**~99 s** net) now ships ([2026-10-24](../snapshots/fetch-pipeline-zero-kept-revalidation-2026-10-24.md)); the read clock rides a sweep's HEAD, but a probe of its own is **closed** — **−838 s** per empty window, as 1 134 of 1 179 boards ignore `If-Modified-Since` ([2026-10-25](../snapshots/fetch-pipeline-revalidation-population-2026-10-25.md)) | **High** — live A/B and cohort |

**S1 settles it and revises two rows:** the loop thread holds only **3.2%** of child CPU, the seat was **~50% used**, and the top cost is `re.sub` normalisation at **31.6%** — the event loop is real but is not the constraint.

## The clock gap: the one that would have silently broken replay

The pipeline's *fetch set* is a function of the wall clock. `get_incremental_cache_decision`
computes `now_dt = datetime.now(UTC)` (`state_incremental.py:185`) and skips any source whose
`nextEligibleCheckAt` is still in the future (`:186-189`); ages are recomputed against the same
clock at `:100`, `:122`, `:125`, `:157`. A replay therefore does **not** reproduce the recorded
run on a different day — it silently fetches a different set of sources, producing neither a
parity failure nor a clean pass, just a different workload wearing the same report.

| Option | Verdict |
|---|---|
| Inject a clock (`now_fn`) so replay can pin time | **Rejected.** `AGENTS.md`: *"Do not expand root-injection or root monkeypatch seams."* |
| Backdate seed timestamps per replay | **Rejected.** Each replay's input would differ from the recorded input, so the seed stops being the control. |
| **Replay with `force_refresh_all`** | **Adopted (D5).** Already a parameter of the real decision function, short-circuiting to `run_now` at `:178-179`. No new seam. |

**Two consequences.** Replay measures the **full-fetch** workload, not a scheduled run — correct
for A/B, since both arms see identical conditions, but an absolute replay figure is not a
production figure and every report must say so. And a corpus recorded from a *normal* run lacks
every skipped source, so record must use `force_refresh_all` too — the single largest driver of
corpus size.

## The no-skipping requirement: met in practice, unverified in policy

Per D8, *"every source runs every run"* remains a live requirement, so it is worth being precise
about what the code does.

The skip mechanism is real: `nextEligibleCheckAt > now` skips any source with a success history,
reported as `skip_fresh` / `within_freshness_window` (`state_incremental.py:186-189`,
`:210-216`). In the available seed the dominant run skipped **503 of 1,098 sources (45.8%)**.

**That figure is pre-fix and must not be quoted as current.** The seed's dominant run is dated
2026-04-10, before the fix in `tools/coverage_freshness_replay.py` under which a skip pushed a
board's deadline forward forever (1,892 of 2,013 boards skipped, median last success 121 days
old). The most recent full acceptance run contradicts it outright:
`_out/perf-runs/full-confirm-3g-20260924-010015/FINAL-REPORT.md` records **"2,056 / 2,056
resolved — 1,911 `ok`, 145 `error`"**, i.e. **zero `excluded`**.

So the honest position is a three-way split:

| Claim | Status |
|---|---|
| Current code skipped nothing in the last full acceptance run | **Evidenced** (Sept 24, post-fix) |
| The skip mechanism can skip ~46% of the catalogue when its pre-fix pathology is live | **Evidenced** (April seed) |
| No-skipping is *enforced*, rather than incidental | **False** |

That last row is the real finding. `tools/coverage_drain.py` already makes skips **observable** —
`:516` defines `_SKIP_DECISIONS` and `:632` buckets a source as `not_selected`, deliberately
separable from `fetched_empty` so "never asked" is never confused with "asked and empty". But it
is a *report*, not a gate. Nothing fails when a run skips sources.

Two consequences for this plan:

1. **S2 must publish a per-run `cacheDecision` histogram.** It is the cheapest instrument for
   both this requirement and corpus sizing.
2. **A no-skip assertion belongs in the acceptance path** — fail when any source lands in
   `not_selected` without a sanctioned reason. This is small, and it converts an incidental
   property into an enforced one. Recorded here rather than silently assumed.

**A useful side effect:** production fetches essentially everything, so replay's
`force_refresh_all` (D5) is representative rather than an approximation.

## CI is not a gap: it already exists

Whether perf verdicts should ever gate CI was raised as an open question. They already do.

`.github/workflows/benchmark.yml` runs on PRs to `main`, path-filtered to `src/bridge/**`,
`src/jobs/**`, `src/source_discovery/**`, the two sanity benchmarks and the perf scripts. It runs
the discovery and fetch **smoke** benchmarks, compares each against a stored baseline via
`scripts/perf_compare.py`, and uploads artifacts. There is **no `continue-on-error`**, and
`perf_compare.py:155` returns `1` when `status == "failed"` — so a regression **fails the job**.

Per D7, replay A/B does **not** join that gate: it needs a corpus artifact, and a corpus is not
reproducible (see [Retention](#retention-a-corpus-is-not-reproducible)). It belongs on a
schedule or `workflow_dispatch`. The existing smoke gate stays as-is.

**Prior-art correction.** `docs/archive/end-to-end-benchmarking-plan.md` lists "CI smoke
benchmark" under *Acknowledged Gaps* as "intentionally deferred until signals are stable". That
is **outdated** — `benchmark.yml` has since shipped and gates.

## Retention: a corpus is not reproducible

A corpus cannot be regenerated on demand. Delete it and the only way back is a fresh live run —
which produces a **different** corpus, because the boards have changed. So a corpus is closer to
a capture than to a cache, and deserves deliberate retention rather than pruning.

Policy: keep **one named reference corpus per measurement epoch**, plus any corpus referenced by
an open or disputed claim. Corpora are local-only (`_out/` is gitignored) and must never be
committed. Size must be **measured in S3 before a layout is chosen** — the only data point
available now is a floor, not an estimate: the processed-rows artifact is 52.5 MB for 45,458 rows
(~1,154 B/row), and raw HTML bodies are larger and unmeasured.

### Epoch naming, derived from existing convention (D10)

Rather than invent a scheme, follow what the repo already does. `_out/perf-runs/` tokens are
already `<slug>-<YYYYMMDD>-<HHMMSS>` — for example `full-confirm-3g-20260924-010015`,
`timing-benchmark-20260925-084737`, `artifact-cleanup-20260925-084635` — and
`scripts/perf_pipeline_stages.py` generates run tokens with
`strftime("%Y%m%d-%H%M%S-%f")`.

So a corpus epoch is:

```
_out/perf-runs/corpus-<scope>-<YYYYMMDD>-<HHMMSS>/
    EPOCH.json          # identity + provenance
    corpus/             # content-addressed store
    coverage-report.json
```

`EPOCH.json` records: `epochId`, `recordedAt`, `gitSha`, `dirtyDiffHash`, `recordedWith`
(`force_refresh_all`, per D5), corpus entry/byte counts, the coverage report summary, and
`supersededBy`.

**Supersession follows the baseline precedent.** `scripts/perf_baseline.py` already models
replacement — records carry a `ts`, live under a baseline dir, and are replaced by
`--record-baseline`. So a new epoch simply becomes current, and the older manifest records
`supersededBy`. **Authority is newest-by-`recordedAt`; there is deliberately no pointer file** —
a missing or corrupt pointer is exactly the failure class the registry work already hit, where a
corrupt live registry silently resolved to the committed seed.

Answering the question that was asked: *nobody needs to be appointed to declare an epoch
superseded.* It happens by recording a newer one.

## Goal

Make three questions cheap and trustworthy to answer:

1. **Where did the time go?** — minutes, not hours, from any recent run.
2. **Is this change faster?** — a verdict with a confidence interval, not a single number
   someone chose to believe.
3. **Why is this source slow?** — per-source, from one report.

Success is a single command per question, and an A/B that reports "inconclusive" rather than
manufacturing a winner.

## Non-negotiable constraints

Carried from the prior program's acceptance rules and repo policy.

- Exercise the real `POST /tasks/run-jobs-pipeline` path with `{"jobsPageLoadedCount":0}`.
- **No** preset, only-source, worker, browser-fallback, or fetch-size overrides for acceptance runs.
- 3.5 GiB is the valid full uncapped acceptance profile; 2.5 GiB stays investigation-only.
- New dev-only tooling lives in **`tools/`**. `build-container.yml:50-73` excludes `tools/**`,
  `tests/**`, `docs/**` from the image but **not** `scripts/**`, so a `scripts/` edit is shipped
  code and trips `container_version_policy`. Existing `scripts/perf_*.py` stay byte-identical.
- No new dependencies. No monkeypatch expansion. No bridge route signature changes.
- Output under `_out/perf-runs/<token>/`; leave no discovery-audit artifacts in repo `data/`.
- Reuse `perf_compare.py` / `perf_baseline.py` rather than inventing an A/B statistic.

## Runtime-cost budget

The user's constraint: extend instrumentation **without taxing the runtime**. Concrete
mechanism, not a promise.

- **Default off, and cheaply off.** Every new counter is gated by an env flag that resolves to
  a module-level constant at import. When off, the hot path must not allocate, lock, or even
  look up a dict. Cheapest check: a micro-benchmark asserting the off-path cost is
  indistinguishable from the pre-change code.
- **Two ceilings, per D1.** **Always-on ≤0.5%** — cheap enough to leave enabled in production,
  because a source that is mysteriously slow must be diagnosable without a special build.
  **Opt-in ≤2%** — the full trace may cost twice that, but only when explicitly requested.
  Both are *measured* in S6 and reported, not asserted here.
- **Aggregate, never per-request.** Counters roll up per source. No per-request timer, no
  per-request dict write, no new lock on the fetch path.
- **Additive keys only.** Stage-timing keys are added, never renamed — renaming silently
  changes every downstream reader and every stored report.
- **Reuse the existing gate.** Extend `BALUFFO_PROFILE` / `profile_utils.py`; do not invent a
  second profiling framework.

## Slices

**Execution order is S1 → S2 → S3 → S4 → S5 → S6 → S7 → S0**, given in
[Order and dependencies](#order-and-dependencies). Sections below are described roughly in that
sequence, except S0 which is documented here but **runs last** (deferred by D3). Each slice is
independently landable, and a later slice failing does not waste an earlier one.

### S0 — Adjudicate the shipped claim *(deferred by D3; runs on replay)*

Re-run the pool A/B **on the replay harness**, paired and alternating order, ≥6 samples per arm,
real path, no overrides. This is deliberately also the **first real exercise of S7**, so it
validates the tooling and the shipped claim in a single pass.

- **Acceptance:** a verdict on −11.3% that is either reproducible or refuted, carrying a
  confidence interval, with an explicit **inconclusive** outcome permitted rather than forced.
- **If refuted (per D2):** release note plus a Basic Memory correction. No history rewrite, no
  force-push, no tag move — `61c43fab` is already pushed.
- **Compromise:** replay measures the engine, not the wire. A **refutation** is trustworthy —
  the work demonstrably did not shrink — but a **confirmation** would still need a live paired
  run before anyone quotes it as a production wall-clock win. The report must say which it is.
- **Depends on:** S6, S7.
- **Result (2026-10-08):** **inconclusive by harness** — the claim's variable is inert in a replay. [Snapshot](../snapshots/fetch-pipeline-corpus-evidence-2026-10-08.md#s0--adjudicating-61c43fab).

### S1 — Attribute the core *(validates the plan's premise)*

Measure **which thread** holds the core before asking which functions. The per-source
`BALUFFO_PROFILE=1` capture cannot answer it: it wraps worker threads while TLS, protocol and
body decode run on the single transport loop thread (`transport.py:369-437`), and its lock
serializes sources, so a full run under it takes hours. S1 instead runs a free stage-share pass
over an existing run report, samples the fetch child's per-thread CPU from outside
(`tools/measurements/pipeline/thread_cpu.py`), and takes per-function weight from the opt-in
all-threads sampler `BALUFFO_PROFILE_SAMPLE`, aggregated by `cpu_attribution.py`.

- **Acceptance:** a ranked attribution that either confirms GIL-bound Python work or overturns it.
- **Result (2026-10-08):** GIL story **overturned** — see the [S1 attribution snapshot](../snapshots/fetch-pipeline-cpu-attribution-2026-10-08.md).

### S2 — Extend source-level fetch counters *(cheap, always-useful)*

Per source: request count, response bytes, status histogram, cache outcome, conditional-GET
outcome, retry count. Surfaced on the existing ops routes and folded into the fetch report.

- **Acceptance:** the "why is this source slow" question answerable from one report; off-path
  cost indistinguishable from baseline.
- **Must publish a per-run `cacheDecision` histogram.** This is the measurement that sizes the
  corpus and quantifies [the no-skipping requirement](#the-no-skipping-requirement-met-in-practice-unverified-in-policy). Until
  it exists, corpus size rests on an assumption.
- **Compromise:** counts, not latency distributions. Full histograms are deliberately out —
  per-request bookkeeping is exactly the tax we refuse to pay.
- **Depends on:** nothing.
- **Result (2026-10-08):** shipped; always-on ceiling **−0.77%**, inside the noise floor.
  [Snapshot](../snapshots/fetch-pipeline-corpus-evidence-2026-10-08.md#s2--per-source-fetch-counters).

### S3 — Corpus store and reader *(offline, unit-tested, no container)*

Content-addressed HTTP corpus. Key = method + normalised URL + the request headers that change
a response (`Accept`, `Cookie`, `Authorization`, `If-None-Match`, `If-Modified-Since`). Entries
hold status, response headers, gzipped body, redirect chain — **and failures as first-class
records** (DNS, timeout, TLS, 5xx as exception class + message).

- **Acceptance:** round-trip fidelity proven offline; gzip and dedupe verified on a
  representative sample; **layout chosen only after size is measured** on a recorded sample, not
  predicted from the 1,154 B/row processed-rows floor.
- **Coverage requirement (D6):** must serve every *registered* source, not only the sources that
  fetch in a normal run — because replay uses `force_refresh_all` (D5). Missing entries surface
  as named, countable misses, never as silent empty results.
- **Why failures are records:** 257 of 1,450 sources error. A corpus of successes alone would
  replay a fictional green run and make every measurement meaningless.
- **Compromise:** bodies may carry CSRF nonces that will not replay faithfully. Detected by S5
  parity, not discovered later.
- **Depends on:** nothing.
- **Result (2026-10-08):** shipped; layout sized first (28.5 MB compressed, 506 MB naive). [Snapshot](../snapshots/fetch-pipeline-corpus-evidence-2026-10-08.md#s3--corpus-store).

### S4 — Record mode *(one real run)*

A `tools/` entry point drives record mode at the existing seam —
`resolve_fetch_text_impl` (`transport.py:533`), called once per run from
`pipeline_run_setup.py:349`, which already short-circuits on a custom callable (`:542-543`).
**Zero adapter changes**, and it uses an existing seam rather than adding a root-injection one.
One full-path run writes the corpus.

- **Acceptance:** corpus completeness report with two halves. (a) *Coverage:* requests offered
  vs recorded, every unattributed request named, and **every caller of the transport enumerated
  and classified covered or uncovered** per D6 — so "we replayed the fetch lane" can never be
  misread as "we replayed all network traffic". (b) *Size:* measured on disk, against the
  retention policy.
- **Must record with `force_refresh_all`** (D5), so the corpus covers all registered sources
  rather than only those a scheduled run would fetch.
- **Compromise:** the corpus is a snapshot of one day. It cannot reflect boards that change,
  and it cannot be regenerated (see [Retention](#retention-a-corpus-is-not-reproducible)).
- **Depends on:** S3.
- **Result (2026-10-08):** shipped; the reference epoch is recorded and enumerated.
  [Snapshot](../snapshots/fetch-pipeline-corpus-evidence-2026-10-08.md#s4--record-mode).

### S5 — Replay mode with a proven egress bar *(the actual hermeticity)*

Replay serves from the corpus with `force_refresh_all` (D5), so the fetch set is fixed and
clock-independent. Any outbound socket **raises**, so "zero egress" is proven by construction
rather than asserted.

- **Acceptance:** a replayed run reproduces the recorded row count **and** an identical
  job-identity set. Any divergence is a corpus defect and blocks the slice.
- **Report must state the workload honestly:** this is the full-fetch workload, not a scheduled
  run. A corpus recorded *without* `force_refresh_all` will not satisfy parity, and the missing
  entries will appear as cache-decision differences rather than as corpus errors — check this
  first when parity fails.
- **Depends on:** S4.
- **Result (2026-10-08):** parity-ok on four units, literal `blockedRemote == 0`, four harness defects found and pinned by tests. [Snapshot](../snapshots/fetch-pipeline-corpus-evidence-2026-10-08.md#s5--replay-mode-and-the-egress-bar).

### S6 — Determinism proof *(the gate on all future claims)*

Three replayed runs of unchanged code.

- **Acceptance:** identical rows and a tight wall-clock spread. **Only after this does any A/B
  on this harness mean anything.**
- **Compromise:** replay removes network time, so it measures the *engine*, not the wire (D5).
- **Depends on:** S5, S2.
- **Result (2026-10-08):** **deterministic** in its graded window (spread 1.63%; a later busy window was refused). Both D1 ceilings measured — cProfile misses the opt-in one. [Snapshot](../snapshots/fetch-pipeline-corpus-evidence-2026-10-08.md#s6--determinism-and-both-d1-ceilings).
- **Result (2026-10-10):** whole-registry drains diverged on completion order; with submission-order
  drains two replays agree on every graded unit and the residue is three named non-ordering causes
  (per-render tokens, the batch deadline, the dayforce transport).
  [Fix section](../snapshots/fetch-pipeline-full-source-epoch-2026-10-10.md#the-submission-order-fix-implemented-measured-and-what-the-re-record-still-refuses-on).

### S7 — One-command A/B *(the "fast and easy" deliverable)*

Paired, alternating-order arms over replay, driving the existing `perf_compare.py` /
`perf_baseline.py`, emitting a verdict with a confidence interval and an explicit
**inconclusive** outcome. Plus an `explain` mode that ranks bottlenecks from any past run token.

- **Acceptance:** one command produces a report a human can act on without reading raw JSON.
- **Compromise:** advisory, not CI-gating, per D7 — the existing smoke gate already blocks PRs.
- **Depends on:** S6.
- **Result (2026-10-08):** exercised for real by S0, `--resume` included; one wrong proxy found and fixed. [Snapshot](../snapshots/fetch-pipeline-corpus-evidence-2026-10-08.md#s7--one-command-and-an-interruption).

## Order and dependencies

```
S1 (attribute the core)  ─┐
S2 (extend counters)     ─┴─ independent, parallelisable

S3 (corpus store) -> S4 (record) -> S5 (replay) -> S6 (determinism)
                                                        |
                                                        v
                                          S7 (one-command A/B)
                                                        |
                                                        v
                                          S0 (adjudicate the claim)
                                                        |
                                                        v
                                    first optimisation pass — chosen on S1's evidence
```

**S1 and S2 lead**, and that is the plan's main assertion: the cheapest possible way to find out
whether the premise is right, and the cheapest permanent win for day-to-day debugging, before
spending anything on the corpus spine.

**S3–S6 are strictly sequential** and are the expensive spine. **S6 is the gate**: nothing
measured on this harness is trustworthy until determinism is demonstrated.

**S7 precedes S0 deliberately.** The first A/B should exercise the tool that will run it, so a
harness defect and a claim verdict arrive in the same pass rather than in two hand-run studies.

**Nothing is pre-selected after S6.** Per D4, the first optimisation target is decided on S1's
attribution, not chosen now. If S1 shows the core is not where the GIL story says, the target
changes — which is the point of running S1 first.

### If only a spine is funded

Take **S1, then S3–S7**. S1 is the cheapest way to find out whether the premise holds; the rest
is the minimum needed to believe any subsequent number. S2 is convenience and can be dropped;
S0 can follow later without blocking anything, since it is a measurement rather than an
enablement.

## Compromises accepted

- **Replay is not production.** It cannot measure politeness, reachability, or delivery. Stated
  in every report it produces, so nobody mistakes a fast replay for a healthy fetch.
- **A replay confirmation is not yet a production win.** Replay can *refute* a wall-clock claim
  confidently — the work demonstrably did not shrink — but confirming one still needs a live
  paired run, because replay removes network time. Every S0 report must label its verdict kind.
- **A corpus cannot be regenerated**, only re-recorded, and a re-record is a different corpus.
  One recorded day: boards that change are invisible until re-recorded. Hence deliberate
  retention rather than automatic pruning.
- **Browser-fallback sources are not replayed** (D9). They stay at whatever refusal state was
  recorded — faithful only while the fallback is refused, which is the common case.
- **D3 left the shipped claim uncorrected until S0 ran.** The cost was real — 0.3.015 ships an
  unverified claim — and accepted because a non-hermetic study would not have settled it, while
  D2 makes a later correction cheap.
- **cProfile distortion in S1.** Directional attribution, not a precise flame graph.
- **The 90 s source budget is not enforced as documented** — `sheet_studios.py:192` sets it to
  `max(5, timeout_s * 2)` and `static_detail_heuristics_entry.py:266` checks it only at decision
  points. Not this plan's job to fix; recorded because it will distort long-tail measurement.

## Out of scope

Deliberately not in this plan, to keep it landable:

- **Any actual speedup.** Long-tail remediation, process sharding. This plan produces the
  evidence those decisions need.
- **Discovery subsystem.** It has its own harness (`discovery_sanity_benchmark.py`) and its own
  transport path (`source_discovery/probe.py` uses `async_fetch_text_with_retry`). Out of corpus
  scope per D6 and D9; probe permits:
  [`discovery-probe-permit-allocation-plan.md`](discovery-probe-permit-allocation-plan.md).
- **Browser fallback.** Playwright runs as a Node driver outside the Python transport, so it
  cannot sit at the `fetch_text` seam. Out of scope per D9; those sources stay at their recorded
  refusal state under replay.
- **Frontend / container UI performance.** Governed by `baluffo-container-performance-triage`,
  which is Browser-evidence-first and explicitly forbids building a benchmark layer ahead of traces.
- **Changing the CI perf gate.** A gate already exists and is not being modified; per D7 replay
  A/B runs on schedule or dispatch instead. See [CI is not a gap](#ci-is-not-a-gap-it-already-exists).
- **Making no-skipping enforced.** Per D8 the requirement stands and is currently met but
  unenforced. The assertion is proposed, not scheduled — see
  [The no-skipping requirement](#the-no-skipping-requirement-met-in-practice-unverified-in-policy).

## Risks

| Risk | Why it matters | Mitigation |
|---|---|---|
| Corpus size | answered by S3 (28.5 MB compressed, five sources) | gzip + content-addressing, sized before the layout choice |
| Nonce/CSRF HTML | may not replay faithfully | detected by S5 parity, not in production |
| Harness measures the wrong thing | a fast replay could be mistaken for a healthy fetch | every replay report carries the "not production" banner |
| Premise is wrong | answered by S1: it was, and D4 reordered accordingly | that is why S1 runs first |
| Instrumentation tax | new counters could tax the hot path | default-off; ceilings measured in S6 |
| `scripts/` edit trips release gate | other sessions release concurrently | all new code in `tools/`; `scripts/perf_*.py` untouched |
| Coverage is narrower than it looks | only the fetch lane is replayed (D6) | S4 enumerates every transport caller and classifies each |
| Retry backoff still sleeps in replay | wall-clock includes backoff the CPU story ignores | S6's determinism spread is the check; report the backoff share in S7 |

## Open questions

**None block the plan.** D1–D10 are settled; these are recorded so they are not rediscovered
as if they were open.

1. **Who enforces no-skipping (D8)?** The requirement stands and is currently met but
   unenforced; `coverage_drain.py` buckets skips as `not_selected` without failing. A no-skip
   assertion in the acceptance path is proposed but not scheduled. *Decide whether that lands
   with this plan or immediately after it.*
2. **Corpus size.** *Answered by S3.*
3. **Does replay's `force_refresh_all` workload distort absolute figures?** Settled as acceptable
   (D5 + the side effect that production fetches ~everything), but if absolute replay wall-clock
   is ever quoted as a production number, that report is wrong. *Convention, not a question.*
4. **D1's opt-in ceiling is missed by its own instrument.** `BALUFFO_PROFILE=1` measured
   **+96.8%**; the sampler that might sit under 2% is invisible from an in-process driver.
   *Restate the ceiling, or wire the sampler where the harness can measure it.*


## Verification ladder

Cheapest first, per repo convention.

- **Cheapest syntax/check:** `python -m ruff check tools/<new modules>` and `python -m ruff format --check` on the same paths only.
- **Cheapest focused test:** `python -m pytest tests/tools -q` for corpus round-trip and parity tests.
- **Broader verification required only if:** the corpus seam touches `src/jobs/transport.py` —
  then `python -m pytest tests/bridge tests/test_jobs_transport*.py -q`, and
  `npm run test:py:extended` before any commit.
- **Guardrails:** `npm run lint:repo-guardrails` before pushing; `check_loc_budget` will need a
  baseline bump for any `src/` line, which is why the spine targets `tools/`.
