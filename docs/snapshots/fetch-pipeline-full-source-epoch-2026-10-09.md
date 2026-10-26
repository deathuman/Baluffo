# Full-source corpus epoch — execution evidence, 2026-10-09

Canonical execution record for the **full-source reference epoch**: a corpus that covers every
registered loader, not a stride sample of them. Part of the fetch-pipeline performance work;
the lane's durable rules live in [`measurement-methods.md`](../measurement-methods.md),
the S2–S7/S0 receipts in
[`fetch-pipeline-corpus-evidence-2026-10-08.md`](fetch-pipeline-corpus-evidence-2026-10-08.md),
and the first optimisation pass measured on the harness in
[`fetch-pipeline-normalization-pass-2026-10-08.md`](fetch-pipeline-normalization-pass-2026-10-08.md).

> **Superseded as the practical reference by
> [`fetch-pipeline-full-source-epoch-2026-10-10.md`](fetch-pipeline-full-source-epoch-2026-10-10.md)**:
> this epoch predates the truncation audit and was recorded with the sheet-redirect pool at its
> default, so it cannot referee a whole-registry delta. It remains the evidence for the
> `--all-sources` selection, the recorded work-bounding knobs, and the two defects it surfaced.

Everything here is local: epochs and studies live under `_out/perf-runs/`, are never committed,
and are cited by name so a disputed number can be re-read rather than re-argued.

## Why a second reference epoch

D6 requires the corpus to serve **every registered source**, because replay runs at
`force_refresh_all`. The S4 reference (`corpus-subset-20261008-192418`, 5 sources) was a
layout-sizing compromise: it proved the store, the guard and parity, but a delta measured on it
speaks for four boards and one sheet, not for the fetch lane. This epoch is the reference for
optimisation deltas measured **across the whole registry**.

| | subset epoch (2026-10-08) | this epoch (2026-10-09) |
|---|---|---|
| scope | `subset`, 5 loaders | `full`, **2,046 registered loaders** |
| entries / bodies | 1,543 / 464 | **13,719 / 6,492** |
| requests offered · recorded · unattributed | 1,547 · 1,547 · 0 | **21,361 · 21,361 · 0** |
| rows | 30,053 | 40,347 |
| on disk | 31 MB | 242 MB |

## The epoch

`_out/perf-runs/corpus-full-20261009-114716/` — recorded 2026-10-09T10:21:07Z from
`gitSha 4ebcc43a5df2` with a dirty tree (the harness itself is uncommitted), `scope=full`.

**Selection.** `--all-sources` on `run_fetch.py`: all 2,046 loaders in registry order (2,018
`static_source::` boards + 28 provider/family loaders), mutually exclusive with `--only-sources`
and superseding `--families`/`--sources`, which are then inert. The coverage report's
`sourceCount` is the effective count; the run's own report resolved **2,046 / 2,046** sources
(1,708 `ok`, 338 `error`) and the epoch records the same histogram.

**Coverage completeness — the two halves S4 asks for.**

* (a) *Coverage*: `requestsOffered == requestsRecorded == 21,361`, `unattributedCount == 0`,
  `misses: []`. Offers by surface: `fetch_text` 10,622, `urlopen` 10,645, `httpx` 94. The
  report names what is **not** intercepted (discovery, browser fallback disabled per D9, the
  bridge, the availability validator, and `AsyncHttpTextFetcher`, which a custom `fetch_text`
  never constructs), so "we replayed the fetch lane" cannot be misread as "we replayed all
  traffic".
* (b) *Size, measured on disk*: 13,719 entries and 6,492 content-addressed bodies;
  **222 MB (212 MiB) compressed** (`gzipRatio` 0.1747) against **2.34 GB naive**, with dedupe
  saving 1.07 GB (`dedupeRatio` 0.4568) — **242 MB total**, the other 20 MB being entry JSON.
  1,659 of the entries are first-class failure records (`ok` 12,060): 338 sources error live,
  and a successes-only corpus would replay a fictional green run.

**Workload.** `force_refresh_all`, `max_workers=12` (the product's non-container default), all
three surfaces patched, browser fallback disabled. The run's report window is
**09:47:22 → 10:20:21 UTC, 33 min 0 s**, over 286.9 min of summed per-source time (parallelism
8.7). The recorded summary a replay is graded against:

| unit | value |
|---|---|
| `rowCount` | 40,347 |
| `identityDigest` | `653104362441faff1e89f9b37a580f08` |
| `rowProjectionDigest` | `ec6da1452767523bf58d93aaf4a5b687` |
| `sourceStatusCounts` | `{ok: 1708, error: 338}` |

## Tooling this epoch needed

* **`--all-sources`** (`run_fetch.py`, threaded through `harness.build_command`, `ab.py run
  --all-sources` and `determinism.py --all-sources`; `--resume` refuses to splice an arm
  recorded with the other selection). Pinned by tests in
  [`test_corpus_parity.py`](../../tests/tools/test_corpus_parity.py) and
  [`test_corpus_ab.py`](../../tests/tools/test_corpus_ab.py).
* **A thread-safe store and ledger.** A full record runs the pipeline's worker pool, and the
  corpus store was only ever exercised at one worker. Two writers can gzip the same
  content-addressed body at once (an unreadable body on replay) and the ledger's `+=` can lose
  an increment (which would make `offered == recorded` false rather than merely noisy). Both are
  now lock-guarded, pinned by
  `test_concurrent_recording_keeps_every_body_readable_and_every_count_exact`.
* **The static per-source clock recorded in the config.** `runConfig.staticSourceTimeBudgetS`
  now states the `BALUFFO_STATIC_SOURCE_TIME_BUDGET_S` value in force (empty = product default),
  because that clock is exactly the kind of workload input a replay silently changes (below).
* **The sheet redirect pool as a first-class flag.** `--google-sheets-redirect-concurrency`
  (`run_fetch.py`, threaded through `harness.build_command`, recorded as
  `runConfig.googleSheetsRedirectConcurrency`, 0 = product default), because that pool bounds how
  much of a failing host's candidate set a record captures — the bound this epoch's replay hit
  (below). Pinned by
  [`test_corpus_parity.py`](../../tests/tools/test_corpus_parity.py) and
  [`test_corpus_ab.py`](../../tests/tools/test_corpus_ab.py).

## Two defects the full run surfaced

**1. One malformed URL aborted the whole run (fixed).** The first attempt died at source 1,579
of 2,046: a static board's templated detail link
(`/careers/" + root + "/moonactive-position?uid=%22...`) reached `urllib` and raised
`http.client.InvalidURL`, which was not in the pipeline's expected per-source failure set, so it
escaped the worker future and killed the run. It is now classified as a per-source failure
(`src/jobs/pipeline_source_results.py`), pinned by
`tests/test_pipeline_source_url_failure_isolation.py` — the fix and its test land with the `src/`
changes this epoch was recorded against, not with the harness that captured it.
The deeper cause — a detail link extracted from page text that can never be a URL — is a
follow-up in the extraction path, not a run-liveness problem: the source is reported as `error`
with the message, like a timeout.

**2. A wall-clock budget made the corpus unreplayable (fixed by recording without one).** The
second attempt recorded with the default 90 s static per-source budget. Six sources stopped at
~90 s (`chessiverse`, `eipix`, `highmoonstudios`, `sledgehammergames`, `infinityward`, and
`unity.com/careers/positions`, which still kept rows so it never showed an error). Replay serves
pages instantly, so those sources **explored further than the live run could** and the replay
died with `corpus miss` at source 451. The mechanism was reproduced on the two loudest sources
with an uncapped clock: record 1,218 entries / both `ok` (versus one `error` live), then replay
**parity-ok, `blocked=0`** — before any full re-record was spent.

This epoch is therefore recorded with `BALUFFO_STATIC_SOURCE_TIME_BUDGET_S=1800` (30 minutes,
20× the default) so the clock cannot bind for any board that traverses at all: the final report
mentions **zero** sources stopped by a budget. The value is in `recordedConfig`; the rule for
every arm of every study on this epoch is **the same value in record and replay** — a mismatch
fails loudly with a corpus miss rather than quietly measuring a different page set.

Superseded and kept as evidence: `corpus-full-20261009-110605` (90 s clock, unreplayable at
source 451), `corpus-full-20261009-103615.crashed-partial` (the `InvalidURL` crash),
`corpus-budgetprobe-20261009` (the two-source clock probe, itself parity-verified), and the two
sheet-pool probes `poolprobe-20261009` / `poolprobe16-20261009`. Nothing was deleted;
`epoch_dirs` ignores the partial directory because it has no manifest.

## Replay verification — partial, and the epoch does not yet replay end to end

**What passed.** The replay ran hermetically for the first **53 of 2,046 sources** — every arm
here is a fresh process under the armed egress guard, and `blockedRemote == 0` throughout
(loopback attempts are counted separately as in-process plumbing). It is the *record* side of
this epoch that is proven whole; the replay half is proven only up to its first divergence.

**Where it stopped.** `corpus miss for https://gracklehq.com/rd/374384` (surface `httpx`), ~47 s
into the run: 53 of 2,046 sources had resolved, and the miss was raised inside `google_sheets`,
which was still `running` in phase `resolving_sheet_redirects` (the replay's
`jobs-fetch-tasks.json`). A miss is loud and fatal by design, so the run exited `corpus-miss`
rather than serving an empty body; **200** missed URLs were named — all `httpx`, all
`gracklehq.com/rd/<id>`, ids `373310`–`374408`.

**Not a truncated range and not a missing body — a work set the record never captured.** That
phase had the same **465 redirect candidates** in both runs, but the record requested only
**47** of them (94 entries = `HEAD`+`GET` each: 79 `ReadTimeout`, 15 ok, ids `374368`–`374581`).
Of the replay's 200 missed ids, **192 sit below the record's lowest recorded id** and 8 fall
inside its range; none sit above it. The two runs walked differently-sized slices of one
candidate list.

**One source reproduces it, so it is not cross-source ordering.** Replaying
`--only-sources google_sheets` against this epoch alone fails the same way: identical miss,
identical 200 ids. The phase is driven by `PooledRedirectResolver` (`src/jobs/transport.py`),
which resolves each supported redirect link `HEAD` then `GET` through the patched httpx client —
exactly the 47 `HEAD` + 47 `GET` entries above, browser `Accept` and all — and which abandons a
host after `max(2, maxConnections * 2)` response-less failures, **short-circuiting the remaining
URLs without issuing a request at all**. Any success clears that count, so the point at which a
phase gives up is a timing outcome, not a fixed number. `googleSheetsRedirectConcurrency` was the
product default 16 and `gracklehq.com` was failing live (79 `ReadTimeout`s): the run captured a
prefix of the host's candidates and left the rest unrequested — never recorded, whatever a later
replay wanted from them. Replay serves the same bytes instantly, walks the same candidate list at
the same pool, and asks for a *different* slice of it — the links the record abandoned — and a
miss is loud, so the run stops there.

**The pool is the lever, and it is now part of the recorded config.** Two single-source records
of `google_sheets`, minutes apart, same host, same output rows (`identityDigest` identical):

| record | pool | candidates requested |
|---|---|---|
| `poolprobe16-20261009` (product default) | 16 | **199** of 465 |
| `poolprobe-20261009` | 64 | **465** of 465 |

The default pool captures roughly 200 of a failing host's candidates — the same order as the
200 links the replay asked for — but not the *same* 200: which candidates are attempted before
the phase gives up moves with timing, and replay removes the timing. A record taken at pool 16
is therefore unreplayable at pool 16 by construction. `run_fetch.py` now exposes
`--google-sheets-redirect-concurrency`, and `runConfig.googleSheetsRedirectConcurrency` records
it, so the next epoch can be recorded with the pool sized to cover a failing host's whole
candidate set (64 cleared 465 here) and be spelled identically in every replay arm.

This is the same defect shape as the 90 s static clock above — *a clock-bounded work set is not
a work set a replay can reproduce* — in a second knob, and the same rule follows: the knob that
bounds a phase's work must be recorded, and must be set so it cannot bind during either the
record or a replay.

**What the epoch is still good for.** (a) A coverage-complete record of all 2,046 loaders —
requests, bodies, failures and statuses — for coverage work, `explain.py`, and failure analysis
with no network. (b) Replays of the sources that are run-stable: `--only-sources` over this
epoch is a sample you choose, at full-fidelity corpus cost — pick it after one cheap replay,
and leave `google_sheets` out until this epoch is re-recorded with the pool raised. (c) As the
reference for a future optimisation once the replay parity bound is lifted.

**What it is not.** A delta measured on it today is **not** a delta across all registered
sources: the first full-source replay diverges inside `google_sheets`' redirect phase, so any
A/B on this epoch either fails loudly or runs a chosen subset — and `--only-sources google_sheets`
is not a safe substitute, since that source is the one that diverges. The subset epoch remains
the cheap referee for changes whose effect is expected to be uniform, and the S6
determinism/ceiling receipts were measured there, not here. Recording this epoch superseded
every other epoch under `_out/perf-runs/` by design (`supersede_older_epochs` marks all of them),
the 5-source subset included; studies pinned to the subset cite it by path and still run.

## The record-time check this epoch needed

A corpus cannot show what a run decided *not* to request, which is why this gap had to be found at
replay. It need not be: the record owns both halves of the join, because the rows it kept are the
rows whose links the resolver was handed. `tools/measurements/corpus/truncation.py` runs at the end
of every record, reads each kept row's supported-redirect link as a candidate, and reports the
candidates the corpus holds no request for — per host, with counts and sample URLs — into
`truncation-report.json`, a `[corpus] WARNING` line on stderr, and a `truncation` block in
`EPOCH.json`. It is not a gate on *recording* — a record with a truncated phase is still a coverage
record and still serves chosen-subset replays — but it is a gate on the epoch's second use. The
record refuses to certify the epoch as a **delta referee** (exit 3) when the redirect pool or the
source clock bound a phase, because replay removes both bounds and walks into work the record
dropped; the refusal names the knob to raise. A declared cap does not refuse it — both cap paths
are count-driven, so replay reproduces the drop — and travels as a note. The verdict, blockers and
notes together, is `EPOCH.json`'s `deltaReferee` block; `--accept-truncated-referee` is the
explicit override, which records the epoch as accepted-but-truncated instead.

This epoch predates the check, so its report was produced by running the audit over the finished
artifacts — the file is in the corpus directory, and its `EPOCH.json` has no `truncation` block,
recorded as it was before that block existed. Every record from now on carries one. Run against
this epoch's artifacts it says what the forensics cost a session to work out:

```
[corpus] WARNING truncation: gracklehq.com - 47 of 443 candidate links requested (396 unrequested)
```

A writing-admits-error control: the same check on `poolprobe-20261009` — the same source, recorded
at pool 64 — is **clean** (443 of 443), so it names a gap rather than a host. Two caveats are in the
report itself: candidates come from kept rows, so the count is a **floor** (this phase's own
telemetry said 465 where the rows declared 443), and the candidate rule is the resolver's, which
today is two spellings of one host (`SUPPORTED_REDIRECT_HOSTS`). The check cannot flag a phase whose
candidates it cannot enumerate — it catches this class, not every class of dropped work.

### Static boards: a per-source clock or detail cap drops pages and details the same way

The redirect phase is not the only one that drops work, and this epoch's predecessor proves the
static lane's version of it. The **second** full-source epoch (`corpus-full-20261009-110605`) was
recorded with the 90 s default clock; five static sources were stopped by it. One was
`eipix.com/join-us/`, and its report says exactly what it was doing when the clock ran out:

```
... static:Eipix (GameDevMap):https://www.eipix.com/career/content-creatorcopywriter: time budget exceeded (90s)
```

The next replay has no clock — the corpus answers instantly — so it walks *past* the recorded stop.
It died on precisely that URL:

```
[corpus] replay hit a corpus miss: corpus miss for 'https://www.eipix.com/career/content-creatorcopywriter'
```

`truncation.py` now reads that family too, from the run's own per-source report: a refusal text
naming a URL the source clock cut (`source_clock`), or the counters a cap booked
(`detail_pages_skipped_by_adaptive_stop`, `external_detail_links_capped`; `detail_cap`). For the
clock family the named URLs are checked against the corpus and the unrequested ones are listed —
declared *and* verified — while a cap is a declaration with counts and no URLs, because nothing
records the candidates it dropped. Run over the two full epochs it separates them cleanly:

| epoch | clock | 90 s (`110605`) | 1800 s (`114716`) | uncapped (`budgetprobe`) | subset (4 epochs) |
|---|---|---|---|---|---|
| static sources audited | 2,018 | 2,018 | 2 | 4 each |
| findings | | 7 | 4 | 0 | 0 |
| clock stops | | 5 | 0 | 0 | 0 |
| URLs refused with no corpus request | | **2** (eipix, chessiverse) | 0 | 0 | 0 |
| detail pages a cap skipped | | 113 | 1,728 | 0 | 0 |

The 1800 s epoch is the control the check is meant to write: its clock could not bind, so it has no
clock-stop findings at all (its 4 findings are adaptive detail stops — work dropped, but driven by
recorded failures, so replay reproduces the stop, which is why they are notes in the referee
verdict rather than blockers). `budgetprobe-20261009` is the sharper control:
it is the same two budget-truncated boards (`chessiverse`, `eipix`) re-recorded with an uncapped
clock, and the check is silent on it — as are the four subset epochs, whose replays all passed.

The bound is in the payload: the static family can only see what the report names or counts, and
the report keeps at most the first six partial errors per source, so a drop with no accounting
stays invisible. That is the same shape as the redirect family's floor — it catches this class of
dropped work, not every class.

## Measuring a delta on this epoch

```bash
# The shape of a study on a full-source epoch: the work-bounding knobs the epoch recorded, in
# both arms, and the epoch named by path. The driver env is inherited, so an export reaches every
# arm; arm args go through --arm-a-arg/--arm-b-arg and need `=`, because argparse will not take a
# `--flag` as the *value* of another flag (`--arm-a-arg --x` fails: "expected one argument").
export BALUFFO_STATIC_SOURCE_TIME_BUDGET_S=1800
python tools/measurements/corpus/ab.py run \
    --epoch _out/perf-runs/corpus-full-<stamp> \
    --scope full --all-sources --max-workers 12 \
    --arm-a-arg=--google-sheets-redirect-concurrency --arm-a-arg=64 \
    --arm-b-arg=--google-sheets-redirect-concurrency --arm-b-arg=64 \
    --pairs 6 [--resume]
```

**Do not run that block on this epoch yet.** It fails where this receipt's replay failed. A
usable full-source arm needs a re-record that passes `--google-sheets-redirect-concurrency`
(64 covered a 465-candidate failing host here; size it above that host's candidate count) in
addition to the 1800 s static clock, and then the same flags in every arm; until then, treat the
epoch as a coverage record and a chosen-subset replay source.

What a study on this epoch costs is **not yet measured**: the only full replay died ~47 s in,
with 53 of 2,046 sources resolved. The subset's ~1 minute per 5 sources does not scale to it
(that arm read 507 MB of bodies against this epoch's 2.34 GB, over 2,046 sources rather than 5),
so treat a full-source arm as tens of minutes and measure before quoting anything. As with every
replay figure, the deltas are engine time: replay removes the wire, so it can refute a
wall-clock claim but never confirm one.

## What this epoch does not cover

* **Out-of-lane callers**, named in `coverage-report.json`: discovery, browser fallback (D9,
  disabled in both modes), the bridge, the availability validator, and the async transport whose
  pool sizing a custom `fetch_text` short-circuits.
* **The wire.** Replay removes network time by construction; absolute replay wall clock is not a
  production number, and boards recorded as failures stay failures.
* **Nonces/CSRF**, per the plan's S4 compromise: detected by parity, not discovered later.
* **One day.** A corpus is a capture, not a cache: a re-record is a *different* corpus, and this
  one cannot be regenerated.
* **Determinism.** S6's three-sample determinism check and the D1 instrumentation ceilings were
  measured on the 5-source subset. They were not re-run here (a full-source determinism pass is
  three more ~arm-length replays); the parity replay is the evidence this epoch ships with.
* **A host that fails every request.** How much of such a host's candidate set a record keeps is
  a function of the redirect pool and of timing, so this epoch keeps 47 of `gracklehq.com`'s 465
  candidates (see the replay section). Ads/boards with a reachable host are unaffected.
