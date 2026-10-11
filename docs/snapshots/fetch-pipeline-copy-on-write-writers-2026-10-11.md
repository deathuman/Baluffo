# Fetch pipeline — copy-on-write for the three kept writer sites

**Date:** 2026-10-11 · **Lane:** fetch-pipeline performance · **Status:** shipped and measured;
**inconclusive at the harness's 2% bar** (+0.36%, 95% CI [−0.32%, +2.16%]) **while provably removing
91,055 whole-row conversions** (−29.6% of the arm's `to_dict` calls, counted).

This closes the residual the row-payload conversion left open. That change converted 43 reader sites
to a payload view and kept three sites as writers because they write to what they were handed; its
snapshot named the exact remedy: *"converting those writers means making them copy-on-write first, a
behaviour change rather than an annotation change."* This is that change, and its measurement is
reported as it came out — the work is gone, the wall clock did not move measurably.

## What shipped

`row_payload.mutable_payload(row)` is the writer's half of the read/write split: one **shallow**
copy for a canonical row (the nested containers stay the row's, so a site that must change one
assigns a fresh container instead of mutating an inherited one), and nothing copied for a dict the
caller already owns. The three sites now read first and copy only where they write:

| Site | Before | After |
|---|---|---|
| `state_lifecycle_orchestration` **observed** list (~30k) | a whole-row payload per row, built up front | payload views; the entry builder no longer writes into the row at all |
| `state_lifecycle_orchestration` **projector** list (~30k) | a whole-row payload per row, built up front | views; one shallow copy only for a row that is kept *and* projected |
| `pipeline_finalize` published output rows (~30k) | a whole-row payload per row, before the sector gate | views through the gate and the audits; materialised once, after the gate |

Two details make those reads safe rather than merely fast:

- **The writes that forced the old shape were dead.** `_lifecycle_entry_from_active_job` wrote twelve
  lifecycle fields into the row on each of its two branches, but the entry it returns is assembled
  from `previous` and the row's *unwritten* fields, so nothing ever read them — not the caller
  (which stores the returned entry and discards the row) and not anyone else, because the payload
  list they came from was a private copy. They are gone, with the values that were previously read
  back out of the row hoisted into locals so the returned entry is byte-identical.
- **The one mutating consumer assigns, not mutates.** The final location guardrail is the only
  post-gate writer: it builds a fresh `locations` list and assigns it, so a shallow payload shares
  the row's containers without ever corrupting them. The other consumers after the gate — the three
  audit reports, the loss counts, the availability validator, tombstone reconciliation and the
  output writer (which serializes `{field: row.get(field, "")}`) — only read.

`BALUFFO_ROW_COPY_ON_WRITE=legacy` restores the up-front whole-row build at exactly those three
sites, so they can be priced on their own. It is intentionally **separate** from
`BALUFFO_ROW_PAYLOAD`: that switch governs the view/cheap-build pair everywhere, and pricing this
change under it would ride on the previous change's −23%.

## Two studies, because one of them would have been a lie

Both on the reference subset epoch (`corpus-subset-20261008-192418`), same flags, one variable each,
four alternating pairs, zero parity divergences in both.

**1. The whole row-payload seam, re-measured on the final tree** (`BALUFFO_ROW_PAYLOAD=legacy` vs
`view`) — this is *not* this change's worth, and quoting it as such would overclaim by ~23 points:

| | A (control, `legacy`) | B (candidate, `view`) |
|---|---:|---:|
| median report wall | 40,711 ms | **31,027 ms** |
| verdict | `arm-b-faster` — **−23.45%** (95% CI [−24.07, −22.22]), 4/4 pairs | |

Consistent with the −22.83% the previous slice measured on the same epoch (overlapping intervals,
different study). It is recorded here as the re-measurement it is.

**2. The isolated copy-on-write change** (both arms on the view path; only
`BALUFFO_ROW_COPY_ON_WRITE` varies):

| | A (control, up-front build) | B (candidate, copy on write) |
|---|---:|---:|
| median report wall | 31,215 ms | 31,249 ms |
| verdict | **`inconclusive`** — +0.36% (95% CI [−0.32%, +2.16%), deltas [2.16, 0.19, 0.52, −0.32] | |

The control arm of study 2 (31.2 s) matches the candidate arm of study 1 (31.0 s), which is the
cross-check that the isolation worked: both are "the shipped view path", differing only in whether
the three sites build their payloads up front.

## The work is gone; the wall clock is flat

Conversions, counted in both arms with the same probe and the same ablation (so the arms differ only
in the sites' call pattern, not in what was measured):

| arm | processes | `to_dict` calls | builds |
|---|---:|---:|---:|
| A — up-front build | 1 | **307,698** | 213,956 |
| B — read through the view | 1 | **216,643** | 183,903 |
| removed | | **91,055 (−29.6%)** | −30,053 |

91,055 is the three sites' ~30k each, to four significant figures. Phase attribution of study 2
(medians per arm, from each arm's own `data/jobs-fetch-report.json`) shows where that goes:

| stage | A ms | B ms | delta |
|---|---:|---:|---:|
| `applying_lifecycle` | 2,460 | 1,976 | **−484** |
| `running_quality_audits` | 1,028 | 1,238 | **+210** |
| `deduplicating` | 8,400 | 8,542 | +142 |
| `writing_outputs` | 4,743 | 4,842 | +99 |
| `reconciling_identities` | 9,546 | 9,532 | −14 |
| summed source duration | 12,217 | 12,224 | +8 |

- The two lifecycle lists pay off exactly where they were: `applying_lifecycle` **−484 ms**, which is
  the ~60k up-front conversions removed from that phase.
- **Site C's saving moved rather than appeared.** The published rows that survive the gate still have
  to be serialized, so deferring their materialisation into the audit phase shows up as
  `running_quality_audits` **+210 ms** — the same work, charged to a different phase. Its deferral
  only pays for rows the sector gate *drops* before materialisation, and
  `BALUFFO_STRICT_GAME_ONLY` is off by default, so on this epoch it is neutral by construction.
- Everything else is inside the harness's noise floor, and the net (+0.03 s, +0.1%) matches the
  verdict.

**So the null result is the expected one, not a surprise.** 91k conversions at the *cheap* build's
cost (~10–20 µs each, because the previous slice already replaced `asdict`) is ~1–1.8 s of profiled
cumulative time, which across 48 threads is ~0.1–0.3 s of wall — 0.5–1% of a 31-second run, below
the harness's 2% materiality bar and inside its own ±1% pair spread. The previous slice's estimate
for these sites ("worth ~1 point, leaving that residual inside the priced interval's own noise") was
right; this study is that estimate being tested rather than repeated.

## What this change is actually worth

Honest split, because the two halves are not equal:

- **The lifecycle pair is a real, located saving** (−484 ms of phase time, −29.6% of the seam's
  conversions) that is simply too small to clear a 2% whole-run bar on a five-source epoch. It also
  removes 24 dead statements per row path, and it is what makes those two lists readable as views.
- **Site C's deferral is neutral by default** and pays only when the sector gate runs. It is kept
  because it is the correct shape for a gate-then-publish pipeline (nothing is materialised for a row
  that will not be published) and because parity confirms it changes no output; anyone reading the
  flat number as "not worth doing" is reading site C correctly for this epoch and the lifecycle pair
  incorrectly.
- **Not CI-gating** (D7), like every replay A/B. Advisory.

## Verification

| Check | Result |
|---|---|
| `python -m pytest -q` (whole default tree, incl. the `slow`/`packaging`/`release` markers) | **6816 passed, 3 skipped, 0 failed** in 7:44, re-run on the final bytes after the switch was added |
| `tests/test_jobs_row_copy_on_write.py` (new) | 5 passed — `mutable_payload`'s copy shape, dict/duck passthrough, the projector's carry-copy-and-drop behaviour, the control arm's restored up-front build, that `apply_job_lifecycle_state` leaves the rows it reads untouched, and that the published-row guardrail assigns a fresh `locations` list rather than editing the shared one (the property a shallow payload makes load-bearing, and which the old deep copies would have hidden) |
| `tests/test_jobs_row_payload.py` (extended) | 9 passed — the previous contracts plus the fresh-interpreter pin for `BALUFFO_ROW_COPY_ON_WRITE`, including that the two switches are independent |
| `python -m mypy --config-file mypy.ini` | **clean on all 1493 files** — the real gate, which also surfaced and then cleared **three latent errors in the previous slice's test file** that would have blocked `git push` (`PRE_PUSH_HOOK_IDS` runs `mypy` with no path, so `files = src, tests` applies) |
| `npm run lint:repo-guardrails` | 15/15 groups, including the per-file `line-budget` check, which forced this slice's tests to split (`test_jobs_row_payload.py` was at 417 lines against a 400 budget → 272 + the new 198) |
| `ruff check` · `ruff format --check` | clean on every touched file |
| `tools/repo_health/loc_budget.py` | passes with the ratchet moved in this change |

## Reproduce

```bash
# 1. The isolated measurement: one variable, both arms on the shipped view path.
python tools/measurements/corpus/ab.py run \
    --epoch _out/perf-runs/corpus-subset-20261008-192418 \
    --out _out/perf-runs/writer-sites-20261011/ab-cow --pairs 4 \
    --arm-a-label control --arm-b-label copy-on-write \
    --arm-a-env "BALUFFO_ROW_COPY_ON_WRITE=legacy" \
    --arm-b-env "BALUFFO_ROW_COPY_ON_WRITE=view" \
    --arm-a-concurrency 48 --arm-b-concurrency 48 \
    --scope subset --sources 4 --families google_sheets --max-workers 1

# 2. The conversion counts, one untimed replay per arm, same probe in both.
PROBE="$PWD/tools/measurements/corpus/probes/row_dict"
for arm in a:legacy b:view; do
  name="${arm%%:*}"; value="${arm##*:}"
  PYTHONPATH="$PROBE" BALUFFO_ROW_DICT_ABLATION=cached \
  BALUFFO_ROW_COPY_ON_WRITE="$value" \
  BALUFFO_PRICE_PROBE_MARKER="_out/perf-runs/writer-sites-20261011/counts/$name" \
  python tools/measurements/corpus/run_fetch.py --mode replay --scope subset \
    --run-dir "_out/perf-runs/writer-sites-20261011/counts/run-$name" \
    --corpus-dir _out/perf-runs/corpus-subset-20261008-192418 \
    --sources 4 --families google_sheets --max-workers 1 --adapter-http-concurrency 48
done
# then sum .calls across each arm's probe-*.json markers

# 3. The contracts:
python -m pytest -q tests/test_jobs_row_copy_on_write.py tests/test_jobs_row_payload.py
```

## Receipts and supersession

Nothing was deleted and no epoch, record or arm was edited. `_out/perf-runs/writer-sites-20261011/`
holds the slice: `test-py{,-2,-3}.log` (the three suite runs), `study.log` + `ab/` (the whole-seam
re-measurement), `study-cow.log` + `ab-cow/` (the isolated study, both with
`ab-report.{json,md}`, `pair-01…04/arm-{a,b}`, `referee-verification/` and `baselines/`) and `counts/`
(the two counted replays, their run dirs and per-process probe markers). All local, never committed,
cited by name.

The previous slice's snapshot is unchanged and still correct about what it measured; it is
cross-linked from here, and its "kept writer sites" entry now has its outcome.

The `tools/repo_health/loc_baseline.json` ratchet moves in the same change: `src` 179944 → 180030
(+86: the `mutable_payload` half, the switch, and the three sites' read-then-copy paths) and `tests`
233204 → 233468 (+264: the new copy-on-write contract file, the switch pin, and the guardrail pin).
No other area moves.
