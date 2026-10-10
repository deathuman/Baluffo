# First optimisation pass — normalisation memoisation, measured on replay (2026-10-08)

Canonical execution record for the **first optimisation pass** the
[fetch performance plan](../plans/fetch-pipeline-performance-instrumentation-plan.md) left open
after S0: *"first optimisation pass — chosen on S1's evidence"*. S1
([snapshot](fetch-pipeline-cpu-attribution-2026-10-08.md)) named the target: not concurrency and
not the transport, but the regex-heavy text/location normalisation path (`text_utils`,
`location_rules`, `location`), where `re.sub` alone held 31.6% of the sampled CPU.

This pass memoises that path and replaces two hot spellings with cheaper, provably identical
ones. The corpus harness is the referee, per S7; the delta below is a paired replay measurement,
not an estimate.

## What ran

| Item | Value |
|---|---|
| Epoch | `corpus-subset-20261008-192418` (the S0/S5 reference corpus, 1,543 entries) |
| Protocol | 6 pairs, alternating order, arms sequential — `ab.py run --pairs 6` |
| Arm A (control) | `BALUFFO_TEXT_NORMALIZATION=legacy` — pre-pass algorithms, no memoisation |
| Arm B (candidate) | `BALUFFO_TEXT_NORMALIZATION=fast` — memoised, cheap spellings (production path) |
| Workload | `--scope subset --sources 4 --families google_sheets --max-workers 1`, replica of the epoch |
| Other config | `--adapter-http-concurrency 48` in **both** arms; `BALUFFO_FETCH_COUNTERS=1` both |
| Receipts | `_out/perf-runs/text-normalization-pass-20261008/ab/` — `ab-report.{json,md}`, `pair-01…06/arm-{a,b}` |

## Verdict

**arm-b-faster** — median paired delta **−33.2%**, 95% CI **[−45.1%, −30.0%]** (negative means B
faster). Per-pair deltas: `[-34.5, -30.8, -42.6, -29.3, -31.9, -47.7]`.

| arm | label | median report wall | median process wall | rows |
|---|---|---|---|---|
| A | legacy-normalization | 98,405 ms | 123,721 ms | 30,053 ×6 |
| B | cached-normalization | 63,276 ms | 85,011 ms | 30,053 ×6 |

- **Parity: `ok`, zero divergences.** All 12 arms matched the epoch's four units (rowCount
  30,053, identityDigest, rowProjectionDigest, sourceStatusCounts) and all recorded
  `blockedRemote == 0`. No normalised value moved anywhere in 12 replays.
- `perf_compare.py` over the arm medians: −35.7%, status `passed`.
- Retry/backoff exposure **rose** in B (8.0% → 22.1%) — dilution, not behaviour: the absolute
  repeated-fetch time is identical (median 3,612 ms vs 3,611 ms) while summed source time fell
  45.0 s → 16.3 s.

## Mechanism, and the stage-level corroboration

The pass makes no behavioural change by construction:

1. **Memoisation of pure string primitives** — `sanitize_public_text`'s dependents
   (`normalize_country_acceptance_token`, `resolve_country_acceptance_value`,
   `looks_like_country_token`, `is_city_noise_fragment`, `classify_city_filter_rejection`,
   `invalid_location_reason`, `get_city_filter_option_values`), and the parse-layer composites
   (`_normalize_location_key`, `parse_generic_location_fields`, `classify_city_garbage`,
   `is_plausibly_location_candidate`, `_fragment_looks_like_location_value`). One fragment is
   re-normalised a dozen times along a single parse chain; the same city/country labels repeat
   across rows. Caches are bounded (4,096 entries each), keyed on `str` only — non-string inputs
   bypass them — and every cached function is pure.
2. **Cheaper spellings of the same normalisation** — whitespace collapse via
   `" ".join(text.split())` instead of `re.sub(r"\s+", " ", …).strip()`; `unescape`/tag-strip
   skipped when `&`/`<` cannot be present; the `NFKD` combining-mark strip via `str.translate`
   with a per-character fallback only for marks outside U+0300–U+036F.
3. **Precompiled** `[\s_-]+` / `[^a-z0-9 ]+` patterns in the location key path.

The per-source stage ledger in the same runs shows the expected shape — the work was removed
from normalisation, not moved elsewhere (medians over the six pairs):

| stage | A | B | Δ |
|---|---|---|---|
| canonicalization | 24.3 s | 7.0 s | **−71.4%** |
| fetchAndParse | 14.7 s | 5.1 s | **−65.4%** |
| parseCsv | 10.6 s | 1.2 s | **−88.3%** |
| candidateExtraction | 0.4 s | 0.2 s | −56.3% |
| unattributedMs | 0.1 s | 2.7 s | (stage keys only B names) |

An **in-process proxy** (`_out/local_normalization_bench.py`; 17,368 location-like strings drawn
from the recorded rows, the same parse chain) ran 11.83 s → 1.26 s, a 9.4× chain speedup —
supporting context, not the receipt: the harness number above is the measurement.

## What this is not

- **Engine wall clock, not the wire.** Replay removes network time; this is evidence about the
  pipeline's own work, exactly as S0's caveat says.
- **A 4-source subset workload.** The epoch was recorded from 4 static sources plus the
  `google_sheets` family, single-worker. 30k rows from a handful of boards have unusually high
  label repetition, so −33% is *this workload's* engine share. Do not quote it as a
  production-wide wall-clock win; the mechanism (memoised pure normalisation) travels, the
  percentage does not.
- **Not CI-gating** (D7). Advisory, like every replay A/B.

## Fidelity and cost of the scaffold

- **Equivalence is tested, not asserted.**
  `tests/test_text_normalization_equivalence.py` drives both modes across a battery of inputs
  (entities, HTML, tabs/newlines/NBSP, NFC/NFD accents, CJK, noise blobs, non-string values)
  and requires identical outputs from every public entry point; it also pins
  `_collapse_whitespace` against the regex it replaced, pins that the cached path hits and the
  legacy path does not, and that cached country-option lists are returned as copies. The 12-arm
  parity line above is the end-to-end version of the same property.
- **The control arm is the pre-pass algorithm plus a dispatcher.** The legacy branch is one
  `isinstance` + one module-global check + the same function body; measured at **~25 ns per
  call** against a direct call. Bounded by the replay's per-row call volume that is well under
  0.1% of a 87–126 s arm, so the delta is not materially inflated by the scaffold.
- **Bounded memory.** 12 caches × 4,096 entries, string keys and small results; no per-row state,
  nothing persisted, no new dependency (`functools` only).
- **The legacy path is not dead code.** It is the A/B's control and is exercised in both
  directions by the equivalence test and by the normal lanes with the env set.

## Reproduce

```
python tools/measurements/corpus/ab.py run \
    --epoch _out/perf-runs/corpus-subset-20261008-192418 \
    --out _out/perf-runs/text-normalization-pass-20261008/ab \
    --pairs 6 \
    --arm-a-label legacy-normalization --arm-b-label cached-normalization \
    --arm-a-env BALUFFO_TEXT_NORMALIZATION=legacy \
    --arm-b-env BALUFFO_TEXT_NORMALIZATION=fast \
    --arm-a-concurrency 48 --arm-b-concurrency 48 \
    --scope subset --sources 4 --families google_sheets --max-workers 1
```

`--resume` rebuilds finished pairs from their own artifacts, so an interruption does not cost the
study.

## Files changed

| File | Change |
|---|---|
| [`src/jobs/text_utils.py`](../../src/jobs/text_utils.py) | mode gate, memo caches, `_collapse_whitespace` / unescape / tag-strip helpers |
| [`src/jobs/adapters/parsers/location.py`](../../src/jobs/adapters/parsers/location.py) | memo `_normalize_location_key` / `parse_generic_location_fields`, combining-mark translate, precompiled patterns |
| [`src/jobs/adapters/location_rules.py`](../../src/jobs/adapters/location_rules.py) | memo `classify_city_garbage` / `is_plausibly_location_candidate` / `_fragment_looks_like_location_value` |
| `tests/test_text_normalization_equivalence.py` | both-mode equivalence, collapse-vs-regex pin, cache-use pins, list-copy pin — lands with this pass, not with the harness that measured it |

**Verified:** 540 focused normalisation/parser/static tests pass in **both** modes; 435
`tests/tools` tests pass; `ruff format --check` and `ruff check` clean; `mypy` clean on the four
touched files. The plan document itself is untouched — it sits at its enforced word tripwire
(4,998 / 5,000), and this snapshot is the receipt rather than a plan edit.
