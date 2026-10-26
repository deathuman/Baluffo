# Pricing the next fetch-lane optimisation — the row-payload conversion (2026-10-10)

Canonical execution record for the step the replay harness leaves after S0 and the
[first optimisation pass](fetch-pipeline-normalization-pass-2026-10-08.md): choosing the
**next** fetch-lane optimisation on measured evidence and **pricing it before committing to it**.
The evidence base is S1's
([attribution snapshot](fetch-pipeline-cpu-attribution-2026-10-08.md)): the transport loop holds
**3.2%** of child CPU and the **source workers hold 76.5%**, so the CPU that exists is the lane's
own Python work on worker threads and in the phases those workers feed.

**Nothing was optimised here.** `src/` is byte-identical to the first pass's tree; what this slice
produces is a decision and its price.

> **Follow-up, same day:** the change this document priced is now
> [implemented and measured](fetch-pipeline-row-payload-conversion-2026-10-10.md) — **−17.49%**
> (95% CI [−18.69, −15.69]), parity intact, its win attributed to `deduplicating` (−4.9 s) and
> `reconciling_identities` (−4.0 s) while the unconverted `applying_lifecycle` phase stays flat.
> Two notes on this document's own instructions stand, and both are findings rather than nits:
> the shipped result lands **below the −25.7%** priced here because the probe's ablation shares
> nested containers (a copy contract the shipped `to_dict` preserves, as step 1 asked) and because
> ~13% of the priced calls stay unconverted; and **`ab.py --resume` does not verify arm
> environments**, so step 2's "re-measure on the same study directory with `--resume`" would have
> spliced four probe-ablated candidate arms into a study about shipped code. The re-measurement was
> run as a fresh paired study on this epoch and these flags instead, which is the comparability the
> step was actually asking for.

## What the first pass already spent

Pass 1 took the target S1 named — the regex-heavy text/location normalisation path, where
`re.sub` alone held 31.6% of the sampled CPU — and measured **−33.2%** (95% CI
[−45.1, −30.0]) on the reference subset epoch by memoising that path. So the normalisation share
is spent, and what remains in S1's own frame table is short:

| S1 frame | Self time | Domain |
|---|---:|---|
| `dataclasses._asdict_inner` | 26.9 s | serialization, worker/phase-side |
| `pipeline_runtime_summary.update_fetch_work_item_progress` | 25.6 s | progress bookkeeping (already signature-gated) |
| `_rendered_cards` | 15.7 s | HTML card extraction |
| `html_parsers.html_fragment_lines` | 9.4 s | HTML fragment splitting |
| `pipeline_runtime_writers.write_task_state` | 7.3 s | task-state writes |

## Re-measuring today's code is what chose the target

S1's numbers predate pass 1, so the choice was made on a fresh profile of the code as it stands:
one replay of the reference subset epoch under `python -m cProfile`
(`_out/perf-runs/price-probe-20261010-183856/`). This is a **ranking**, not an absolute: cProfile
inflates a 61–81 s arm to 284 s of accounted time, and it over-weights high-call-count functions,
which is exactly the class it is being used to rank.

The call count this produced is not a profiler artifact: the priced ablation's own counter (below)
reports **exactly** the same 2,076,173 calls in a run that never touched cProfile.

| Frame | Calls | Self | Cumulative |
|---|---:|---:|---:|
| `dataclasses._asdict_inner` | 116,426,747 | **54.5 s** | 81.4 s |
| `src/jobs/models.py:166 to_dict` | **2,076,173** | 0.9 s | **85.4 s (30.1%)** |
| `dataclasses.fields` | 2,076,218 | 9.1 s | 15.4 s |
| the field-filter genexpr in `asdict` | 78,894,761 | 5.7 s | 5.7 s |
| `builtins.getattr` | 80,124,989 | 9.9 s | 9.9 s |
| `text_utils.normalize_url` (with ~34 s of `urllib.parse` self time under it) | 3,205,071 | 9.4 s | 57.8 s |
| `json.dump` / JSONL row writes | 121,106 / 12.8 M | 3.9 s / 4.7 s | 24.4 s / 11.9 s |

**The row-payload conversion is the largest named cost in the lane**, and it is pure waste:
`CanonicalJob.to_dict()` runs `dataclasses.asdict` — a recursive deep copy of all 40 fields and
their lists and dicts — **2,076,173 times for 30,053 kept rows (~69 per row)**, across 71 caller
frames (57 of them in `src/`), each doing it to read one or two fields. The idiom repeats:

```python
payload = job.to_dict() if isinstance(job, CanonicalJob) else dict(job)
source = clean_text(payload.get("source"))
```

Where the calls come from — the profiler's caller attribution, which is a ranking (it accounts for
~1.4 M of the 2.08 M calls; the ablation's own counter above is the total):

| Caller | Calls |
|---|---:|
| `dedup_identity._is_elevato_static_google_sheets_pair` (a *predicate*: it converts **both** sides of every candidate comparison) | 277,036 |
| `availability_identity.prepare_availability_identities` (one whole-corpus pass) | 202,779 |
| `availability_identity._apply_prepared_identity` · `_resolve_residual_identity_conflicts` · `_identity_audit`, `state_lifecycle_orchestration.apply_job_lifecycle_state`, `reporting_social.summarize_social_experiment`, `dedup_record_merge._enrich_unknown_company_from_gracklehq_redirect` | ~60k each |
| `dedup_identity._has_meaningful_locations` · `dedup_secondary_key` · `_sparse_identity_key` | ~32k each |
| one conversion per row from `dedup.py`, `dedup_gate.py`, `dedup_targeting.py`, `canonicalize_locations.py`, `pipeline_source_loop.py`, `pipeline_source_results.py`, `reporting_summary.py`, `pipeline_finalize.py` | 30,053–30,949 each |

Two candidates ranked below it, and neither is priced here:

- **The URL family.** `normalize_url` (3.2 M calls, 57.8 s cumulative) plus ~34 s of
  `urllib.parse` self time under it is the second-largest cluster, with 853k `fingerprint_url` /
  `canonical_url_fingerprint_seed` calls and 728k `is_public_job_url` calls. It is the same seam
  pass 1 memoised, so it is the natural *next* target after this one — but the to_dict storm is
  larger, and its work is a copy nobody reads. It was taken next and measured **−15.10%**
  ([snapshot](fetch-pipeline-url-family-2026-10-11.md)).
- **Progress and task-state bookkeeping** (S1's 25.6 s + 7.3 s). Ticks scale with sources, not
  rows, and the reference epoch has five sources: this epoch cannot price it, so it stays
  unpriced rather than guessed.

## Pricing it without committing to it

`tools/measurements/corpus/probes/row_dict/sitecustomize.py` is a **priced-ablation payload**.
It is injected into one arm through `PYTHONPATH` (the interpreter imports `sitecustomize` before
the driver runs), so:

- `src/` is untouched — the two arms differ by exactly one environment variable, which is what
  makes the delta attributable;
- the ablation is *not* shipped, which is the point: a switch in the product would answer the
  question with the thing being asked about;
- the payload carries the `cached` ablation: one payload built per instance (a plain field→value
  map, no deep copy), and every later caller gets a fresh **top-level** copy of it.

Two properties make the measured delta a **lower bound** on the candidate's ceiling, which is the
safe direction for a price: the probe still allocates a 40-key dict per call (a real fix reads the
field and allocates nothing), and it still builds one payload per instance. Its nested containers
are shared with the instance where `asdict` deep-copied them — so the arms' parity line is also the
check that no caller depends on that copy.

**The probe proves it applied.** It writes a marker per process (calls, builds, distinct
instances, cache overflows, payload mismatches), and it *raises* if the first 25 payloads do not
equal `asdict`'s — an arm whose probe silently failed would otherwise report a delta of zero,
which is the failure mode S1 was corrected for. Equivalence is pinned offline by
[`tests/tools/test_corpus_row_dict_probe.py`](../../tests/tools/test_corpus_row_dict_probe.py):
both-mode payload equality over a battery (entities, CJK, empty containers, long descriptions),
one-dict-per-caller isolation, the counting, the unarmed no-op, and the mismatch failure.

## The price

| Item | Value |
|---|---|
| Study | 4 pairs, alternating order, arms sequential — `_out/perf-runs/row-dict-price-20261010-185005/ab/` |
| Epoch | `corpus-subset-20261008-192418` (the S0/S5/pass-1 reference corpus, 1,543 entries) |
| Arm A (control) | `PYTHONPATH=<probe dir>` — no ablation armed |
| Arm B (candidate) | the same, plus `BALUFFO_ROW_DICT_ABLATION=cached` |
| Verdict | **`arm-b-faster`** — median paired delta **−25.721%**, 95% CI **[−28.175, −23.731]** |
| Per-pair deltas | `[-27.51, -23.933, -28.175, -23.731]` |
| Arm medians | A 61,028 ms · B 46,414 ms (report window); process wall 81,079 ms · 65,988 ms |
| Parity | **zero divergences** — 8 arms × 30,053 rows, all four units equal to the record, every arm `blockedRemote == 0`, every exit 0 |
| Referee | the pinned epoch verified by one fresh replay before the first pair (exit 0, `problems: []`, `misses: []`) |
| `perf_compare.py` over the arm medians | −23.95%, status `passed` |
| Probe markers (4 armed runs) | 8,304,692 calls, 1,164,224 builds → **86.0% of conversions redundant**; 0 payload mismatches |

Per-source stage timings are unchanged in both arms — `canonicalization` 7,210 ms → 7,464 ms,
`fetchAndParse` 5,193 ms → 5,170 ms, `parseCsv` 1,321 ms → 1,302 ms, summed source time 16.8 s in
both — which is the corroboration that matters: **the 14.6 s the arm shed (61.0 s → 46.4 s) is
not source work.** Pass 1's win lived in those stages; this one lives in the phases after them
(dedup, identity, lifecycle, reporting), where the 2.07 million conversions are, and it moves the
arm wall without touching a single per-source number.

## What the price does not say

- **Engine wall clock, not the wire.** Replay serves bytes from a corpus, so the source stages
  are ~17 s of a 61 s arm here while a live run's are network-dominated. The *share* of a
  production run is smaller than 26%; the work removed is real, the percentage is this workload's.
- **One epoch, one workload.** 30,053 rows from four static boards plus the `google_sheets`
  family, single-worker: unusually high label and URL repetition, which is why the redundancy rate
  is 86%. A whole-registry epoch would show a different (plausibly lower) rate.
- **It does not decompose.** The probe removes both the deep copy *and* 86% of the builds, so it
  does not say which of the two carries the 26%. Both are candidate implementations, and the
  commit order below is chosen partly to find out.
- **Not CI-gating** (D7), like every replay A/B. Advisory.
- **The probe is not free and the price is not the shipped cost.** It caches payloads in-process
  (bounded at 200,000 entries, overflowing once per run) and keeps strong references so a
  collected id cannot be reused; the real change has no such retention. Any retained-memory
  question is therefore a question about the shipped version, not about this number.

## Decision

**Commit it.** −25.7% (CI [−28.2, −23.7]) is an order of magnitude above the 2% minimum effect and
far outside the 1.63% within-window noise floor the harness measured on this epoch, with parity
intact on every graded unit. The order that keeps each step measurable:

1. **One site first: a cheap payload build in `CanonicalJob.to_dict`.** Hand-rolled field map with
   the containers copied exactly as `asdict` copied them (the mutability contract is the one thing
   the price does not test). Smallest diff, no caller churn, and it prices the half of the storm
   that is *deep copy*.
2. **Re-measure** on the same study directory with `ab.py run --resume` — the same epoch, the same
   pairs, so the two commits are comparable rather than argued about.
3. **Then the whole-row conversion sites**, each turned into field access (or a per-row payload
   hoisted where several fields are read): the one-line idiom alone appears 20 times, and 71 caller
   frames convert at least once. That is where the 86% redundancy lives, and it is the half the
   probe could not isolate.

Two checks belong to whichever step lands first: `to_dict()`'s output is read by tests and by
callers that rebind keys on the dict they were handed, so the copy contract has to survive
(`tests/tools/test_corpus_row_dict_probe.py` pins the per-caller isolation property for the probe's
own build, and the eventual implementation should pin the same thing for `to_dict`), and
`ruff`/`mypy` stay clean on the touched files.

The URL family is the next target after this one, and it is worth noting that the two do not
compete: they are different seams in the same lane. **Taken on 2026-10-11:** it measured
**−15.10%** median paired engine wall clock on this same subset epoch, parity intact — see
[`fetch-pipeline-url-family-2026-10-11`](fetch-pipeline-url-family-2026-10-11.md). This ranking is
unchanged by that outcome; the entry it named was the entry that was next.

## Reproduce

```bash
# 1. rank today's costs on the reference epoch (cProfile; ranking, not absolute)
python -m cProfile -o _out/perf-runs/<token>/replay.prof \
    tools/measurements/corpus/run_fetch.py --mode replay --scope subset \
    --run-dir _out/perf-runs/<token>/run \
    --corpus-dir _out/perf-runs/corpus-subset-20261008-192418 \
    --sources 4 --families google_sheets --max-workers 1 --adapter-http-concurrency 48

# 2. one armed replay: the probe must be visible in its marker before a study is paid for
PROBE_DIR="$PWD/tools/measurements/corpus/probes/row_dict"
PYTHONPATH="$PROBE_DIR" BALUFFO_ROW_DICT_ABLATION=cached \
BALUFFO_PRICE_PROBE_MARKER=_out/perf-runs/<token>/markers \
python tools/measurements/corpus/run_fetch.py --mode replay --scope subset \
    --run-dir _out/perf-runs/<token>/run \
    --corpus-dir _out/perf-runs/corpus-subset-20261008-192418 \
    --sources 4 --families google_sheets --max-workers 1 --adapter-http-concurrency 48

# 3. the price itself: control and candidate differ by one variable only
python tools/measurements/corpus/ab.py run \
    --epoch _out/perf-runs/corpus-subset-20261008-192418 \
    --out _out/perf-runs/<token>/ab --pairs 4 \
    --arm-a-label control --arm-b-label row-dict-cached \
    --arm-a-env "PYTHONPATH=$PROBE_DIR" \
    --arm-b-env "PYTHONPATH=$PROBE_DIR" \
    --arm-b-env "BALUFFO_ROW_DICT_ABLATION=cached" \
    --arm-b-env "BALUFFO_PRICE_PROBE_MARKER=_out/perf-runs/<token>/markers" \
    --arm-a-concurrency 48 --arm-b-concurrency 48 \
    --scope subset --sources 4 --families google_sheets --max-workers 1
```

## Receipts and supersession

Nothing was deleted and no epoch, record or arm was edited. `_out/perf-runs/` holds: the ranking
profile (`price-probe-20261010-183856/`), the armed smoke replay whose marker verified the probe
(`price-smoke-20261010-184841/`), and the study (`row-dict-price-20261010-185005/` — `ab/ab-report.{json,md}`,
`ab/pair-01…04/arm-{a,b}`, `ab/referee-verification/`, and one `markers/probe-<pid>.json` per
armed arm). All local, never committed, cited by name so a disputed number can be re-read rather
than re-argued.
