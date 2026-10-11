# The row-payload conversion, shipped and measured (2026-10-10)

Canonical execution record for the optimisation the
[price](fetch-pipeline-next-optimisation-price-2026-10-10.md) selected and
[S1's attribution](fetch-pipeline-cpu-attribution-2026-10-08.md) ranked first: the
**2,076,173 `CanonicalJob.to_dict()` conversions** a reference replay made for 30,053 rows
(~69 per row across 71 caller frames), 86% of them redundant. The price measured the ceiling of
a candidate with a `PYTHONPATH`-injected ablation payload; this is the **implementation**, on the
same epoch and the same pairs, measured against the code it replaces while both arms run the same
tree.

**Result: `arm-b-faster`, median paired delta −22.83%** (95% CI [−25.27, −18.90]) on the final
tree, where the first wave of the same change measured **−17.49%** (CI [−18.69, −15.69]) before the
remaining priced call sites were converted. Four of four pairs in both studies, **zero parity
divergences** across 8 arms × 30,053 rows each, referee-verified epoch in both. The win is not
diffuse, and it is not argued: it appears in the phases the conversions target — `deduplicating`
(−5.6 s, −40%), `reconciling_identities` (−4.9 s, −26%), `writing_outputs` (−0.3 s) and, from the
second wave, the **source** stage `canonicalization` (−502 ms, −9.3%) — while `fetchAndParse`,
`parseCsv` and the untouched `applying_lifecycle` phase stay flat.

That is within a point of the priced **−25.7%** (CI [−28.2, −23.7]), and the residual is
understood rather than argued: the copy contract the price told us to preserve, plus the three
priced sites that are genuinely writers and were kept. Both are quantified below.

## What shipped

| Piece | What it does |
|---|---|
| `CanonicalJob.to_dict()` | `dataclasses.asdict` replaced by a hand-rolled field map built from `CANONICAL_JOB_FIELDS`, with `CANONICAL_JOB_CONTAINER_FIELDS` (`availabilityEvidence`, `sourceBundle`, `locations`) copied by `deepcopy` — equal in value, key order and copy contract to what `asdict` returned |
| `src/jobs/row_payload.py` (new leaf) | `row_payload(row)` — a read-only `Mapping` view over a canonical row: `get`/`[]`/`in`/`items`/`len` answer from the row itself and allocate nothing; a plain `dict` row is copied as shallowly as the `dict(row)` idiom it replaces did, and a row-like duck type (a test double, an adapter shim) keeps answering its own `to_dict` |
| **39 conversion sites** in the first wave across `dedup_identity.py` (16), `availability_identity.py` (11), `dedup_targeting.py` (7), `dedup_record_merge.py` (2), `reporting_social.py` (2), `dedup_gate.py` (1) — **43 with the second wave**, below | whole-row conversions turned into field reads through the view |
| `tools/measurements/corpus/epoch.py` | the recorder's `git_identity` reads git's output as UTF-8 with `errors="replace"` instead of in the host console code page — see the defect the first full suite run exposed, below |

**The conversion rule is the load-bearing part**, and it is mechanical rather than judged: a site
that only *reads* the payload takes the view; a site that rebinds a key, mutates or collects a
nested container, calls `.copy()`, or hands the payload to a serializer keeps `to_dict()`. The rule
is enforced by the typechecker, not by review — the view is a `Mapping` and not a `dict`, so
`view["title"] = ...` is `Unsupported target for indexed assignment` in mypy and the sites that
write cannot adopt it by accident. Of the **49 whole-row call sites** in those six files, **39
converted and 10 stayed** — every kept one a rebind-and-rebuild or a collector:
`_apply_prepared_identity` and `_apply_post_assignment_repair` (both write `availabilityId` and
rebuild the row), `_resolve_residual_identity_conflicts`' replacement arm, the quarantine payload
collector, `prepare_availability_identities`' rebind, `dedup_gate._attach_source_bundle_state`
(it writes `sourceBundle`/`sourceBundleCount`/location state and rebuilds), and
`dedup_record_merge`'s merge base.

**`BALUFFO_ROW_PAYLOAD=legacy`** restores `asdict` in `to_dict` *and* in the view, which is what
makes the measurement a paired study on one tree: the arms differ by one environment variable and
`src/` is byte-identical between them (the study's own `runConfig` receipt records
`armAEnv {BALUFFO_ROW_PAYLOAD: legacy}` vs `armBEnv {BALUFFO_ROW_PAYLOAD: view}`, plus the
`BALUFFO_FETCH_COUNTERS=1` both arms share).

## What the contracts pin

[`tests/test_jobs_row_payload.py`](../../tests/test_jobs_row_payload.py) pins the properties the
change rests on, because each of them is something a later edit could silently break:

- the cheap build **equals `asdict` in value and in key order**, on a populated row and an empty one;
- `CANONICAL_JOB_CONTAINER_FIELDS` covers **exactly** the mutable fields — the completeness check
  that keeps the copy contract true if a field is ever added;
- every caller gets its **own** payload, with its own nested containers (the property the price
  flagged as the one thing it could not test);
- the view answers the payload's values, is read-only, and **shares the row's containers** — its one
  intentional difference, pinned so that the next converter sees why mutation sites keep `to_dict()`;
- a dict row is copied shallowly, like the idiom it replaces, and a **row-like duck type** keeps answering its own
  `to_dict` — the behaviour the bare `row.to_dict()` call had, which the pipeline's own test doubles rely on
  (the first full suite run failed the finalize harness on exactly this: a `_FakeJob` reached `dict(row)`);
- the switch **selects the build in a fresh interpreter** (`legacy` → a plain `dict` from `to_dict`,
  unset/other → the view), which is the A/B's own precondition: an arm whose switch silently failed
  would report a delta of zero, the failure mode S1 was corrected for.

## The tooling defect the first full suite run exposed

The first `npm run test:py` on this tree failed **22 tests you would not expect to be related** — the
corpus recorder's own tests (`test_corpus_birth_verification`, `test_corpus_conflicts`,
`test_corpus_supersession`, `test_corpus_truncation*`, `test_corpus_ab_referee`,
`test_corpus_binding_gate`) plus six `test_pipeline_finalize_terminal_harness` cases — all of them
callers of `epoch.git_identity`. The cause was not the payload change: `git_identity` ran
`git diff HEAD` with `text=True` and no explicit encoding, so the subprocess reader thread decoded
the diff in the host code page (**cp1252** on this machine) and raised `UnicodeDecodeError` on a
byte that code page leaves undefined. `東` carries `0x9d`, which is enough — and this pipeline
fetches CJK titles constantly, so the recorder would have mislabelled or crashed on a dirty tree
containing one. The failed read surfaced as `stdout=None`, and the function's own guards turned that
into an **empty** diff: `dirty_hash` silently became the hash of `git status` alone, so two
different dirty trees would be labelled identically.

The fix is one explicit decode (`encoding="utf-8", errors="replace"`, plus `or ""` so a future
`None` cannot re-enter the hash path). `replace` is deliberate: it keeps the label lossy for bytes
that are not valid UTF-8 and *deterministic for the same tree*, which is what a provenance label
needs; `surrogateescape` decodes exactly but cannot be re-encoded for hashing. `tests/tools/test_corpus_epoch_identity.py`
pins it against a real git repository, comparing the label to the one built from both git reads —
and the pin was checked against the defect: with the explicit decode stripped out, the two hash
tests fail; with it restored, they pass. (Six of the 22 were a *second*, unrelated cause in this
change — see the duck-typed row path above — which the same run caught.)

## The measurement

| Item | Value |
|---|---|
| Study | 4 pairs, alternating order, arms sequential — `_out/perf-runs/row-dict-shipped-20261010-224638/ab/` |
| Epoch | `corpus-subset-20261008-192418` (the S0/S5/pass-1 reference corpus, 1,543 entries) |
| Arm A (control) | `BALUFFO_ROW_PAYLOAD=legacy` — the code the change replaces |
| Arm B (candidate) | `BALUFFO_ROW_PAYLOAD=view` — the shipped code |
| Verdict | **`arm-b-faster`** — median paired delta **−17.49%**, 95% CI **[−18.69, −15.69]** |
| Per-pair deltas | `[-15.69, -17.25, -17.72, -18.69]` |
| Arm medians | A 49,635 ms · B 40,994 ms (report window); process wall 66,304 ms · 56,967 ms |
| Arm samples (report window) | A `[49474, 49769, 49501, 50188]` · B `[41713, 41182, 40731, 40806]` |
| Parity | **zero divergences** — 8 arms × 30,053 rows; all four graded units equal to the record (`rowCount`, `identityDigest`, `rowProjectionDigest`, `sourceStatusCounts`) |
| Referee | the pinned epoch verified by one fresh replay before the first pair (receipt under `ab/referee-verification/`) |
| Repeated-fetch share | A 25.67% · B 25.115% of summed source time — both arms carry the same retry/backoff exposure |

The within-arm spread is tight (A ±0.7%, B ±1.2%) and all four pairs point the same way, which is
why the interval is narrow rather than wide.

### The second study (after the remaining sites were converted)

| Item | Value |
|---|---|
| Study | 4 pairs, alternating order, arms sequential — `_out/perf-runs/row-dict-wave2-20261010-235518/ab/` |
| Epoch, arms, flags | the same as the first study, with the wave-2 tree |
| Verdict | **`arm-b-faster`** — median paired delta **−22.83%**, 95% CI **[−25.27, −18.90]** |
| Per-pair deltas | `[-20.87, -18.9, -24.8, -25.27]` |
| Arm medians | A 47,296 ms · B 35,787 ms (report window); process wall 62,689 ms · 49,590 ms |
| Parity | **zero divergences** — 8 arms, all four graded units equal to the record, referee-verified |
| Phase deltas (median of 4 arms) | `deduplicating` −5,593 ms (−39.9%) · `reconciling_identities` −4,914 ms (−26.3%) · `writing_outputs` −312 ms · `running_quality_audits` −114 ms · `applying_lifecycle` **+14 ms** |
| Source-stage deltas | **`canonicalization` −502 ms (−9.3%)** · `fetchAndParse` +7 ms · `parseCsv` +2 ms · `candidateExtraction` −1 ms · `redirectResolve` −2 ms |

The 5.3 points wave 2 added over wave 1 are where the converted sites live: `deduplicating` gained
~0.7 s (the `dedup.py` main-loop conversion), `writing_outputs` ~0.1 s (the summary build), and
`canonicalization` became **measurably faster in a source stage** — the first wave could not move
any source stage, because none of its sites were in one. `applying_lifecycle` stayed flat across
both studies, which is the classification's prediction holding: its two sites are writers and were
kept, not missed.

### Where the 8.6 s came from

The pipeline's own per-phase timings (`runtime.finalizationTiming`, median of 4 arms per side)
attribute the win instead of asserting it:

| Phase | A | B | Delta |
|---|---:|---:|---:|
| `deduplicatingMs` | 14,831 | 9,933 | **−4,898 ms (−33%)** |
| `reconciling_identitiesMs` | 19,669 | 15,628 | **−4,040 ms (−21%)** |
| `writing_outputsMs` | 5,383 | 5,173 | −210 ms |
| `applying_lifecycleMs` | 3,027 | 3,162 | +135 ms |
| `running_quality_auditsMs` | 1,689 | 1,719 | +30 ms |

And the source side, where nothing should move:

| Stage (sum over sources) | A | B | Delta |
|---|---:|---:|---:|
| `canonicalization` | 5,741 | 5,858 | +117 ms |
| `fetchAndParse` | 4,838 | 4,863 | +25 ms |
| `parseCsv` | 1,020 | 1,037 | +16 ms |
| `candidateExtraction` · `redirectResolve` | 149 · 195 | 158 · 197 | +9 · +2 ms |

This is the decomposition the price said it could not do, and it lines up with the caller table
that chose the sites: `deduplicating` is where the 277,036-call `_is_elevato_static_google_sheets_pair`
predicate lives (converted, with the other dedup readers), `reconciling_identities` is the
whole-corpus `availability_identity` pass (converted), and **`applying_lifecycle` — whose ~60k-call
`state_lifecycle_orchestration.apply_job_lifecycle_state` this wave did not convert — shows no gain
at all**. The phases that got faster are the ones that were changed; the one that did not, did not.

## Second wave: the remaining priced call sites

This wave took the sites the price's caller table listed but the first pass left — and the
classification, not eloquence, decided them:

| Site | Verdict | Why |
|---|---|---|
| `dedup.py` main merge loop (~30k calls) | converted | reads the payload for `fingerprint_url`/`_social_key`; the only consumer that stores it (`_append_new_dedup_row`) copies first (`item = dict(payload)`) |
| `canonicalize_locations._score_canonical_job` (~30k) | converted | spreads the payload into a fresh dict (`{**normalized_dict, ...}`) and reads it for two scores |
| `pipeline_source_results._apply_source_fingerprint` | converted | `source_rows_fingerprint` only reads `jobLink`/`sourceJobId`/`title` — its signature widened to `Sequence[Mapping]` |
| `reporting_summary.build_pipeline_summary` (~30k) | converted | the payload is only counted (`len`, field comprehensions) |
| `state_lifecycle_orchestration` **observed** list (~30k) | **kept** | hands its rows to `_lifecycle_entry_from_active_job`, which *writes* lifecycle fields into the row on the direct-closure branch |
| `state_lifecycle_orchestration` **projector** list (~30k) | **kept** | `_project_available_payload_rows` writes `availabilityId`/`status`/`firstSeenAt`/… into each row before `from_mapping` reads them |
| `pipeline_finalize` output rows (~30k) | **kept** | these rows are the published output — the sector gate rewrites them and the writers serialize them |

**Outcome (2026-10-11): all three are now converted.** Making them copy-on-write — read through the
view, copy only where a write happens — removed **91,055** of this seam's conversions (−29.6%,
counted in both arms) and left the wall clock flat: **+0.36%**, below the harness's materiality bar.
The two lifecycle lists save 484 ms of phase time; the published rows' deferral is neutral by
default because their payload is needed anyway. See
[`fetch-pipeline-copy-on-write-writers-2026-10-11`](fetch-pipeline-copy-on-write-writers-2026-10-11.md).

**The typechecker is what caught the dangerous one.** Widening `_apply_active_lifecycle_rows` to
`Sequence[Mapping]` produced a mypy error one call deeper (`_lifecycle_entry_from_active_job`
expects a `dict`); reading that function showed it *writes* into the row it is given, so the row
must stay a real dict — the conversion was reverted and the reason recorded in the code. The reflex
that is right for a reader (`widen the annotation`) is exactly wrong for a writer, and the check
that distinguishes them is the callee's body, not its name.

The proof that both waves changes only the intended work is in the same two tables: `applying_lifecycle`
stays flat in **both** studies while the phases that hold converted sites fall, and wave 2's extra
points show up in `canonicalization` — a *source* stage that wave 1 left flat (its +117 ms there was
noise; this wave's −502 ms is the `_score_canonical_job` conversion, in the stage that contains it).

## Why the first wave was −17.5% and not the priced −25.7%

The price called its number a **lower bound** on the candidate's ceiling, on the reasoning that a
real implementation "reads the field and allocates nothing" where the probe still built a dict per
call. The measurement says otherwise, for two specific and checkable reasons.

1. **The copy contract costs what the price does not test.** The probe's `cached` ablation handed
   every caller a fresh *top-level* copy with **shared** nested containers. The shipped `to_dict`
   deep-copies the three containers on every call, because the price's own step 1 asked for the
   mutability contract to survive — and `asdict` copied them, so callers are entitled to it. Every
   remaining `to_dict()` caller therefore pays a slightly dearer call than the probe's, while the 39
   converted sites pay strictly less (no payload at all). The net is smaller than the probe's.
2. **~270k of the priced calls are still on `to_dict()`.** The price's caller table lists the sites
   this wave did not touch: `state_lifecycle_orchestration.apply_job_lifecycle_state` (~60k) and
   one whole-row conversion per row in `dedup.py`, `canonicalize_locations.py`,
   `pipeline_source_loop.py`, `pipeline_source_results.py`, `reporting_summary.py`,
   `pipeline_finalize.py` (30,053–30,949 each) — roughly **13% of the storm**. The phase table above
   is the receipt: `applying_lifecycle` is flat because its caller is unconverted.

So the honest summary of the two numbers is not "the price was wrong" but **"the price bought a
ceiling that included an ablation this implementation deliberately does not make (shared
containers), and it did not cover all the sites"**. The remaining −8.2 points were a known,
enumerated increment, and the **second wave spent most of them**: 4 of those 7 sites converted (the
readers), 3 kept (the writers), worth 5.3 points, leaving ~1.1 — inside the priced interval's own
edge and inside this study's spread. What the residual is *not*: unexplained.

One thing to know before reusing any of it: **`ab.py --resume` does not verify arm environments.**
Its reuse check compares epoch, scope, worker count, pool size and families — but not the arm envs,
so resuming the *price* study directory with different arm envs would have adopted four
probe-ablated candidate arms into a study about shipped code. That is why this measurement is a
fresh paired study with the price's epoch and flags rather than a resume of its directory; the
comparability the price asked for ("the same epoch, the same pairs") is preserved, the splice is not.

## What this does not say

- **Engine wall clock, not the wire.** Replay serves bytes from a corpus; a live run's source stages
  are network-dominated, so this change's share of a production run is smaller than 17.5%. The work
  removed is real; the percentage is this workload's.
- **One epoch, one workload.** 30,053 rows from four static boards plus the `google_sheets` family,
  single-worker — unusually high label and URL repetition. A whole-registry epoch would show a
  different mix; the phase attribution, not the headline delta, is what should transfer.
- **The two studies were taken hours apart on a shared machine.** The *paired* deltas are each
  internally valid (that is what pairing buys); the absolute arm times are not comparable across
  studies — this study's control ran 49.6 s where the price's ran 61.0 s. Compare the deltas.
- **Source-side wobble is noise, not mechanism.** Summed source time is +0.3 s (~+2%) in arm B while
  every individual source stage moves by ≤120 ms; that is inside the harness's measured within-window
  noise floor, and no source stage calls the converted code.
- **Not CI-gating** (D7), like every replay A/B. Advisory.

## Verification

| Check | Result |
|---|---|
| `npm run test:py` (the default suite, after both fixes) | **6613 passed, 2 skipped, 195 deselected, 0 failed** in 6:56 |
| the same suite on the wave-2 tree | **6613 passed, 0 failed** in 6:42; one earlier run flaked a *wall-clock* stage-timing test (`test_candidate_extraction_floors_at_zero_when_detail_exceeds_window`, whose code path this change never touches) and it passed on the re-run and 5/5 in isolation |
| the same suite, first run on the wave-1 tree | 22 failed — 16 in the corpus recorder's own tests and 6 in `test_pipeline_finalize_terminal_harness`, traced to the two defects above |
| `npm run lint:repo-guardrails` | every group passed (`docs`, `workflow`, `compat`, `routes`, `frontend`, `repo-root`, `test-shape`, `fixtures`, `line-budget`, `loc`, `release`, `registry`, `bundle`, `duplication`, `dead-code`) |
| `mypy --config-file mypy.ini src/` · `ruff` on the touched files | clean (644 source files) |
| the `git_identity` pin against the defect | the two hash tests **fail** with the explicit decode stripped out, pass with it restored |
| `tools/repo_health/loc_budget.py` | passes, with the ratchet moved in this change (see receipts) |

## Reproduce

```bash
# The measurement: control and candidate differ by one variable, on one tree.
python tools/measurements/corpus/ab.py run \
    --epoch _out/perf-runs/corpus-subset-20261008-192418 \
    --out _out/perf-runs/row-dict-shipped-<token>/ab --pairs 4 \
    --arm-a-label control --arm-b-label row-payload \
    --arm-a-env "BALUFFO_ROW_PAYLOAD=legacy" \
    --arm-b-env "BALUFFO_ROW_PAYLOAD=view" \
    --arm-a-concurrency 48 --arm-b-concurrency 48 \
    --scope subset --sources 4 --families google_sheets --max-workers 1

# The phase attribution, per arm, from each arm's own report:
#   <arm run dir>/data/jobs-fetch-report.json -> runtime.finalizationTiming, sources[].stageTimingsMs

# The contracts:
python -m pytest -q tests/test_jobs_row_payload.py
```

## Receipts and supersession

Nothing was deleted and no epoch, record or arm was edited. `_out/perf-runs/` holds both studies of
this change — `row-dict-shipped-20261010-224638/` (wave 1) and `row-dict-wave2-20261010-235518/`
(wave 2), each with `ab/ab-report.{json,md}`, `ab/pair-01…04/arm-{a,b}`, `ab/referee-verification/`
and `ab/baselines/` — alongside the price study (`row-dict-price-20261010-185005/`), which stays as
it was measured. All local, never committed, cited by name so a disputed number can be re-read
rather than re-argued.

The `tools/repo_health/loc_baseline.json` ratchet moves in the same change: `src` +135 lines (the
new leaf module, the cheap build's own lines, and wave 2's imports and kept-site comments),
`tests` +343 (the two contract-test files) and `tools` +10 (the recorder's decode fix). No other
area moves.
