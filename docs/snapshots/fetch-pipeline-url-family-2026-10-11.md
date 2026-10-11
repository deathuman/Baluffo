# Fetch pipeline — the URL family memo (`normalize_url` and its two wrappers)

**Date:** 2026-10-11 · **Lane:** fetch-pipeline performance · **Status:** shipped, measured,
**−15.10%** median paired engine wall clock on the reference subset epoch, parity intact.

This is the third seam taken from the price doc's ranking
([`next-optimisation-price-2026-10-10`](fetch-pipeline-next-optimisation-price-2026-10-10.md)), after
the normalisation pass and the row-payload conversion. It closes the "next target" that price doc
named: the URL family, the second-largest cluster in the profiled run.

## What shipped

Three pure functions are now memoised behind one switch, keyed on the value each one already
derives first:

| Function | Module | What it does per call | Memo |
|---|---|---|---|
| `normalize_url` | [`src/jobs/text_utils.py`](../../src/jobs/text_utils.py) | `urlparse` + a `parse_qs` tracking-param filter + sort + `urlencode` + `urlunparse` | `_normalize_url_cached`, keyed on `clean_text(url)` |
| `canonical_url_fingerprint_seed` | [`src/jobs/common/url.py`](../../src/jobs/common/url.py) | normalise, then `urlparse` plus a host-dispatch regex chain (greenhouse, smartrecruiters, personio…) | `_canonical_url_fingerprint_seed_cached` |
| `is_public_job_url` | [`src/jobs/common/url.py`](../../src/jobs/common/url.py) | normalise, then host checks and an `ipaddress.ip_address` call that raises for every non-IP hostname | `_is_public_job_url_cached` |

Each public signature is unchanged (`url: Any`); the uncached body keeps its own name
(`_normalize_url_uncached`, …) and is what the control arm calls. The recorded run called
`normalize_url` **3,205,071 times** with ~34 s of `urllib.parse` self time beneath it, plus 853k
`fingerprint_url`/`canonical_url_fingerprint_seed` calls and 728k `is_public_job_url` calls — for
codes covering 30,053 rows.

Two properties make the memo safe rather than merely fast:

- **The key is derived, not the raw argument.** `clean_text(url)` is exactly what each body computes
  before doing any work, and `clean_text` is idempotent, so a hit holds no input the function did
  not first re-derive, every non-string input lands on the key its cleaned form would, and two
  inputs that clean equal are the same call by construction. `tests/test_url_normalization_equivalence.py`
  pins this ("the key is the cleaned url…").
- **The results are immutable.** `str` and `bool` — sharing one cached value between callers is
  observably the same value, which is why this seam can be memoised outright while the row-payload
  conversion could not (that one had to preserve a per-caller copy contract).

Callers inside `text_utils` and `common/url` reach the memo too: they call the module-level name,
which resolves at call time, so `fingerprint_url`'s sha1 sits on a cached seed and
`resolve_supported_redirect_url` sits on a cached normalisation. What is *not* memoised is the
sha1 itself (measured at 0.50 s for 853k calls on this machine) and the redirect resolvers, neither
of which is in the hot three.

## The switch

`BALUFFO_URL_NORMALIZATION=legacy` restores the uncached bodies and inserts nothing into any cache;
unset — production and every other caller — runs the cached path. It is a *separate* switch from
pass 1's `BALUFFO_TEXT_NORMALIZATION`, because the two seams are priced on separate studies: the URL
study runs both arms with text normalisation at its production setting, so the URL memo is the only
moving part. Both `normalize_url` and the two `common/url` wrappers read the flag as a module global
at call time (one load, never an env lookup), which is what lets the equivalence test exercise both
paths in a single process.

## The price, measured on the corpus lane

Same epoch as every other study in this lane (`corpus-subset-20261008-192418`), same flags, one
variable, fresh study directory, pairs alternating order:

```bash
python tools/measurements/corpus/ab.py run \
    --epoch _out/perf-runs/corpus-subset-20261008-192418 \
    --out _out/perf-runs/url-family-20261011/ab --pairs 4 \
    --arm-a-label control --arm-b-label url-family \
    --arm-a-env "BALUFFO_URL_NORMALIZATION=legacy" \
    --arm-b-env "BALUFFO_URL_NORMALIZATION=fast" \
    --arm-a-concurrency 48 --arm-b-concurrency 48 \
    --scope subset --sources 4 --families google_sheets --max-workers 1
```

| | A (control, `legacy`) | B (candidate, `fast`) |
|---|---:|---:|
| median report wall | 35,333 ms | **30,052 ms** |
| median process wall | 48,709 ms | 42,308 ms |
| rows | 30,053 | 30,053 |

- **Verdict:** `arm-b-faster` — median paired delta **−15.10%** (95% CI [−15.19%, −13.86%]), 4/4
  pairs negative ([−15.19, −13.86, −15.08, −15.13] — a 1.3-point spread).
- **Parity:** **0 divergences** against the pinned epoch on 8 arms × 30,053 rows. A divergence would
  have suppressed the verdict; the memo changed no output.
- **Referee:** the epoch was verified by one fresh replay before any pair ran
  (`ab/referee-verification/referee-verification.json`).
- **Repeated-fetch exposure:** A 29.1% · B 29.9% of summed source time, i.e. the arms meet the same
  retry/re-fetch workload, so the delta is not a shift in what was fetched.

Mechanism and size match, so this is not a mystery number.

## Where the time moved

Medians per arm, from each arm's own `data/jobs-fetch-report.json`:

| stage | A ms | B ms | delta |
|---|---:|---:|---:|
| `reconciling_identities` | 13,618 | 8,852 | **−4,767 (−35.0%)** |
| `running_quality_audits` | 1,508 | 969 | **−539 (−35.7%)** |
| `applying_lifecycle` | 2,579 | 2,351 | −228 (−8.8%) |
| `deduplicating` | 8,257 | 8,063 | −194 (−2.3%) |
| `writing_outputs` | 4,502 | 4,491 | −10 (−0.2%) |
| summed source duration | 12,384 | 12,074 | −310 (−2.5%) |
| **finalization total** | ~30,464 | ~24,726 | **−5,738** |

- The win lands where the family is called: `reconciling_identities` (identity reconciliation, the
  heaviest consumer of the seed and public-URL verdicts) gives up 4.8 s and the quality audits
  another 0.5 s, each ~35% off.
- `writing_outputs` — which does no URL work — is the control: **−0.2%**. Nothing drifted into it.
- Source stages move only −2.5% (−310 ms), and essentially all of it is one stage: `google_sheets`
  at −306 ms median. The four static sources move ≤2 ms median (max 8 ms) — the adapters barely
  touch this family, so the seam is a *finalization* cost, not a fetch cost.
- Summed stage deltas (−5.7 s finalization, −0.31 s sources) match the report-wall delta (−5.28 s)
  within the harness's noise floor.

## What the first attempt taught (the ablation that could not reach the seam)

The lane's usual pricing tool is a `PYTHONPATH` `sitecustomize.py` ablation that swaps a symbol
without editing `src/`. It was written first
([`tools/measurements/corpus/probes/url_family/sitecustomize.py`](../../tools/measurements/corpus/probes/url_family/sitecustomize.py))
and it **failed for a reason worth recording**:

- Its wrap of `canonical_url_fingerprint_seed` counted **853,325 calls** on a full replay — matching
  S1's profile exactly — so the probe was armed and its counter worked.
- Its wrap of `normalize_url` counted **0 calls** in the same replay.

The difference is the binding, not the function. `CanonicalJob.to_dict` is a class attribute, so it
is always looked up through the class and a patch always lands. These three are *module functions*,
bound by name in 51 modules (`from src.jobs.text_utils import normalize_url`), and importing
`src.jobs.common.url` or `src.jobs.text_utils` to find the function already pulls the package's
modules in — several of which bind the original at import time. A module-attribute patch therefore
reaches only the callers imported after it, which is why one wrapper counted and the other did not.
The counter was not broken; the seam was unreachable that way.

So the price was taken from the shipped implementation's own switch instead — one variable, one
tree, the same paired protocol the row-payload conversion used for its second wave. The abandoned
probe stays in the tree as the receipt for the attempt rather than being deleted, and the lesson is
narrow and reusable: **price a module-function seam through a switch inside the function, not
through `PYTHONPATH` attribute patching.**

## Verification

| Check | Result |
|---|---|
| `python -m pytest -q` (whole default tree, includes the `slow`/`packaging`/`release` markers) | **6812 passed, 3 skipped, 0 failed** in 8:00 — a superset of `npm run test:py`'s selection (which deselects those markers) |
| `tests/test_url_normalization_equivalence.py` (new) | 5 passed — mode equivalence on 51 URL shapes, memo-hit/memo-miss instrumentation per cache, the cleaned-key property, repeatability, and private-vs-public verdicts |
| `npm run lint:repo-guardrails` | every group passed (15/15, including `loc`, `line-budget`, `dead-code`) |
| `mypy --config-file mypy.ini src/jobs/text_utils.py src/jobs/common/url.py` | clean |
| `ruff check` · `ruff format --check` on the three touched files | clean |
| `tools/repo_health/loc_budget.py` | passes with the ratchet moved in this change (below) |

The equivalence test is the check that matters: it compares both arms' outputs on every public entry
point across the same shapes the pipeline sees (tracking params, blank values, repeated keys,
uppercase hosts, ports, fragments, userinfo, IDN, localhost/private/global IPs, trailing dots,
non-strings), so a memo that collapsed two distinct URLs or cached a host verdict the wrong way
would fail here rather than silently invalidate the A/B's parity line.

## Reproduce

```bash
# The measurement: control and candidate differ by one variable, on one tree.
python tools/measurements/corpus/ab.py run \
    --epoch _out/perf-runs/corpus-subset-20261008-192418 \
    --out _out/perf-runs/url-family-20261011/ab --pairs 4 \
    --arm-a-label control --arm-b-label url-family \
    --arm-a-env "BALUFFO_URL_NORMALIZATION=legacy" \
    --arm-b-env "BALUFFO_URL_NORMALIZATION=fast" \
    --arm-a-concurrency 48 --arm-b-concurrency 48 \
    --scope subset --sources 4 --families google_sheets --max-workers 1

# The phase attribution, per arm, from each arm's own report:
#   <arm run dir>/data/jobs-fetch-report.json -> runtime.finalizationTiming, sources[].durationMs

# The contracts:
python -m pytest -q tests/test_url_normalization_equivalence.py
```

## Caveats, and what is left in this seam

- **Replay measures the engine, not the wire.** Network time is removed, so −15.10% is a statement
  about the pipeline's own work. A refutation would be trustworthy at face value; a confirmation
  still needs a live paired run before anyone quotes it as a production wall-clock win.
- **One subset workload** (five registered sources, one worker, 48-way adapter concurrency). The
  family's cost is row- and URL-shaped, and this epoch's label/URL repetition is unusually high —
  `tools/coverage_drain.py --verify-collected` remains the delivery gate, not this number.
- **Not CI-gating** (D7), like every replay A/B. Advisory.
- **The residual is the same gap the previous two slices showed:** 57.8 s of profiled cumulative
  time becomes 5.3 s of wall because 3.2 M calls spread across 48 threads — cumulative profile time
  is not wall time, and the difference is *expected*, not slack.
- **Still uncached in this family:** `fingerprint_url`'s sha1, `is_supported_redirect_url` and
  `resolve_supported_redirect_url`. None is hot in the recorded run; converting them would need the
  same treatment and would buy little.
- **Larger seams remain**, ranked but unpriced beyond this lane's epoch: the JSON/JSONL write path
  (`json.dump` 24.4 s cumulative) and progress/task-state bookkeeping, which this subset epoch
  cannot price at all.

## Receipts and supersession

Nothing was deleted and no epoch, record or arm was edited. `_out/perf-runs/url-family-20261011/`
holds the whole slice — `test-py.log` (the suite), `study.log`, and `ab/` with
`ab-report.{json,md}`, `pair-01…04/arm-{a,b}`, `referee-verification/` and `baselines/`. All local,
never committed, cited by name so a disputed number can be re-read rather than re-argued.

The price doc stands as it was measured: it ranked the URL family second and called it the next
target, and this change is that target being taken rather than that ranking being revised.

The `tools/repo_health/loc_baseline.json` ratchet moves in the same change: `src` +68 lines (the
memo wrappers and the uncached-body split), `tests` +197 (the equivalence test) and `tools` +168
(the abandoned probe, kept as a receipt). No other area moves.
