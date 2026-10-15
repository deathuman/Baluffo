# Discovery receipt LOC allowance

The user explicitly authorized a justified LOC allowance for discovery receipt producer work. The earlier sections below are historical snapshots; the superseding dated section at the end records current P1b acceptance and baseline values. This note documents the allowance and does not itself change implementation or the baseline file.

## Tracked LOC adjustment

The fresh measured output reports these tracked totals. Only the `src` and `tests` baseline entries were raised, exactly to those totals; all other entries and the gate implementation remain untouched.

| Area | Previous baseline | Fresh measured tracked total | Delta |
| --- | ---: | ---: | ---: |
| `src` | 181,372 | 181,725 | +353 |
| `tests` | 237,556 | 237,599 | +43 |

These are inherited aggregate deltas, not causal attribution to this work or to any particular file or change. The allowance reflects honest lifecycle transport/state and producer wiring in the scoped work, together with regression coverage. It adds no speculative headroom. Downstream gate verification is still pending.

## Owned untracked-file forecast

The supplied measurement lists these owned untracked files:

- `src/shared/page_fetch_lifecycle.py`: 308 LOC
- `tests/source_discovery/test_page_fetch_lifecycle_propagation.py`: 112 LOC
- `tests/test_shared_http_batch_lifecycle.py`: 287 LOC
- `tests/source_discovery/test_page_fetch_lifecycle_callback_compatibility.py`: 314 LOC

Together, that is a forecast of +308 `src` LOC and +713 `tests` LOC. If these files are later tracked with no other count changes, the corresponding area totals would be 182,033 (`src`) and 238,312 (`tests`). These forecast values are not included in the current baseline adjustment and are not spare headroom. Newly tracked owned files need an exact measured baseline adjustment in their eventual change.


## Follow-on P1b allowance (2026-10-12)

The user authorized a justified LOC allowance for P1b. Fresh tracked totals: src 181742 (previous baseline 181725, delta +17), tests 237599 (previous baseline 237599, delta +0). Baselines were set to these exact measured totals only, without spare headroom. This aggregate delta is not causally attributed to this attempt or particular files because the working tree contains inherited edits. P1b source changes are in web_page_job_stage.py, web_search_scan.py, web_search_candidates.py, and gameprog.py; producer-wide acceptance remains pending.

## Superseding P1b closeout (2026-10-13)

This section supersedes the 2026-10-12 P1b snapshot above: its totals `181742` (`src`) and `237599` (`tests`), and its pending-acceptance wording, are historical rather than current.

Independent P1b acceptance is **PASS** (`orun-40-fca70204`). The exact current entries in `tools/repo_health/loc_baseline.json` are:

| Area | Current exact baseline | Superseded snapshot |
| --- | ---: | ---: |
| `src` | 181766 | 181742 |
| `tests` | 237692 | 237599 |

All other tracked-area entries are unchanged from the measurement. The user authorized this justified allowance; the values are exact measured baselines with no spare headroom. The aggregate change is not causally attributed to this session because the working tree includes inherited changes. This documentation closeout does not edit the baseline file.

## Superseding P1c allowance (2026-10-13)

This section supersedes the 2026-10-13 P1b closeout above for the two areas P1c touched; its `181766` (`src`) and `237692` (`tests`) remain the correct *inherited-tree* values, but they were never written to `tools/repo_health/loc_baseline.json`, which still carried the older committed `180100` / `234555`. Running the updater therefore also ratcheted areas this slice did not cause. That split is recorded here so the baseline change is not read as a P1c delta.

Exact current entries in `tools/repo_health/loc_baseline.json` after the P1c update:

| Area | Previous committed baseline | Current exact baseline | Cause |
| --- | ---: | ---: | --- |
| `src` | 180100 | 182112 | inherited tree (+1666) plus P1c (+346) |
| `tests` | 234555 | 238186 | inherited tree (+3137) plus P1c (+494) |
| `frontend` | 48248 | 48318 | inherited tree only (+70); not P1c |
| `scripts` | 24780 | 24783 | inherited tree only (+3); not P1c |

P1c's own causal share, measured by running the updater with the two new files removed and then with them restored:

- `src` +346: the new `src/source_discovery/page_fetch_lifecycle_sidecar.py` is 298 lines; the two wiring edits (`src/source_discovery/directory_audit.py`, `src/source_discovery/web_search_candidates.py`) add 48.
- `tests` +494: the new `tests/source_discovery/test_page_fetch_lifecycle_sidecar.py` is 493 lines; the sidecar name in `tests/helpers/discovery_artifact_hygiene.py` adds 1.

The inherited portion is not causally attributed to this slice, and no speculative headroom was added: both areas are set to their exact measured totals. `frontend` and `scripts` moved only because the committed baseline had fallen behind the inherited working tree; their growth is not part of this change and this note does not claim it.

## P1c independent-review fixes (2026-10-13)

An independent review returned PASS-WITH-FINDINGS against the first P1c implementation (two majors, three minors). Fixing all five moved the exact baselines again, and this time the delta is fully attributable to the fixes because the baseline was already current:

| Area | Baseline after the initial P1c pass | Current exact baseline | Delta | Cause |
| --- | ---: | ---: | ---: | --- |
| `src` | 182112 | 182245 | +133 | the two major fixes (deferred rollup rebuilds, `_enforce_bounds` returning None on an unbounded envelope) plus the shared `receipt_run_id_or_none` helper and corrected docstrings |
| `tests` | 238186 | 238340 | +154 | five new constraint tests, including the cost test that fails on the quadratic shape |

`frontend`, `scripts` and every other area are unchanged from the previous measurement. No headroom was added; both areas are set to their exact measured totals. The notable point for the record is that the first P1c pass shipped a *major* defect (an incomplete quadratic fix) alongside **no test that could detect it**; the +154 test lines are what closes that gap, so they are load-bearing rather than incidental.

## P1c fix-verification round (2026-10-13)

The same independent reviewer re-verified the five fixes empirically and returned **PASS**. Acting on its recommendation to pair the cost test with a value assertion surfaced a further contract gap, so the baselines moved once more:

| Area | After the first fix round | Current exact baseline | Delta | Cause |
| --- | ---: | ---: | ---: | --- |
| `src` | 182245 | 182262 | +17 | the truncated-entry rollup `state` guard and the publish-return contract docstring |
| `tests` | 238340 | 238384 | +44 | `test_sidecar_truncated_entry_rollup_never_reports_clean` and the rollup-value assertions added inside the cost test |

All other areas unchanged. No headroom. Recorded because the +44 test lines are what make the cost test discriminate in both directions: it now fails both on the quadratic shape and on an implementation that skips rollup recomputation entirely.

## P1d projection and fingerprint allowance (2026-10-13)

P1d consumed P1c's producer output: the bridge now reads the page-fetch envelope, projects it alongside `probeLifecycle`, and stops hashing large artifacts on every poll. This is the first slice in the sequence to move the `frontend` area, because it is the first to render the receipt.

| Area | Previous exact baseline | Current exact baseline | Delta |
| --- | ---: | ---: | ---: |
| `src` | 182262 | 182459 | +197 |
| `tests` | 238384 | 238806 | +422 |
| `frontend` | 48318 | 48409 | +91 |

Attribution: `src` +197 is the bridge work (sidecar loader and projection, the fingerprint helper, the row fields, and the two ops-health projection sites). `tests` +422 is the new `tests/bridge/test_discovery_audit_artifacts_receipts.py` (211) plus the updated route assertions and the two frontend merge/render cases counted in the `tests` area. `frontend` +91 is the merge-model change (including the deeper `sources` merge, which a test proved was necessary) and the two new renderers.

All other areas unchanged from the previous measurement, and no headroom was added. The `frontend` increase is a real cost of the Ops surface, not incidental: without it the receipt would have no operator-visible rendering, which is the closure criterion P1d exists to meet.

## P2 and P3 allowance (2026-10-13)

P2 (lifecycle safety) added `src/source_discovery/probe_runtime.py` changes and
`tests/source_discovery/test_probe_batch_late_entry_race.py`. P3 (page-batch fairness) added the
host-ready acquisition swap in `src/shared/http_batch.py` and
`tests/test_http_batch_fairness_experiment.py`.

| Area | Previous exact baseline | Current exact baseline | Delta |
| --- | ---: | ---: | ---: |
| `src` | 182459 | 182495 | +36 |
| `tests` | 238806 | 238845 | +39 |

Attribution: `src` +36 is probe_runtime's `_close_and_notify` helper, the `retire_when_idle`
deferred-close parameter, the batch-exit `pool_still_active` flag, and the `http_batch` semaphore
order swap. `tests` +39 is the new `test_probe_batch_late_entry_race.py` and
`test_http_batch_fairness_experiment.py`.

All other areas unchanged, and no headroom was added. The `http_batch` swap is a reorder only (one
line moved), so its attribution is in the +0 line-level change to `src` beyond the new helpers.

## P3 reviewer follow-on (2026-10-13)

After the P3 independent review (PASS), one minor finding was closed: the fairness experiment modeled
the scheduler's semaphore order locally instead of driving the shipped `fetch_pages_batched`, so a
future revert of the host-ready ordering would escape the suite. Added
`test_real_http_batch_host_ready_lets_fast_hosts_finish_before_slow_host` to
`tests/test_shared_http_batch.py` — a direct-coverage test that drives the real function with a
grouped fixture and asserts fast-host completion is early. Confirmed discriminating: it FAILS under
total-before-host (fast max 0.527s vs slow max 1.05s, not < half) and PASSES under host-ready.

| Area | Previous exact baseline | Current exact baseline | Delta |
| --- | ---: | ---: | ---: |
| `tests` | 238845 | 238911 | +66 |

Attribution: `tests` +66 is the new direct-coverage test (from the +39 P2/P3 total already recorded,
of which +27 was this follow-on beyond `test_probe_batch_late_entry_race.py` +
`test_http_batch_fairness_experiment.py`).

## P4 allowance (2026-10-13)

P4 (deadline-capacity correctness) added the per-row request bound to the probe path.

| Area | Previous exact baseline | Current exact baseline | Delta |
| --- | ---: | ---: | ---: |
| `src` | 182495 | 182566 | +71 |
| `tests` | 238911 | 239094 | +183 |

Attribution: `src` +71 is `probe.py:probe_row_timeout_units` (the adapter-aware per-row ceiling), the
`requests_per_row` parameter and deadline re-derivation in `probe_runtime.py`, the
family-constrained `effective_concurrency` (aggregate-bucket-capacity) correction, and the
`probe_candidates_async` threading of the wave's max bound. `tests` +183 is the new
`tests/source_discovery/test_probe_deadline_capacity.py` (multi-request oracle, unit seam, and
pathological control). Because the LOC gate counts only `git ls-files` tracked files, the new test
file was staged (`git add`) before re-baselining so the ratchet reflects the real growth — matching
the plan's explicit warning that untracked new files can slip past the LOC gate.

All other areas unchanged, and no headroom was added.

## P5 allowance (2026-10-13)

P5 (homepage no-candidate CPU cleanup) removed a redundant second provider-inference run on
browser-recovery pages that yield no candidates, and added oracles proving the removal is
byte-identical.

| Area | Previous exact baseline | Current exact baseline | Delta |
| --- | ---: | ---: | ---: |
| `src` | 182566 | 182579 | +13 |
| `tests` | 239094 | 239256 | +162 |

Attribution: `src` +13 is the `_analyze_success` reorder in
`src/source_discovery/gamedevmap_active_dry_run.py` (run the page outcome first, skip the redundant
`_provider_candidates_from_html_text` pass on a no-candidate page, prepend its rows on a candidate
page to preserve assembly order). `tests` +162 is the new
`tests/source_discovery/test_gamedevmap_no_candidate_cpu_cleanup.py`: the failing-before call-count
oracle (one inference on a no-candidate page), the output-parity oracle (only the outcome's pass runs
on a no-candidate page), the candidate-full-pipeline oracle, and the candidate finalized
byte-identical parity test. The new test file was staged (`git add`) so the ratchet counts it,
matching the plan's explicit warning about untracked new files.

All other areas unchanged, and no headroom was added.

## P5 reviewer follow-on: 1MB-dropped-middle regression test (2026-10-14)

After the P5 independent review (PASS), the reviewer's strongest probe — a >1MB page whose only
provider URL sits strictly in the dropped middle of `_append_analyzed_candidates`' truncation
(`[:500k]+[-500k]`), which the new `len(html) > 1_000_000` guard was proven to preserve — moved from a
scratch probe into the shipped oracle file as `test_over_1mb_page_provider_in_dropped_middle_is_not_lost`.
It asserts the mid-provider slug still survives the full pipeline, with a sanity check that the marker
is genuinely absent from the truncated html. Discriminating: removing the >1MB guard drops the URL and
fails the test.

| Area | Previous exact baseline | Current exact baseline | Delta |
| --- | ---: | ---: | ---: |
| `tests` | 239256 | 239291 | +35 |

Attribution: `tests` +35 is the new guarded test in
`tests/source_discovery/test_gamedevmap_no_candidate_cpu_cleanup.py` (staged before re-baseline).
All other areas unchanged, no headroom added.

## P1d finding-1 close: copy-diagnostics carry assertions (2026-10-14)

Independent P1d review (`2059ed77`) returned PASS-WITH-FINDINGS. The sole substantive finding was a
non-discriminating copy-diagnostics test: the diagnostics-render assertions did not pin that
`compactAuditArtifact`'s allowlist carries `fingerprintKind` / `fingerprint` / `pageFetchLifecycle`,
so a revert dropping them would have escaped the suite. Closed by adding assertions on
`copied[0].artifacts[1]` (the page-fetch-lifecycle row) in
`tests/frontend/unit/admin-ops-diagnostics-render.test.mjs`.

| Area | Previous exact baseline | Current exact baseline | Delta |
| --- | ---: | ---: | ---: |
| `tests` | 239291 | 239300 | +9 |

Attribution: `tests` +9 is the hardened copy-diagnostics test (frontend `.mjs` under
`tests/frontend/unit/`, tracked and counted by the ratchet). All other areas unchanged, no headroom
added.

## Full-milestone 0.3.019 re-baseline (2026-10-14)

This section supersedes all earlier slice snapshots above: it records the exact measured totals for
the *whole* staged milestone that ships as 0.3.019 (the discovery-performance/TLS/frontend work
through P5, plus this release-docs guardrail fix), which was never re-baselined as one unit. The
previous committed baselines were the P1d finding-1 values; the milestone accumulated several new
tracked files and edits since then, so every delta here is the inherited-milestone aggregate, not a
single-slice causal attribution.

| Area | Previous committed baseline | Current exact baseline | Delta |
| --- | ---: | ---: | ---: |
| `src` | 182579 | 182887 | +308 |
| `tests` | 239300 | 240406 | +1106 |
| `tools` | 34269 | 34299 | +30 |

- `src` +308 and `tests` +1106 are the full staged milestone: the new
  `src/source_discovery/*` producer files, the TLS/HTTPS and page-fetch lifecycle pieces, the
  ops health/merge frontend work counted under `tests`, and their sizeable regression suites.
- `tools` +30 is this change: the `release_docs_policy.py` predicate rewrite (the "phantom is a
  section strictly below the last tagged release" model) widened the guardrail, its docstring, and
  the registration rule text.

All other areas unchanged. Values are the exact measured totals with no speculative headroom,
written to `tools/repo_health/loc_baseline.json` by `loc_budget.py --update`. The guardrail fix is
justified separately in the release-docs section of the change; this block only records that the
milestone-wide totals moved.

## Coverage-audit F-1 slice allowance (2026-10-14)

The F-1 JSON-LD raw-parse fix and its regression suite are a small, scoped change to the
coverage-audit plan (not the discovery milestone). The `src` and `tests` baselines were raised to
the exact measured totals for the tracked working tree at the F-1 commit.

| Area | Previous committed baseline | Current exact baseline | Delta |
| --- | ---: | ---: | ---: |
| `src` | 183087 | 183125 | +38 |
| `tests` | 240591 | 240664 | +73 |
| `tools` | 34591 | 34616 | +25 |
| `scripts` | 24783 | 24793 | +10 |

- `src` +38: `_load_jsonld_block` + `_deep_unescape` helpers in `html_parsers.py` and
  `_strip_leading_slug_id` in `teamtailor_runner.py`.
- `tests` +73: three regression tests in `test_jobs_fetcher_providers_individual.py`, the
  `teamtailor_vivid_entity_encoded.html` fixture, the plan-doc runbook edits, and the
  gate-alignment test edit in `test_precommit_gate.py` (its fake now accepts the
  `skip_container_version_gate` parameter). The per-file line budget for
  `tests/test_precommit_gate.py` was raised from the 400 default to 402 in
  `tools/repo_health/test_line_budget_baseline.json` to match its exact measured size.
- `tools` +25 / `scripts` +10: the commit-time gate alignment — `repo_guardrails.py` gained a
  `--skip-container-version-gate` flag and `precommit_gate.py` passes it on the local commit
  path so the pre-push container version gate no longer forces a release decision at commit
  (the extra `scripts` +2 beyond the first measure is ruff-format finishing the function
  signature wrap).

All other areas unchanged. Exact measured values, no spare headroom, written by
`loc_budget.py --update`.

## Coverage-audit F-2 slice allowance (2026-10-14)

F-2 adds the delivery-evidence guard before a pending provider may retire an active static
fallback (the 09-07 Vivid-class decision).

| Area | Previous committed baseline | Current exact baseline | Delta |
| --- | ---: | ---: | ---: |
| `src` | 183125 | 183151 | +26 |
| `tests` | 240664 | 240825 | +161 |

- `src` +26: `_has_delivery_evidence` in `registry_conflicts_row_core.py` (with its
  docstring), the re-export line in `registry_conflicts_row.py`, and the guard plus comment
  in `_analyze_pending_provider_replacement_automation`.
- `tests` +161: six new guard tests in
  `tests/bridge/test_registry_conflicts_provider_replacement.py` (probe-only, excluded,
  zero-kept, kept-delivery, plus a direct `_has_delivery_evidence` unit test) and the
  delivery evidence added to the two pre-existing promotion fixtures — the bamboohr
  weak-static fixture was converted from "promotes a zero-delivery provider" to the new
  blocked assertion, since that shape is exactly what the guard forbids.

The per-file line budget for `tests/bridge/test_registry_conflicts_provider_replacement.py`
was raised from the 800 integration default to 878 in
`tools/repo_health/test_line_budget_baseline.json`. `tools`/`scripts` are unchanged (the
JSON baseline edit is not counted by the LOC gate). All other areas unchanged; exact
measured values, no spare headroom.

> Note: the catalogue-coverage-gap plan's word count was already at the 5000-word tripwire.
> The F-2 runbook rule therefore lives in this opening-coverage plan's F-2 slice, and the
> catalogue plan carries only a one-clause pointer; three redundant sentences in the
> catalogue plan were condensed to make room without dropping information.
