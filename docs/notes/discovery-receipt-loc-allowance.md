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

> **Superseded 2026-10-19.** Skipping the gate at commit time left the commit that needs it as
> the one place it never ran, and the window check cannot see a commit that does not exist yet.
> The flag is now `--pending-container-version-gate`, and `precommit_gate.py` passes it so the
> staged commit is checked against the released version (see `docs/RELEASE.md`).

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

## Coverage-audit F-3 slice allowance (2026-10-14)

F-3 was **rescoped at slice time**: the planned registry retirement was withdrawn (its
premise — a redirect failure — was contradicted by the live evidence), and the real root
cause was fixed instead: the frozen portable build never bundled `tldextract`'s suffix
snapshot, so every `is_rebrand` call in the shipped app raised `[Errno 2]` (491 rows in the
2026-10-12 report).

| Area | Previous committed baseline | Current exact baseline | Delta |
| --- | ---: | ---: | ---: |
| `tests` | 240825 | 240868 | +43 |
| `scripts` | 24793 | 24802 | +9 |

- `scripts` +9: the `tldextract` entry plus its explanatory comment in
  `MAIN_RUNTIME_COLLECT_DATA_PACKAGES` (`scripts/build_portable_exe.py`).
- `tests` +43: three regression tests in `tests/test_build_portable_exe.py` — collect-data
  tuple membership, the snapshot's presence in this environment, and the real
  `PyInstaller.utils.hooks.collect_data_files("tldextract")` mapping.

`src` is unchanged (the defective call site `rebrand_detection.py` was correct; the build
config was not). The per-file line budget for `tests/test_build_portable_exe.py` was raised
from the 400 default to 443 in `tools/repo_health/test_line_budget_baseline.json`. All other
areas unchanged; exact measured values, no spare headroom.

## Coverage-audit F-4 slice allowance (2026-10-14)

F-4 adds the `dover` provider adapter (the Hatsu Labs opening) — a new leaf, runner, and
parser plus the peer-provider wiring, the curated row, and 34 tests.

| Area | Previous committed baseline | Current exact baseline | Delta |
| --- | ---: | ---: | ---: |
| `src` | 183151 | 183902 | +751 |
| `tests` | 240868 | 241305 | +437 |
| `scripts` | 24802 | 24803 | +1 |

- `src` +751: `src/dover_board_urls.py` (the slug/URL leaf, also registered in the
  ship-bundle runtime manifest), `src/jobs/adapters/plugins/provider_api/dover_runner.py`
  (the two-step slug → client_id → jobs runner), `parse_dover_jobs_payload` in
  `src/jobs/adapters/parsers/json_payloads.py`, the Dover probe counter in
  `src/source_discovery/probe.py`, the inference/pattern candidate builders, and the
  single-token `dover` additions at each peer-provider seam (`PROVIDER_ADAPTERS`, host maps,
  `SUPPORTED_PROVIDERS` and its weights, dedup namespace, registry loader selection,
  source-state keying, `SOURCE_REPORT_META`).
- `tests` +437: the new `tests/test_dover_provider.py` (34 tests) plus the one-line
  provider-weight expectation update in `tests/source_discovery/test_config_and_helpers.py`.
- `scripts` +1: the `dover_board_urls.py` entry in `scripts/ship_bundle_manifest.py`'s
  `APP_RUNTIME_SCRIPTS` — without it the packaged app would not ship the new leaf (the same
  class of defect F-3 fixed).

## Coverage-audit F-5 slice allowance (2026-10-14)

F-5 adds a Studio Gloam static bundle extractor, the plugin-specific filter exception,
registration and regression tests. The measured tracked-area changes from the inherited
baseline are:

| Area | Previous exact baseline | Current exact baseline | Delta |
| --- | ---: | ---: | ---: |
| `src` | 183903 | 184152 | +249 |
| `tests` | 241305 | 241579 | +274 |

`src` +249 comprises the 239-line dedicated plugin and +10 lines in the narrowly scoped
fast-path exception/dispatch. `tests` +274 is the new parser, source-flow, decoy regression,
and unrelated-plugin negative-control coverage. This exact F-5 worktree measures
`src=184156` and `tests=241579`; the source increase comprises the prior +249 allowance plus
four lines already present in F-5's shared-filter exception. Concurrent frontend work is
excluded from this F-5 commit and from its baseline. All tracked areas are ratcheted to exact
measured totals with no spare headroom.

The per-file line budget for `tests/test_dover_provider.py` was raised from the 400 default
to 437 in `tools/repo_health/test_line_budget_baseline.json`. All other areas unchanged;
exact measured values, no spare headroom. The new files were staged before re-baselining so
the LOC ratchet counts them (the plan's explicit warning about untracked files).

## Jobs launch and startup gate slice (2026-10-19)

One slice: the Jobs page stopped reporting a failed update while the run was still
starting, the packaged start gate became observable, and both paths stopped parsing an
artifact they only needed a count from. Measured against the baseline committed at
`a04cfe10`.

| Area | Previous baseline | Current exact baseline | Delta |
| --- | ---: | ---: | ---: |
| `frontend` | 49475 | 49570 | +95 |
| `scripts` | 24872 | 25422 | +550 |
| `src` | 186421 | 187186 | +765 |
| `tests` | 250758 | 252872 | +2114 |
| `tools` | 35382 | 36146 | +764 |
| `<root>` | 1902 | 1903 | +1 |

- `frontend` +95: the unconfirmed-start state fields in `runtime/state.js`, and the
  classification, grace expiry, and pending-start copy in `runtime/pipeline-controller.js`.
- `scripts` +550: `precommit_gate.py` passes the staged-commit gate, and the release commit
  carried the manual `ui_review_harness.mjs` that was already staged beside it.
- `src` +765: the launch telemetry wrapper and phase timers in `pipeline_service_stages.py`;
  the background manifest rebuild (`job_availability_service.py`, `api.py`,
  `admin_task_runtime.py`); the gate journaling across `runtime_launcher.py`,
  `startup_telemetry.py`, `startup_profile.py`, and `packaged_smoke/*`; the bounded
  feed-presence read and gate evaluation in `jobs_first_run_state.py`; the output-count
  fast path (`fetch_report_identity.py`, `fetch_report_summary.py`,
  `task_launch_api_bootstrap.py`); and the default parallel bridge spawn in
  `desktop_app/launcher_flow.py`.
- `tests` +2114: the new regression files — startup gate check (+590), jobs row quarantine
  (+387), fetch output count (+210), fetch report success metadata (+263), feed presence
  (+101), bootstrap sidecar (+160), commit-time gate (+167) — plus additions to the
  runtime-launcher, startup-profile, launch-stage, launcher timing, admin thin-wrapper,
  frontend start-state, and precommit-gate suites.
- `tools` +764: the two measurement tools, `fetch_report_read_audit.py` (+264) and
  `launch_baseline_read.py` (+419), with their README section.
- `<root>` +1: the vulture whitelist entry for the output-count cache reset that tests and
  the measurement tools call.

The window-close follow-up sits on the same slice: `tools` +4 rewrote the window failure
message in `container_version_policy.py` to name the shipped-commit requirement, `scripts`
+5 documented the same rule where the commit gate runs, and `tests` +29 added the wording
and docs-only-intent tests in `test_container_version_policy.py`. The per-file line budget
for that file was raised from 453 to 482, its exact measured size.

The end-to-end coverage follow-up adds `tests` +60 (`252901` → `252961`): two
temporary-git-history tests in `test_container_version_policy.py` that build a bump-first
window (bump commit, then a shipped commit, then a docs commit carrying
`Release-tag:`), assert the rendered failure both names the shipped-commit requirement
and lists the shipped commit while excluding the bump anchor, and then tag that exact
history to show ancestry clears it before a post-tag commit reopens it as a
released-version failure. The per-file budget for that file moves to 542, its exact
measured size.

Sharing that remedy with the released branch costs `tools` +11 (`36150` → `36161`: the
`_WINDOW_REMEDY_RULE` constant and the two message tails that now use it) and `tests` +41
(`252961` → `253002`: one test asserting the shared sentence appears in both failures, and
one real-history test showing a bump committed before the shipped code reopens the window
and a bump after it closes). The per-file budget for that file moves to 583, its exact
measured size.

`styles` and `probes` are unchanged. Exact measured values, no spare headroom.

## Container-target dependency audit slice (2026-10-20)

The dependency audit described the developer's machine rather than the image:
`pip-audit -r requirements-lock.txt` built a venv with this host's OS and interpreter,
pulled Windows-only PyInstaller transitives into the report, and re-resolved the lock on
every run. It now resolves the lock against the container's own target (CPython 3.13,
manylinux x86_64 — taken from the `Dockerfile` stage and pinned by a test) and then audits
the declared pins with `--no-deps --disable-pip`, so nothing is installed or re-resolved
locally. A pin the image cannot install fails the audit by name.

| Area | Previous baseline | Current exact baseline | Delta |
| --- | ---: | ---: | ---: |
| `scripts` | 25427 | 25576 | +149 |
| `tests` | 253002 | 253072 | +70 |

- `scripts` +149: the target constants and platform tags, the lock-pin reader, the
  cross-platform resolution command and its verify step (which names any pin the image
  cannot install), and the two audit flags.
- `tests` +70: the resolution-command and Dockerfile-alignment tests, the
  missing-distribution failure test, and the updated command/exit-code tests.

`docs/testing.md` records the new audit shape. All other areas unchanged; exact measured
values, no spare headroom.

## Discovery lane-parity tool (2026-10-26)

The 32/64-lane parity study that answered P6's R4 ran from an untracked harness under `_out/`. It is
now a reusable tool, so `tools` and `tests` grow by exactly its size; `src` is untouched because the
tool only reads artifacts the audit already writes.

| Area | Previous baseline | Current exact baseline | Delta |
| --- | ---: | ---: | ---: |
| `tools` | 38763 | 39550 | +787 |
| `tests` | 263145 | 263592 | +447 |

- `tools` +787: `tools/measurements/discovery/lane_parity.py` (new) — the `arm` driver and its
  lane proof, the counterbalanced `study` runner, and the `report` that judges pairs on boards
  keyed by host + tenant and on whole-run failure counts against same-lane churn, attributing each
  differing board to its host's record in the other arm. `tools/README.md` and
  `tools/measurements/README.md` route to it; Markdown is not counted here.
- `tests` +447 across three files: `_lane_parity_fixtures.py` (102) writes an arm's artifact and
  record so a study is described as boards and failures; `test_lane_parity.py` (101) pins the
  record's population digest, the lane-proof refusals, arm immutability and the moved-study
  artifact fallback; `test_lane_parity_report.py` (244) covers tenancy-keyed board identity, the
  population pin, whole-run counts against the capped sample, host-failure attribution, the churn
  floor and every verdict path including `inconclusive`, and the counterbalanced run order. Each
  stays under the 400-line default, so no per-file exception.

All other areas unchanged; exact measured values, no spare headroom.

## Lane-parity 429 attribution repair (2026-10-26)

The correction that read the lane study's Workable losses as 429s rather than zero-job outcomes grew
the tool's attribution inputs and pinned the trap with two tests.

| Area | Previous baseline | Current exact baseline | Delta |
| --- | ---: | ---: | ---: |
| `tools` | 39550 | 39585 | +35 |
| `tests` | 263592 | 263707 | +115 |

- `tools` +35: boards carry `sourceId` and the endpoint, rejections carry identity, name, adapter and
  endpoint, `_hosts_for` reads the endpoint host, `_same_candidate` separates two tenants of one
  studio, and a matched rejection with no `failureErrorCounts` hit counts as the host failure it is.
- `tests` +115: the fixtures can write rejections and endpoint-bearing boards; the report tests add a
  429 that must beat a sibling's zero-job row and two Square Enix tenants that must keep their own zero.

All other areas unchanged; exact measured values, no spare headroom.

## Homepage-analysis worker transport (2026-10-27)

The overlap that hides the GameDevMap audit's homepage-analysis phase behind the next batch's fetch
grew `src` by a new worker transport and a head/tail split of the batch runtime, and `tests` by the
transport's own suite and the pipelined loop's.

| Area | Previous baseline | Current exact baseline | Delta |
| --- | ---: | ---: | ---: |
| `src` | 190710 | 191643 | +933 |
| `tests` | 264128 | 264667 | +539 |

- `src` +933: `src/source_discovery/homepage_analysis_child.py` (new, 445) is the transport — spawn,
  hand-off on its own thread, collect, fallback, retire, and its receipt. The other 488 sit in
  already-tracked modules: `active_audit_loop_runtime.py` gains the pipelined loop beside the
  sequential one, `active_audit_batch_runtime.py` the batch head/tail split and the two phase-receipt
  drains, `gamedevmap_active_dry_run.py` its wiring and the receipt publication,
  `gamedevmap_artifact_store.py` the receipt writer, `active_audit_runtime.py` and
  `active_audit_contracts.py` the strategy surface (`analysis_start`/`analysis_collect`,
  `ActiveAuditBatchHead`, the drain contract) and `active_audit_batch_strategy.py` the passthrough.
- `tests` +539: `test_homepage_analysis_child.py` (new, 292) pins out-of-process execution, the
  disabled and fallback paths, the env knob, and a whole audit run whose candidates and rejections
  match the in-process run; `test_active_audit_batch_pipeline.py` (new, 230) pins the head/tail split
  and the overlap order; `tests/conftest.py` (+15) and `test_phase_runtime_receipt.py` record the
  drain contract.

All other areas unchanged; exact measured values, no spare headroom.

## Homepage-analysis worker instrument corrections (2026-10-27)

The two instrument defects the discarded runs exposed — a deferred receipt paired by stack, and a
receipt created at the hand-off rather than at the collect — were fixed before the measured build,
which rewrote comments in the source and replaced a flaky magnitude bound with a pairing pin in the
tests.

| Area | Previous baseline | Current exact baseline | Delta |
| --- | ---: | ---: | ---: |
| `src` | 191643 | 191650 | +7 |
| `tests` | 264667 | 264746 | +79 |

- `src` +7: the corrected interval's rewritten comments, plus the pass the pre-push hooks require —
  the in-process timing is returned as `(result, collect_ms)` and handed to the recorder as explicit
  arguments (typed without a mapping the dead-code hook reads as unused), and every call of the
  strategy's optional callables goes through a bound local.
- `tests` +79: `test_phase_runtime_receipt.py` gains the pairing pin (a deferred batch's phase
  receipt is its own batch's collect, mutation-checked against the stack pairing that reported
  769 s in an 831 s arm) and drops the old magnitude bound that could pass on wall noise; the new
  suites cast their stand-in transport and results at the boundary the real types define.

All other areas unchanged; exact measured values, no spare headroom.

## Homepage-analysis no-candidate redundancy cuts (2026-10-28)

The audit's no-candidate handler stops re-running the provider inference its page outcome already
ran and stops computing the no-careers reason detail a second time; the queue takes both as inputs
from the caller that has them, and the tests pin the skipped work with an oracle file that already
covered the same shape on the browser path.

| Area | Previous baseline | Current exact baseline | Delta |
| --- | ---: | ---: | ---: |
| `src` | 191650 | 191672 | +22 |
| `tests` | 264746 | 264926 | +180 |

- `src` +22: `gamedevmap_recovery_queue._queue_no_careers_recovery` takes `reason_detail` and
  `provider_pass_done`, with the docstring that says what each caller guarantees, and
  `gamedevmap_active_dry_run._handle_no_candidate` computes the detail once, hands it down, and
  records the removed work it was measured to cost (5,874 second passes / 15.3 s, 8.5 s of duplicate
  detail per production arm).
- `tests` +180: `test_gamedevmap_no_candidate_cpu_cleanup.py` gains the audit-path section — one
  inference and one detail per no-candidate page, the output-parity check that forces the old
  behaviour back on and compares all seven outputs, the js_shell branch still firing through the
  handed-down value, a candidate page that never reaches the handler, and the skipped pass returning
  no rows on the fixture.

All other areas unchanged; exact measured values, no spare headroom.

## Homepage-analysis host-normalization memo (2026-10-28)

`normalized_host` is memoized on the text it produces: the transform moves into a bounded
`lru_cache` and the public function keeps the exact conversion it had for every non-`str` value.
The URL-inference memo was measured and refused (0.8% repeat rate), so it contributes no lines.

| Area | Previous baseline | Current exact baseline | Delta |
| --- | ---: | ---: | ---: |
| `src` | 191672 | 191688 | +16 |
| `tests` | 264926 | 264969 | +43 |

- `src` +16: the cache function, its bound and the public function's conversion, plus the comment
  recording the measured price (27.2 ns a call, 0.79 s over 29.19M calls, 29.2M hits / 28.2k misses
  on the corpus) and why the cache is bounded and keyed on the produced text.
- `tests` +43: `test_url_hosts.py` gains the input table the memo must reproduce exactly (hosts,
  patterns, empty, `None`, falsy non-strings, numbers, bytes, a tuple), the cache-hit pin that could
  not exist before the memo, and the bound pin that keeps a long-lived process from growing it.

All other areas unchanged; exact measured values, no spare headroom.

## Homepage-analysis provider-walk host hoist (2026-10-28)

`infer_provider_adapter` normalizes the host once per admission and compares pre-normalized
pattern text, instead of re-normalizing the same host once per pattern through the memo. The
normalizing helpers stay and delegate to two new text-level leaf functions, so every other caller
keeps its behaviour; the helper the walk no longer uses is deleted.

| Area | Previous baseline | Current exact baseline | Delta |
| --- | ---: | ---: | ---: |
| `src` | 191688 | 191720 | +32 |
| `tests` | 264969 | 265048 | +79 |

- `src` +32: `url_hosts` gains `host_matches_normalized_domain` and
  `host_matches_normalized_pattern`, whose docstrings say which duplicate work they remove, and the
  four normalizing helpers delegate to them; `provider_inference` gains the pattern table
  normalized once at import, normalizes the host once in the walk (with the comment recording the
  52 calls per admission, the recorded 629,560-admission population and the measured 3.28 s
  against the memo's 0.79 s), and normalizes in `_oracle_hcm_candidate` so a
  builder that used to go through the normalizing helper keeps the same answer; the unused
  `host_matches_any_domain_pattern` wrapper is removed (-6).
- `tests` +79: `test_url_hosts.py` gains the host/pattern tables that pin both text-level
  helpers against the normalizing ones they replace; `test_provider_inference.py` gains the walk
  table over real provider hosts, comparing `infer_provider_adapter` row by row against the generic
  per-pattern helper and asserting non-matches stay `None`.

All other areas unchanged; exact measured values, no spare headroom.

## Homepage-analysis per-body link index (2026-10-28)

One fetched body gets one lazily derived link index (`PageLinks`), shared by the page outcome, the
no-careers reason detail and both recovery waves; the recovery planner also stops running its
jobish pass twice in one plan. The extractors keep their public signatures and gain an optional
index, so every other caller is unchanged.

| Area | Previous baseline | Current exact baseline | Delta |
| --- | ---: | ---: | ---: |
| `src` | 191720 | 191909 | +189 |
| `tests` | 265048 | 265210 | +162 |

- `src` +189: the new `page_links` leaf holds the index and its six views plus the
  `page_links_for` identity guard (about 140 lines with docstrings); `web_search_extract` splits
  the href scan from the derivations it feeds and compiles the pattern once (+30); `page_analysis`,
  `static_candidates`, `web_candidate_inference`, `recovery_url_planner`, `directory_page_recovery`,
  `gamedevmap_recovery_queue` and `gamedevmap_active_dry_run` thread the optional index through
  their signatures (+30 net, less the duplicate jobish block the planner no longer carries);
  `scoring` gains `careers_keyword_count_lowered`.
- `tests` +162: `test_page_links.py` pins every view against the public extractor it replaces over a
  body that exercises all branches, pins one href scan across all views, and pins that an
  equal-but-distinct body is a different page; the no-candidate CPU oracle gains a batch-scan
  oracle that pins a whole no-candidate page to one href scan.

All other areas unchanged; exact measured values, no spare headroom.
