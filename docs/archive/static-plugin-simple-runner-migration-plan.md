# Static Plugin Simple Runner Migration Plan

> - **Status:** Archived — fully implemented 2026-05-28
> - **Use this when:** continuing the migration of custom static adapter plugins onto the shared simple static plugin runner
> - **Canonical for:** phased migration strategy, candidate ordering, acceptance criteria, and risk boundaries for `SimpleStaticPlugin` adoption
> - **Not canonical for:** live source inventory, static plugin runtime contracts, provider/static suppression policy, or current extraction behavior
> - **Then inspect:** [`../adapter-plugin-inventory.md`](../adapter-plugin-inventory.md), [`../scraping-pipeline.md`](../scraping-pipeline.md), [`../architecture-ai-map.md`](../architecture-ai-map.md), and [`../testing.md`](../testing.md)
> - **Last updated:** 2026-05-28

## Confidence statement

This plan has been loophole-audited against the codebase on 2026-05-28. Every custom static plugin was read and assessed for migration compatibility. 14 loopholes were identified and closed across candidate classification, runner extension needs, test validation strategy, row construction consistency, and code example accuracy.

## Summary

This plan is now complete. 5 plugins were converted to `SimpleStaticPlugin`, 1 shared helper was extracted, and 10 plugins were assessed and left custom with documented rationale.

| Outcome | Count | Plugins |
|---------|-------|---------|
| **Converted** | 5 | remedy, supercell, larian, activision, riot |
| **Not convertible** | 10 | blizzard, milestone, frontier, ncsoft, nintendo_csod, sheet_studios, amanotes, littlechicken, ats_wrappers, kojima |
| **Runner extended** | 1 | `generic_parser_then_detail_links` helper for supercell/larian pattern |
| **Existing adopters** | 8 | cdprojektred, climax, embark, globalstep, hrmos, jobvite, lionbridge, naconstudiomilan |
| **Left as shared module** | 1 | `_rendered_cards.py` (not a plugin — shared extraction module) |

All 183 static plugin tests pass. All pre-commit gates pass.

Original plan summary:

Continue the existing static plugin migration by converting straightforward custom static plugins to the shared `SimpleStaticPlugin` runner in small, behavior-preserving batches.

This is not greenfield architecture work. The shared runner already exists at `src/jobs/adapters/plugins/static/_runner.py` (444 lines), and 8 plugins already use it. The next value is to reduce repeated fetch, parse, row-stamping, and static diagnostic boilerplate without weakening source-report classification, browser fallback routing, or plugin-specific extraction behavior.

The shared runner's contract: **one page → one fetch (with optional Playwright fallback) → one `parse_html(ctx)` call → automatic row stamping → automatic meta recording.** Any plugin that fetches sub-pages during parsing, does multi-page crawling, or extracts JSON/non-HTML data cannot fit this model without a different runner contract.

## Loophole audit (2026-05-28)

Each loophole was validated against the actual codebase. The fix describes what changed in this plan.

**L1 — Blizzard cannot be migrated (closed):** Blizzard (`blizzard.py`, 217 lines) does multi-page crawling: extracts role links from listing → fetches each role page → extracts search result links → fetches search result pages → parses individual jobs. This fetches dozens of sub-pages inside the parse loop. The shared runner expects one HTML input per call. Fix: blizzard is **not convertible** — reclassify from Phase 1 second batch to "deferred — not convertible to SimpleStaticPlugin."

**L2 — Milestone cannot be easily migrated (closed):** Milestone (`milestone.py`, 179 lines) does Intervieweb iframe extraction: searches the listing HTML for a script tag pointing to `announces_js.php`, extracts query parameters, builds an iframe URL, fetches it, and parses Intervieweb's proprietary format. This needs `fetch_text` available inside the parse step. Fix: milestone is **not convertible** — reclassify from Phase 1 second batch to "deferred — not convertible to SimpleStaticPlugin."

**L3 — Activision's mid-parse browser retry needs wrapping (closed):** Activision (`activision.py`, 167 lines) has a non-standard flow: try generic parser → if zero rows AND `try_playwright` available → re-fetch with browser → parse again → if still zero → custom anchor regex fallback. The `SimpleStaticContext` doesn't expose `try_playwright` or fetching. Fix: activision stays in Phase 1 but the conversion wraps the mid-parse retry inside `_parse_html()` by passing `try_playwright` via closure, rather than changing the runner. The parse function calls `try_playwright` to re-render the HTML, then re-runs the generic parser on the new HTML. If browser retry also fails, falls back to anchor regex. The `SimpleStaticPlugin` spec sets `playwright_on_js_shell=True` so the runner handles the first fetch's JS shell case; the mid-parse retry is an additional safety net for pages that look like HTML but produce zero generic parser rows.

**L4 — record_failure_meta=False has no runner equivalent (closed):** Both `supercell.py` (line 88) and `larian.py` (line 88) pass `record_failure_meta=False` to `fetch_static_plugin_html_with_browser_fallback`. The shared runner's `_fetch_html` does NOT use that function — it has its own fetch implementation that calls `fetch_text` + optional `try_playwright` (lines 305-333 of `_runner.py`). The runner records fetch failure meta only when `spec.parser_stale_hint` is set. When supercell and larian are migrated, their entire fetch path is replaced by the runner's `_fetch_html`. Behavior: if `parser_stale_hint` is set on the spec, the runner records fetch failure meta AND stale meta. If not set, no meta is recorded (similar to the custom plugins' `record_failure_meta=False` behavior). Fix: set `parser_stale_hint` on the spec for both supercell and larian to ensure the runner records stale/failure meta appropriately. The custom `record_failure_meta=False` call is irrelevant after migration since `fetch_static_plugin_html_with_browser_fallback` is no longer called.

**L5 — Remedy's Jobylon wrapping pattern documented (closed):** Remedy (`remedy.py`, 82 lines) has a Jobylon API pre-parse step: before trying the generic HTML parser, it calls `extract_jobylon_v1_jobs()` to fetch structured data. If Jobylon succeeds, it returns immediately. Fix: the conversion wraps `run_simple_static_plugin`: try Jobylon API first; if it returns rows, stamp them with `stamp_static_plugin_rows` and return; if Jobylon fails or returns empty, proceed with `run_simple_static_plugin(..., require_generic_parser=True)`.

**L6 — No test fixture comparison strategy (closed):** The plan said "preserve behavior" but didn't describe how to verify. Fix: for each converted plugin, save the HTML output of a representative fetch page(s) as a local test fixture before conversion. After conversion, run both old and new plugin against the same fixture HTML and assert that output rows have identical `sourceJobId`, `company`, `jobLink`, `adapter`, `studio`, and `source` fields. The fixture lives in `tests/fixtures/static-plugin-migration/` and is NOT committed (too large). The comparison is done manually during the conversion session using the fixture.

**L7 — Riot is structurally similar to migrated plugins (closed):** Riot (`riot.py`, 149 lines) uses the same anchor-iteration → text-extraction → location-normalization pattern as already-migrated plugins (embark, globalstep, climax, lionbridge). Differences: manual dict construction instead of `static_job_row`, custom `sector` field assignment based on craft tokens, and no `stamp_static_plugin_rows` usage. Fix: promote riot from "review before converting" to Phase 1 second batch. The migration switches to `static_job_row(ctx, ...)` for row construction, which handles adapter/studio/source stamping automatically. The custom sector logic is preserved in `_parse_html()` by setting `sector=craft or "Game"` on the raw row before returning.

**L8 — Non-convertible plugins explicitly classified (closed):** The plan listed all 15 custom plugins as "need individual review." The audit found 7 plugins that cannot fit the single-page SimpleStaticPlugin model. Fix: explicit classification added:

| Plugin | Reason non-convertible |
|--------|----------------------|
| `blizzard.py` | Multi-page crawling (sub-page parsing loop) |
| `milestone.py` | Iframe sub-page fetch during parsing |
| `frontier.py` | CSS-class-specific li-block + window-based parsing |
| `ncsoft.py` | Per-job detail-page fetching loop |
| `nintendo_csod.py` | CSS-class-specific li-block parsing |
| `sheet_studios.py` | Multi-module integration (rendered_cards + detail resolution) |
| `amanotes.py` | JSON-LD extraction (no HTML parsing) |
| `littlechicken.py` | Multi-page listing + detail merge loop |

**L9 — ats_wrappers.py has an alternative migration path (closed):** `ats_wrappers.py` uses `_rendered_cards.extract_rendered_card_jobs()`. The `_rendered_cards.py` module provides its own `run_rendered_cards_plugin()` runner. Fix: `ats_wrappers.py` could be migrated to use `run_rendered_cards_plugin()` instead of SimpleStaticPlugin. This is a different migration (rendered-cards-based, not simple runner). Keep it in "review before converting" with a note about this alternative.

**L10 — supercell and larian share the same two-stage parse pattern (closed):** Both use: generic parser first → if zero rows → `static_detail_link_rows()` with `domain_profiles` anchor link fallback. This is the same pattern in two plugins. Per the plan's own rule ("extend SimpleStaticPlugin only when at least two candidate plugins need the same option"), this justifies a shared helper. Fix: extract a `generic_parser_then_detail_links` helper during Phase 0 or Phase 2 that wraps the parse function and the detail-link fallback into one callable. Both supercell and larian use it.

**L11 — hrmos.py already demonstrates custom overrides pattern (closed):** `hrmos.py` (130 lines) already uses `run_simple_static_plugin()` directly with `company_override` and `source_id_override`. This proves the pattern works for plugins that need per-call overrides. Fix: document this as the reference pattern for plugins that need runtime overrides (like activision's URL canonicalization).

**L12 — Remedy code example used wrong function names (closed):** The code example referenced `_heuristics.build_static_plugin_meta_result(source_row)` which doesn't exist. The actual function is `build_static_plugin_meta(classification, *, ...)` at `_heuristics.py:108`. Also, `stamp_static_plugin_rows` takes `company` and `source_name` as keyword arguments, not `source_row` (line 220 of `_runner.py`). The `run` function returns `list[RawJob]` only — meta is written to `source_row` as a side effect by the runner's internal `_meta()` calls, not returned. Fix: updated the code example to use `stamp_static_plugin_rows(rows=jobylon_rows, company=company, source_name=source_name)` and return just the row list. The `simple_static_run()` call passes kwargs correctly.

**L13 — Runner's internal fetch path differs from assumed behavior (closed):** The shared runner's `_fetch_html` (lines 305-333) does NOT use `fetch_static_plugin_html_with_browser_fallback`. It has its own implementation: `fetch_text` → optional `try_playwright` on error → optional `try_playwright` for JS shell. The runner records fetch failure meta at lines 320-328 only when `spec.parser_stale_hint` is set. Fix: L4 updated to describe the actual runner behavior. Both supercell and larian need `parser_stale_hint` set on the spec.

**L14 — Only one existing plugin uses `require_generic_parser=True` (closed):** `cdprojektred.py` is the only existing adopter that sets `require_generic_parser=True` and uses `simple_static_run()`. The runner validates this flag at line 368 — if set but `parse_jobpostings_from_html` is not callable, the runner bails. The Remedy code example correctly sets `require_generic_parser=True`. Fix: verified against cdprojektred's pattern.

## Decision

This is worth pursuing as future work, with the bounded first batch described below.

Expected payoff:
- Reduce duplicated static plugin scaffolding across 5 plugins in Phases 0-1.
- Make static plugin behavior easier to audit.
- Preserve a single place for common static fetch, Playwright fallback, row stamping, and parser-stale metadata behavior.
- Lower the cost of adding future static plugins that fit the simple listing-parser model.

Primary risk:
- Some static plugins are intentionally custom extractors. 8 plugins are now explicitly classified as non-convertible. The remaining 4 (after batch 1) are reviewed individually.

## Current State

### Shared runner
- `src/jobs/adapters/plugins/static/_runner.py` (444 lines)
- Provides: `SimpleStaticPlugin` spec, `SimpleStaticContext`, `run_simple_static_plugin()`, `simple_static_run()` factory, 11 helper functions (identity handler, row builders, fetch/browser helpers, stamping, meta recording, detail link extraction)

### Existing SimpleStaticPlugin adopters (8)
`cdprojektred.py`, `climax.py`, `embark.py`, `globalstep.py`, `hrmos.py` (partial — uses `run_simple_static_plugin` directly), `jobvite.py`, `lionbridge.py`, `naconstudiomilan.py`

### Conversion candidates

#### Phase 0 — First batch (confirmable, ~3 plugins)
| Plugin | Lines | Key behavior to preserve | Migration risk |
|--------|-------|------------------------|----------------|
| `remedy.py` | 82 | Jobylon API pre-parse → generic parser fallback. Wraps runner. | Low |
| `supercell.py` | 106 | Generic parser → domain-profile `static_detail_link_rows` fallback. `record_failure_meta=False` accepted. | Low |
| `larian.py` | 108 | Same pattern as supercell. | Low |

#### Phase 1 — Second batch (reviewed, ~2 plugins)
| Plugin | Lines | Key behavior to preserve | Migration risk |
|--------|-------|------------------------|----------------|
| `activision.py` | 167 | URL canonicalization + mid-parse browser retry + custom anchor regex fallback. `try_playwright` passed via closure. Custom `extractor_hint` for empty recording. | Medium |
| `riot.py` | 149 | Promoted from "review before converting." Anchor iteration + text extraction + craft/sector assignment. Switch to `static_job_row` for row stamping. No Playwright needed. | Low-Medium |

#### Review before converting (2 plugins, both reviewed 2026-05-28)

| Plugin | Lines | Review outcome |
|--------|-------|---------------|
| `ats_wrappers.py` | 139 | **Not convertible.** Uses `extract_rendered_card_jobs()` — rendered-cards-based, not simple static. Alternative: migrate to `run_rendered_cards_plugin()` (separate migration). |
| `kojima.py` | 216 | **Not convertible.** Unique `maybe_fetch_kojima_job_listing_html` pre-fetch injection + role-pattern filter + excluded paths logic. No other plugin shares this pattern. |

#### Not convertible to SimpleStaticPlugin (8 plugins)
| Plugin | Lines | Reason |
|--------|-------|--------|
| `blizzard.py` | 217 | Multi-page crawling (sub-page parsing loop) |
| `milestone.py` | 179 | Intervieweb iframe sub-page fetch during parsing |
| `frontier.py` | 321 | CSS-class-specific li-block + window-based parsing |
| `ncsoft.py` | 311 | Per-job detail-page fetching loop |
| `nintendo_csod.py` | 326 | CSS-class-specific li-block parsing |
| `sheet_studios.py` | 388 | Multi-module integration (rendered_cards + detail resolution) |
| `amanotes.py` | 129 | JSON-LD extraction from script tags — no HTML parsing |
| `littlechicken.py` | 219 | Multi-page listing + detail merge loop |

(closed L1, L2, L8, L9, L10, L11)

## Goals

- Convert the 5 plugins in Phase 0 and Phase 1 to `SimpleStaticPlugin` without changing output rows or diagnostics.
- Keep static plugin registration stable in `src/jobs/adapters/plugins/static/register.py`.
- Preserve `_staticPluginMeta` classifications, browser fallback recommendations, empty confirmed behavior, parser stale hints, and source report outcomes.
- Extract a `generic_parser_then_detail_links` shared helper for the supercell/larian pattern (closed L10).
- Keep complex extractors readable even if they remain custom.

## Non-Goals

- Do not change source registry rows, source policy review state, dynamic provider/static suppression, or `REDUNDANT_STATIC_IF_PROVIDER`.
- Do not add new Python or Node dependencies.
- Do not rewrite complex extraction logic just to hit a line-count target.
- Do not alter static adapter public loader names, plugin family names, or report payload contracts.
- Do not attempt to migrate the 8 plugins classified as non-convertible.
- Do not change browser fallback queue eligibility unless the specific plugin already had incorrect diagnostics and the behavior change is explicitly reviewed.

## Phase 0: Preparation

Before converting a batch:

- Pick at most three plugins (recommended: remedy, supercell, larian).
- For each plugin, save the HTML output of a representative fetch as a local fixture in `tests/fixtures/static-plugin-migration/{plugin_name}/` (closed L6).
- Record the behavior each plugin must preserve: handled host identity, fetch fallback behavior, empty parse behavior, row fields, source naming, and parser stale metadata.
- Run the existing focused static plugin unit tests to establish a baseline.
- Do NOT commit the HTML fixtures (too large). Use them locally during conversion to compare old vs. new output.

## Phase 1: Convert First-Batch Plugins

### Remedy (Jobylon wrapper)

The Remedy migration wraps the runner: try Jobylon API first; if it returns rows, stamp them and return; if not, proceed with the shared runner. The `run` function returns `list[RawJob]` only — meta is written to `source_row` by the runner internally.

```python
def _parse_html(ctx):
    return ctx.parse_jobpostings_from_html(ctx.html, ctx.page_url)

def run(fetch_text, timeout_s, retries, backoff_s, pages, source_row,
        parse_jobpostings_from_html=None, **kwargs):
    if not pages or not callable(parse_jobpostings_from_html):
        return []
    page_url = _runner.first_static_page(pages)
    if not page_url:
        return []
    company, source_id, source_name = _runner.static_plugin_context_values(
        source_row=source_row,
        default_company="Remedy Entertainment",
        default_source_id="remedy",
    )
    jobylon_rows, _stats, _errors, _rejects = extract_jobylon_v1_jobs(
        source_name=source_name, studio=company, page_url=page_url,
        timeout_s=max(15, min(timeout_s, 45)),
    )
    if jobylon_rows:
        return _runner.stamp_static_plugin_rows(
            rows=jobylon_rows, company=company, source_name=source_name,
        )
    return _runner.simple_static_run(
        spec=SimpleStaticPlugin(
            source_id="remedy",
            default_company="Remedy Entertainment",
            require_generic_parser=True,
        ),
        parse_html=_parse_html,
    )(fetch_text=fetch_text, timeout_s=timeout_s, retries=retries,
      backoff_s=backoff_s, pages=pages, source_row=source_row,
      parse_jobpostings_from_html=parse_jobpostings_from_html, **kwargs)
```

(closed L5, L12)

### Supercell / Larian (two-stage parse with detail link fallback)

Both follow the same pattern: generic parser → if zero rows → `static_detail_link_rows` fallback. In Phase 1, both plugins implement this logic inline in their `_parse_html()` functions (no shared helper yet). After both are converted and verified in Phase 1, Phase 2 extracts the `generic_parser_then_detail_links` helper:

```python
def generic_parser_then_detail_links(
    ctx, *, extra_anchor_filter=None
):
    rows = ctx.parse_jobpostings_from_html(ctx.html, ctx.page_url)
    if rows:
        return rows
    filter_fn = extra_anchor_filter or (lambda href: True)
    return static_detail_link_rows(
        ctx.html, ctx.page_url, ctx.company, ctx.source_id,
        is_probable_detail_url=filter_fn,
    )
```

Then Phase 2 refactors both plugins to use the extracted helper (no behavior change, just code move).

(closed L10)

## Phase 2: Runner Extension Review (supercell/larian helper)

After the first batch, extract the `generic_parser_then_detail_links` helper into `_runner.py`. This helper is justified because it is needed by at least two plugins (supercell, larian).

## Phase 3: Convert Second-Batch Plugins

### Activision

Custom parse function wrapping: pass `try_playwright` through closure for mid-parse browser retry (closed L3). URL canonicalization: the wrapper function resolves the canonical listing URL via `domain_profiles` before calling the runner, passing the canonical URL as `pages[0]`. This follows the same principle as hrmos.py's `company_override` — the wrapper handles the variation, not the runner (closed L11).

The `SimpleStaticPlugin` spec sets `playwright_on_js_shell=True` for first-fetch browser fallback. The mid-parse retry is inside `_parse_html()`:

```python
def _make_activision_parse_html(try_playwright):
    def _parse_html(ctx):
        rows = ctx.parse_jobpostings_from_html(ctx.html, ctx.page_url)
        if rows:
            return rows
        # Mid-parse browser retry
        if callable(try_playwright):
            browser_html = try_playwright(ctx.page_url)
            if browser_html:
                rows = ctx.parse_jobpostings_from_html(browser_html, ctx.page_url)
                if rows:
                    return rows
        # Anchor regex fallback
        return _activision_anchor_rows(ctx.html, ctx.page_url, ctx.company, ctx.source_id)
    return _parse_html
```

### Riot

Straightforward migration following the embark/globalstep pattern: `_parse_html()` uses `iter_anchor_fragments` → `extract_first_tag_text` → token-based location normalization → `static_job_row(ctx, ...)` for each row. Set `sector=craft or "Game"` on the raw row. No Playwright needed. No sub-page fetching. (closed L7)

## Phase 4: Review Remaining Custom Plugins

Review completed 2026-05-28.

### ats_wrappers.py — Not convertible to SimpleStaticPlugin

`ats_wrappers.py` (139 lines) uses `extract_rendered_card_jobs()` from `_rendered_cards.py` — not the generic HTML parser. It has no `parse_jobpostings_from_html` parameter. The plugin is a rendered-cards-based extractor with custom `_ATS_HREF_TOKENS`, `allow_any_anchor=True`, manual source name stamping, and multi-branch empty/no-openings/ATS detection.

**Outcome: Leave custom.** This plugin could potentially be migrated to `_rendered_cards.run_rendered_cards_plugin()` instead of SimpleStaticPlugin, but that is a separate migration path (rendered-cards-based, not simple runner). The `run_rendered_cards_plugin()` runner at `_rendered_cards.py:906` handles fetch + Playwright fallback + meta recording, which would replace most of `ats_wrappers.py`'s current boilerplate. This would be a single-plugin migration using a different runner — outside the scope of the SimpleStaticPlugin migration.

### kojima.py — Not convertible to SimpleStaticPlugin

`kojima.py` (216 lines) has a unique pre-fetch injection: `maybe_fetch_kojima_job_listing_html`. This custom function is called during fetching to potentially replace the HTML with a dynamically-fetched version before any parsing happens. The SimpleStaticPlugin runner's `_fetch_html` does not support custom pre-fetch hooks, and adding a spec field for this one plugin's unique need would violate the "extend only when at least two candidates need the same option" rule.

Additional custom behaviors that don't fit the runner model:
- Role-pattern filter (`re.compile(r"(programmer|artist|designer|...)"`) that rejects link text not matching game-industry roles
- Excluded paths list (`/en/careers`, `/en/careers_interview`, etc.)
- `<br>`-based line splitting for text extraction
- Default country fallback to "Japan"
- The `maybe_fetch_kojima_job_listing_html` parameter is passed through `**kwargs` to the run function. If the runner received it, it would be silently discarded (line 367 of `_runner.py`).

**Outcome: Leave custom.** The `maybe_fetch_kojima_job_listing_html` injection is a unique pre-fetch requirement. No other plugin has this behavior. The role-pattern filtering and excluded-paths logic are site-specific.

## Row Construction Consistency Rule (closed L7)

All converted plugins MUST use `static_job_row(ctx, ...)` for row construction within `_parse_html()`. Manual dict construction is forbidden in converted plugins. This ensures:
- `sourceJobId`, `company`, `jobLink` fields are derived from the context consistently.
- `adapter`, `studio`, `source` are stamped automatically by `stamp_static_plugin_rows` in the runner — no inline stamping needed.
- `source_id`, `source_name` match the plugin identity.

Custom fields (like riot's `sector=craft`) are set on the raw row dict BEFORE returning from `_parse_html()`. The runner's stamping appends adapter/studio/source after the parse function returns.

## Testing Strategy

For Phase 0-1 conversions:
- Use the saved HTML fixtures to compare old vs. new plugin output row-for-row (closed L6).
- Assert identical `sourceJobId`, `company`, `jobLink`, `adapter`, `studio`, `source` fields between old and new.
- Run `python -m pytest tests/jobs_static/ -q` if the plugin has existing tests.
- Run `python -m pytest tests/jobs/adapters/plugins/static/test_standard_plugins.py -q` if the shared runner changes.

For Phase 2 runner changes:
- Run the full `tests/jobs_static/` slice.
- Run `tests/jobs/adapters/plugins/static/test_standard_plugins.py`.

Do not run full pipeline validation unless the conversion changes behavior outside the static adapter.

## Stop Conditions

Pause the migration if any of these happen:
- A candidate needs multiple one-off runner options.
- Source-report classification changes without a deliberate behavior fix.
- Browser fallback queue eligibility changes unexpectedly.
- A converted plugin becomes harder to understand than the original custom implementation.
- The batch touches static adapter runtime files outside `_runner.py` without a clear compatibility reason.

## Closeout Criteria

This plan can close when:
- The first batch (remedy, supercell, larian) is converted. ✅ `913a473c`
- The second batch (activision, riot) is converted. ✅ `bfd01a5c`
- The `generic_parser_then_detail_links` helper is extracted. ✅ `bfd01a5c`
- The remaining plugins in "review before converting" have documented outcomes. ✅ 2026-05-28
- The 8 non-convertible plugins are left custom with the documented rationale in this plan. ✅
- `docs/adapter-plugin-inventory.md` reflects the final state if plugin ownership or guidance changes.
- Tests cover the shared runner behavior that future simple plugins rely on. ✅ All 183 pass.

## Loophole summary

All 14 identified loopholes are closed in-line above:

- **L1: Blizzard** — non-convertible (multi-page crawling).
- **L2: Milestone** — non-convertible (iframe sub-page fetch).
- **L3: Activision mid-parse retry** — wrapped via `try_playwright` closure in `_parse_html`.
- **L4: record_failure_meta** — runner `_fetch_html` has different impl; spec `parser_stale_hint` controls meta.
- **L5: Remedy Jobylon wrapping** — wrapper pattern documented with corrected code example.
- **L6: Test fixture strategy** — save HTML fixture, compare old vs new output row-for-row.
- **L7: Riot promoted** — structurally close to migrated pattern; moved to Phase 1 second batch.
- **L8: Non-convertible classification** — 8 plugins explicitly classified as non-convertible.
- **L9: ats_wrappers** — alternative `run_rendered_cards_plugin()` migration path.
- **L10: Two-stage parse helper** — `generic_parser_then_detail_links` extracted in Phase 2.
- **L11: hrmos.py override pattern** — documented as reference for runtime overrides.
- **L12: Remedy code example** — fixed `stamp_static_plugin_rows` args and return type.
- **L13: Runner fetch path** — `_fetch_html` doesn't use `fetch_*_with_browser_fallback`; behavior documented.
- **L14: require_generic_parser** — verified against cdprojektred's pattern; Remedy code example correct.
