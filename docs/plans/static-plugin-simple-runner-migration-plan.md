# Static Plugin Simple Runner Migration Plan

> - **Status:** Future work
> - **Use this when:** continuing the migration of custom static adapter plugins onto the shared simple static plugin runner
> - **Canonical for:** phased migration strategy, candidate ordering, acceptance criteria, and risk boundaries for `SimpleStaticPlugin` adoption
> - **Not canonical for:** live source inventory, static plugin runtime contracts, provider/static suppression policy, or current extraction behavior
> - **Then inspect:** [`../adapter-plugin-inventory.md`](../adapter-plugin-inventory.md), [`../scraping-pipeline.md`](../scraping-pipeline.md), [`../architecture-ai-map.md`](../architecture-ai-map.md), and [`../testing.md`](../testing.md)
> - **Last updated:** 2026-05-13

## Summary

Continue the existing static plugin migration by converting straightforward custom static plugins to the shared `SimpleStaticPlugin` runner in small, behavior-preserving batches.

This is not greenfield architecture work. The shared runner already exists at `src/jobs/adapters/plugins/static/_runner.py`, and several plugins already use it. The next value is to reduce repeated fetch, parse, row-stamping, and static diagnostic boilerplate without weakening source-report classification, browser fallback routing, or plugin-specific extraction behavior.

The right shape is incremental migration. Do not attempt a single broad conversion of every remaining custom plugin.

## Decision

This is worth pursuing as future work now, with a bounded first batch.

Expected payoff:

- Reduce duplicated static plugin scaffolding.
- Make static plugin behavior easier to audit.
- Preserve a single place for common static fetch, Playwright fallback, row stamping, and parser-stale metadata behavior.
- Lower the cost of adding future static plugins that fit the simple listing-parser model.

Primary risk:

- Some static plugins are intentionally custom extractors. Forcing them into `SimpleStaticPlugin` may hide important behavior, create awkward runner options, or regress source diagnostics.

## Current State Observed

Observed on 2026-05-13 from `src/jobs/adapters/plugins/static/` and `docs/adapter-plugin-inventory.md`.

Shared runner:

- `src/jobs/adapters/plugins/static/_runner.py`

Existing `SimpleStaticPlugin` adopters:

- `cdprojektred.py`
- `climax.py`
- `embark.py`
- `globalstep.py`
- `hrmos.py`
- `jobvite.py`
- `lionbridge.py`
- `naconstudiomilan.py`

Registered custom static plugins that still need individual review:

- `activision.py`
- `amanotes.py`
- `ats_wrappers.py`
- `blizzard.py`
- `frontier.py`
- `kojima.py`
- `larian.py`
- `littlechicken.py`
- `milestone.py`
- `ncsoft.py`
- `nintendo_csod.py`
- `remedy.py`
- `riot.py`
- `sheet_studios.py`
- `supercell.py`

Special registered path:

- `_rendered_cards.py` is registered directly as `rendered_cards` and should be treated as a shared extractor, not as a normal simple-plugin conversion target.

## Goals

- Convert the next simple custom plugins to `SimpleStaticPlugin` without changing output rows or diagnostics.
- Keep static plugin registration stable in `src/jobs/adapters/plugins/static/register.py`.
- Preserve `_staticPluginMeta` classifications, browser fallback recommendations, empty confirmed behavior, parser stale hints, and source report outcomes.
- Extend `SimpleStaticPlugin` only when at least two candidate plugins need the same option.
- Keep complex extractors readable even if they remain custom.

## Non-Goals

- Do not change source registry rows, source policy review state, dynamic provider/static suppression, or `REDUNDANT_STATIC_IF_PROVIDER`.
- Do not add new Python or Node dependencies.
- Do not rewrite complex extraction logic just to hit a line-count target.
- Do not alter static adapter public loader names, plugin family names, or report payload contracts.
- Do not change browser fallback queue eligibility unless the specific plugin already had incorrect diagnostics and the behavior change is explicitly reviewed.

## Candidate Priority

| Priority | Plugins | Rationale |
|----------|---------|-----------|
| First batch | `remedy.py`, `supercell.py`, `larian.py` | Already small or already use shared `_runner` helpers; likely convertable with low runner churn. |
| Second batch | `activision.py`, `blizzard.py`, `milestone.py` | Similar HTML-first patterns with shared fetch, JS-shell, stamp, and empty-parse handling. |
| Review before converting | `amanotes.py`, `ats_wrappers.py`, `kojima.py`, `littlechicken.py`, `riot.py` | May fit with small runner extensions, but inspect behavior first. |
| Defer by default | `frontier.py`, `ncsoft.py`, `nintendo_csod.py`, `sheet_studios.py`, `_rendered_cards.py` | Larger or specialized extractors; forcing them into the simple runner is likely counterproductive. |

## Phase 0: Preparation

Before converting a batch:

- Pick at most three plugins.
- Inspect only those plugin files and the shared runner.
- Record the behavior each plugin must preserve: handled host identity, fetch fallback behavior, empty parse behavior, row fields, source naming, and parser stale metadata.
- Prefer existing focused tests if they cover the plugin.
- Add narrow tests only when a plugin currently lacks coverage for behavior that could regress during conversion.

Recommended first batch:

- `remedy.py`
- `supercell.py`
- `larian.py`

## Phase 1: Convert Low-Risk Plugins

For each plugin:

- Replace custom `run` boilerplate with `simple_static_run(...)` when the parser can be expressed as `parse_html(SimpleStaticContext)`.
- Keep `can_handle` semantics unchanged.
- Keep source IDs and source names unchanged.
- Preserve row stamping fields: `adapter`, `studio`, and `source`.
- Preserve empty parse handling, including explicit no-openings markers and parser-stale metadata.
- Preserve browser fallback behavior for fetch failures and JS-shell pages.
- Avoid broad runner option additions for one-off behavior.

Acceptance criteria for each converted plugin:

- The plugin remains registered under the same name and priority.
- The same host or source identity is handled.
- Existing rows keep stable `sourceJobId`, `company`, `jobLink`, `adapter`, `studio`, and `source` semantics.
- Empty or blocked pages still set equivalent `_staticPluginMeta`.
- No unrelated static plugin files are changed in the same commit.

## Phase 2: Runner Extension Review

After the first batch, decide whether the runner needs small shared extensions.

Good extension candidates:

- A reusable no-openings detector hook.
- A reusable detail-link extractor hook.
- A configurable JS-shell Playwright fallback path already needed by multiple plugins.
- A configurable parser-stale metadata path already needed by multiple plugins.

Bad extension candidates:

- Options that encode one studio's HTML quirks.
- Large callback webs that make simple plugins harder to read than the custom version.
- Generic flags that change browser fallback queue behavior without explicit tests.

## Phase 3: Convert Second-Batch Plugins

Convert `activision.py`, `blizzard.py`, and `milestone.py` only after Phase 1 proves the runner shape is stable.

Keep this phase separate from Phase 1 so regressions are easier to isolate.

## Phase 4: Review Remaining Custom Plugins

For each remaining plugin, choose one outcome:

- Convert to `SimpleStaticPlugin`.
- Leave custom and document why in the plugin or plan closeout.
- Extract a new shared helper if multiple custom plugins share the same non-simple behavior.

Default stance:

- Leave `frontier.py`, `ncsoft.py`, `nintendo_csod.py`, `sheet_studios.py`, and `_rendered_cards.py` custom unless there is a clear repeated pattern worth extracting.

## Testing Strategy

Use the narrowest checks for the converted plugins.

Recommended checks:

- Run focused static plugin unit tests for converted plugins when available.
- Run `tests/jobs/adapters/plugins/static/test_standard_plugins.py` if the shared runner changes.
- Run the relevant `tests/jobs_static/` test slice when browser fallback, parser-stale metadata, or static source execution behavior changes.
- Run one source-specific fetch only when the change affects real network extraction and the user explicitly wants runtime validation.

Do not run full pipeline validation just for simple plugin conversions unless behavior changes cross the static adapter boundary.

## Stop Conditions

Pause the migration if any of these happen:

- A candidate needs multiple one-off runner options.
- Source-report classification changes without a deliberate behavior fix.
- Browser fallback queue eligibility changes unexpectedly.
- A converted plugin becomes harder to understand than the original custom implementation.
- The batch touches static adapter runtime files outside `_runner.py` without a clear compatibility reason.

## Closeout Criteria

This plan can close when:

- The easy first and second batches are either converted or explicitly marked not worth converting.
- The remaining custom plugins have documented ownership rationale.
- `docs/adapter-plugin-inventory.md` reflects the final state if plugin ownership or guidance changes.
- Tests cover the shared runner behavior that future simple plugins rely on.
