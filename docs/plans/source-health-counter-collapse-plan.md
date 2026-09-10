# Source Health Counter Collapse Plan

> - **Status:** Executed through Phase 4 + Phase 6 (2026-09-10) and live-audited on the 2026-09-09 forced full pass (two persistence-funnel defects found and fixed in place — see acceptance audit below). Phase 5 (bridge contract change) remains **awaiting operator approval**; the guardrail test `tests/test_source_counter_alias_guardrails.py` pins the executed invariants.
> - **Use this when:** touching `lastJobsKept`/`zeroJobStreak`/`failureCount` (aliases) or `lastKeptCount`/`consecutiveZeroKept`/`consecutiveFailures` (canonical) read/write precedence, extending source-health fields, or executing the remaining phase
> - **Canonical for:** the phased migration design, site inventory, and phase acceptance criteria for collapsing the three counter alias pairs
> - **Not canonical for:** the split-brain root cause (recorded in commit `ca778258` and the changelog) or the availability health payload contract
> - **Then inspect:** `src/shared/source_counter_aliases.py` (the policy leaf), `src/jobs/state_source_records.py`, `src/jobs/common/contracts_source_health.py`, `src/shared/fetch_report_normalization.py`, `docs/DATA_CONTRACT.md`
> - **Last updated:** 2026-09-10

## Execution state (2026-09-10)

- **Phase 1** — policy leaf `src/shared/source_counter_aliases.py` (`COUNTER_ALIASES`, `CANONICAL_COUNTERS`, `read_counter`, `emit_with_aliases`, `heal_counter_aliases`); derive readers refactored; behavior identical to `ca778258`.
- **Phase 2** — heal-assertion test in `test_pipeline_storage_gzip.py`: legacy alias-only rows converge through the normal read-modify-write cycle (no bulk rewrite). The divergence audit against real state (`data/jobs-source-state.json.gz`, 4,988 rows) found **zero divergence and zero alias-only rows** pre-migration.
- **Phase 3** — `source_registry_policy`, `registry_conflicts_row_core` (`_fresh_jobs_found_count`), `registry_conflicts_automation_provider`, `pipeline_loader_selection`, and `state_incremental` (including retiring the defensive third spelling `zeroKeptStreak` from the reader — zero occurrences in real state) all read via `read_counter`; `registry_conflicts_row_audit._join_source_health_aliases` fills aliases via `heal_counter_aliases`.
- **Phase 4** — `derive_source_health_fields` emits **canonical counters only** (no alias keys into persisted state); the state normalizer heals legacy alias-only rows on load through the leaf; `normalize_fetch_report_source_row_base` option defaults flipped canonical-preferred. Wire emitters (`_source_health_row`, `_fetch_report_source_state_row`) still carry aliases for the Admin UI.
- **Phase 5** — NOT executed (bridge contract change; operator approval required). Emit surface unchanged; guardrail test pins it.
- **Phase 6** — guardrail test added: alias-map completeness, persistence single-writer grep guard, wire-emit still carries aliases, canonical-first + absent-aware read contract.

## Phase-4 acceptance evidence

- Divergence audit (plan script): `none` across 4,988 rows; zero `zeroKeptStreak` occurrences.
- Nested counter-name audit: no alias keys anywhere besides top-level source rows.
- Suites: `test_jobs_source_health`, `test_pipeline_storage_gzip`, `test_fetch_report_normalization_parity`, `test_state_incremental_empty_streak`, `test_source_counter_alias_guardrails`, `test_fetch_report_source_row_enrichment`, `test_browser_fallback`, `test_structured_migration_state` — 53 passed; `tests/bridge/` + `test_admin_bridge_source_health` — 648 passed; changed-mode gate exit 0.
- Next full consolidation pass will write the first alias-free state file; the heal keeps old files self-correcting on load (no bulk migration needed, per plan).

### Phase-4 acceptance audit (forced full pass, 2026-09-09) — two funnel defects found and fixed

The forced full pass (`tmp/alias-collapse-20260910/run1.log`, output 42,114, 72 failed sources, verdict healthy 100/22/Δ−1) wrote state at 18:35:26Z — and the audit found **all 4,989 rows still carrying all three alias keys**, fully synced to canonical. Root cause was the plan's own heal definition, not the derive:

1. **Alias re-add through the funnel.** `heal_counter_aliases` fused both heal directions; its alias-fill branch re-added alias keys on every normalize+save cycle, re-emitting what the derive no longer writes. Fix: persistence uses the new `fill_canonical_counters` (canonical := alias, alias keys **consumed**); alias-fill is reserved for the display/wire join (`heal_counter_aliases`, kept for the Phase-5 surface).
2. **Vacuous Phase-2 heal.** The state normalizer's whitelist reads canonical keys only and coerces absent → 0, so the post-whitelist heal arrived too late: alias-only legacy rows were silently zeroed (the Phase-2 convergence test passed on all-zeros and asserted the wrong final shape). Fix: `fill_canonical_counters` runs **before** the whitelist (copying each raw row first), plus a real value-preservation round-trip test and a persisted-text alias guard.

Re-acceptance: in-memory normalize audit of the real payload (4,989 rows: 0 aliases, 0 canonical drift); sanctioned read→write round-trip proven byte-clean in temp (0 dropped, 0 field diffs); in-place rewrite via `read_source_state`/`write_source_state` landed the first **alias-free state file** (0 alias keys, 0 counter drift, metadata intact). Guardrails updated to pin both heal directions separately; focused suites 28 passed; grep-guard now also fails if the persistence funnel imports the alias-fill heal.

## Problem

One fact (per-source fetch counters) is stored under six names — three canonical
pairs maintained by the run appliers, three legacy aliases written back by the
health derive. The alias-first reader ordering caused the split brain fixed in
`ca778258`; the dual-write remains. Every future reader must re-learn the
canonical-first precedence, and every stale alias in persisted state is one
reordering away from re-corrupting a verdict. The collapse removes the hazard:
**canonical names become the only stored fact; aliases survive only on wire
surfaces with external consumers.**

## Site inventory (audited 2026-09-09, `rg` over src/tests/frontend)

### Same-fact sites (in scope)

| Surface | File(s) | Reads | Writes | Classification |
|---|---|---|---|---|
| State persistence derive | `src/jobs/state_source_records.py` (`derive_source_health_fields`, `normalize_source_state_payload`) | canonical-first (post-`ca778258`) | dual-write + alias→canonical heal on load | **persistence — collapse target** |
| Report triage contract | `src/jobs/common/contracts_source_health.py` (`_source_health_row`) | canonical-first | dual-write in row output | **wire (bridge emit) — keep aliases until Phase 5** |
| Fetch-report normalizer | `src/shared/fetch_report_normalization.py` (`_source_row_count_fields`) | option-gated: alias-first with canonical fallback | dual-write | **wire — keep; flip option defaults in Phase 4** |
| Registry-conflicts source-state merge | `src/bridge/registry_conflicts_row_source_state.py` (`SOURCE_HEALTH_FIELD_NAMES`, `_fetch_report_source_state_row`, merger) | both | dual-write | **wire — keep; add canonical to merge list** |
| Registry-conflicts audit/core/provider | `registry_conflicts_row_audit.py`, `registry_conflicts_row_core.py`, `registry_conflicts_automation_provider.py` | alias-first with canonical fallback | merge/demote propagation (`_merge_source_state_row_from_fetch_report` copies both) | **in-repo reads — policy-read in Phase 3** |
| Policy dispositions | `src/source_registry_policy.py` (L114, L134–136, L391–408) | alias-first, `str()` fallbacks | none | **in-repo reads — policy-read in Phase 3** |
| Loader selection / incremental cache | `src/jobs/pipeline_loader_selection.py` (L60), `src/jobs/state_incremental.py` (`_zero_kept_streak`, incl. third name `zeroKeptStreak`) | alias fallbacks | none | **in-repo reads — policy-read in Phase 3** |
| Admin UI | `frontend/admin/render/registry-conflicts.js` (L468–500) | alias with canonical fallback | none | **display — canonical-only in Phase 5** |

### Unrelated same-named fields (out of scope — do not touch)

`src/pipeline_audit.py` (`failureCount` = failure row count), all of
`src/source_discovery/` (`failureCounts`/`failureCount` on audit artifacts),
`frontend/admin/app/discovery/*`, `app/live-task.js`, `app/ops/health.js`
(poll-guard backoff counters).

## Guiding rules

1. **Persistence surfaces collapse; wire surfaces keep both names.** The
   runtime state file (`data/jobs-source-state.json.gz`) is internal; the
   bridge/contracts payloads are read by the Admin UI and outside filters.
2. **Heal, don't rewrite.** `normalize_source_state_payload` already heals both
   directions on load (canonical from alias when absent, aliases from canonical
   on derive). Old state converges through the normal read-modify-write cycle —
   no bulk data migration.
3. **One policy leaf.** All precedence knowledge lives in one shared leaf;
   consumers never hand-roll alias fallbacks again.
4. **Contract changes are their own operator-approved phase** (per
   `AGENTS.md` compatibility work: route/bridge changes check call sites and
   payload builders together).

## The policy leaf (Phase 1)

New `src/shared/source_counter_aliases.py` (importable from `src/jobs`,
`src/bridge`, `src/shared`, root — no composition-root imports):

- `COUNTER_ALIASES: dict[str, str]` — `{"lastJobsKept": "lastKeptCount",
  "zeroJobStreak": "consecutiveZeroKept", "failureCount": "consecutiveFailures"}`
  plus `zeroKeptStreak` → `consecutiveZeroKept` (defensive; audit real state
  for occurrences before removing from readers).
- `read_counter(row, canonical_name)` → first non-empty of canonical then
  aliases (coercion stays at the call site).
- `emit_with_aliases(row)` → row + aliases filled from canonical (wire writer).
- `heal_counter_aliases(entry)` → aliases := canonical when canonical present,
  canonical := alias when canonical absent (single definition of "heal").

Refactor the two derive readers to the policy. **No precedence or payload
change** — behavior identical to `ca778258`; tests unchanged; gate green.

## Phases

| Phase | Change | Risk | Acceptance |
|---|---|---|---|
| 1. Policy leaf + derive refactor | New leaf; derive reads via policy; dual-write unchanged | None (behavior-identical) | `test_jobs_source_health`, `test_pipeline_storage_gzip`, parity suite green; gate exit 0 |
| 2. Heal assertion | Focused test: a payload with stale aliases and no canonical, after `normalize_source_state_payload` + re-save, converges to alias==canonical | None | New test in `test_pipeline_storage_gzip.py` shape; documents "no bulk rewrite" |
| 3. In-repo reader convergence | `source_registry_policy`, `registry_conflicts_row_core/_audit/_automation_provider`, `pipeline_loader_selection`, `state_incremental` read via policy; fixtures that assert alias-first precedence updated | Low — precedence now uniform (canonical first) | Registry-conflicts suites, `test_state_incremental_empty_streak`, policy unit tests green |
| 4. Single-writer at the derive | `derive_source_health_fields` stops emitting the 3 alias keys into **persisted state**; `fetch_report_normalization` option defaults flip to canonical-preferred for repo-side producers; wire emitters (`_source_health_row`, `_fetch_report_source_state_row`) KEEP alias emission via `emit_with_aliases` | Medium — state file loses alias keys; verify nothing repo-side reads them from state | Full grep audit: zero src readers of state-only aliases; full consolidation pass; report emit still carries aliases (bridge) |
| 5. Bridge contract change (operator-approved) | `_source_health_row` + `registry_conflicts_row_source_state` stop dual-writing; parity test updated to canonical-only; `registry-conflicts.js` reads canonical; Admin payload loses aliases | Deliberate contract change — external/older consumers | Full py suite + frontend unit + smoke; staged behind a release note; changelog records the surface change |
| 6. Guardrail + closeout | Test asserting the alias map covers every dual-written name (fails if a fourth alias appears unowned); changelog + snapshot update | None | Gate green; docs updated in the same change |

## Read-only divergence audit (run before Phase 4)

```bash
python - <<'EOF'
import gzip, json
from collections import Counter
with gzip.open("data/jobs-source-state.json.gz", "rt", encoding="utf-8") as h:
    state = json.load(h)
rows = state.get("sources") or {}
c = Counter()
for name, row in rows.items():
    if not isinstance(row, dict):
        continue
    for canon, alias in (
        ("lastKeptCount", "lastJobsKept"),
        ("consecutiveZeroKept", "zeroJobStreak"),
        ("consecutiveFailures", "failureCount"),
    ):
        a, b = row.get(canon), row.get(alias)
        if a is not None and b is not None and int(a or 0) != int(b or 0):
            c[f"{canon}!={alias}"] += 1
        if a is None and b is not None:
            c[f"{canon} missing, alias present"] += 1
print(dict(c) or "no divergence")
EOF
```

Expected post-`ca778258` + one full pass: zero or near-zero divergence; the
Phase-4 drop of alias keys from state is then information-loss-free.

## Risks

- **Unknown external readers of the state file** — treat as none (runtime
  artifact, gitignored), but Phase 5's changelog entry covers the wire surface.
- **The parity-test contract** (`test_fetch_report_normalization_parity.py`
  asserts both names in bridge output) — deliberate change in Phase 5 only.
- **Hidden stale writers** — grepped 2026-09-09: the only repo writers are the
  derive, the bridge merger, and the state normalizer; the normalizer heals.
- **Rollback** — every phase is an independent revert; the persistence heal
  makes the state file self-correcting after any rollback.

## Suggested verification (each phase)

```bash
python -m pytest tests/test_jobs_source_health.py tests/test_pipeline_storage_gzip.py tests/test_fetch_report_normalization_parity.py tests/test_state_incremental_empty_streak.py -q
python -m pytest tests/bridge/test_registry_conflicts_static_fragment_aliases.py tests/admin/test_admin_bridge_source_health.py -q   # Phases 3–5
python scripts/precommit_gate.py --mode changed
# Phase 4/5: one full consolidation pass + the divergence audit above
```
