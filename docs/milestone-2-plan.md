# Milestone 2 — Provider Drift Fixes

> **Status:** Planning
> **Window:** Week 2–3 (Q2 2026)
> **Builds on:** M1 (taxonomy + health scoring, verified 2026-03-25)

---

## Problem Statement

Four provider adapters produce zero kept output in every full run despite appearing to work in targeted audit runs. One adapter (`personio`) is registered through the plugin framework but is still seed-pending, so it dispatches without producing kept jobs from the current live seeds. This wastes runtime, inflates error counts, and silently leaves studios uncovered.

### Adapter Drift Summary (2026-03-25 full run, 645 sources, commit `1482d98`)

| Adapter | Plugin type | Registration | Full-run keptCount | Diagnosis |
|---------|-------------|--------------|-------------------|-----------|
| `workable` | JSON feed | ✅ registered | 0 | Fetches payload but parser returns empty or all filtered |
| `personio` | XML feed (legacy runner) | ✅ **wired through plugin framework; seed-pending** | 0 in live targeted run | `run_personio_sources_source` now runs through the plugin registry via shared helper logic, but the current live seeds still keep nothing. Reuse the existing logic, avoid circular imports, and record the source as seed-pending until registry seeds are refreshed. |
| `breezy` | HTML board | ✅ registered | 0 | HTML parser returns empty for all seeds |
| `jazzhr` | HTML board | ✅ registered | 0 | HTML parser returns empty for all seeds |

### Healthy Structured Adapters (reference)

`greenhouse`, `lever`, `ashby`, `smartrecruiters`, `recruitee`, `pinpoint`, `teamtailor` — all registered, all produce non-zero kept output in the same run.

---

## Goals

1. Diagnose the root cause per adapter (seed invalidity vs parser bug vs API drift).
2. Fix or suppress each broken adapter with a testable outcome.
3. Wire `personio_sources` into the plugin framework using the existing runner logic.
4. Produce one targeted verification artifact per fixed adapter.
5. Update `data/jobs-fetch-report.json` after fixes if a meaningful full run is feasible.

---

## Deliverable Breakdown

### 2.1 — Parity diff tool

**File:** `scripts/audit_diff.py` (new standalone script — keep in `scripts/`, not in runtime source)

- Compare `adapter` field in `data/jobs-fetch-report.json` against a targeted audit run
- Flag adapters where full-run `keptCount == 0` but targeted single-source audit returns non-zero
- Flag adapters where audit runs fine but full-run `status == error`
- Output: printed table or JSON report

**Verification:** `python scripts/audit_diff.py` — no network required if given a cached report.

---

### 2.2 — Workable: diagnose and fix

**Relevant files:**
- `src/jobs/adapters/plugins/provider_api/register.py` — `_json_feed_plugin("workable")`
- `src/jobs/adapters/provider_parsers.py` — `parse_workable_jobs_payload`

**Investigation steps:**
1. **Seed validity first.** The source registry is a hardcoded Python list in `src/jobs/common/registry_defaults.py` (`DEFAULT_STUDIO_SOURCE_REGISTRY`). There are no per-adapter JSON files under `data/`. Open `registry_defaults.py` and locate entries with `"adapter": "workable"`. For one or two `account` slugs, manually fetch `https://apply.workable.com/api/v1/widget/accounts/<slug>?details=true` and confirm it returns a non-empty `jobs` array. If all seeds 404 or return empty, the problem is stale seeds — not the parser.
2. Run `--only-sources workable_sources --force-refresh-all` to isolate.
3. If seeds resolve: inspect raw API response shape — does `payload["jobs"]` contain entries? Are field names as expected by `parse_workable_jobs_payload`?
4. If seeds are stale: update/remove invalid `account` entries in `src/jobs/common/registry_defaults.py` before touching parser code.

**Fix options (pick first that applies):**
- **Parser bug:** fix field extraction in `parse_workable_jobs_payload`.
- **Stale seeds:** update/remove invalid `account` entries in registry.
- **API endpoint drift:** update `build_url` lambda in `register.py`.

**Exit:** `workable_sources` returns `keptCount >= 1` in a targeted run.

---

### 2.3 — Personio: wire up existing logic into the plugin framework safely

**The full Personio logic already exists** in the repo:

- `run_personio_sources_source` in `src/jobs/adapters/provider_api.py`
- `parse_personio_feed_xml` in `src/jobs/parsers.py`
- `registry_entries("personio")` using `feed_url` XML seeds from `src/jobs/common/registry_defaults.py`
- rate-limit detection and cooldown skipping in the legacy runner

The only missing piece is that `personio_sources` is **not registered in `plugins/provider_api/register.py`**, so the plugin dispatcher never calls it.

**Important implementation constraint:**
Do **not** make `plugins/provider_api/register.py` call back into `provider_api.py` in a way that creates a circular import. `provider_api.py` already imports and dispatches through the plugin registry.

**Safe implementation options (pick one):**

1. **Extract shared Personio execution logic** into a lower-level helper module that both the legacy compatibility entrypoint and the plugin layer can call.
2. **Implement a plugin-native Personio runner** inside `plugins/provider_api/register.py` using the existing XML parser, registry shape, and cooldown behavior as the source of truth.

**Steps:**

1. Confirm seed entries exist in `src/jobs/common/registry_defaults.py` with `"adapter": "personio"` and XML `feed_url` values like `https://<token>.jobs.personio.de/xml`.
2. Choose one of the safe implementation options above; do **not** introduce a circular import.
3. Register `personio_sources` in `ensure_registered()` with the same priority band as the other HTML/XML adapters.
4. Validate that at least one seed URL resolves and returns parseable XML before the first run.
5. Test with `--only-sources personio_sources --force-refresh-all`.

**Do not rewrite the XML parser unless the live payload proves it is broken.** Reuse `parse_personio_feed_xml` and the existing cooldown behavior.

**If no valid seeds exist:** register the plugin path and note it as seed-pending in the roadmap.

**Exit:** `personio_sources` dispatches through the plugin framework without circular-import risk; either returns `keptCount >= 1` in a targeted run or is explicitly marked seed-pending.

---

### 2.4 — Breezy: diagnose and fix

**Relevant files:**
- `register.py` — `_html_board_plugin("breezy")`
- `provider_parsers.py` — `parse_breezy_jobs_html` (regex-based, keyed to `/p/` URL shape)

**Investigation steps:**
1. **Seed validity first.** The source registry is `src/jobs/common/registry_defaults.py`. Open it and locate entries with `"adapter": "breezy"`. For one seed `board_url`, fetch it manually (`curl -L <url>`) and confirm it resolves and returns Breezy HTML with job listings. If the URL 404s or redirects away from a Breezy board, the problem is stale seeds.
2. Run `--only-sources breezy_sources --force-refresh-all`.
3. If seeds resolve but parser returns empty: fetch the raw HTML and run `parse_breezy_jobs_html` against it locally. Check if URL patterns like `/p/` are still present or if Breezy has changed its HTML structure.
4. Fix parser selectors or update seeds as appropriate.

**Fix options:**
- Update HTML selectors in `parse_breezy_jobs_html`.
- Validate/refresh seeds in registry.

**Exit:** `breezy_sources` returns `keptCount >= 1` for at least one seed in a targeted run.

---

### 2.5 — JazzHR: diagnose and fix

**Relevant files:**
- `register.py` — `_html_board_plugin("jazzhr")`
- `provider_parsers.py` — `parse_jazzhr_jobs_html` (regex-based, keyed to `/apply/` URL shape on `*.applytojob.com`)

**Investigation steps:**
1. **Seed validity first.** The source registry is `src/jobs/common/registry_defaults.py`. Open it and locate entries with `"adapter": "jazzhr"`. For one seed `board_url`, fetch it manually and confirm it resolves and returns a JazzHR job listing page (`*.applytojob.com/apply`). If the URL 404s or is no longer a JazzHR board, the problem is stale seeds — not parser drift.
2. Run `--only-sources jazzhr_sources --force-refresh-all`.
3. If seeds resolve but parser returns empty: fetch raw HTML and run `parse_jazzhr_jobs_html` locally. Verify the `/apply/` URL pattern and CSS selectors still match the live HTML.
4. Fix parser or update seeds as appropriate.

**Exit:** `jazzhr_sources` returns `keptCount >= 1` for at least one seed in a targeted run, or is explicitly quarantined with rationale recorded in the commit message.

---

### 2.6 — Adapter smoke tests (dedicated file)

For each fixed adapter, land one **targeted fetch fixture test** in `tests/test_provider_adapters.py` (new file).

- **Do not add these to `tests/test_jobs_fetcher.py`.** That file covers general pipeline/registry behavior; per-adapter parser coverage belongs in a dedicated module.
- Structure: mock `fetch_text` returning a saved HTML/JSON fixture saved under `tests/fixtures/provider_adapters/`, assert parser returns at least one valid job with non-empty `title`, `company`, and `jobLink`.
- One test function per adapter, grouped by adapter name.
- For Personio: if the parser is newly written, the fixture test is the primary verification before wiring it into `register.py`.

**Verification command:** `npm run test:py` — the full Python test suite must still pass after adding new tests.

---

## Implementation Order

Execute strictly in this order to avoid cross-contamination:

```
2.1 → parity diff (read-only, safe first)
2.2 → workable fix (highest ROI, already registered)
2.3 → personio registration
2.4 → breezy fix
2.5 → jazzhr fix
2.6 → smoke tests per fixed adapter
```

---

## Exit Criteria

- [ ] `workable_sources` returns non-zero kept in a targeted run
- [ ] `personio_sources` dispatches through the plugin framework using existing Personio logic without circular-import risk; either yields in a targeted run or is explicitly marked seed-pending
- [ ] At least one of `breezy_sources` / `jazzhr_sources` returns non-zero kept in a targeted run; the other is either fixed or explicitly quarantined with rationale in the commit message
- [ ] No adapter is judged healthy based only on audit output
- [ ] Smoke tests for fixed adapters land in `tests/test_provider_adapters.py`
- [ ] The full Python test suite passes (`npm run test:py`)

---

## KPIs

- `workable` keptCount ≥ 1 in a targeted run
- `personio` dispatches via plugin framework using existing logic; yielding or seed-pending
- At least one of `breezy` / `jazzhr` keptCount ≥ 1 in a targeted run
- Total provider adapter error rate reduced vs M1 baseline

---

## Files Touched

| File | Change |
|------|--------|
| `src/jobs/adapters/provider_parsers.py` | Parser fixes (workable, breezy, jazzhr) if parser drift is confirmed |
| `src/jobs/adapters/plugins/provider_api/register.py` | Add personio plugin wiring or plugin-native runner; fix workable `build_url` if needed |
| `src/jobs/adapters/provider_api.py` or a new lower-level helper module | Optional extraction of shared Personio execution logic to avoid circular imports |
| `src/jobs/common/registry_defaults.py` | Seed cleanup / updates for workable, breezy, jazzhr, personio |
| `tests/test_provider_adapters.py` (new) | Per-adapter parser smoke fixtures |
| `scripts/audit_diff.py` (new, optional) | Artifact-level parity reporter — keep in `scripts/` |
| `docs/quality-improvement-roadmap.md` | M2 exit criteria updated at completion |

---

## Guardrails

- Do **not** add new dependencies (follow `AGENTS.md` dependency guardrail).
- Do **not** import composition-root modules from any helper script.
- Each adapter fix lands in its **own commit** with before/after kept count in the commit message.
- If a seed is removed (invalid account slug), document the removal in the commit message.
- Test every targeted fix with `--only-sources <adapter>_sources --force-refresh-all` before committing.

---

## Related Files

- [`src/jobs/adapters/plugins/provider_api/register.py`](../src/jobs/adapters/plugins/provider_api/register.py)
- [`src/jobs/adapters/provider_api.py`](../src/jobs/adapters/provider_api.py)
- [`src/jobs/adapters/provider_parsers.py`](../src/jobs/adapters/provider_parsers.py)
- [`docs/quality-improvement-roadmap.md`](quality-improvement-roadmap.md)
- [`docs/adapter-plugin-inventory.md`](adapter-plugin-inventory.md)
- [`docs/architecture-ai-map.md`](architecture-ai-map.md)
