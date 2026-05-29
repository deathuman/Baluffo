# ATS Custom-Domain Detection Plan — 2026-05-29

> - **Status:** Active, high priority
> - **Use this when:** discovering new career sites, fixing the static adapter's Playwright gate, reclassifying misregistered sources, or auditing ATS-coverage gaps
> - **Canonical for:** ATS detection on custom domains, static→teamtailor/greenhouse/workday/etc. reclassification, and web-search HTML-signature detection
> - **Not canonical for:** the ATS runner implementations themselves, or the static adapter's general extraction heuristics
> - **Then inspect:** [`provider_inference.py`](../../src/source_discovery/provider_inference.py), [`web_search_candidates.py`](../../src/source_discovery/web_search_candidates.py), [`static_listing.py`](../../src/jobs/adapters/static_listing.py), [`source-registry-active.seed.json`](../../data/defaults/source-registry-active.seed.json)
> - **Last updated:** 2026-05-29

Systematic fix to detect ATS-powered career sites on **custom domains** (e.g., `careers.foolstheory.com/jobs` — Teamtailor, but no `teamtailor.com` in the host). The current inference system only recognises ATS providers by known hosting-domain patterns (`*.teamtailor.com`, `boards.greenhouse.io`, etc.). Custom-domain sites always fall through to `"adapter": "static"`, where the generic scraper misses jobs or produces noisy results.

## Root Cause

1. **`provider_inference.py:56-70`** — `infer_provider_adapter()` checks host patterns only. `careers.foolstheory.com/jobs` matches no pattern → returns `None` → defaults to `static`.
2. **`web_search_candidates.py:129-130`** — Teamtailor HTML content detection exists but feeds back through the same host-pattern inference → silently skipped on custom domains.
3. **`static_listing.py:1502-1519`** — Playwright fallback gate requires `< 3` links AND `< 180` visible chars. Teamtailor JS shells with 3+ nav links bypass Playwright, produce zero job links.

## Scope

| Category | Count | Impact |
|----------|-------|--------|
| Static sources with `/jobs` in URL | 487 | Potential ATS-powered sites using generic adapter |
| High-suspicion `careers.*/jobs` or `jobs.*/jobs` (likely Teamtailor custom domains) | ~16 | **Jobs silently missed** (e.g., Fool's Theory missing 5 of 11 listings) |
| Unambiguous ATS domains mislabeled as static (SmartRecruiters, Jobvite, HRMOS, Comeet, Simplicant, GoHire) | 12 | Static adapter produces noisy/poor results vs dedicated ATS runner |
| Studios with duplicate static + ATS adapter entries | 31 | Configuration drift; some point the static entry at the exact same URL the ATS entry uses |

## Phased Plan

---

### Phase 1 — Fix HTML-content-based ATS detection at discovery time

**Priority: High.** Prevents future sources from being misclassified.

#### 1a — Fix `web_search_candidates.py:129-130` Teamtailor path

When `"teamtailor"` is detected in fetched HTML (existing check at line 129) AND `infer_provider_adapter()` returns None (custom domain), create a `teamtailor` adapter candidate directly via `provider_candidate()` with the fetched URL as `listing_url`/`base_url`. This gives the discovery pipeline an actionable candidate rather than silently discarding it.

```python
# Current (non-functional):
if "teamtailor" in str(html or "").lower() and careers_keyword_count(page_url):
    embedded_urls.append(page_url)  # → infer_web_candidate → infer_provider_adapter fails

# Fixed:
if "teamtailor" in str(html or "").lower() and careers_keyword_count(page_url):
    inferred = provider_candidate(
        studio=studio,
        adapter="teamtailor",
        url=page_url,
        nl_priority=nl_priority,
        discovery_method=discovery_method,
        evidence_types=["html_embed", "html_ats_signature"],
        evidence_source="html",
        evidence_score=32,  # base 28 + 4 for careers_keyword
    )
    if inferred:
        inferred["careersUrl"] = page_url
        candidates.append(inferred)
```

**File:** `src/source_discovery/web_search_candidates.py` (lines 129-130)
**Effort:** ~0.5h

#### 1b — Add HTML-signature detection for other ATS providers

Extend `infer_provider_candidates_from_html()` with similar HTML-substring checks for every ATS that has a dedicated runner and supports custom domains at runtime:

| ATS | HTML Signature | Runtime Support | Effort |
|-----|---------------|-----------------|--------|
| Teamtailor | `"teamtailor"` (existing + fix 1a) | Full (any domain) | 0.5h |
| Greenhouse | `"greenhouse.io"` | API-bound (no custom domain runtime) | 0.5h |
| Workday | `"myworkdayjobs"` or `"workday"` | Full (any domain) | 0.5h |
| BambooHR | `"bamboohr"` | Full (any domain) | 0.5h |
| SmartRecruiters | `"smartrecruiters"` | API-bound (no custom domain runtime) | 0.5h |
| Lever | `"lever.co"` | Partial (subdomain-flexible, API-bound) | 0.5h |
| Workable | `"workable"` | Partial (subdomain-flexible, API-bound) | 0.5h |

Each check: `if "{signature}" in str(html).lower() and careers_keyword_count(page_url) and infer_provider_adapter(host, path) is None: → provider_candidate(adapter="{ats}", url=page_url, ...)`

For API-bound providers (Greenhouse, Lever, SmartRecruiters), the candidate would need additional fields (slug, company_id, account). For custom domains, the inference builder should still construct the correct API URL from the standard template — detection means "this page IS powered by X", even if X requires a known API host at runtime.

**File:** `src/source_discovery/web_search_candidates.py` (in `infer_provider_candidates_from_html`, after line 130)
**Effort:** ~2h

#### 1c — Add HTML fallback parameter to `infer_provider_adapter()`

Add an optional `html: str | None` parameter to `infer_provider_adapter()`. When host-pattern matching fails AND html is provided, check for known ATS HTML signatures. This catches cases NOT routed through the web-search pipeline (e.g., sheet-directory sources, direct registry entries).

```python
def infer_provider_adapter(host: str, path: str, html: str | None = None) -> str | None:
    # existing host-pattern matching...
    for adapter, patterns in _HOST_DOMAIN_PATTERNS:
        if host_matches_any_domain_pattern(host, patterns):
            return adapter
    # existing ad-hoc checks (Lever, SmartRecruiters)...

    # NEW: HTML fallback
    if html and _html_matches_any_provider(host, path, html):
        return _html_matches_any_provider(host, path, html)

    return None
```

Where `_html_matches_any_provider()` uses the same signature map as 1b.

**File:** `src/source_discovery/provider_inference.py`
**Effort:** ~1h

---

### Phase 2 — Fix static adapter's Playwright gate

**Priority: High.** Reduces job loss from already-misclassified sources.

#### 2a — Relax the JS shell gate

In `_prepare_listing_htmls()` (`static_listing.py:1502-1519`), the current gate:

```python
if detect_js_shell(html) and len(all_links) < 3 and len(all_jobs) == 0:
    try_playwright(page_url, timeout_s)
```

Change to: if `detect_js_shell(html)` is True AND the listing URL contains `/jobs` in its path (strong signal it's an ATS page), always try Playwright regardless of link count. This catches Teamtailor custom domains with 3+ nav links.

```python
listing_path = urlparse(ctx.listing_url).path or ""
should_playwright = detect_js_shell(html) and (
    len(all_links) < 3
    or "/jobs" in listing_path  # ATS custom-domain signal
)
if should_playwright and len(all_jobs) == 0:
    try_playwright(page_url, timeout_s)
```

**File:** `src/jobs/adapters/static_listing.py` (lines 1502-1519)
**Effort:** ~0.5h

#### 2b — Add ATS detection advisory in the static pipeline

When Playwright successfully renders a JS shell and the rendered HTML contains an ATS signature ("teamtailor", "greenhouse", "smartrecruiters", "myworkdayjobs", "bamboohr", "workday", "lever", "workable"), log a source diagnostic suggesting reclassification:

```python
for ats_name, signature in _ATS_HTML_SIGNATURES:
    if signature in rendered_html.lower():
        ctx.set_diagnostic("hint", f"Page appears to be {ats_name}-powered; consider adapter: {ats_adapter_map[ats_name]}")
```

**File:** `src/jobs/adapters/static_listing.py` (in `_prepare_listing_htmls` or `_run_static_detail_traversal`)
**Effort:** ~0.5h

---

### Phase 3 — Remediate existing misclassified sources

**Priority: Medium.** Fixes currently silent job loss in production.

#### 3a — Fix unambiguous ATS-domain misclassifications (12 sources)

These sources use URLs on known ATS domains but are registered as `static`. The domain itself is the evidence — no probing needed.

| Source | URL | Current Adapter | Correct Adapter |
|--------|-----|-----------------|-----------------|
| People Can Fly Studio | `careers.smartrecruiters.com/PeopleCanFly` | static | smartrecruiters (*duplicate exists, remove static*) |
| Epoch Games | `careers.smartrecruiters.com/EpochGames` | static | smartrecruiters (*duplicate exists, remove static*) |
| Heart Machine | `careers.smartrecruiters.com/HeartMachine` | static | smartrecruiters |
| GIANTS Software | `careers.smartrecruiters.com/GIANTSSoftwareGmbH` | static | smartrecruiters |
| Ludeo | `www.comeet.com/jobs/ludeo/D7.008` | static | (no Comeet adapter — consider adding or keeping static) |
| Mindstorm Studios | `mindstormstudios.simplicant.com/jobs/board` | static | (no Simplicant adapter — keep static) |
| Mighty Bear Games | `jobs.gohire.io/wearemighty-*` | static | (no GoHire adapter — keep static) |
| Cygames Group | `hrmos.co/pages/cygames/jobs` | static | (no HRMOS adapter — keep static) |
| GAME FREAK | `hrmos.co/pages/gamefreak/jobs` | static | (no HRMOS adapter — keep static) |
| KOJIMA PRODUCTIONS | `hrmos.co/pages/2052996596448374784/jobs` | static | (no HRMOS adapter — keep static) |
| Amber | `jobs.jobvite.com/amberstudiocareers/...` | static | (no Jobvite adapter — keep static) |
| Capcom | `jobs.jobvite.com/capcomusa` | static | (no Jobvite adapter — keep static) |

For sources without a dedicated adapter (Comeet, Simplicant, GoHire, HRMOS, Jobvite), leave as `static` but benefit from the Phase 2 Playwright gate fix.

**File:** `data/defaults/source-registry-active.seed.json`
**Effort:** ~0.5h

#### 3b — Audit high-suspicion custom-domain static sources (~16 URLs)

Fetch each of the ~16 `careers.*/jobs` and `jobs.*/jobs` static sources, check HTML for ATS signatures. Reclassify confirmed adapters.

| Studio | URL | Likely ATS |
|--------|-----|------------|
| Radical Forge | `careers.radicalforge.com/jobs` | Teamtailor |
| Nimble Giant | `careers.nimblegiant.com/jobs` | Teamtailor |
| Playnetic | `careers.playnetic.com/jobs` | Teamtailor |
| 10 Chambers | `careers.10chambers.com/jobs` | Teamtailor |
| Redhill Games | `careers.redhillgames.com/jobs` | Teamtailor |
| OtherSide Entertainment | `careers.otherside-e.com/jobs` | Teamtailor |
| Steel City Interactive | `careers.steelcityinteractive.co.uk/jobs` | Teamtailor |
| Lionbridge Games | `careers.lionbridge.com/jobs/search/...` | Teamtailor |
| Raw Power Games | `careers.rawpowergames.com/jobs` | Teamtailor (also has teamtailor entry) |
| Arrowhead Game Studios | `jobs.arrowheadgamestudios.com/jobs` | Teamtailor |
| Star Stable Entertainment | `jobs.starstableentertainment.com/jobs` | Teamtailor |
| Fatshark AB | `jobs.fatsharkgames.com/#jobs` | Teamtailor |
| Resolution Games | `jobs.resolutiongames.com/` | Teamtailor |
| Moon Rover | `jobs.moonrover.games/#jobs` | Teamtailor |
| Studio Drydock | `jobs.studiodrydock.com/` | Teamtailor |
| Vivid Games | `jobs.vividgames.com/#jobs` | Teamtailor |

Strategy: use `webfetch` to verify each URL returns a Teamtailor-powered page (footer "Career site by Teamtailor"), then change `adapter` from `"static"` to `"teamtailor"`.

**Effort:** ~1-2h (mostly manual fetch + verification)

#### 3c — Resolve static duplicates (31 studios)

Review the 31 studios where both a `static` entry and a dedicated ATS entry exist. For cases where the static entry points at a URL that the ATS adapter already covers (e.g., Epoch Games: static entry at `careers.smartrecruiters.com/EpochGames` + smartrecruiters entry for same URL), remove the static entry. For cases where they point to different URLs (e.g., a homepage + a careers subpage), leave both.

**File:** `data/defaults/source-registry-active.seed.json`
**Effort:** ~1h

---

### Phase 4 — Probe-time ATS detection signal

**Priority: Low (ongoing prevention).**

#### 4a — Add ATS detection in the probe

In `src/source_discovery/probe.py`, when probing a static source whose listing URL contains `/jobs` or a `careers.*` subdomain, optionally fetch the page and check for ATS HTML signatures. If detected, emit a probe diagnostic suggesting adapter reclassification.

This is additive — the probe already works. The diagnostic is mostly informational unless the probe finds zero jobs (which is a strong signal the static adapter is failing on this source).

**File:** `src/source_discovery/probe.py`
**Effort:** ~1h

#### 4b — Add ATS detection in the Orchestrator finalization report

In the source-policy soak report (`source-policy-runbook.md` pathway), add a report section that lists static sources with `/jobs` in their URL, grouped by suspected ATS (based on HTML fetch). This gives an ongoing audit surface.

**File:** `src/source_discovery/reporting.py` or `tools/measurements/pipeline/`
**Effort:** ~0.5h

---

## Key Files

| File | Role | Changes |
|------|------|---------|
| `src/source_discovery/web_search_candidates.py` | Web search candidate pipeline | Fix Teamtailor detection; add other ATS HTML signatures |
| `src/source_discovery/provider_inference.py` | Provider adapter inference | Add HTML fallback parameter to `infer_provider_adapter()` |
| `src/jobs/adapters/static_listing.py` | Static adapter pipeline | Relax Playwright gate for `/jobs` URLs; add ATS detection advisory |
| `data/defaults/source-registry-active.seed.json` | Source registry | Fix 12 misclassified sources; resolve 31 duplicates |
| `src/source_discovery/probe.py` | Source probe | Add ATS detection advisory for static sources |
| `src/source_discovery/reporting.py` | Discovery reporting | Add ATS-suspicious-source report section |

## Verification

| Phase | Verification |
|-------|-------------|
| Phase 1 | Run web search discovery against a studio with a Teamtailor custom domain; verify candidate has `adapter: "teamtailor"`. Run pipeline tests. |
| Phase 2 | Run static pipeline against a Teamtailor JS shell with 4+ nav links; verify Playwright is triggered and jobs are extracted. |
| Phase 3 | `git diff` registry changes; verify each changed source produces jobs in dedicated adapter. |
| Phase 4 | Run probe against a static source with `/jobs` URL; verify diagnostic mentions suspected ATS. |
| All | Pre-commit gate passes (`npm run lint:precommit:changed`). Run full test suite: `python -m pytest tests/test_jobs_fetcher_quality.py tests/test_jobs_fetcher_pipeline.py tests/test_source_discovery*.py tests/test_provider*.py -q`. |

## Known Limitations

- **HTML-content-based detection is not 100% reliable.** A page containing "greenhouse" in text could be a blog post about greenhouse gases, not Greenhouse ATS. The `careers_keyword_count()` gate reduces false positives.
- **Some ATS platforms have no dedicated runner** (Jobvite, HRMOS, Comeet, Simplicant, GoHire). Detection can flag them but the fix is only better static-adapter handling (Phase 2).
- **Phase 3b fetch-based audit is O(~16 fetches).** Each URL needs a real HTTP fetch to check the HTML. This is one-time; the Phase 1 fix prevents future misclassification.
- **API-bound ATS providers** (Greenhouse, Lever, SmartRecruiters) require fixed API hosts even when detected on custom domains. The candidate builder must still construct `boards-api.greenhouse.io` URLs for the API path, using the custom domain only as the listing URL.
