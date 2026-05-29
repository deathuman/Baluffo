# Static Inference & ATS Custom-Domain Detection Plan — 2026-05-29

> - **Status:** Active, high priority
> - **Use this when:** improving static inference quality, detecting ATS on custom domains, reclassifying misregistered sources, auditing static-adapter job loss, or expanding discovery heuristics
> - **Canonical for:** ATS detection on custom domains, static→{ats} reclassification, web-search HTML-signature detection, static-adapter Playwright gate heuristics, discovery-pipeline keyword expansion, and systemic static-inference improvements
> - **Not canonical for:** the ATS runner implementations themselves, static plugin registry maintenance, or the generic static extraction heuristics
> - **Then inspect:** [`provider_inference.py`](../../src/source_discovery/provider_inference.py), [`web_search_candidates.py`](../../src/source_discovery/web_search_candidates.py), [`static_listing.py`](../../src/jobs/adapters/static_listing.py), [`source-registry-active.seed.json`](../../data/defaults/source-registry-active.seed.json)
> - **Last updated:** 2026-05-29 (Phase 2a restructured as independent `/jobs` path gate; expanded with Phases 5–7: domain heuristic, non-English keywords, broader JS-shell detection, dead-listing retry, empty-page fallback, ATS feedback, re-probing, reclassification, threshold reduction)

Systematic fix to detect ATS-powered career sites on **custom domains** (e.g., `careers.foolstheory.com/jobs` — Teamtailor, but no `teamtailor.com` in the host). The current inference system only recognises ATS providers by known hosting-domain patterns (`*.teamtailor.com`, `boards.greenhouse.io`, etc.). Custom-domain sites always fall through to `"adapter": "static"`, where the generic scraper misses jobs or produces noisy results.

## Root Causes

### 1. Host-pattern-only ATS inference
**`provider_inference.py:28-70`** — `_HOST_DOMAIN_PATTERNS` maps specific hosted domains (`boards.greenhouse.io`, `*.teamtailor.com`, etc.) to their adapters. Any ATS on a vanity custom domain (`careers.{studio}.com`) produces zero matches → `infer_provider_adapter()` returns `None` → the fallback pipeline in `page_analysis.py:120-167` always produces a generic static candidate.

### 2. Broken HTML-signature detection
**`web_search_candidates.py:129-130`** — Teamtailor HTML content detection exists but feeds back through `infer_web_candidate()` → `infer_provider_adapter()` → same host-pattern inference. On custom domains the check is silently skipped because the host doesn't match. No other ATS has any HTML-signature detection at all.

### 3. Conservative static candidate gating
**`static_candidates.py:67-79`** — Three rejection filters compound to reject legitimate career pages:
- Needs EN careers keywords in BOTH URL and HTML (no non-English support)
- Needs ≥2 detail links OR JSON-LD in raw HTML (JS-rendered pages fail)
- Homepage URLs need even stricter evidence

### 4. Conservative Playwright gate
**`static_listing.py:1502-1519`** — Requires BOTH `visible_text < 180` chars AND SPA framework tokens AND `< 3` anchor links AND `< 1` parsed job. Any single condition missed and the page gets basic-HTTP only, producing zero extractions. Teamtailor JS shells with nav menus and 3-5 links reliably bypass this.

### 5. Disconnected discovery and adapter pipelines
**`page_analysis.py:138`, `probe_runtime.py:94-122`** — The discovery pipeline classifies sources and never re-evaluates the classification. The adapter pipeline, when running a static source, detects ATS links (`detect_outbound_ats_links()`, `_heuristics.py:65-101`) but has no feedback mechanism to suggest reclassification. No probe-time ATS-signature detection exists.

### 6. Harsher treatment of static sources
**`core_thresholds.py` and `core_queue.py`** — Static sources have higher evidence thresholds (22 to probe vs 18 for providers, 34 to queue vs 26), are always queued after providers regardless of evidence score, and have a lower cap (8 vs 12 for Greenhouse). Legitimate static candidates are deprioritized even with strong evidence.

## Scope

| Category | Count | Impact |
|----------|-------|--------|
| Static sources with `/jobs` in URL | 487 | Potential ATS-powered sites using generic adapter |
| High-suspicion `careers.*/jobs` or `jobs.*/jobs` (likely Teamtailor custom domains) | ~16 | **Jobs silently missed** (e.g., Fool's Theory missing 5 of 11 listings) |
| Unambiguous ATS domains mislabeled as static (SmartRecruiters, Jobvite, HRMOS, Comeet, Simplicant, GoHire) | 12 | Static adapter produces noisy/poor results vs dedicated ATS runner |
| Studios with duplicate static + ATS adapter entries | 31 | Configuration drift; some point the static entry at the exact same URL the ATS entry uses |
| Non-English career-site URLs (German, French, Japanese, etc.) | Unknown — systemic gap | Career pages using non-English keywords get zero keyword evidence → rejected at candidate-gate |
| Static sources that would produce jobs with Playwright but fail the JS-shell gate | ~20-30% of the 487 | Pages with nav menus + JS-rendered listings get basic HTTP only, produce zero jobs |
| Static sources that could be auto-reclassified via adapter-time ATS detection | All 487 | No feedback mechanism to upgrade static→{ats} even when adapter detects ATS at runtime |

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

#### 2a — Add independent `/jobs` path Playwright gate

In `_prepare_listing_htmls()` (`static_listing.py:1502-1519`), the current gate is entirely inside a `detect_js_shell(html)` check:

```python
if detect_js_shell(html) and ...:
    # only reached when JS shell detected
```

This means pages with visible text > 180 chars (nav menus, cookie banners, footer) never trigger Playwright, even if they're clearly career pages. Add an **independent** Playwright gate that fires whenever the listing URL's path contains `/jobs` — a strong signal it's an ATS career page regardless of JS shell detection:

```python
# ── Existing gate (unchanged) ──
if self.deps.try_playwright and html and detect_js_shell(html):
    parsed_pre = parse_jobpostings_from_html(html, ...)
    link_count = ...
    if not parsed_pre and link_count < 3 and dynamic_listing_timeout_s > 0:
        html2, _ = self.deps.try_playwright(page_url, dynamic_listing_timeout_s)
        if html2:
            html = html2

# ── NEW: independent /jobs path gate ──
# Catches ATS custom-domain pages that are not minimal JS shells
# (e.g. Teamtailor with nav + cookie banner + footer > 180 chars).
# This fires independently of detect_js_shell — the /jobs path is
# itself strong evidence the page is a career listing.
listing_path = urlparse(page_url).path or ""
if self.deps.try_playwright and html and "/jobs" in listing_path:
    jobs_path_timeout_s = effective_timeout_for_remaining_budget(
        timeout_s=max(1, effective_timeout_s),
        remaining_budget_s=self.ctx.remaining_budget_s(),
    )
    parsed_pre = parse_jobpostings_from_html(html, ...)
    if not parsed_pre and jobs_path_timeout_s > 0:
        html2, _ = self.deps.try_playwright(page_url, jobs_path_timeout_s)
        self._log_playwright_fallback(page_url, "jobs_path", html2)
        if html2:
            self.stage_state.increment_browser_fallbacks()
            html = html2
```

This is **not gated behind `detect_js_shell`** — it fires independently for any URL with `/jobs` in its path, regardless of text length, link count, or SPA tokens. Together with Phase 6a (broader JS shell detection) and Phase 6c (empty-page fallback), the three adapter-time Playwright triggers form a layered safety net.

**File:** `src/jobs/adapters/static_listing.py` (lines 1502-1519, new block inserted after existing gate)
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

### Phase 5 — Broader discovery heuristics

**Priority: Medium.** Catches sites that pass through the URL-pattern and HTML-signature gaps.

#### 5a — Add domain-name heuristic to `infer_provider_adapter()`

Add a new `_infer_from_domain_name()` function to `provider_inference.py`. When host-pattern matching fails AND HTML fallback (Phase 1c) also fails, check whether the domain or path contains strong career signals:

```python
def _infer_from_domain_name(host: str, path: str) -> str | None:
    """Return adapter hint based on domain/path structure.

    Patterns matched (in order):
    - careers.{anything}.com/jobs/... → strong signal, return None (candidate for browser probe)
    - jobs.{anything}.com/... → strong signal, return None
    - {anything}.com/careers/... → moderate signal
    - {anything}.com/jobs/... → moderate signal
    """
    if re.search(r"careers?\.", host):
        return "static"  # keep static but with elevated evidence
    if re.search(r"jobs\.", host):
        return "static"
    if "/careers" in path or "/jobs" in path:
        return "static"
    return None
```

This doesn't identify the *specific* ATS but signals "this is clearly a career page, treat it with more evidence." The elevated evidence helps it pass the static evidence thresholds (22 to probe, 34 to queue).

**File:** `src/source_discovery/provider_inference.py` (new function called from `infer_provider_adapter`)
**Effort:** ~0.5h

#### 5b — Expand career keyword lists

Add non-English equivalents to `CAREERS_URL_HINTS` in `scoring.py:11-13` and `_LANDING_PAGE_SEGMENTS` in `page_analysis.py:11-25`:

```python
# scoring.py
CAREERS_URL_HINTS = {
    "careers", "career", "jobs", "join-us", "open-positions",
    "vacancies", "work-with-us",
    # Non-English:
    "stellenanzeigen", "karriere", "offene-stellen",  # German
    "emploi", "recrutement", "carriere", "offres",     # French
    "trabajos", "empleo", "carreras", "vacantes",      # Spanish
    "lavora", "carriera", "offerte", "posizioni-aperte",  # Italian
    "vagas", "carreiras",                               # Portuguese
    "jobs", "careers",                                    # Also in non-English contexts
    "recruitment", "opening", "position",
    "recruit", "hiring",
    "採用", "キャリア", "募集", "求人",              # Japanese
    "채용", "경력", "구인",                          # Korean
    "招聘", "职业", "工作",                          # Chinese
}
```

**File:** `src/source_discovery/scoring.py` (CAREERS_URL_HINTS), `src/source_discovery/page_analysis.py` (LANDING_PAGE_SEGMENTS)
**Effort:** ~0.5h

#### 5c — Relax candidate gating for known-career URLs

In `build_static_candidate_from_page()` (`static_candidates.py:67-79`), the keyword check at line 69 currently requires career keywords in BOTH URL and HTML. For URLs that contain `/jobs`, `/careers`, or match the domain-name heuristic (5a), relax to requiring keywords in EITHER URL OR HTML:

```python
# Current (both required):
if not (url_has_careers_keywords or html_has_careers_keywords):
    return None

# New (relaxed for known-career URLs):
if is_careers_url(page_url):
    pass  # URL itself is sufficient evidence
elif not (url_has_careers_keywords or html_has_careers_keywords):
    return None
```

Where `is_careers_url()` checks the domain-name heuristic from Phase 5a.

**File:** `src/source_discovery/static_candidates.py` (line 69-70)
**Effort:** ~0.5h

---

### Phase 6 — Aggressive adapter-time fallbacks

**Priority: Medium.** Reduces zero-job outcomes from already-misclassified static sources at runtime.

#### 6a — Broader JS shell detection

In `_heuristics.py:34-57`, decouple the text-length gate from SPA-token detection. Currently both are required. Change to: if SPA framework tokens are present, try Playwright regardless of visible text length. If visible text is very short (< 180) but no SPA tokens, also try Playwright.

```python
def detect_js_shell(html: str) -> bool:
    text = clean_text(html)
    visible_len = len(text)
    has_spa_token = _has_spa_framework_token(html)

    # SPA token alone is sufficient — Nav menus + JS listings produce
    # long text but still need browser rendering
    if has_spa_token:
        return True

    # Very short text is suspicious even without SPA tokens
    return visible_len < 180
```

**File:** `src/jobs/adapters/plugins/static/_heuristics.py:34-57`
**Effort:** ~0.5h

#### 6b — Dead-listing browser retry

In `_runner.py:315-327`, when `classify_job_page()` returns `dead_listing_page` AND `try_playwright` is available, retry with Playwright before finalizing:

```python
# In _record_empty_plugin_result() or similar:
if classification == "dead_listing_page" and try_playwright:
    rendered_html = try_playwright(page_url, timeout_s)
    if rendered_html:
        rendered_result = extract_jobs(rendered_html)
        if rendered_result:
            return rendered_result  # Playwright recovered jobs
```

This catches JS-rendered career pages where the basic-HTTP fetch returned a non-functional shell that looks like a dead listing.

**File:** `src/jobs/adapters/plugins/static/_runner.py` (in empty-result handling) or `src/jobs/adapters/static_listing.py` (in `_finish_generic_source`)
**Effort:** ~1h

#### 6c — Generic empty-page Playwright fallback

In `_prepare_listing_htmls()` (`static_listing.py:1502-1519`), after the basic-HTTP parse produces zero jobs AND the page URL contains career keywords (from `careers_keyword_count`), try Playwright even if `detect_js_shell()` returns False:

```python
# After existing JS shell check...
if len(all_jobs) == 0 and careers_keyword_count(page_url) > 0 and try_playwright:
    rendered = try_playwright(page_url, timeout_s)
    if rendered and rendered != html:
        html = rendered
        parsed_pre = parse_jobpostings_from_html(html)
        if parsed_pre:
            increment_browser_fallbacks()
```

This is the "last resort": any career page that yielded zero jobs via basic HTTP gets a browser rendering attempt. Adds overhead for every zero-job static page but significantly reduces false negatives.

**File:** `src/jobs/adapters/static_listing.py` (in `_prepare_listing_htmls`)
**Effort:** ~0.5h

---

### Phase 7 — Architectural improvements

**Priority: Low but high long-term value.** Systemic guarantees and ongoing prevention.

#### 7a — Adapter-time ATS detection feedback to discovery

In `_heuristics.py:65-101`, `detect_outbound_ats_links()` already scans HTML for recognized ATS domains. When the static adapter finds ATS links AND produces zero jobs, write a diagnostic to the source record suggesting reclassification:

```python
# In static_listing.py or _runner.py, after detect_outbound_ats_links():
ats_links = detect_outbound_ats_links(html)
if ats_links and len(all_jobs) == 0:
    ctx.set_diagnostic(
        "reclassification_hint",
        {
            "detected_ats": list(ats_links.keys()),
            "current_adapter": "static",
            "suggested_adapter": "review",
            "empty_reason": classification,
        }
    )
```

This makes downstream audit tools (source-policy soak report, `tools/measurements/pipeline/`) surface static sources that the adapter itself agrees shouldn't be static.

**File:** `src/jobs/adapters/plugins/static/_heuristics.py` and `src/jobs/adapters/static_listing.py`
**Effort:** ~1h

#### 7b — Periodic static re-probing with ATS detection

Add a script (`scripts/reprobe_static_sources.py`) that:
1. Reads the active registry for `adapter: "static"` sources with `/jobs` or `careers.*` in URL
2. Fetches each page (HTTP + optional Playwright)
3. Runs HTML-signature detection against the fetched content
4. Outputs a reclassification report: `{url, current_adapter, detected_ats, confidence}`

```python
# Usage
python scripts/reprobe_static_sources.py --limit 50 --output reclassification_report.json
```

This is a one-time audit tool powered by the Phase 1c `html` fallback and Phase 5a domain heuristic.

**File:** `scripts/reprobe_static_sources.py` (new)
**Effort:** ~2h

#### 7c — Unified adapter reclassification path

Design and implement a clean mechanism to change a source's adapter from `static` to a provider without risking duplicate entries or identity conflicts:

1. Define a `reclassification` table in the source registry (or use diagnostics) mapping `{original_identity, new_adapter, new_identity, timestamp, reason}`
2. In the orchestrator's dedup step (`_dedupe_discovered_candidates`, `orchestrator_generation.py:724`), check the reclassification table: if a static source has been reclassified to a provider, drop the static version
3. After auto-approval or manual reclassification, remove the old static entry and activate the provider entry

**Files:** `src/source_registry.py`, `orchestrator_generation.py`, and new `src/source_discovery/reclassification.py`
**Effort:** ~3-4h (architectural — needs careful identity migration)

#### 7d — Reduce static evidence thresholds for `/jobs` URLs

In `core_thresholds.py`, when a static candidate's `listing_url` contains `/jobs` or matches the Phase 5a domain-heuristic, reduce the thresholds: `MIN_STATIC_EVIDENCE_TO_PROBE` from 22 to 16, `MIN_STATIC_EVIDENCE_TO_QUEUE` from 34 to 24. This reflects higher confidence that URLs with career-path patterns are legitimate career pages.

```python
# core_thresholds.py — estimate_probe_priority() or similar:
bonus = 0
if candidate.get("listing_url") and "/jobs" in candidate["listing_url"]:
    bonus += 8  # career-path bonus
if candidate.get("pages") and any("/careers" in p for p in candidate["pages"]):
    bonus += 6
```

This provides an alternate path to queue for static sources that have strong URL signals but weak HTML evidence.

**File:** `src/source_discovery/core_thresholds.py`
**Effort:** ~0.5h

#### 7e — Probe-time ATS detection

In `src/source_discovery/probe.py`, when probing a static source whose listing URL contains `/jobs` or a `careers.*` subdomain, optionally fetch the page and check for ATS HTML signatures. If detected, emit a probe diagnostic suggesting adapter reclassification.

This is additive — the probe already works. The diagnostic is mostly informational unless the probe finds zero jobs (which is a strong signal the static adapter is failing on this source).

**File:** `src/source_discovery/probe.py`
**Effort:** ~1h

#### 7f — Add ATS-suspicious-source report

In the source-policy soak report (`source-policy-runbook.md` pathway), add a report section that lists static sources with `/jobs` in their URL, grouped by suspected ATS (based on HTML fetch). This gives an ongoing audit surface.

**File:** `src/source_discovery/reporting.py` or `tools/measurements/pipeline/`
**Effort:** ~0.5h

---

## Key Files

| File | Role | Changes |
|------|------|---------|
| `src/source_discovery/web_search_candidates.py` | Web search candidate pipeline | Fix Teamtailor detection; add other ATS HTML signatures |
| `src/source_discovery/provider_inference.py` | Provider adapter inference | Add HTML fallback parameter; add domain-name heuristic |
| `src/jobs/adapters/static_listing.py` | Static adapter pipeline | Relax Playwright gate for `/jobs` URLs; add ATS detection advisory; add generic empty-page fallback |
| `data/defaults/source-registry-active.seed.json` | Source registry | Fix 12 misclassified sources; resolve 31 duplicates |
| `src/source_discovery/probe.py` | Source probe | Add ATS detection advisory for static sources |
| `src/source_discovery/reporting.py` | Discovery reporting | Add ATS-suspicious-source report section |
| `src/source_discovery/scoring.py` | Career keyword scoring | Expand `CAREERS_URL_HINTS` with non-English keywords |
| `src/source_discovery/page_analysis.py` | Page landing analysis | Expand `_LANDING_PAGE_SEGMENTS` with non-English equivalents |
| `src/source_discovery/static_candidates.py` | Static candidate building | Relax gating for known-career URLs |
| `src/jobs/adapters/plugins/static/_heuristics.py` | Static adapter heuristics | Broaden JS shell detection; add ATS-detection feedback diagnostic |
| `src/jobs/adapters/plugins/static/_runner.py` | Static plugin runner | Add dead-listing browser retry |
| `src/source_discovery/core_thresholds.py` | Evidence thresholds | Reduce static thresholds for `/jobs` URLs |
| `src/source_registry.py` | Source registry identity | Add reclassification table support |
| `src/source_discovery/reclassification.py` (new) | Reclassification path | Unified adapter change mechanism |
| `scripts/reprobe_static_sources.py` (new) | Audit tool | Periodic static re-probing with ATS detection |

## Verification

| Phase | Verification |
|-------|-------------|
| Phase 1 | Run web search discovery against a studio with a Teamtailor custom domain; verify candidate has `adapter: "teamtailor"`. Run pipeline tests. |
| Phase 2 | Run static pipeline against a Teamtailor JS shell with 4+ nav links; verify Playwright is triggered and jobs are extracted. |
| Phase 3 | `git diff` registry changes; verify each changed source produces jobs in dedicated adapter. |
| Phase 5a | `infer_provider_adapter("careers.foolstheory.com", "/jobs")` returns `"static"` with elevated evidence. |
| Phase 5b | `careers_keyword_count("stellenanzeigen")` returns ≥1. Page with Japanese "採用" passes keyword gate. |
| Phase 5c | `build_static_candidate_from_page` accepts page with `/jobs` in URL but no HTML keywords. |
| Phase 6a | `detect_js_shell("<div id=\"root\">...long text...</div>")` returns True. |
| Phase 6b | Static source that returns dead-listing HTML via HTTP retries with Playwright and recovers jobs. |
| Phase 6c | Static source with zero HTTP jobs but career keywords triggers Playwright fallback. |
| Phase 7a | Static source with ATS links + zero jobs produces `reclassification_hint` diagnostic. |
| Phase 7b | `scripts/reprobe_static_sources.py --limit 5` outputs JSON with detected ATS signatures. |
| Phase 7c | Static source reclassified to `teamtailor` appears in registry with new identity; old identity dropped. |
| Phase 7d | Static candidate with `/jobs` URL gets evidence threshold reduced by 8 points. |
| Phase 7e | Probe against `careers.example.com/jobs` produces diagnostic if HTML contains `teamtailor`. |
| Phase 7f | Report includes "static sources with /jobs" section grouped by suspected ATS. |
| All | Pre-commit gate passes (`npm run lint:precommit:changed`). Run full test suite: `python -m pytest tests/test_jobs_fetcher_quality.py tests/test_jobs_fetcher_pipeline.py tests/test_source_discovery*.py tests/test_provider*.py -q`. |

## Known Limitations

- **HTML-content-based detection is not 100% reliable.** A page containing "greenhouse" in text could be a blog post about greenhouse gases, not Greenhouse ATS. The `careers_keyword_count()` gate reduces false positives.
- **Some ATS platforms have no dedicated runner** (Jobvite, HRMOS, Comeet, Simplicant, GoHire). Detection can flag them but the fix is only better static-adapter handling (Phase 2 / Phase 6).
- **Phase 3b fetch-based audit is O(~16 fetches).** Each URL needs a real HTTP fetch to check the HTML. This is one-time; earlier phases prevent future misclassification.
- **API-bound ATS providers** (Greenhouse, Lever, SmartRecruiters) require fixed API hosts even when detected on custom domains. The candidate builder must still construct `boards-api.greenhouse.io` URLs for the API path, using the custom domain only as the listing URL.
- **Non-English keywords (Phase 5b) are a best-effort expansion.** The list covers major European and East Asian languages but cannot be exhaustive. Community contributions or data-driven discovery (e.g., clustering unknown career pages) would be more comprehensive but is out of scope.
- **Phase 6c (generic empty-page fallback) adds HTTP + Playwright overhead** for every static source that yields zero jobs via basic HTTP. In the worst case, this doubles page-fetch time for all zero-job static sources. Mitigation: skip if source already had a recent Playwright attempt (cooldown).
