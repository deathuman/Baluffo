# Hold-Tail Repair Plan — the 9 quarantined sources behind the 31-row overdue floor

> - **Status:** Wave 0 (read-only re-evidence) executed 2026-09-10 — see "Wave-0 execution" below; **Wave 1 (S1/S2/S3) landed 2026-09-10** — see "Wave-1 execution"; S4 and every registry mutation remain separate (operator-approved) work
> - **Use this when:** adjudicating or repairing the 9 hold-tail sources (Mundfish, Steer, Reflector, Big Moxi, Exit VR, Astrum, Konami, Inverge, SNK), lowering the overdue floor below 31, or extending the systemic fixes S1–S4
> - **Canonical for:** per-source repair designs, wave sequencing, and acceptance criteria for the 31-row floor
> - **Not canonical for:** the guard's promotion/refusal mechanics (`docs/snapshots/zero-kept-guard-2026-09-10.md`), the 2026-09-09 triage verdicts it builds on (`tmp/overdue-triage-20260909/dispositions.json`), or adapter-family approval authority (iCIMS/Dayforce decisions stay in their own thread)
> - **Then inspect:** `src/jobs/adapters/static_zero_kept_guard.py`, `src/jobs/page_gating.py` (`looks_like_server_template_artifact`), `docs/adapter-plugin-inventory.md` (targeted-run commands)
> - **Last updated:** 2026-09-10 (Wave 0 executed)

## Why the floor exists (mechanism, 2026-09-10)

Run5 of the zero-kept-guard verification (forced full pass) ended with **overdue 31 rows /
9 sources, verdict healthy**: three consecutive source errors put the 9 extraction-broken
hold-tail sources into circuit-breaker quarantine (`quarantinedUntilAt=2026-09-10T00:02:31Z`),
so the pass excluded them — and skipped sources provide no availability evidence, so their
lifecycle rows were **correctly preserved** as `verification_overdue`. Every drainable row
drained; these 31 rows cannot drain until their sources produce a trusted read or get real
repairs. The quarantine has since expired; sources retry on the next pass and will keep
re-quarantining (3-consecutive-error cycle) without intervention.

| Source (registry identity) | Rows | Failure mode (2026-09-09 probe + targeted pass) |
|---|---|---|
| `static:listing_url:https://mundfish.com/en/careers` | 10 | Listing server-renders 12 real role links; **detail pages fail** (500-class; targeted pass bucketed js_required — reconcile). Origin-broken, not JS. |
| `static:listing_url:https://www.steerstudios.com/careers#joblist` | 10 | WP site-level 500 since Sep-6; **decisive**: `steerstudios.bamboohr.com` tenant live but EMPTY (`careers/list` totalCount=0; the 10 overdue bamboo detail URLs 404). Rows are real-but-removed. |
| `static:listing_url:https://emplois.reflectorentertainment.com/l/en` | 3 | Dayforce JS portal (signature ×21 → CANDIDATEPORTAL, 0 roles server-rendered). No dayforce adapter (record-only hold, Sep-08 precedent). |
| `static:listing_url:https://bigmoxigames.com/careers` | 2 | Live 200, ~1KB JS board, no server-rendered listings, no ATS signature. 2 overdue `/careers/<slug>` rows once real. |
| `static:listing_url:https://exit-vr.de/jobs/` | 2 | WP fatal 500 site-level ("WordPress › Error"), carried transient since Sep-6. |
| `static:listing_url:https://astrum-entertainment.ru/en/careers` | 1 | Fetcher 'Static redirect loop' (geo/cookie redirect our client won't follow); direct probe 200 with **19 fresh server-rendered job links**. The 1 overdue row is junk (legal page). |
| `static:listing_url:https://www.konami.com/games/us/en/jobs/` | 1 | Fetcher extraction defect: EJS href template literal extracted as URL (`/jobs/<%= official_site %>` → HTTP 400) + stale 404s. Real board is a different Konami URL family (not yet probed). |
| `static:listing_url:https://invergestudios.com/jobs/` | 1 | Bot-wall 403 interstitial ("Checking your browser…", Cloudflare-class), degraded from Sep-6 live-200 with 10 job links; detail candidates 404 → site_changed/needs_review. |
| `static:listing_url:https://www.snk-corp.co.jp/recruit/` | 1 | Real board is SNK's axol ATS (`job.axol.jp/pm/c/snk-corp`, 5 category links server-rendered, jobs behind JS); no axol adapter; extracted detail URL stale-404s. |

Totals: 31 rows / 9 sources. Probe evidence: `tmp/overdue-triage-20260909/probe-results.json`;
targeted-pass adjudication: `docs/snapshots/zero-kept-guard-2026-09-10.md` (the guard refused
all 9 — correctly, none is a legitimate empty).

## Guiding rules

1. **Rows are preserved, never deleted.** Drains go through the Fix A missing path with
   evidence; registry exits are sanctioned tombstones (`superseded_by_provider`,
   `chronic_live_empty_confirmed_3x`) or repoints — never raw deletes.
2. **Every registry mutation is its own operator-approved action** (staging → validation →
   promotion, like the High-5 and bamboo waves). This plan designs; it does not authorize.
3. **Prefer the cheapest correct exit per source:** transient origins get wait-and-reverify;
   real-but-removed rows drain via a trusted zero read; JS/bot-walled boards get one
   browser-fallback adjudication before any adapter talk; adapter families are decisions,
   not reflexes.
4. **No fabricated empties.** Any drain that depends on a zero read must come from the
   guard's trusted paths (no-openings marker or second consecutive clean zero).

## Wave-0 execution (2026-09-10, read-only)

Probes: `tmp/holdtail-20260910/probe-results.json` (33 probes incl. the 10 Mundfish
detail URLs, the steer bamboo JSON endpoint, Konami/Dayforce/axol discovery guesses);
row inventory: `tmp/holdtail-20260910/overdue-rows.json`; refreshed verdicts:
`tmp/holdtail-20260910/dispositions.json`; summary: `tmp/holdtail-20260910/SUMMARY.md`.
No registry mutations, no runtime passes.

1. **S2 resolved without a repo change:** Astrum's Sep-09 `noOpeningsMarker: true` was
   probe-tool noise — the naive probe regex matched a Russian JSON config string
   (`no_vacancy:"At the moment, we have no open positions…"`) inside a `<script>`
   block; the repo's real visible-text detector (`src/jobs/common/no_openings.py`)
   returns **False** on the same page. S2 downgrades to a guardrail test.
2. **Mundfish reconfirmed 10/10 details-broken** (all detail URLs 500, listing alive
   with 12 roles) — S1's exact shape; hold unchanged.
3. **Steer's twin is staged-ready:** bamboo `careers/list` returns structured JSON
   with `totalCount=0` (empty result array) and both sampled overdue detail URLs
   redirect to the empty careers page. The 10th row (WP `/internship`) 200s live-empty
   and re-verifies independently when the WP site recovers — it does **not** ride the
   bamboo migration, so the Steer win is −9, not −10.
4. **SNK's axol tenant went dark:** tenant, category URLs, and the `job.axol.jp`
   platform root all return bare 9-byte 404s (new since Sep-09, which never probed
   the tenant directly). Adapter decision deferred to a browser-context re-verification.
5. **Konami's listing is nearly trusted-empty:** 0 real job hrefs **and a repo-visible
   no-openings marker** — after S3, one clean read should promote ok/0 and drain the
   junk row. Real-board repoint is optional, not drain-blocking.
6. **Reflector nuance:** 1 of the 3 rows is a real job (Technical Director Studio);
   guessable Dayforce API endpoints are 404 — real endpoint discovery needs a
   browser-context session.

Updated floor projection: 31 → **~20 after Wave 1** → **~6 after Wave 2** (the plan's
earlier ~5 becomes ~6 because of the Steer internship row). Verdict table and per-source
deltas live in `tmp/holdtail-20260910/dispositions.json`.

## Wave-1 execution (2026-09-10, code-only)

Landed as one change set; suites `tests/jobs_static/` (360 incl. the new
`test_taxonomy_details_broken.py`), `tests/test_jobs_fetcher.py` + `tests/jobs/` +
storage/state suites — 1,022 tests green; changed-mode gate exit 0.

1. **S1 `details_broken` bucket** (`src/jobs/common/taxonomy.py`): new
   `FailureBucket.DETAILS_BROKEN` + `has_details_broken_signal` — fires only when
   details were actually visited (≥1), candidates or listing links ≥3, detail-fetch
   failures ≥80% of visited, no browser-fallback recommendation and not
   empty-confirmed. The counter `detail_fetch_failed` is stamped at the traversal
   detail-error site (`_record_detail_fetch_error`) and initialized in the entry
   report; the context builder reads both spellings (`detailFetchFailedCount`,
   `stats.detail_fetch_failed`). `map_error_to_failure_bucket` lets details_broken
   beat only the evidence-free classifications; adapter-stamped evidence
   (anti-bot, site_changed, timeout, parse, dead listing, empty, ok_no_jobs,
   fetch_ok_extract_zero) keeps its own bucket (`_DETAILS_BROKEN_STRONG_CLASSIFICATIONS`).
   Deliberate finding: adding `needs_review` to the guard's diagnosis refusal set
   breaks every marker promotion (the trusted-empty page's own diagnosis IS
   needs_review fall-through) — reverted; the prior-bucket refusal is the hardening.
   Guard (`static_zero_kept_guard.py`): `details_broken` added to
   `_BROKEN_PRIOR_BUCKETS` (a details-broken prior read is not clean-zero evidence).
   Mundfish shape pinned: error + generic no-jobs error text + 12 listing links +
   10/10 detail failures → `details_broken` (was js_required via the Sheet text rule).
2. **S2 as guardrail tests** (per Wave 0: no repo defect existed):
   `test_repo_marker_detector_ignores_script_block_config_strings` (Astrum shape:
   script-block JSON marker + 19 real links → repo detector False),
   `test_marker_with_visible_job_links_is_not_promoted` (visible marker + 19 links
   → guard refuses via candidate-links), `test_guard_refuses_details_broken_prior_bucket`,
   and the Wave-1 decision-record test pinning why needs_review stays out of the
   refusal set.
3. **S3 template seams** (`src/jobs/page_gating.py`):
   `looks_like_server_template_artifact` extended to client-side EJS/ERB
   (`<%= x %>`, `<% code %>`) and JS `${…}` interpolation; same two-layer rejection
   (detail-candidate ingestion via `_append_detail_candidate` + noise-title
   extension). Konami's exact `<%= official_site %>` URL pinned at ingestion;
   `$`/`%`-bearing legitimate URLs and titles pinned as non-matches.

S4 (cookie-jar retry) remains unimplemented — it is a fetch-client behavior change
and lands separately.

## Systemic fixes (designed here; each lands as its own change)

### S1 — detail-vs-listing divergence attribution (`details_broken` class)

**Finding:** the deepest mechanism in this tail — most rows are overdue because *detail*
pages die (404/500) while *listing* pages live (Mundfish 12 links, Inverge pre-degradation,
Konami). The current classification collapses both into source-level buckets (`js_required`,
generic extraction error), so a monitor cannot tell "board content alive, details broken"
from "board gone". That distinction decides repair vs prune per source, and today it costs a
manual probe per source to recover.

**Design:** when a fetch's listing page is live-200 **with** job/detail candidates but the
candidate fetches fail en masse (≥N candidates 404/5xx), stamp a distinct
`details_broken` bucket (alongside `site_changed`/`js_required`) carrying counts
(`detailCandidates`, `detailFailures`, status histogram). Classification-only: no verdict,
drain, or row semantics change — the guard still refuses these sources (they are not
trustworthy empties), and rows keep preserving. The win is honest attribution in
`overdueBySource`/failure buckets so this tail never needs hand-probes again.

**Tests:** bucket derivation on listing-live/details-dead vs both-dead fixtures; guard
refusal unchanged under the new bucket; `overdueBySource` attribution unchanged.
**Unblocks:** Mundfish, Konami, Inverge adjudication (and future tails).

### S2 — no-openings marker: non-English false-positive guardrail

**Finding:** the Astrum probe returned `noOpeningsMarker: true` **alongside 19 job links**
(`tmp/overdue-triage-20260909/probe-results.json`). Either the probe's marker heuristic or
the guard's `contains_no_openings_marker` (likely tuned on English phrases) fires on a
Russian-language page. A false-positive marker on a page whose job links the parser missed
(encode/charset issues) is exactly the input the guard promotes to a fabricated empty.

**Wave-0 outcome:** the flagged input was probe-tool noise, not a detector defect —
the repo's visible-text detector correctly ignores script-block JSON config strings
(the naive Sep-09 probe regex did not). Downgraded to a guardrail test: pin the
`jobLinks > 0 → never marker-promote` invariant in
`tests/jobs_static/test_static_zero_kept_guard.py` as cheap insurance, with the
Astrum-shaped fixture (script-block marker + 19 job links → detector False).
**Unblocks:** Astrum (S4 depends on a trustworthy zero/empty read for its junk-row drain).

### S3 — client-side template-literal extraction rejection (Konami class)

**Finding:** Konami's listing leaks EJS href templates (`<%= official_site %>`); the parser
extracts them as detail candidates → HTTP 400 → source errors run-over-run. This is the
client-side sibling of the ESAPI/velocity server-template class fixed in
`looks_like_server_template_artifact` (`src/jobs/page_gating.py`).

**Design:** extend the artifact detector to client-side template seams in hrefs —
`<%= … %>`, `<% … %>`, `${…}` — rejected at `_append_detail_candidate` (same two-layer
shape as the ESAPI fix: candidate-level rejection + noise-title extension). Verify
non-matching on legitimate URLs containing `$` or `%` in query strings (fixture-pinned).

**Tests:** extend `tests/jobs_static/test_static_parser_noise_titles.py` (EJS seam matches;
salary/query-string non-matches; Konami-shaped fixture).
**Unblocks:** Konami's source stops flapping; its 1 stale row then drains through the
normal missing path once the board read is trusted (or gets repointed per the Wave-2 probe).

**Wave-1 addendum (2026-09-10): second-funnel closure — the rejection had one funnel too few.**
Post-landing live verification found Konami still fetching
`https://www.konami.com/games/us/en/jobs/<%= official_site %>` (HTTP 400 in the final forced
pass's error string) despite the landed S3 check. Root cause: the Wave-1 rejection was wired
only into the traversal funnel (`_append_detail_candidate`, `static_listing_state.py`), but
detail candidates are built in three places and the heuristic funnel `add_detail_link`
(`static_detail_heuristics_filter.py`) — the path every parse/rows flow uses
(`_collect_listing_detail_links`, the parse extractors) — had no template check; with
`/jobs/` a default detail path token, the seam passed the `probable_job_detail` heuristic
and reached the fetcher. The traversal-funnel rule now applies at the two remaining
construction sites: `add_detail_link` rejects seams in the raw candidate, the joined
absolute, and the anchor text (counted as `dead_listing_page`); detection subtlety — a
relative `<%= %>` href is detectable only pre-join (`normalize_url` alone drops the
relative shape) while percent-encoded drift only appears post-join, so both are checked.
`_nested_detail_candidates` (`static_listing_traversal.py`) applies the same url+title rule
to depth+1 links scraped from detail pages — the last direct `StaticDetailCandidate`
construction site. No behavior change for healthy URLs (the check only fires on template
seams, which always 400). Tests: `tests/jobs_static/test_detail_link_filtering.py` +5
(Konami exact URL, relative EJS shape, `${jobId}` seam, seam anchor text, nested-candidate
filtering); 383 static tests green; changed-mode gate exit 0.

**Live confirmation (2026-09-10, forced targeted pass over the 9 hold-tail sources,
`RUN_EXIT=0`):** Konami's error flap dropped to the 404-only shape —
`HTTP 404 for https://www.konami.com/games/us/en/pages/sns_account`, **no template URL
fetched or errored** (previous pass: 452-char error ending in the
`/jobs/<%= official_site %>` HTTP 400). Konami stays honestly `site_changed` (the dead
`sns_account` scrape target remains the real defect, adjudicated in Wave 2). 9-source
distribution unchanged: `details_broken ×3` (Inverge, Mundfish, SNK), `site_changed ×3`
(Konami, Astrum, Reflecto), `js_required ×2` (Steer, Big Moxi), `needs_review ×1` (Exit VR).

### S4 — cookie-jar/UA retry for geo-cookie redirect loops (Astrum class) — **LANDED 2026-09-10, flag-off by default**

**Finding:** Astrum's board 200s with 19 fresh links to a direct client but the fetcher
hits a 'Static redirect loop': the origin sets a geo/cookie round-trip our static client
refuses to follow.

**Design:** scoped fetch-client experiment: on a detected redirect-loop terminal reason,
retry once with a per-attempt cookie jar (honor `Set-Cookie` through the loop) and a
browser-like UA, behind an explicit allowlist/flag (never global default). Boundary note:
touch the static fetch client only, not the composition root. If the experiment recovers
Astrum cleanly, document the pattern in `docs/adapter-plugin-inventory.md`; if not, Astrum
stays a documented client limitation (record-only, like the Sep-09 hold).

**Tests:** loop-detection retry unit tests with a fixture redirect chain (cookie honored →
200; no cookie → loop classified as before); no behavior change for non-loop sources.

**Wave-1 execution (code only, no live pass):**
- `src/jobs/common/http.py`: additive `default_fetch_text_with_response_headers` — same
  read-at-most/byte-cap behavior as `default_fetch_text`, plus lowercased multi-valued
  response headers; 3xx `HttpStatusError` now carries `headers` (original
  `default_fetch_text` contract unchanged, headers stay empty there).
- New leaf `src/jobs/adapters/static_cookie_retry.py`: `BALUFFO_STATIC_COOKIE_RETRY`
  truthy flag AND `BALUFFO_STATIC_COOKIE_RETRY_HOSTS` (www-stripped, case-insensitive,
  comma-separated) allowlist; `StaticCookieJar` parses `Set-Cookie` via stdlib
  `CookieJar.make_cookies`, stores per-host, emits a `Cookie` header per hop.
- `StaticHtmlFetcher`: the hop chain was factored into `_fetch_hop_chain` (byte-for-byte
  the pre-S4 behavior); a `Static redirect loop` RuntimeError on an allowlisted host
  under the flag triggers `_cookie_jar_retry_fetch` — one attempt per hop, 6-hop budget
  (`COOKIE_RETRY_MAX_HOPS`), same-site redirect safety rules still enforced
  (cross-host/downgrade rejections unchanged), and a cookie that fails to break the loop
  re-raises the loop error with a `(cookie-jar retry exhausted)` marker — never a new
  failure class.
- Design nuance found during implementation: with a same-URL bounce the base lane loops
  **without a second network call** (the loop is detected from the redirect target), so
  the retry lane treats a cookie-less round-trip at its first hop as the geo-cookie shape
  itself and continues with the freshly absorbed cookie; a round-trip after a cookie was
  already sent is exhaustion.
- Tests: `tests/jobs_static/test_static_cookie_retry.py` (13 cases: gate matrix,
  cookie-honored recovery, flag-off/unlisted-host loop preservation, exhausted-cookie
  loop reclassification, runaway-chain bound, unsafe-redirect rejection, jar unit,
  primitive success/redirect/cap/contract pins).
- **Operator activation for Astrum:** set `BALUFFO_STATIC_COOKIE_RETRY=1` and
  `BALUFFO_STATIC_COOKIE_RETRY_HOSTS=astrum-entertainment.ru`, then a forced targeted
  pass (`--only-sources ... --ignore-circuit-breaker`). The Astrum junk row drains via
  the guard once the fetcher reads the live board; without the flag, behavior is
  unchanged and Astrum stays record-only.

## Per-source repair designs

### 1. Mundfish — 10 rows (details-broken; listing alive with 12 real roles)

- **Wave 0 evidence:** read-only probe of the 10 overdue `/careers/<slug>` detail URLs and
  the listing; reconcile the targeted pass's js_required bucket against the probe's
  server-rendered links (S1 will make this distinction durable).
- **If details 200:** the rows are stale-failing over a live board — run the forced
  targeted pass; rows re-verify and drain normally (the Brain Up precedent from the
  targeted pass).
- **If details still fail but listing lives:** no registry action — Mundfish is Mondoire/
  Atomic Heart with a real, active board; wait-and-reverify with the S1 bucket surfacing
  the state. Optionally one browser-fallback adjudication run (details may render where
  the static client 500s).
- **If the listing later reads trusted-empty** (studio lapsed): rows drain via the guard +
  missing path; no tombstone until a third-consecutive-probe dead/empty confirmation.
- **Explicit non-goal:** no adapter — the WP structure needs none (Sep-09 triage holds).

### 2. Steer — 10 rows (highest-value repair: provider twin to the live bamboo tenant)

- **Design:** the rows are **genuinely gone** (bamboo tenant live, totalCount=0, the 10
  detail URLs 404) but the *carrying static row* is a WP 500 that cannot produce the
  trusted zero read that would drain them. Migrate the identity: stage
  `https://steerstudios.bamboohr.com/careers` through the sanctioned pending
  provider-migration lane with `migrationSourceIdentity` → the steer WP row (High-5
  precedent, `docs/snapshots/overdue-repair-prune-wave-2026-09-10.md`), then
  `transition_registry_to_active` + `normalize_manual_promotion_rows`; tombstone the WP
  row `superseded_by_provider` at promotion.
- **Why this drains the rows:** the bamboo adapter's structured read of an empty tenant is
  a trustworthy clean zero (the bamboo-wave pattern: live-empty tenants poll directly, so
  any future Steer post lands within one cadence — yield 0 today, future yield secured).
  With a trusted zero on the row's provenance, the 10 rows fail missing evidence and drain.
- **Operator gate:** registry mutations (staging, promotion, tombstone).
- **Verification:** 3-family validation pass (bamboo 0 failed), read-back of registry
  buckets, then overdue delta −10 on the next pass.

### 3. Reflector — 3 rows (adapter decision, not a data mutation)

- **Design:** Dayforce portal, 0 roles server-rendered; only a structured adapter can read
  it. Time-boxed **read-only API discovery** first: probe whether the CANDIDATEPORTAL
  tenant exposes a public JSON endpoint (Dayforce boards commonly do; unverified).
  - Endpoint exists → fold into the standing iCIMS/Dayforce adapter-decision thread
    (memory sequence item 3) as a `dayforce_sources` candidate with Reflector as the pilot
    source; rows stay held until the adapter lands, then re-verify or drain normally.
  - No endpoint → stays record-only (existing Sep-08 precedent); rows remain the honest
    floor.
- **Explicit non-goal:** no tombstone — the studio is alive; this is a reader gap.

### 4. Big Moxi — 2 rows (JS board; browser-fallback adjudication)

- **Design:** one forced targeted run with browser fallback enabled
  (`--only-sources "static_source::static:listing_url:https://bigmoxigames.com/careers"
  --ignore-circuit-breaker`). No ATS signature and a ~1KB shell means the fallback pool is
  the only reader. Three adjudication branches: rendered jobs → rows re-verify;
  rendered-empty → second clean render on a later pass gives the guard its
  two-consecutive-zero basis and the rows drain; fallback blocked/challenged → hold
  (re-classify with S1 semantics if the board is unreachable, not empty).
- **No registry action** in any branch until the board's truth is rendered-confirmed.
- **Adjudication (2026-09-12, forced targeted pass — HOLD, rendered-empty ×1 of ×2):**
  the listing 200s with the same ~1KB JS shell; the fallback pool fired 3×
  (`js_shell`, `empty_page`) and **got HTML every time** (pool acquisitions=2, no relaunches
  — the render lane itself is healthy). An independent real-browser probe
  (`tmp/holdtail-wave2b-20260912/probe_render.py`, Playwright Chromium,
  `domcontentloaded` + 6s hydration — `networkidle` never settles on this site) confirms
  the automation-visible page is **genuinely empty**: status 200, title rendered
  ("Big Moxi — Scale Game Development Smarter"), **0 body text, 0 links, 0 console errors**,
  and the DOM never grows beyond the 1.1KB shell — the board script fetches nothing and
  renders nothing. No hidden XHR surface to capture (unlike the Dayforce case): the page
  is not an SPA loading a board, it is an empty shell with a jobs URL. The guard refused
  the zero (browser_fallback_attempted) — correct, and the source stays `js_required` with
  rows preserved. **This is rendered-empty confirmation #1**; the two-consecutive-zero
  basis needs one more clean render on a later pass, then the 2 rows
  (`/careers/unreal-programmer`, `/careers/Game-Systems-Engineer`) drain via the guard.
  No registry action.
- **Adjudication #2 (2026-09-12, forced targeted pass #2 — rendered-empty ×2 confirmed
  evidentially, but the automatic drain lane does not exist in code; HOLD stands, no
  registry action):** pass #2 (isolated output dir `tmp/holdtail-wave2b-20260912/bigmoxi2/`,
  live `data/` state untouched by targeted runs — they write state into their own output
  dir) reproduced #1 exactly: listing 200 with the ~1KB shell, fallback pool fired 2×
  (`js_shell`, `empty_page`), HTML every time, 0 extracted, generic zero error; detail-level
  stamp `site_changed`/`broken_extraction`, source bucket `js_required`. The independent
  Playwright probe re-run matched #1: status 200, title rendered, **0 text chars, 0 links,
  0 console errors**. The two-clean-render basis is complete. **The discovery:** the branch's
  "drain via guard" mechanism is not realizable — the zero-kept guard's
  `prior_clean_zero_read` path (the intended ×2 consumer) is unreachable for this shape:
  (a) the guard unconditionally refuses any zero read with listing browser fallbacks
  (`listing_browser_fallbacks > 0` → `browser_fallback_attempted`; by design a
  browser-rendered zero is indistinguishable from a JS-shell trap without new evidence
  plumbing), and (b) errored reads never increment `consecutiveZeroKept`, and Big Moxi's
  prior bucket is `js_required` (broken set). Confirmed against the guard's own test pins
  (no browser-fallback promotion shape exists) and both zero-kept funnels. The rows also
  cannot drain on dead-link evidence: both row URLs soft-200 the same 1KB shell (catch-all
  routing), so no 404 shape reaches the stale-detail demotion or the direct-check lane.
  **Disposition:** HOLD — the evidence says empty, but the trust boundary says a rendered
  zero is not yet admissible proof. The honest paths forward: (1) a small future S6 leaf —
  persist a `renderedEmptyConfirmedAt` timestamp from the browser-fallback lane when the
  render returns full HTML, and let the guard accept two such timestamps as the emptiness
  evidence (equivalent discipline to the manual probe, no widening of the JS-shell trap
  surface); or (2) an explicit operator decision to adjudicate the 2 rows manually on the
  ×2 evidence. Until one of those, the rows remain the floor.
  **Update (2026-09-12, later same day):** path (1) is now built — see the S6 execution
  record below. The ×2 basis re-accumulated live through the new lane and the guard
  promoted on pass 3: Big Moxi's state row reads `lastStatus: ok`,
  `lastFailureBucket: no_openings`. The 2 rows keep their missing-universe drain lane on
  a full default pass with the source eligible.

### 5. Exit VR — 2 rows (transient origin; wait-and-reverify)

- **Design:** none beyond monitoring — WP fatal 500, studio alive, disposition policy
  identical to Steer's: the board's rows re-verify when the site recovers (they point at
  real `/jobs/<slug>` pages) or drain via trusted zero + missing path if the recovered
  board is empty.
- **Escalation trigger:** if the 500 persists ~4+ weeks (chronic, not transient), run the
  third-probe dead/lapse confirmation and only then consider tombstone criteria. Until
  then the 2 rows are correctly preserved.
- **Re-adjudication (2026-09-12, live probes — wait-and-reverify unchanged):** the origin
  still WP-fatal-500s, now **site-wide** — `/`, `/jobs/`, and `/data-protection` all serve
  the same 13.4KB WordPress error page (listing probe: 500, "WordPress › Error"), so this
  is an origin outage, not a `/jobs/`-only regression. Chronic day ~7 of ~28: the
  escalation trigger is not met. The 2 overdue rows (`/jobs`, `/data-protection` — both
  junk-class ingests) cannot drain or re-verify while the site 500s, and correctly so:
  500 is transient-or-broken, never gone — a recovery re-verifies the rows, a chronic-day
  ~28 third-probe opens the dead/lapse lane. **No action this pass.**

### 6. Astrum — 1 row (fetch-client fix + junk-row drain)

- **Design:** S4 is the real repair. Once the fetcher reads the board (19 live links), the
  junk overdue row (legal page) fails missing evidence and drains; real rows land.
- **Sequencing note:** S2 lands first or with it — the board's eventual zero/empty states
  must not be marker-fabricated (the probe's contradictory `noOpeningsMarker: true`).
- **Fallback:** if S4's experiment cannot follow the geo/cookie round-trip, the source
  stays record-only with its documented client limitation; the 1 junk row remains held
  (acceptable: it is 1 row).

### 7. Konami — 1 row (S3 extraction fix + board discovery)

- **Design:** S3 unblocks the flap. **Wave 0 probe** the real global careers portal (the
  Sep-09 triage identified the family but never probed it): if it is an addressable board
  (or an ATS the repo already adapters), design a repoint/migration in a follow-up
  disposition (GSC-repoint class, commit `67884314` precedent); if it is out of reach, the
  row drains via missing path once the fixed source produces a trusted read of the current
  page (it 200s with 1 self-href — effectively empty), which S3 makes reachable.
  **Wave-0 update:** the guesses 404'd, but the listing now carries a repo-visible
  no-openings marker with 0 real job hrefs — the drain path is primary; a repoint is
  optional and not drain-blocking.
- **Registry gate:** any repoint is operator-approved.
- **Adjudication (2026-09-12, live probes + guard-spy run — HOLD on automated drain; the
  promotion path is blocked by a funnel gap, not by the evidence):** the listing still
  reads trusted-empty (200, repo `contains_no_openings_marker` → **True**, 0 real job
  hrefs out of 70 — nav/self only), but the stale-detail demotion built for exactly this
  shape never fires. Two blockers, confirmed by running the real source through the real
  runner with the guard instrumented (`tmp/holdtail-wave2-20260910/konami-guard-debug.py`:
  **0 guard calls**): (1) Konami rides the plugin fast path, where
  `_probe_empty_plugin_listing` classifies the nav-only listing `dead_listing_page` and
  `_record_empty_plugin_result` early-returns before `promote_clean_zero_kept` — the
  stale-detail demotion only lives inside the guard, so it is unreachable on this funnel;
  (2) the rows flow fetched the stale Community nav link
  (`/games/us/en/pages/sns_account`, no trailing slash → HTTP 404), appended the error
  line, and the entry wrapper raised `AdapterValidationError` — the source-level report is
  `status=error`, bucket `site_changed`, `zeroKeptClassification broken_extraction`, and
  the availability drain treats any error as broken missing evidence. The overdue row's
  own `jobLink` **is the dead nav page** (`availability_0676ed10…` →
  `https://www.konami.com/games/us/en/pages/sns_account`), and the page has drifted shape,
  not died: the slashless URL 404s while the slashed twin (`…/sns_account/`) serves 200.
  The row is junk either way (a Community nav page ingested as a job — it could never
  verify available as a job). **Paths forward (recorded, not executed):** (a) small S7
  funnel fix — extend the stale-detail demotion to the plugin empty-result path (and
  ordering: judge emptiness on the listing's own marker evidence before the
  `dead_listing_page` short-circuit), making the guard's documented Konami shape actually
  reachable end-to-end; (b) operator adjudication — drain the junk row on the completed
  evidence (marker-confirmed empty board + stale-detail 404 with a live slashed twin).
  Until one lands, the row stays the floor. **S7 executed (2026-09-12):** path (a) landed
  the same day — see the S7 execution record below; the next full default pass retires
  the row (expected floor 9 → 8).

- **Design:** one forced targeted run with browser fallback (the 403 interstitial is the
  fallback pool's target case). Branches mirror Big Moxi: through-the-wall render with
  jobs → re-verify; rendered-empty (×2) → drain via guard; still challenged → hold and
  re-probe on the next tail wave. Sep-6 evidence showed a live board behind the wall
  (10 links), so favor patience over registry action.
- **Adjudication (2026-09-12, forced targeted pass — HOLD; the question inverted):**
  the Sep-6 403 bot wall is **gone** — the listing now 200s to plain httpx (143KB,
  full server-rendered board with 5 "SEE OFFER" links: concept-artist,
  3d-character-artist, senior-environment-artist, quality-assurance, 3d-artist-internship;
  no JS render needed at all; the pipeline's 3 fallbacks still fired and got HTML). But
  **every detail URL 404s** — including with the trailing slash the real links carry —
  serving the site's Spanish 404 template ("Página no encontrada - Inverge Studios",
  126KB soft bodies, 0 job content), and the site's own href for senior-environment-artist
  is malformed (`invergestudios.comjobs/…`, missing the slash). Locale-prefixed and
  alternate-shape guesses all 404 too. The rows flow aborted on the first detail 404
  (abort-on-first-failure semantics unchanged) and S1 stamped **`details_broken`**
  honestly — the Wave-1 machinery's first fully-correct live attribution on this shape.
  This is NOT rendered-empty (the board renders WITH jobs — they are just unreachable),
  so neither the ×2 drain branch nor the re-verify branch applies: the site reads as
  **mid-rebuild** (Spanish-default locale, stale board links, malformed href, WP 404
  templates) and the disposition is wait-and-reverify. The 1 overdue row is the nav-link
  junk row (`/jobs` "Jobs") and would drain via the finalize missing path only if a full
  pass reads the source clean — it cannot while details keep 404ing the rows flow. No
  registry action; re-probe details on the next tail wave.

### 9. SNK — 1 row (adapter-gap decision; record-only until then)

- **Design:** the real board is the axol ATS (5 category links server-rendered, jobs
  behind JS). Two honest options: (a) record-only continuation (current precedent — the
  row stays held; axol joins the adapter-gap ledger as the 4th family alongside zoho-
  recruit-closed, iCIMS, Dayforce), or (b) bundle an `axol` adapter decision into the
  iCIMS/Dayforce thread if a public per-category JSON exists (Wave-0 read-only probe of
  `job.axol.jp/pm/c/snk-corp` endpoints decides cheaply). Either way the 1 row is
  preserved until the decision; no tombstone (the studio and board are alive).
  **Wave-0 update:** the probe found the tenant, its category URLs, **and the axol
  platform root** returning bare 404s — the adapter option is deferred until a
  browser-context re-verification distinguishes real platform absence from a
  tooling artifact.
  **Resolution (2026-09-11 probe, plan §Wave-3 addendum): option (a) — record-only,
  no adapter migration.** The CSRF two-step hypothesis from the Dayforce discovery
  is void: header shape was exonerated first (bot headers and the full browser trio
  get identical 9-byte 404s — not a Mundfish-style bot filter), no SPA/XHR surface
  exists at any probed axol URL, and the platform is **alive** (live tenants answer:
  `/jn/c/kadokawa` 302, `/bx/c/eytax` 200, and the registry's own Marv row
  `qd/c/marv/job/search` 200 with 123 lifecycle rows available — Marv is the only
  axol-registered row and the static adapter already reads it). What died is SNK's
  **`/pm/` tenant**: the mid-career product line appears retired platform-wide
  (legacy `/vb/` paths also 404; Wayback's last job.axol.jp captures are 2022–2024
  `/vb/`; `/pm/` was never archived), SNK's tenant 404s under every prefix, and
  SNK's live recruit page still links only axol — no replacement board. The 1
  overdue row is the nav-link junk row (`/recruitment` "RECRUIT") over the
  `recruit/` listing source stuck at 9 consecutive `details_broken` failures
  (listing links 4 dead axol categories; rows flow aborts on first 404): it drains
  via the finalize missing path whenever a full pass runs with the source eligible
  (listing 200s, dead links fall out) — no code work, the S5 pattern covers it.
  Wait-and-reverify: either SNK restores a board or the listing eventually reads
  trusted-empty and the guard path takes over.
  **Re-probe with the dayforce two-step (2026-09-12, `tmp/snk-axol-reprobe/`):
  record-only disposition stands — the tenant is gone, and the CSRF hypothesis is now
  empirically closed, not just header-exonerated.** The platform's real CSRF surface is
  a form token, not NextAuth: the live marv board (`/qd/c/marv/job/search`) embeds an
  `fb_csrf` hidden input and the GET→POST same-session round-trip answers 200 — the
  two-step *mechanism* works on axol, so a token gate was never what hid SNK. SNK's
  tenant 404s under every product-line prefix (`/pm/`, `/qd/`, `/bx/`, `/jn/`, `/vb/`
  × `snk-corp`/`snk`) and the subdomain form doesn't resolve; the live marv control
  answers 200 throughout. SNK's own recruit page still links four axol category URLs
  today (`/pm/c/snk-corp/public/job/category/{token}` — a deeper path shape than any
  prior probe tried) and **all four 404 too**: the site links a tenant the platform no
  longer serves. No replacement board (mynavi/Twitter links only). The 1 overdue junk
  row's drain lane is unchanged (finalize missing path on a full pass with the source
  eligible).

**Axol adapter sketch and build decision (2026-09-12, `tmp/axol-adapter-sketch/`):
reads need no CSRF at all — and for marv alone, no-build.** Probing the live marv
tenant for the sketch produced a platform contract that supersedes the Wave-3
"CSRF-gated SPA" framing twice over: the `fb_csrf` hidden input belongs to the
**search-filter form** (`action=ob/search` in the re-probe's own capture; the POST
round-trip returned the identical 45,801-char page), not to the listing — job reads
are **plain GETs with no token, no POST, and no cookie requirements** (verified with
a fresh no-cookie client). Verified URL map: `/qd/c/{tenant}/job/list` server-renders
the full offer list on one page with all detail URLs (`/job/detail/{encid}`, 29 hrefs
on marv); `/job/search` is the paginated card view (`?page=N&searchKey=…` with a
server-issued `searchKey` — pagination must follow rendered hrefs, never be
constructed); `/job/detail/{encid}` is fully server-rendered (title in `<title>`,
`job__offer__detail__{item,title,text}` label/value blocks, `job__jusho__*` location
block, ~8.8 KB/page). Row selectors: `jsAxolJob_box` / `jsAxolJob_title` /
`job_encid`. **The sketch, if ever built:** a plugin-family HTML adapter — register
on the `job.axol.jp` host, listing lane prefers `/job/list`, details through the
existing static traversal (**no** `_skipDetailFetch` — details render server-side,
unlike Dayforce), row id `axol:{tenant}:{encid}`, empty boards ride the existing
zero-extract taxonomy (server-rendered empties are trusted-empty evidence; no special
empty class needed). **Decision: no-build.** Marv is the only axol tenant in the
registry (active 2,167: one axol.jp row [marv] + one Axolot Games name collision;
pending 830: zero; tombstones: zero), and it is **already active and healthy on the
static lane** — `lastStatus: ok`, `lastKeptCount: 38`, `consecutiveFailures: 0` on
today's full pass, reading the same server-rendered HTML an adapter would read. A
structured migration would be a re-registration with identical output: zero yield
delta, nonzero migration risk — the opposite of the Dayforce case (SPA-invisible
jobs, a tenant-addressable family). Revisit triggers: a second axol tenant appears
in discovery; marv's static lane degrades; or axol ships an SPA migration (then the
S6 rendered-empty lane becomes the reading lane and `/job/list` is the cheapest
re-entry point). This also retro-closes the §9 design note's "no axol adapter" gap
framing: an adapter was never the missing piece for SNK (tenant gone) and static
reads every live axol board fine.

## Wave sequencing

| Wave | Contents | Mutations | Expected floor effect |
|---|---|---|---|
| **0. Read-only re-evidence** | Fresh probes: Mundfish listing + 10 details, Steer bamboo recheck, Konami real-board discovery, Reflector endpoint discovery, SNK axol endpoints, Inverge/Big Moxi interstitial status. Refresh dispositions into `tmp/holdtail-20260910/`. | None | Decision basis only |
| **1. No-mutation repairs** | S1 (details_broken), S2 (reduced to a guardrail test — Wave 0 cleared the detector), S3 (EJS/template seams), S4 (cookie-jar experiment); Konami trusted-read drain after S3 | None (code only) | 31 → ~20 (Konami 1, Astrum 1) |
| **2. Registry adjudications (operator-approved per action)** | Steer bamboo twin staging→promotion (**−9**; the WP /internship row tracks the WP site, not the twin); Big Moxi + Inverge browser-fallback forced runs | Steer stage/promote/tombstone | 31 → ~6 (Reflector 3, SNK 1, Exit VR 2 remain; Big Moxi/Inverge drain if rendered-empty ×2) |
| **3. Adapter decisions** | Reflector dayforce (browser-context endpoint discovery — guessable paths are 404) into the iCIMS/Dayforce thread; SNK axol **re-verification first** (tenant/platform currently 404) | None in this plan | Potential −4 (Reflector 3 + SNK 1) if both adapters materialize |

Honest floor after full execution: **~6** (Reflector 3 + SNK 1 + Exit VR 2 pending their
external triggers, minus any Wave-2/3 luck). Zero is not promised — the remainder is
genuinely blocked on adapter decisions or the sites themselves. Risk case: if Mundfish's
origin never recovers, its 10 rows keep the floor at ~16 until chronic-scale tombstone
criteria eventually apply.

## Verification

```bash
# Wave 0 probes (read-only, bounded, no registry writes)
#   reuse the triage probe shape: tmp/overdue-triage-20260909/probe.py

# Forced targeted runs (per adjudication; bypasses circuit breaker explicitly)
python src/jobs_fetcher.py --only-sources "static_source::static:listing_url:https://mundfish.com/en/careers" --ignore-circuit-breaker
python -m src.jobs.pipeline --output-dir <tmp> --only-sources <ids> --no-seed-existing-output \
  --no-preserve-previous-on-empty --force-refresh-all --ignore-circuit-breaker --timeout 12

# Systemic-fix suites
python -m pytest tests/jobs_static/test_static_zero_kept_guard.py tests/jobs_static/test_static_parser_noise_titles.py -q
python scripts/precommit_gate.py --mode changed

# Floor tracking after each wave: overdueBySource top-N + availability verdict on the next full pass
```

### Wave-1 live verification record (2026-09-10, forced targeted pass ×3, `tmp/mundfish-verify-20260910/`)

The first forced pass verified the health side (overdue 31/9, Mundfish 10 in `overdueBySource`, Δ0) but
stamped Mundfish `js_required` with all-zero detail stats: production Mundfish runs the **generic runner**
(no plugin handles `mundfish.com`), and the failure path is the **rows flow** — rendered cards re-verified
against their detail URLs — where the first detail 500 aborted the source before any counter landed, so the
Wave-1 traversal-only counter hook never saw the evidence. Two merge gaps then surfaced in the second pass
(detail-level bucket honestly `details_broken`, source-level still `js_required`): the persisted-report
detail stats whitelist (`normalize_fetch_report_detail_stats`) lacked `detail_fetch_failed`, and the
source-level zero-kept classification runs on the report only, so adapter-stamped detail evidence never
reached it.

Landed (all additive to Wave 1; abort-on-first-failure semantics unchanged):

1. `static_listing_rows.py` — the rows flow now counts the detail attempt + failure and appends the error
   before re-raising (ratchet-safe: only expected static fetch fallbacks), and stamps `listingJobsFound`
   (the live board's visible card count) before per-row verification, because an abort on row 1 means the
   post-return counters cannot see the other 11 cards.
2. `pipeline_source_results.py` — new `_apply_static_detail_evidence_to_report` projects the detail entry's
   evidence (candidateLinksFound / detailPagesVisited / detailFetchFailedCount / listingJobsFound,
   nonzero-only) onto the source-level report and stamps `details_broken` when the signal fires; wired on
   both the success and `_SourceExecutionFailure` paths of `execute_loader`.
3. `fetch_report_normalization.py` — `detail_fetch_failed` added to the persisted detail stats whitelist and
   `listingJobsFound` to the detail-item projection.

Verification (final pass): Mundfish **source- and detail-level `failureBucket=details_broken`**
(`classification=needs_review` migration intact), 3 of 9 hold-tail sources now honestly
`details_broken` (Mundfish, Inverge, SNK — all rows-flow detail-abort shapes), bucket distribution
`{details_broken: 3, site_changed: 3, js_required: 2, needs_review: 1}`, health verdict **healthy**
(overdue 31/9, Δ0 — rows correctly preserved), state `lastFailureBucket=details_broken` with
`lastDetailPagesVisited=1` and the breaker re-quarantined under the honest bucket. Note: the guard's
`prior_clean_zero_read` path cannot mistake these for clean reads (`details_broken` is in its broken-prior
set). Known ordering nuance: the projection runs before finalize rewrites the report error string, so the
source-level evidence keys may read null in the *current-run* report while the detail entry (and next run's
classification) carry them; the detail entry stays authoritative. Tests: `tests/jobs_static/test_static_rows_detail_error_accounting.py`
(9 cases: counter-before-reraise, not-swallowed, unexpected-bug ratchet, evidence stamp before row loop,
end-to-end details_broken, no-evidence → js_required separator, projection surface + inertness,
normalizer round-trip); 378 static tests + pipeline/taxonomy/enrichment suites green; changed-mode gate exit 0.

### Wave-2 execution record (2026-09-10, operator-approved: the Steer provider-twin migration; evidence `tmp/holdtail-wave2-20260910/`)

**Steer (#2, the plan's single registry mutation) — executed end-to-end through the sanctioned lanes:**

1. **Pre-flight:** bamboo tenant reconfirmed live-empty via the structured endpoint
   (`careers/list` → `{"meta":{"totalCount":0},"result":[]}`); WP listing still 500;
   registry pre-flight clean (no existing steer bamboo row in active or pending; the WP
   row is the only steer registration). Exit VR re-probed in the same step: still WP
   fatal 500 (5 days chronic — well below the ~4-week escalation trigger).
2. **Staging** (`stage_steer.py`, dry-run then `--apply`): staged
   `bamboohr:listing_url:https://steerstudios.bamboohr.com/careers` via
   `transition_registry_to_pending(reason=provider_migration_candidate)` with
   `migrationSourceIdentity` → the WP row, `candidateState=staged_provider_candidate`,
   `createdFromAdvisory=true` (widget-wave shape). Pending 830 → 831, read-back OK.
3. **Validation** (`validate.log`): bamboo-family targeted pass with
   `--include-pending-provider-migration` — exit 0, **0 failed sources**, family
   99 fetched / 96 kept, and **Steer Studios (BambooHR): status ok, fetched 0 / kept 0,
   no error** — the structured clean zero the WP 500 could never produce.
4. **Promotion** (`promote_steer.py`, mirrors the 9-row bamboo wave):
   `transition_registry_to_active` → active 2,167 → 2,167 (1:1 swap), pending 831 → 830,
   WP row tombstoned `superseded_by_provider` (tombstones 158 → 159, active bucket),
   `normalize_manual_promotion_rows: 1`, active seed 1,891 → 1,891 (dropped the WP row,
   added the promoted row). Full read-back verification OK (state/candidateState/
   enabledByDefault, twin absent from both buckets, no pending leak).
5. **Verification pass** (`verify-run.log`, `RUN_EXIT=0`): promoted row + 8 remaining
   hold-tail sources; 0 loader failures; verdict healthy, floor 31/Δ0 in the same pass
   as expected — and the availability sweep plan (created 23:12Z, shadow mode) **selected
   the 9 steer bamboo-provenance URLs (`view.php?id=…`) for re-verification**, so the
   −9 drain lands with the sweep/next pass exactly per the High-5 precedent (drain on
   the pass after the identity migration). The WP `/internship` row tracks the WP site
   and re-verifies independently when it recovers.

**Reflecto (#3) — adapter disposition recorded, no mutation:** Wave-2 re-probe corrected
the Wave-0 evidence: the Dayforce portal **catch-all-routes every unknown path to the SPA
shell** (`text/html`, identical body for the API guesses *and* a deliberately fake
control path), so "404 at guessable paths" was an artifact of naive probing — there is
**no public JSON API** discoverable without browser-context network inspection of the
CANDIDATEPORTAL XHR. Disposition: record-only hold (3 rows remain the honest floor; 1 is
a real job), Dayforce adapter decision stays with the iCIMS/Dayforce thread, **no
tombstone** (studio alive; reader gap). Thread note created:
`adapter-decisions-icims-dayforce-thread` (Basic Memory) with Reflector as the pilot
candidate and the axol deferral.

**Exit VR (#5) — wait-and-reverify unchanged:** origin still WP fatal 500 on the Wave-2
re-probe (chronic-day 5 of ~28; escalation trigger not met). 2 junk rows preserved; no
mutation; re-probe on the next tail wave.

Floor projection: 31 → ~22 once the steer sweep drains (Reflector 3, Exit VR 2, Steer WP
`/internship` 1, plus Mundfish 10 pending origin recovery — Wave-2 target ~6 reachable
only if Mundfish's details recover).

### Wave-2 drain verification (2026-09-11, full default-loader pass; evidence `tmp/holdtail-wave2-20260910/`)

**The steer drain landed: overdue floor 31 → 21 (`overdueDelta: −10`), steer overdue 0.**
All 10 WP-attributed overdue rows (the `view.php?id=…` bamboo-provenance URLs) closed
`unavailable` with `source_absent`/`definitive` (origin `source_absent`) at 10:13Z, and
the WP `careers` + `/internship` rows re-verified drained in the same pass — matching the
plan's −10 projection (9 bamboo-provenance rows + the WP row's own identity). The
promoted bamboo row participates normally (this pass: `excluded/within_freshness_window`
post-validation cadence; the trusted zero stays banked from the validation pass), and the
health payload is clean end-to-end (`preservedBecauseSourceFailedCount: 0`, baseline
re-captured at 21 overdue / 8 sources). Remaining floor: Mundfish 10, Reflecto 3,
Exit VR 2, Big Moxi 2, Konami 1, Astrum 1, Inverge 1, SNK 1.

Two mechanism findings worth keeping (they correct the "sweep drains it" intuition from
step 5 above — right outcome, different lane):

1. **The drain lane is the finalize missing-universe path, not the availability sweep.**
   The finalize guard (`_lifecycle_missing_context` → `known_missing_evidence_sources`)
   only permits the `source_absent` drain when the run carries the **full registered-run
   universe** (default loaders: eligible + failed + skipped). Targeted `--only-sources`
   passes can therefore never drain absent-source rows — and the shadow sweep plan never
   closes anything by itself (`enforce_direct=False` records shadow results only).
   Production's direct-enforced bridge lane (`BALUFFO_AVAILABILITY_DIRECT_ENFORCE=1`,
   launcher flow) is real, but for this shape it could not close either: bamboo serves
   deleted postings as **200 redirects to the board list**, which the validator classifies
   `generic_redirect`/`ambiguous` — and ambiguous evidence deliberately never closes a row
   on one strike. For provider-migrated (tombstoned) sources, the honest drain is the
   full pass: the tombstoned WP row is provably un-re-observable, so its rows close
   `source_absent` without any direct check. **Operational rule: after any provider-twin
   promotion, run one full default pass to land the drain.**
2. **Bridge-lane direct-evidence commits require an in-process published feed
   generation.** A CLI/service harness invoking `JobAvailabilityService._commit_direct_evidence`
   outside the app fails in `reconcile_jobs_feed_availability` with "SQLite jobs feed
   authority has no published generation" (the reconcile is a same-process generation
   operation; the diagnostic is in-memory only — `record_storage_diagnostic` does not
   persist). No corruption risk: the commit path is transactional and rolled back cleanly
   in the harness. Consequence: availability drains are pipeline-finalize work; do not
   script bridge-service commits from tooling.

### Wave-3 Dayforce probe (2026-09-11, #3 Reflector): the CANDIDATEPORTAL XHR contract captured — adapter decision resolved

Executed with the repo's bundled Playwright Chromium (pool-identical launch profile;
request/response listeners — the pool's `fetch()` returns rendered HTML only). Evidence:
`tmp/holdtail-wave2-20260910/dayforce-findings.md` (+ `dayforce_capture.py`,
`dayforce_replay.py`, raw captures `dayforce-capture.json`/`dayforce-search-capture.json`).

Findings:

- The portal (`emplois.reflectorentertainment.com/l/en`) redirects to
  `jobs.dayforcehcm.com/en-CA/ref/CANDIDATEPORTAL` and renders the board **entirely from
  XHR** — the rendered listing HTML contains zero job links (why the static adapter reads
  an empty shell). The feed endpoint is
  `POST https://jobs.dayforcehcm.com/api/geo/{clientNamespace}/jobposting/search`
  with body `{clientNamespace, jobBoardCode: "CANDIDATEPORTAL", cultureCode,
  distanceUnit: 1, paginationStart: 0}` returning `{jobPostings[], maxCount, offset,
  count}` with full `jobDescription`, `jobPostingId`/`jobReqId`, `postingLocations[]`,
  `postingExpiryTimestampUTC`.
- The apparent bot wall is a **NextAuth CSRF pair**: replays with browser headers, fresh
  Playwright contexts, post-navigation cookies (incl. `cf_clearance`), and even in-page
  fetch without the SPA headers all 403; with the SPA's `x-csrf-token` header it 200s;
  and **plain httpx with a two-step contract** (`GET /api/auth/csrf` → token → POST with
  `x-csrf-token`) **returns all 3 live postings**. No browser, no Cloudflare clearance,
  no login. (`cf_clearance` unnecessary — Cloudflare runs detection-only for CSRF-pair
  API clients.)
- Live board 2026-09-11: 3 postings — Programmer Engine and Tools (161), Technical
  Animator Senior (149), VFX Artist Expert (115); cultures en-CA/fr-CA/fr-FR/en-US
  mirror with stable IDs. **The 3 overdue Reflector rows match none of them** — the
  thread's "1 REAL job" (Technical Director Studio) is stale (closest, id 149, is a
  different role); all 3 rows drain via the migration flow once the adapter lands.

**Decision (recorded in the iCIMS/Dayforce adapter thread): build the `dayforce`
structured adapter**, pilot Reflector (`clientNamespace: ref`), via the High-5/Steer
lane: stage `dayforce:client_namespace:ref` as `provider_migration_candidate` with
`migrationSourceIdentity` → the static twin, validate, promote, tombstone
`superseded_by_provider`, then one full default pass (Wave-2 drain rule). Every Dayforce
tenant is `{clientNamespace}/{careerSiteXRefCode}`-addressable with the same
two-request contract — the axol (SNK) deferral should be re-probed with the CSRF
two-step before calling the platform dead.

**Adapter built and Reflector migrated (2026-09-12, Wave-3 execution; evidence
`tmp/dayforce-wave/`):**

1. **The leaf** (`src/jobs/adapters/plugins/provider_api/dayforce.py`): the runner owns
   the two-request contract with a self-contained cookie-jar urllib client (the
   workday-CXS pattern — the shared GET-text `fetch_text` lane cannot execute the
   token+cookie POST pair). Empirical contract check on 2026-09-12 confirmed the
   cookie pair is load-bearing: POST with `x-csrf-token` + the same client's cookies
   → 200; the identical header without the cookies → 403 (NextAuth double-submit).
   The CSRF check is the only gate — `cf_clearance` and browser headers unnecessary.
   Row shape: `sourceJobId dayforce:{ns}:{jobPostingId}`, full `jobDescription`
   (HTML-entity-decoded) carried on the row with `_skipDetailFetch` (the SPA detail
   route serves an empty shell unauthenticated), `postingLocations` → city/country/
   summary, stable detail URL `…/CANDIDATEPORTAL/jobs/{jobPostingId}`, pagination via
   `paginationStart += count` until `offset+count >= maxCount` (10-page bound),
   empty boards classified `legit_empty` via the provider-empty path. Wired through
   the standard seams: `register.py` plugin (`dayforce_sources`), dispatch shim in
   `provider_api.py`, loader name + `SOURCE_REPORT_META`, and `dayforce` added to
   `PROVIDER_REGISTRY_ADAPTERS` (the staged-pending lane). Tests:
   `tests/test_provider_dayforce_adapter.py` (URL/namespace derivation, row
   normalization from the real capture, CSRF two-step request shapes, 403
   classification) + `tests/test_provider_dayforce_runner.py` (end-to-end over
   registry rows, pagination continuation, empty-board classification, expected
   error surfaces) via `tests/helpers/dayforce_fixtures.py`; 18 cases. Live
   validation against the real endpoint: **3/3 postings fetched and normalized**
   (161 Programmer Engine and Tools, 149 Technical Animator Senior, 115 VFX Artist
   Expert) — matches the Wave-3 capture.
2. **Staging** (`tmp/dayforce-wave/stage_reflector.py`, dry-run then `--apply`):
   staged `dayforce:client_namespace:ref` (listing_url
   `https://jobs.dayforcehcm.com/en-CA/ref/CANDIDATEPORTAL`) via
   `transition_registry_to_pending(provider_migration_candidate)` with
   `migrationSourceIdentity` → `static:listing_url:https://emplois.reflectorentertainment.com/l/en`.
   Pending 830 → 831, read-back OK.
3. **Validation** (`validate.log`): dayforce-family targeted pass with
   `--include-pending-provider-migration` — exit 0, 0 failed sources,
   **dayforce_sources ok fetched 3 / kept 3** with full Montréal attribution —
   the structured read the SPA shell could never produce.
4. **Promotion** (`promote_reflector.py`, mirrors promote_steer.py):
   `transition_registry_to_active` → active 2,167 → 2,167 (1:1 swap), pending
   831 → 830, static SPA-shell row tombstoned `superseded_by_provider`
   (tombstones 159 → 160, active bucket), `normalize_manual_promotion_rows: 1`,
   active seed 1,891 → 1,892 (+1 dayforce row). Full read-back OK.
5. **Drain — verified:** the 3 stale Reflector overdue rows
   (`directeurtrice-technique-studio`, `legal-notice`, `skip-to-content` — none
   matches a live posting) closed via the finalize missing-universe path on the
   following full default pass (Wave-2 drain rule: provider-migrated tombstoned
   sources are provably un-re-observable). The pass manifest carries **0 reflector
   work items** and the report's `availabilityHealth.overdueBySource` re-captured at
   **9 overdue / 6 sources** (Big Moxi 2, Midgar 2, Exit VR 2, Inverge 1, Konami 1,
   SNK 1 — exactly the predicted composition minus Reflecto). **Floor measured
   12 → 9**; the pass itself: 41,036 jobs, 70 failed (the usual transient tail).

### S4 activation executed (2026-09-11, #8 Astrum): listing-funnel coverage gap closed, junk row drained

The Astrum activation (flag-gated cookie-jar retry for its `bp_chl` geo-cookie
redirect round-trip) exposed and fixed an **S4 coverage gap with the same shape as
the S3 story**: the S4 lane only guarded `fetch_html_cached` (the traversal/detail
funnel), while the runner's two *listing* fetch funnels (`_fetch_listing_html_sync`
and its async twin in `static_listing_runner.py`) each had their own redirect
mini-chain that raised `Static redirect loop` via `_safe_redirect_url` before any
S4 logic. Production rides the async batched funnel (`httpx_async` strategy, batch
client `follow_redirects=False`), which is exactly where Astrum's 307 → same-URL
`set-cookie: bp_chl=…` bounce died in the first activation pass (loop error →
browser fallback → JS shell → 0 candidates → `needs_review`). Fix, additive and
gate-bound: the async funnel re-issues the identical request through the shared
batch client (its jar carries the bounce's `Set-Cookie` — transport-native, one
continuation); the sync funnel delegates to the S4 jar lane. Flag + allowlist
gate and exhausted-retry loop classification unchanged. Regression tests in
`tests/jobs_static/test_static_cookie_retry.py` (18 cases, +2 funnel cases: async
bounce-continue under flags, sync jar-lane delegation, flag-off loop preservation);
full static suite 388 passed; changed-mode gate exit 0.

Environment findings pinned during activation: this Windows machine's system
trust store fails urllib TLS to astrum-entertainment.ru (verify code 10 against a
fully valid served chain) while httpx/certifi verifies fine — production's
transport is httpx-async, so unaffected; the bridge `DirectLinkValidator` follows
redirects without a cookie jar, so the direct availability lane can never close a
geo-loop row (`max_redirects` exhaustion → `direct_unverified`); targeted
`--only-sources` passes set `using_default_loaders=False` and can never drain.
The drain landed via the sanctioned bypass for the incremental empty-source
window (`nextEligibleCheckAt` 12h after the TTL skip): `--force-refresh-all` full
universe with the S4 flags — Astrum ran, its listing no longer links `/en/legal`,
and the finalize source-eligible missing path closed the row. Run it detached:
the pass is 20+ minutes.

**Outcome:** junk row `astrum-entertainment.ru/en/legal` ("Legal") → `unavailable`
(`source_absent`/`definitive`); the real row (privacy-policy link source) stays
`available`; Astrum healthy again (`consecutiveFailures` 8 → 0, status ok,
kept 1). Floor read 22 immediately after — the +2 vs the 21 baseline is
**Midgar Studio**, an upstream `www.afjv.com` TLS hostname-mismatch break,
unrelated to S4 and expected to re-verify when afjv fixes its cert. S4 activation
remains operator-gated (both env vars): without them the code path is inert.

### S5 Mundfish repair executed (2026-09-11, #1): the 10-row block drains — floor 22 → 12

Wave 1 made Mundfish honestly `details_broken` but deliberately preserved
abort-on-first-failure semantics — the repair this wave needed came from live
re-evidence, and it was not a pipeline defect: **the site healed, then filtered
our header shape**. Re-probe (browser-headered httpx): every Wave-0 500 detail
URL now serves **200** behind a `302 → /en/<path>` locale redirect — identical
to control paths, so Wave-0's "detail-serving broken" was transient origin
downtime. But the first forced targeted pass still 500'd: Mundfish's edge bot
handling answers the production default header shape (bot `User-Agent: BaluffoJobsFetcher/1.0`
+ JSON-first `Accept`, no `Accept-Language`) with **HTTP 500** instead of a
403 — a header-shape filter, not content unavailability. Live discrimination:
bot-UA → 500; bot-UA + browser Accept → 500; browser-UA only → 500;
browser-UA + browser Accept + Accept-Language → **200**. The trio is load-bearing
down to the last header.

Fix (**S5**, continuing the numbered transport-gap series): new leaf
`src/jobs/common/browser_headers.py` — flag-gated, host-allowlisted browser
header profile (generic versionless Chrome UA + browser Accept + `Accept-Language`),
applied per-URL at transport header construction via a new `build_fetch_headers`
(both lanes wired: urllib `default_fetch_text` and httpx `async_fetch_text_httpx`;
stateless — first-attempt override, no retry lane needed). Gate conventions
mirror S4: `BALUFFO_BROWSER_HEADERS` truthy AND host on the comma-separated,
www-stripped `BALUFFO_BROWSER_HEADERS_HOSTS` allowlist — both required; without
the flag, every request's headers are byte-for-byte unchanged. Tests:
`tests/jobs_static/test_browser_header_profile.py` (8 cases: gate matrix,
profile application without input mutation, transport wiring both lanes with
captured headers, default invariance, trio shape pin); 396 static tests green;
changed-mode gate exit 0 (after re-pointing the new test at leaf-module imports —
the broad-barrel guardrail correctly rejected `from src.jobs.common import …`).

Activation: targeted forced pass (`--ignore-circuit-breaker` — breaker was at 9
consecutive `details_broken` failures) with the S5 flags → **Mundfish ok, 32/32
kept, no error**, breaker freed. 6 of 9 real roles re-verified `available`
immediately (floor 22 → 16); the other 3 (`lead-technical-ui-designer`,
`middle-keyframe-animator`, `hr-manager`) rotated off the live board (visible
link set = exactly the 6), plus the `/legal` junk row — all 4 closed
`source_absent`/`definitive` via the finalize missing path on a detached
`--force-refresh-all` full-universe pass (~35 min). **Mundfish overdue 0; floor
22 → 12.** The `details_broken` Wave-1 accounting stays intact as the regression
net — it was the right diagnosis of a genuinely broken window; this repair is
the recovery, not a reclassification.

### S7 executed (2026-09-12, #7 Konami): the stale-detail demotion made reachable end-to-end — promotion verified live

**Design as recorded vs. what the trace found.** The §7 adjudication's path (a) said
"extend the demotion to the plugin empty-result path" — but the instrumented pipeline
trace (`tmp/konami-s7-pipeline-spy.py`) found Konami rides the **generic** funnel, and
the real blockers were three, not one:

1. **Generic finish gate** (`_finish_generic_source`): the zero-kept guard chance
   required an empty classification AND zero dead-listing rejections — extraction
   pre-stamped `dead_listing_page` (58 rejections on the nav-only board), so the guard
   was unreachable. Fixed: the gate defers to the guard's own refusals (they encode
   every case the old pre-filter excluded, fail-closed), and the widened-gate decline
   path preserves the pre-S7 outcome byte-for-byte (no error stamp on shapes the old
   gate excluded from the error path; the dead-listing re-stamp still applies on
   decline, and a promoted `empty_confirmed` stamp is protected from the re-stamp).
2. **Canonical-URL dead end in the guard's marker probe** (`_fetch_listing_bodies`):
   the cache-backed fetcher canonicalizes URLs (`normalize_url` strips trailing
   slashes) and Konami's host 404s the slashless form — the probe could never re-read
   a listing extraction itself had just fetched via raw-URL `fetch_text`. Fixed: a
   single raw-URL `fetch_text` fallback per page, keeping the all-or-nothing refusal
   (no live-200 read → no promotion).
3. **`browser_fallback_attempted` refusal**: with the canonicalized 404, the runner
   attempts a listing browser fallback on every Konami read — the attempt counter
   alone shadowed all evidence. Fixed: the refusal now relaxes only under the
   stale-detail demotion shape, which requires the demotable 404 line to exist at all;
   promotion still requires emptiness evidence on freshly re-read bodies. The Big Moxi
   JS-shell shape (fallbacks, no demotable line, no marker, broken prior bucket) keeps
   refusing — pinned by test (`test_guard_browser_fallback_with_demotable_404_but_no_marker_declines`).

The plugin funnel got the same guard-first ordering fix (the original §7 finding was
real for that funnel too) — the dead-listing short-circuit no longer shadows the guard;
the promote-first/fall-through shape preserves every prior outcome on decline.

**Live verification** (`tmp/konami-s7-pass2/`, targeted forced pass): the guard was
reached with zero refusals, the raw-URL fallback read the real 86,844-char listing,
`no_openings_marker` evidence fired, the stale 404 line was demoted (no
`AdapterValidationError`), and the source state landed **`lastStatus: ok`,
`lastFailureBucket: no_openings`, `consecutiveZeroKept: 1`, `lastError: None`** — the
promoted empty-confirmed stamp end-to-end. The next full default pass retires the junk
row via the missing-universe drain (expected floor 9 → 8).

**Tests:** 9 new cases across `test_zero_kept_guard_plugin_demotion.py` (6: promote
shape, decline byte-parity, challenge-403 refusal, no-evidence refusal, recompute
survival) and `test_zero_kept_guard_generic_demotion.py` (generic promote through the
widened gate, decline byte-parity, browser-refusal pin, canonical-404 fallback, all-or-
nothing fallback failure, marker-free JS-shell pin). 455 static/fetcher/dayforce tests
green; changed-mode gate exit 0.

### S6 executed (2026-09-12, #1 Big Moxi): rendered-empty confirmations became admissible guard evidence — promotion verified live

The §4 adjudication's path (1), built as specified: the browser-fallback lane now stamps
emptiness evidence, per-source state persists it across passes, and the guard accepts two
confirmations as a third emptiness evidence kind (alongside `no_openings_marker` and
`prior_clean_zero_read`).

- **Producer** (`static_listing_runner._try_playwright_fallback`, one stamp per run —
  the ×3 fallback pool must never fabricate a ×2 basis from a single pass): a Playwright
  render that `detect_js_shell` matches, does **not** match the new public
  `detect_cookie_challenge_shell` (challenge interstitials are JS shells too — a bot-wall
  render must never fabricate emptiness), and surfaces ≤240 visible text chars stamps
  `stats["renderedEmptyConfirmedAt"]`. The stamp rides the detail entry's stats, the same
  carrier `apply_static_detail_stats` already reads.
- **Persistence** (`state_source_records.apply_rendered_empty_state`, wired before the
  status appliers so an errored run can't erase accumulated evidence):
  `renderedEmptyConfirmationsAt` bounded list (cap 8) on the per-detail state row,
  whitelisted in `normalize_source_state_payload`, appended per run (same-timestamp idem-
  potent), cleared when the source keeps >0 jobs — two stamps with a success in between
  can never satisfy the guard.
- **Guard** (`static_zero_kept_guard._two_rendered_empty_confirmations`): two distinct
  stamps on `ctx.state_entry` relax exactly the `browser_fallback_attempted` and
  `js_required`-diagnosis refusals (S7's demotion relaxation stays a separate, narrower
  lane). Every other refusal still fires; promotion still requires 0 kept, no demotable
  stale line, and the S2-preserved rows contract.

**Live verification** (spy pass 1 isolated → pass 2 against the same output dir so state
accumulates): pass 1 stamped `2026-09-12T19:30:16Z` on the real render (js shell, no
challenge), pass 2 grew the list to 2, pass 3's guard accepted the basis and promoted —
Big Moxi's state row landed **`lastStatus: ok`, `lastFailureBucket: no_openings`,
`consecutiveZeroKept: 1`, `lastError: None`** (exit 2 is the pipeline's normal empty-
board code). A full default pass with the source eligible drains the 2 rows via the
missing-universe path (floor 9 → 6 when it lands alongside the S7 Konami drain:
Big Moxi 2 + Konami 1). One deliberate gap
(vs. the §4 manual probes): the automation lane stamps on the ~1.1KB shell it actually
sees and has no independent console/DOM-growth check — accepted because the guard lane
carries the same zero-kept discipline as `no_openings_marker` evidence, and an operator
probe remains the escalation path if the promoted board ever misbehaves.

**Tests:** 10 new cases in `test_rendered_empty_confirmations.py` (heuristic wrapper,
producer stamp/fail-closed shapes, persistence round-trip and clear, guard promote-on-
two, single-confirmation and challenge-shell fail-closed pins). 427 static + 38
fetcher/dayforce tests green; changed-mode gate exit 0.

### Drains verified (2026-09-12/13, forced passes; evidence `tmp/holdtail-drain-20260912/`): floor 9 → 6, exactly the projection

The S7 Konami drain (−1) and the S6 Big Moxi drain (−2) both landed through the finalize
missing-universe path, with the health verdict healthy throughout.

- **Konami (pass B, forced full universe, 1,962 sources):** the S7-promoted source read
  `ok fetched 0 / kept 0` (no `AdapterValidationError`, weeks after the 404-abort shape),
  detail row banked zero #14, and the junk "Community" row (`/pages/sns_account`)
  drained `likely_removed` with `removedAt` stamped. State: source `ok`/`no_openings`/
  `consecutiveZeroKept: 1`; floor 9 → 8 (delta −1).
- **Big Moxi (stamps 1–2 targeted → pass D promote+drain):** pass B's under-load renders
  returned empty (`got_html=False`) so no stamp landed, and its error pushed
  `consecutiveFailures` to 15 → the breaker quarantined the source, benching it from
  pass C entirely (universe 1,962 vs 1,969). The ×2 was then driven in live state by two
  **targeted** stamp passes (`--force-refresh-all --ignore-circuit-breaker --only-sources
  …`, no `--output-dir` — omitting it is what routes state to live `data/`; the Wave-2b
  runs' isolated dirs are why their stamps never reached live state) and pass D — full
  forced universe with `--ignore-circuit-breaker` — promoted on the ×2 (`ok fetched 0 /
  kept 0`), banked its own stamp as #3, and both rows (`/careers/unreal-programmer`,
  `/careers/Game-Systems-Engineer`) drained `likely_removed`/`removedAt`. State: both
  rows `ok`/`no_openings`/`consecutiveZeroKept: 1`; floor 8 → 6 (delta −2).
- **Operational gotchas for the next drain campaign:** (1) a *regular* full pass
  excludes error-state targets via `cache_within_freshness_window` (the incremental
  scheduler benches them despite error status) — forced full-universe passes remain the
  drain lane; (2) a forced-pass failure on a near-threshold source triggers the breaker
  on the *next* pass (quarantine `circuit_breaker_active_until`) — the sanctioned bypass
  is `--ignore-circuit-breaker` (S5 Mundfish precedent); (3) targeted passes write full
  stub state rows for untouched sources when run without `--output-dir` — harmless
  because the next full pass rewrites every row (verified: all 4,993 rows carry the
  drain pass's finish stamp), but two targeted runs in a row should not be treated as a
  substitute for the closing full pass.
- **Remaining floor (6):** Midgar 2 (afjv TLS upstream), Exit VR 2 (WP 500, chronic-day
  wait-and-reverify), Inverge 1 (mid-rebuild board), SNK 1 (tenant gone, record-only) —
  all blocked on external triggers, as projected. The hold-tail repair work is complete.

## Risks and rollback

- **Trusted-zero fabrication** (the one way this plan could destroy real rows): mitigated
  by S2 before any drain that leans on a zero read, the guard's two-path design, and the
  rule that no drain fires without the guard's trusted evidence.
- **Steer twin staging** is the only promotion in this plan; it reuses the twice-executed
  bamboo lane (High-5, 9-row wave) with read-back verification. Rollback: tombstone is
  reversible via the sanctioned flow; the WP row's seed entry is untouched.
- **S4's retry could mask real loops** on other sources: flag-gated, allowlisted, loop
  classification unchanged when the retry fails.
- **Quarantine re-entry** between waves is expected and harmless (rows preserve); waves
  should be adjudicated per source with `--ignore-circuit-breaker` targeted runs, not by
  disabling the breaker globally.
