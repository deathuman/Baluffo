# Changelog

> All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and Baluffo desktop releases use the project-specific `0.1.x` ordering documented in
[`RELEASE.md`](RELEASE.md).

---

## [Unreleased]
### Added

- **Bandai Namco Studios' 10 hidden SOL-AVES roles enter the pipeline via a category-filtered hrmos registry window (2026-09-14, evidence `tmp/bandai-trace-20260914/`).** The root-cause trace of the careers-page funnel (same day) proved the studio's engineering-family postings are dropped by two independent mechanisms — the language-switch title classifier flagging real CJK titles with ASCII parentheses (`家庭用ゲームエンジニア (新規格闘アクション)`) as UI artifacts, then a budget death guaranteeing the demoted candidates never convert — while the careers page also links a category-filtered hrmos listing (`jobs?category=…`, its "エンジニア" button) that no registered window follows. Rather than patching the classifier inline, the 10-role family now has its own first-class source: a new registry row `static:listing_url:https://hrmos.co/pages/bandainamcostudios/jobs?category=2226903550201643009` written via the sanctioned runtime+seed mutation (`add_category_window.py`, provenance-stamped, `save_registry_state_atomic`, jawltd/joybits + Cygames registry-window precedents; runtime 2,167 → 2,168, seed 1,892 → 1,893). The row rides the hrmos plugin fast path (identity = netloc, unaffected by query params), which has no provisional-title gate — sidestepping the classifier bug entirely — and a pre-flight parse of the live listing confirmed all 10 titles are clean (`【ゲームエンジン】…`, no ASCII-paren shape). Live verification: targeted pass on the new source **10 output rows, all 10 SOL-AVES roles, `ok`/`healthy`/100% kept in 860ms** (single listing fetch, no detail fanout), all 10 net-new to the store (zero overlap with existing rows); the rows enter the production feed on the next regular pass. The classifier fix itself (narrow the CJK-in-parens fallback + provisional-row card-fallback parity, blast radius gracklehq ×12 + greenhouse ×8) remains recorded as follow-up work in the trace report.

- **Static pagination follow: boards that server-paginate now sync their full listing window — Cygames' 70% hrmos blind spot closed live (338/338 postings, was 100; 2026-09-14 hrmos coverage-evaluation follow-up, evidence `tmp/hrmos-pagination-20260914/`).** The hrmos evaluation (measure-first, axol precedent) found the platform fully server-rendered with `?page=N` anchors right in the listing HTML, and Cygames advertising 338 postings while the static lane's single-page window rendered only 100 — the source read "healthy, kept 100%" because nothing measured the fetch window. The anchor's presence is the "board advertises more" signal (deliberately no facet-count heuristics — the hrmos probe measured overlapping facet sums inflating board universes). New leaf `src/jobs/adapters/static_listing_pagination.py` extracts same-listing `?page=N` anchors (same host, same path, non-`page` query params equal order-insensitively, digit page values ≥2 — page 1 is the canonical registry window under either spelling, so following never goes backwards) with a run-level 4-page discovery bound shared across chained discoveries and a `BALUFFO_STATIC_PAGINATION_FOLLOW` kill switch (default on). Wired into both lanes: the generic `StaticFetchRunner` queues discovered pages behind the registry window within the same run and source budget, queued before the listing-fingerprint skip so a changed board still syncs its full window (steady-state unchanged-fingerprint passes pay nothing extra); the shared plugin runner (`plugins/static/_runner.py`) follows the same anchors for every `run_simple_static_plugin` board — the live hrmos path, since Cygames rides the hrmos plugin fast path rather than the generic funnel (first targeted pass proved the generic wiring alone was a no-op for it) — with best-effort continuation fetches so a failing page 2 never drops page-1 rows. Live verification: targeted Cygames pass went 100 → **338 output jobs, exactly the proved universe, 338 distinct links, zero pager-link junk rows**, at +2.7s source cost. **Registry-window follow-up (same day)**: the plugin runner now honors the full registry `pages` list as seed windows (first-seed failure aborts as before; later seeds skip best-effort so one dead page never zeroes the harvest; the kill switch gates only anchor *discovery* — a configured multi-page window stands alone), and the Cygames row's `pages` list now carries `?page=2..4` via the sanctioned runtime+seed registry mutation (`tmp/hrmos-pagination-20260914/update_cygames_window.py`, provenance-stamped, `save_registry_state_atomic`, jawltd/joybits precedent) — making the coverage durable under the kill switch or anchor-markup drift. Verified: normal pass 338 **and** a `BALUFFO_STATIC_PAGINATION_FOLLOW=0` pass still 338 (config-only), 338 distinct rows, zero junk rows. Tests: 16 new in `tests/jobs_static/test_static_pagination_follow.py` (anchor units incl. real-shape/param-order/page-1/foreign-host guards, runner follow/cap/kill-switch/self-loop/fingerprint funnels, plugin follow/best-effort/kill-switch) plus 4 registry-window follow-ups (multi-seed window, best-effort later seed, kill-switch keeps configured windows, first-seed abort); full suite 5,107 passed / 2 skipped, zero failures; changed-mode gate exit 0.

- **Fully-parseable static listing pages no longer escalate to Playwright: the js_shell oracle consults the rendered-card extractor before rendering, eliminating the per-page render bill the PS board paid after its Nuxt flip (2026-09-15, evidence `tmp/card-lane-render-skip-20260915/`).** After the flip, every listing page is a JS shell for the generic parser (0 rows) while the card lane extracts all 62 rows from the same static document — so the escalation oracle (`parse_jobpostings_from_html` == 0) fired on all 7 pages and each page gambled on a render it did not need. Fix in the generic runner's `_try_playwright_fallback` only: when the generic parse is empty, a new `_card_lane_parses_static_html` oracle runs the same extractor the card-row flow runs next with the same config (`allow_any_anchor=True`, real company/source id), and a static document the lane already parses is kept as-is — a skip can never starve the downstream lane of rows it would find. The oracle is pure parsing (no network) and fails open in both layers (a crash inside the oracle method and a crash at the call site both fall back to the legacy render path — pinned separately); kill switch `BALUFFO_STATIC_CARD_LANE_RENDER_SKIP` (default on). Plugin-lane escalations are untouched (per-plugin `playwright_on_js_shell` opt-ins). Live verification: the PS targeted pass logged **zero `playwright_fallback_used` events** (the previous day's identical pass escalated every page) and kept the identical universe (`fetched=421 kept=421` in 175s); store closure probe: **300/300 live postings covered by active rows** store-wide over the board's real root + `/page/2../page/6` surfaces with both link dialects matched (the probe's two regex corrections — `/jobs/<numeric>` greenhouse rows; the canonical `.gz` store — are documented in the evidence dir). Tests: 6 new in `tests/jobs_static/test_card_lane_render_skip.py` (skip pin, escalate-when-nothing-parses, generic-parse early-return untouched, kill switch, fail-open at the call site, downstream-config parity spy); static-lane directory 499 green.
- **Detail-flow budget propagation repaired: the card-row detail verification now anchors at the effective source budget (floor + pagination extension), not the payload's base — recovering PlayStation from 132 to 421 kept postings after the board's Nuxt 3 flip (2026-09-15, evidence `tmp/ps-flip-20260915/`).** The board flipped to a Nuxt 3 SSR variant (mid-path `/​{slug}/job/{id}` link shape with raw CJK slugs, SPA tokens tripping `detect_js_shell`, greenhouse signature on every page), which exposed a latent choke point: `parse_jobpostings_from_html` returns 0 on the new DOM, so every listing page escalates to a Playwright render (the render is healthy — a pool-identical probe extracts all 62 cards), the card extractor then yields 62 rows/page, but each row's detail verification (`process_detail_link`) anchors its per-detail deadline at `source_started + budget` where `budget` was the payload's **base** 25s — after the run's 25th second every verification raises `TimeoutError`, the expected-fallback catch aborts that page's card loop after ~a dozen rows (85 appends from 409 extracted rows), and the detail traversal starves with it (16 verifications, 2 shell strikes from external studio hosts). Fix in `_listing_result_context` only: hand the detail flow `ctx.effective_source_budget_s()` so the adaptive budget reaches every per-detail deadline anchor; the pagination-extension test file gains the effective-budget pin (13 total). Live verification: `fetched=421 kept=421` in 161s (was kept-132 in 43.5s; the 421 vs 409-extracted delta is the detail traversal's nested links), lifecycle active 289 → 292, and the store closes the board exactly: **325/325 live postings covered, each with an active row** (greenhouse-embedded links re-homed to their canonical `job-boards.greenhouse.io` rows by the existing cross-host dedup — store-wide id coverage, not just the PS source's own rows).
- **The per-source time budget now scales with discovered pagination pages: each discovered continuation page earns one extra base-budget unit (capped by the same page count the pagination follow allows), so multi-page boards finish inside one pass instead of truncating against the pre-pagination budget (2026-09-15, evidence `tmp/fullpass-pathdialect-20260915/`).** The full-pass exercise surfaced the second half of the oscillation defect: the default 25s budget predates pagination-follow, so 7-page PlayStation kept 117/442 and 3-page nexon kept 4/51, and the truncated universes fed the lifecycle drain. The preserve shield (previous bullet) stops the drain damage; this change removes the truncation itself. Design: `StaticSourceContext` tracks `pagination_budget_floor_s` (min of synced per-page/domain budgets — byte-equivalent to the legacy min-of-deadlines semantics) and `pagination_budget_extension_s`, rebuilt as `started + floor + extension` via `_rebuild_source_deadline`; `sync_source_deadline` keeps its tighten-only min semantics over the floor while the extension raises the ceiling, and `extend_source_deadline_for_pagination` is idempotent per discovered-page count. The generic lane's `_queue_discovered_pagination_pages` bumps the extension per newly discovered page (discovery already gated by `BALUFFO_STATIC_PAGINATION_FOLLOW`; the extension has its own kill switch `BALUFFO_STATIC_PAGINATION_BUDGET_EXTENSION`, default on). `stop_for_budget_exhaustion` now reports the truthful effective budget (`floor + extension`) instead of the base config number. Calibration: PlayStation (7 pages, 441 details, ~141s) → 25 + 6×25 = 175s suffices; nexon (3 pages, ~27s) → 75s. Live verification under the default 25s budget (no env override): PS followed all 7 pages with **zero budget stops**, completed in 43.5s (the previous 25s wall truncated at ~4 pages), store preserved at 289 active; nexon's listing pages all ran budget-free (kept 34 ≈ the day's live board; the plugin lane enforces per-detail budget anchors, not listing ones — the 51-keep repair pass earlier the same day simply ran under a 300s override), lifecycle intact at 17 active. Plugin-lane note: sheet-plugin sources (nexon) enforce per-detail budgets (`source_started=now, 2×timeout`) and run listing pagination budget-free, so their listing phase is not budget-truncated — nexon's 34-kept matches its live board; the generic lane carries the scaling. Same-run observed PS board flip (Nuxt SSR variant, greenhouse signature on every page, kept 132 vs the repair pass's 441) is a board-side shape change for extraction adjudication, not a budget effect. Tests: 12 new in `tests/jobs_static/test_static_pagination_budget_extension.py` (extension math incl. operator-budget scaling and the 6-page cap, kill switch, floor/extension deadline semantics with byte-equivalence pin, idempotency, runner wiring, capped runner run, no-pagination no-op, wall-clock anchoring); static-lane directory 492 green.
- **Budget-truncated partial runs no longer mark their unfetched tail missing: lifecycle missing-evidence now shields `status=ok, kept>0` runs whose error text carries the time-budget-exhaustion marker (full-pass path-dialect exercise, 2026-09-15, evidence `tmp/fullpass-pathdialect-20260915/`).** The first full default pass over the pagination-expanded universe surfaced an oscillation defect: PlayStation (110→316 and nexon (16→21 active rows) collapsed to 67 and 1 under the *default* 25s per-source budget — the pagination follow now fetches 7- and 3-page boards, which no longer fit the pre-pagination budget, the truncated run kept a prefix (kept 117/4), and the lifecycle drain marked the un-fetched tail `likely_removed`/`archived` because `_source_report_missing_evidence_kind` classified every `status=ok, keptCount>0` run as missing-eligible. The evidence was already in the report row: the runtime stamps `classification="timeout"` + budget text into the entry report even on partial-keep runs, and the marker survives aggregation inside the `error` string (`src/fetch_incremental_sanity_benchmark.py` already parses both spellings). Fix in `state_lifecycle_availability.py` only: `_source_report_was_truncated_by_budget` checks the error text for `time budget exceeded` / `time_budget_exceeded`, and a truncated partial run returns `"skipped"` (preserve) instead of `"eligible"` (drain) — zero-kept runs keep the existing broken-evidence path, clean runs stay eligible, and the failed-source shield's other drains are untouched. Blast radius from that one pass's log: **30 sources** kept budget-truncated partial universes — all now protected. Recovery: both victims repaired via targeted 300s-budget passes (nexon kept 51, active 1→17 via the reappearance path; PS kept 441 over all 7 pages in 141s, active → 289); the two full-pass drains were fully resurrected, no data loss. Tests: 4 new in `tests/test_jobs_lifecycle_source_evidence.py` (partial-truncated shield, clean-run eligibility, zero-kept truncation, snake-case marker); targeted lifecycle families 16+19+59 green.
- **Post-pass active-junk monitor: the pipeline report's `lifecycleSummary` now carries `activeJunkClassRowCount` — the zero-active-stock invariant is checked automatically on every pass (2026-09-14 widget survey follow-up, evidence `tmp/widget-harvest-audit-20260914/`).** The survey (post-`88a454dc` tooling, all 1,582 static sources' rows classified by target host vs. registry identity) found the LinkedIn guest-view harvest class effectively extinct — zero active predicate-class rows, no re-accumulation since the guard landed (938 `firstSeenAt >= 09-13` rows are create-and-close artifacts of pre-fix stock exiting, all terminal), and a healthy pipeline's invariant is simply "zero active junk-class rows." The monitor pins that invariant in code: `count_active_junk_class_rows` in the origin-junk leaf counts lifecycle entries that are both `active` and junk-class (LinkedIn guest-view shapes + the nav-anchor self-page shape, origin-aware, fail-open on unidentifiable sources) with cheap prefilters (status, static-source id, LinkedIn-substring-or-titled) keeping the ~122k-entry scan at ~0.2s; the orchestration stamps the count into the summary after all transitions, and it reaches the wire through the finalize payload and the contracts `lifecycleSummary` allowlist. Deliberately kill-switch-independent: the count uses the shape logic (`is_junk_class_row`) without the `BALUFFO_GUEST_JUNK_GUARD` gate, so a disabled guard cannot blind the monitor. The monitor's wider net immediately beat the survey's own query (which prefiltered on "linkedin in url" and missed the nav-anchor shape): it surfaced **four stranded "JOBS" nav-anchor self-page rows** (metricminds, funkitron, thegamekitchen, mightygamesgroup — active since 2026-05-22 on chronically zero-kept sources, health `broken`) plus the known projectwhitecard share-article row — all on the established drain paths (source zero-kept banking / shield), needing no new code. Tests: 9 new (monitor units incl. kill-switch independence and both id spellings, summary flag on skipped-source stock, self-clearing flag after drain, contracts passthrough); full suite 5,087 passed / 2 skipped; changed-mode gate exit 0.

- **External-trigger re-verify sweep for the remaining floor of 6 (hold-tail repair plan monitoring leaf, 2026-09-13, tool + baseline `tmp/external-trigger-sweep/`).** One read-only probe per upstream trigger the plan's dispositions wait on, each appending a JSON verdict record to `history.jsonl` for diffable recovery evidence: Midgar afjv TLS handshake (RECOVERED ⇒ re-verify lane), Exit VR WP 500 on `/jobs/` (any non-500 ⇒ RECOVERED; the record carries the §5 chronic-day counter, outage start 2026-09-06, escalation day ~28), Inverge's five SEE OFFER detail URLs (any 200 ⇒ RECOVERED), and SNK's axol tenant under every product-line prefix via the server-rendered `/job/list` shape (any non-404 ⇒ RECOVERED). Baseline verdicts all match the plan's known state — TLS mismatch unchanged, 500 with chronic day 7, five 404s, 9-byte 404s — zero recovered; on any future RECOVERED: re-adjudicate, then the targeted `--only-sources` pass or the §5 tombstone lane when Exit VR crosses day ~28.
- **Hold-tail drains verified live: floor 9 → 6 (Δ −3, healthy throughout) — the S7 Konami and S6 Big Moxi promotions both landed their missing-universe drains on forced full passes (hold-tail repair plan drain-verification record, 2026-09-12/13, evidence `tmp/holdtail-drain-20260912/`).** Konami (pass B, forced full universe): the S7-promoted source read `ok fetched 0 / kept 0` — no `AdapterValidationError` weeks after the 404-abort shape — detail row banked zero #14, and the junk "Community" row drained `likely_removed`/`removedAt`; state `ok`/`no_openings`/`consecutiveZeroKept: 1`, floor 9 → 8. Big Moxi: pass B's under-load renders returned empty (no stamp) and its failure triggered the circuit breaker (quarantine benched it from pass C entirely), so the ×2 was driven in **live state** by two targeted stamp passes (`--force-refresh-all --ignore-circuit-breaker --only-sources …` with no `--output-dir` — omitting it is what routes state to live `data/`; the Wave-2b runs' isolated dirs are why their stamps never reached live state), then pass D — full forced universe with `--ignore-circuit-breaker` — promoted on the ×2, banked its own stamp as #3, and both rows (`/careers/unreal-programmer`, `/careers/Game-Systems-Engineer`) drained `likely_removed`/`removedAt`; state `ok`/`no_openings` on both rows, floor 8 → 6. Operational gotchas recorded: a regular full pass excludes error-state targets via `cache_within_freshness_window` (forced full-universe passes remain the drain lane); a forced-pass failure on a near-threshold source triggers the breaker on the *next* pass (`--ignore-circuit-breaker` is the sanctioned bypass, S5 precedent); targeted no-`--output-dir` runs write stub state rows for untouched sources — harmless because the closing full pass rewrites every row (verified: all 4,993 rows carry the drain pass's finish stamp). Remaining floor (6): Midgar 2 (afjv TLS upstream), Exit VR 2 (WP 500 chronic wait), Inverge 1 (mid-rebuild), SNK 1 (tenant gone) — all external-trigger-blocked, as projected; the hold-tail repair plan's execution work is complete.
- **Axol adapter sketch from the re-probe's platform facts; build decision: no-build for marv alone (hold-tail repair plan §9 record, 2026-09-12, evidence `tmp/axol-adapter-sketch/`).** Probing the live marv tenant for the sketch corrected the platform contract once more: `fb_csrf` guards the **search-filter form**, not the listing — job reads are plain GETs with no token, POST, or cookies (fresh no-cookie client verified). URL map: `/job/list` server-renders the full offer list with all detail URLs (`/job/detail/{encid}`, 29 hrefs on marv), `/job/search` is the paginated card view (`?page=N&searchKey=…` — pagination must follow rendered hrefs, the key is server-issued), details are fully server-rendered (~8.8 KB, `job__offer__detail__*` label/value blocks); row selectors `jsAxolJob_box`/`jsAxolJob_title`/`job_encid`. The sketch, if ever built, is an ordinary HTML plugin-family adapter (row id `axol:{tenant}:{encid}`, details through the static traversal — no `_skipDetailFetch`, unlike Dayforce). **No-build rationale:** marv is the only axol tenant in the registry (pending 830 and tombstones: zero axol rows) and is **already active and healthy on the static lane** (`ok`/`lastKeptCount: 38`/`consecutiveFailures: 0` on today's full pass, reading the same server-rendered HTML an adapter would) — a structured migration would be a re-registration with identical output: zero yield delta, nonzero migration risk. Revisit triggers recorded: a second axol tenant in discovery, marv's static lane degrading, or an axol SPA migration (then the S6 rendered-empty lane becomes the reading lane and `/job/list` is the re-entry point). Also retro-closes the §9 "no axol adapter" gap framing: an adapter was never the missing piece for SNK (tenant gone), and the static adapter reads every live axol board fine.
- **S6 executed: rendered-empty confirmations are now admissible zero-kept-guard evidence — Big Moxi's board promotes, the 2 rows can drain on the next full pass (hold-tail repair plan §4/S6 record; expected floor 9 → 6 when it lands alongside the S7 Konami drain).** The Wave-2b adjudication's path (1), built as specified. Producer (`static_listing_runner._try_playwright_fallback`, **one stamp per run** — the ×3 fallback pool must never fabricate a ×2 basis from a single pass): a Playwright render that `detect_js_shell` matches, does **not** match the new public `detect_cookie_challenge_shell` (challenge interstitials are JS shells too — a bot-wall render must never fabricate emptiness), and surfaces ≤240 visible text chars stamps `stats["renderedEmptyConfirmedAt"]` on the detail entry. Persistence (`state_source_records.apply_rendered_empty_state`, wired before the status appliers so an errored run can't erase accumulated evidence): `renderedEmptyConfirmationsAt` bounded list (cap 8) on the per-detail state row, whitelisted in `normalize_source_state_payload`, appended per run (same-timestamp idempotent), cleared when the source keeps >0 jobs — two stamps with a success in between can never satisfy the guard. Guard (`static_zero_kept_guard._two_rendered_empty_confirmations`): two distinct stamps on `ctx.state_entry` relax exactly the `browser_fallback_attempted` and `js_required`-diagnosis refusals (S7's stale-detail demotion relaxation stays a separate, narrower lane; every other refusal still fires). Live verification (spy pass 1 isolated → pass 2 against the same output dir so state accumulates): pass 1 stamped on the real render (js shell, no challenge), pass 2 grew the list to 2, pass 3's guard accepted the basis and promoted — Big Moxi's state row landed `lastStatus: ok` / `lastFailureBucket: no_openings` / `consecutiveZeroKept: 1` / `lastError: None`. One deliberate gap vs. the §4 manual probes (recorded in the plan): the automation lane stamps on the ~1.1KB shell it actually sees and has no independent console/DOM-growth check — accepted because the lane carries the same zero-kept discipline as `no_openings_marker` evidence. Tests: `tests/jobs_static/test_rendered_empty_confirmations.py` (10 cases: heuristic wrapper, producer stamp/fail-closed shapes, persistence round-trip and clear, guard promote-on-two, single-confirmation and challenge-shell fail-closed pins); 427 static + 38 fetcher/dayforce tests green; changed-mode gate exit 0.
- **SNK axol re-probed with the dayforce CSRF two-step; record-only disposition stands — the tenant is dead, the CSRF hypothesis empirically closed (hold-tail repair plan §9, 2026-09-12, evidence `tmp/snk-axol-reprobe/`).** The platform's real CSRF surface is a form token, not NextAuth: the live marv board (`/qd/c/marv/job/search`) embeds an `fb_csrf` hidden input and the GET→POST same-session round-trip answers 200 — the two-step mechanism works on axol, so a token gate never hid SNK. SNK's tenant 404s under every product-line prefix (`/pm/`, `/qd/`, `/bx/`, `/jn/`, `/vb/` × `snk-corp`/`snk`), the subdomain form doesn't resolve, and — decisive — SNK's own live recruit page still links four axol category URLs (`/pm/c/snk-corp/public/job/category/{token}`, a deeper shape no prior probe tried) and all four 404: the site links a tenant the platform no longer serves. No replacement board (mynavi/Twitter only). Durable platform facts for any future axol adapter: product-line URL prefix (`/pm/`, `/qd/`, …) + `/c/{tenant}`, form-token CSRF, tenant-scoped `axol_{prefix}_entry{tenant}` + `ROUTE_SESSIONID` cookies. The 1 overdue junk row's drain lane is unchanged (finalize missing path on a full pass with the source eligible).
- **S7 executed: the zero-kept guard's stale-detail demotion made reachable end-to-end — Konami's trusted-empty board promotes, the junk row can drain on the next full pass (hold-tail repair plan §7/S7 record; expected floor 9 → 8).** The instrumented pipeline trace (`tmp/konami-s7-pipeline-spy.py`) found the §7 adjudication's plugin-funnel finding was real but not the production blocker: Konami rides the **generic** funnel, where three layered blockers hid the guard — (1) `_finish_generic_source`'s zero-kept gate required an empty classification AND zero dead-listing rejections while extraction pre-stamped `dead_listing_page` (58 rejections on the nav-only board); (2) the guard's marker probe (`_fetch_listing_bodies`) re-fetched through the cache-backed fetcher whose `normalize_url` strips trailing slashes — Konami's host 404s the slashless canonical form the extraction itself never used (raw-URL fetch_text works: 86,844 chars with the marker); (3) with that canonicalized 404 the runner attempts a listing browser fallback on every read and the `browser_fallback_attempted` refusal shadowed all evidence. Fixes: the generic gate now defers to the guard's own refusals (fail-closed — they encode every case the old pre-filter excluded) with a byte-parity decline path and promoted-stamp protection against the post-taxonomy `dead_listing_page` re-stamp; the marker probe gained a single raw-URL `fetch_text` fallback per page keeping the all-or-nothing refusal; the browser-attempt refusal relaxes **only** under the stale-detail demotion shape (which requires the demotable 404 line to exist at all) — the Big Moxi JS-shell shape keeps refusing, pinned by `test_guard_browser_fallback_with_demotable_404_but_no_marker_declines`. The plugin funnel got the same guard-first ordering (promote before the `dead_listing_page` short-circuit, byte-parity fall-through on decline). Live verification (`tmp/konami-s7-pass2/`, targeted forced pass): guard reached with zero refusals, raw-URL fallback read the real listing, `no_openings_marker` fired, the stale 404 line demoted (no `AdapterValidationError`), and the source state landed `lastStatus: ok` / `lastFailureBucket: no_openings` / `consecutiveZeroKept: 1` / `lastError: None` — the Wave-2 rule applies: one full default pass lands the row's missing-universe drain. Tests: 9 new guard/funnel cases (`tests/jobs_static/test_zero_kept_guard_plugin_demotion.py`, `test_zero_kept_guard_generic_demotion.py`); 455 static/fetcher/dayforce tests green; changed-mode gate exit 0.
- **Konami + Exit VR floor adjudications (hold-tail repair plan §7/§5, 2026-09-12; HOLD/HOLD, floor unchanged at 9).** Konami: the listing still reads trusted-empty (repo marker detector True, 0 real job hrefs out of 70) but the stale-detail demotion built for exactly this shape never fires — a guard-spy run of the real source (`tmp/holdtail-wave2-20260910/konami-guard-debug.py`, 0 guard calls) proved Konami rides the plugin fast path, where `_probe_empty_plugin_listing` classifies the nav-only listing `dead_listing_page` and `_record_empty_plugin_result` early-returns before `promote_clean_zero_kept`; separately the rows flow fetched the stale Community nav link (slashless `/pages/sns_account` → 404, slashed twin 200s — shape drift, not a dead page) and the entry wrapper raised `AdapterValidationError` (source `error`/`site_changed`/`broken_extraction`). The overdue row's own `jobLink` is the dead nav page itself — junk either way. Paths forward recorded: S7 funnel fix (extend the demotion to the plugin empty path + ordering before the `dead_listing_page` short-circuit) or operator adjudication on the completed evidence. Exit VR: origin now 500s **site-wide** (`/`, `/jobs/`, `/data-protection` all WP fatal — origin outage, not `/jobs/`-only); chronic day ~7 of ~28, escalation trigger not met; the 2 junk rows cannot drain or re-verify while the site 500s, correctly (recovery re-verifies; chronic-day ~28 third-probe opens the dead/lapse lane).
- **Big Moxi rendered-empty ×2 basis completed; automatic drain lane found unavailable (hold-tail repair plan Wave-2b adjudication #2; HOLD stands, floor unchanged).** The second forced targeted pass (`tmp/holdtail-wave2b-20260912/bigmoxi2/`, isolated output dir — targeted runs do not touch live `data/` state) reproduced #1 exactly: listing 200 with the ~1KB JS shell, fallback pool `js_shell`+`empty_page` all got HTML, 0 extracted, bucket `js_required`; the independent Playwright probe matched (200, title rendered, 0 text, 0 links, 0 console errors). The two-clean-render evidential basis is complete, but the code trace found the plan's "drain via guard" branch not realizable: the zero-kept guard unconditionally refuses browser-fallback-attempted zeros (`browser_fallback_attempted` — a rendered zero is indistinguishable from a JS-shell trap without new evidence plumbing), errored reads never increment `consecutiveZeroKept`, the prior bucket `js_required` blocks the prior-clean-zero path, and both row URLs soft-200 the SPA shell so no 404 shape reaches the stale-detail demotion. Honest paths forward recorded in the plan: a small future S6 leaf persisting `renderedEmptyConfirmedAt` timestamps from the fallback lane for the guard's ×2 evidence, or an explicit operator adjudication on the completed ×2 basis.

- **`dayforce` structured adapter for Dayforce CANDIDATEPORTAL boards, Reflector pilot migrated (hold-tail repair plan Wave-3; floor 12 → 9 after drain).** Dayforce-hosted boards (`jobs.dayforcehcm.com/{culture}/{clientNamespace}/CANDIDATEPORTAL`) render entirely from XHR behind a NextAuth CSRF gate — the static adapter reads a zero-link SPA shell. The Wave-3 capture plus a 2026-09-12 empirical check pinned the exact contract: `GET /api/auth/csrf` (cookie jar on) → `csrfToken`, then `POST /api/geo/{clientNamespace}/jobposting/search` with `x-csrf-token` **and the same client's cookies** (NextAuth double-submit — the identical header without cookies 403s; `cf_clearance` and browser headers unnecessary). New leaf `src/jobs/adapters/plugins/provider_api/dayforce.py` owns the two-step with a self-contained cookie-jar urllib client (the workday-CXS pattern; the GET-text `fetch_text` lane cannot execute the POST pair), normalizes `jobPostings[]` to the shared row shape (`dayforce:{ns}:{jobPostingId}`, full HTML-entity-decoded `jobDescription` with `_skipDetailFetch` — the unauthenticated detail route serves an empty shell, so the search payload IS the body; `postingLocations` → city/country/summary; stable detail URLs; `paginationStart += count` until `offset+count >= maxCount`), classifies empty boards `legit_empty`, and paginates behind a 10-page bound. Wired through the standard seams (plugin registration, dispatch, loader name + SOURCE_REPORT_META, `dayforce` in `PROVIDER_REGISTRY_ADAPTERS` for the staged-pending lane). Reflector migrated through the sanctioned lanes (evidence `tmp/dayforce-wave/`): staged `dayforce:client_namespace:ref` with `migrationSourceIdentity` → the static SPA-shell row (pending 830 → 831); dayforce-family validation pass `--include-pending-provider-migration` exit 0, **dayforce_sources ok fetched 3 / kept 3** (161 Programmer Engine and Tools, 149 Technical Animator Senior, 115 VFX Artist Expert — live-validated 3/3 against the real endpoint pre-staging); promotion → active 2,167 → 2,167 (1:1), pending 831 → 830, static twin tombstoned `superseded_by_provider` (tombstones 159 → 160), active seed 1,891 → 1,892; the 3 stale Reflector overdue rows (none matches a live posting) drain via the finalize missing-universe path on the following full default pass (Wave-2 drain rule). Any Dayforce tenant is `{clientNamespace}`-addressable with the same two-request contract. Tests: `tests/test_provider_dayforce_adapter.py` + `tests/test_provider_dayforce_runner.py` (18 cases via `tests/helpers/dayforce_fixtures.py`); 443 provider/static tests green; changed-mode gate exit 0.
### Fixed

- **Static listing pagination gains the path-pager dialect (`/page/N`) and document-URL anchor resolution: careers.playstation.com goes 110 → 316 active store rows (kept 442 over all 7 pages) and recruit.nexon.co.jp 16 → 21 (kept 51 over all 3 pages), closing the two real blind spots from the 2026-09-14 two-dialect coverage sweep (2026-09-15, evidence `tmp/path-dialect-20260915/` + `tmp/coverage-sweep-20260914/`).** The 2026-09-14 `?page=N` leaf is query-dialect-only: path-paged boards announce continuation pages as trailing `/page/N` path segments (PlayStation's `href="/page/2"` pager anchors off its root, Nexon's `/job/page/2/`), which the same-path check refused. Two production facts shaped the design: (1) the pipeline's transport follows registry-window redirects *inside* httpx (`follow_redirects=True`), so the runner never learns a window's final URL — and PlayStation's registered window (`www.playstation.com/en-us/corporate/playstation-careers/#listings`) redirect-serves `careers.playstation.com`, whose host-relative anchors would resolve against the wrong host; (2) the PS document self-identifies via `rel=canonical`/`og:url` (`https://careers.playstation.com/`). The same-listing matcher therefore gains two capabilities in `static_listing_pagination.py` only — no registry changes, both static lanes benefit: (a) **path dialect**: a candidate matches when its path equals the base's minus a trailing `/page/N` segment (trailing slashes ignored, page ≥ 2, backward-to-`page/1` refused, non-`page` query equality still enforced with the piggyback `?page=` param some path-pagers append excluded — Nexon's historic `/job/page/2?page=2` follows, a real filter like `?jobtype=contract` still gates; the WP noise class `/jobs/2` and intermediate `/page/N/...` segments never match because the segment is trailing-and-structural); (b) **document-URL anchor base**: `<base href>` → `rel=canonical` → `og:url` evidence replaces `page_url` when it is a valid absolute http(s) URL on the same **registrable domain** (bounded eTLD+1 with a second-level-suffix set so `www.playstation.com` ↔ `careers.playstation.com` and `recruit.nexon.co.jp` ↔ `www.nexon.co.jp` are in-site, cross-domain evidence is never trusted, same-host-and-path evidence is ignored so non-redirecting boards keep byte-identical behavior, and an https window never downgrades to http). The discovered-page cap rises 4 → 6 (PlayStation advertises 7 pages; the source time budget still bounds each run). Live verification: PS targeted pass fetched pages 1–7 (log: "Fetching listing page 7/1"), kept 442/442, state `ok / kept 442 / consecutiveFailures 0`; store 110 → 316 active rows (227 on-host `careers.playstation.com` links + 81 re-homed Greenhouse embeds owned by the greenhouse adapter's canonical rows + external extras), 281/318 live on-host job ids present — the 37-gap traces to the per-page low-yield detail cap (candidates found 41–46, traversed 39–44 per page), the budget-bounding throttle; the sweep's 637 headline counted repeated cards across pages, not distinct postings. Nexon targeted pass: all 3 pages, kept 51, store 16 → 21 active. Kill switch unchanged (`BALUFFO_STATIC_PAGINATION_FOLLOW`); suites: pagination 28/28 (20 existing pins + 8 new dialect blocks), static lanes 645 green.

- **Gamefreak's hrmos registry window repaired from a silent full-time-only filter to the full board: 43 → 60 postings, all 17 contract-class roles landed (2026-09-15, evidence `tmp/gamefreak-filter-audit-20260915/`).** The audit asked how many postings the registered `?jobtype=full` window excludes — answer: none (hrmos ignores the lowercase parameter; the full board and the window render the identical 60-code universe) — but the row's four fetch fields (`pages`/`listing_url`/`careersUrl`/`sourceDirectoryEntryUrl`) carried the discovery sheet's verbatim `?jobType=FULL`, which hrmos treats as a real full-time-only filter returning exactly 43 postings, while the canonical id's normalizer had lowercased the same URL to the harmless form. Every run since 2026-04-10 registration therefore harvested the filtered board, silently excluding all 17 contract-class postings (【契約】/【業務委託】/フリーランス — 8 engineering roles among them: VFX/サーバー/インフラ/アニメーションツール/R&D グラフィックス programmers, テクニカルアーティスト×2). Verified NOT a predicate drop (all 60 titles pass `_is_provisional_static_artifact_row`, `looks_like_static_parser_noise_title`, and full `canonicalize_job_with_reason` with zero flags; zero contract-class rows in lifecycle history ever — they never entered, never drained). Repair: the four fetch fields pointed at the id's exact lowercase URL via `save_registry_state_atomic`, id byte-identical so source state/history are preserved; seed patched identically; defect class confirmed gamefreak-exclusive (4 occurrences vs 1 id in the whole seed). Live targeted pass after repair: **60/60 store closure vs the live board, state `ok / kept 60 / consecutiveFailures 0`**. Discovery-quality note recorded: sheet-derived fetch URLs are stored verbatim while ids are normalized — any filter-bearing query whose casing is semantic can diverge the same way.

- **Language-switch title classifier narrowed: real CJK job titles with ASCII parentheses and language-qualifier-in-parens roles stop being flagged as switcher artifacts — the bandai careers funnel now emits its complete live universe (bandai trace remediation 3, 2026-09-14, evidence `tmp/bandai-trace-20260914/`; guard restored against the real switcher corpus 2026-09-15, evidence `tmp/lsw-casualty-sweep-20260915/`).** The trace's root cause: `_looks_like_language_switch_title`'s final fallback flagged ANY title containing ASCII parens plus any CJK character (collateralizing the JP market's common `家庭用ゲームエンジニア (新規格闘アクション)` style), and its word branch matched language words as substrings anywhere (catching grackle's `Localization Quality Assurance (Simplified Chinese)` and siei's CJK titles). Both branches now read the text OUTSIDE the last paren pair only, per the switcher contract — a switcher puts the language name as the outer label (`English ( Inglese )`, `EN (日本語)`), while job titles put the qualifier inside parens under a role-shaped outer: (1) the word branch matches outer whole tokens (grackle's paren content stops firing; the veto tokens still exempt `English Teacher (Tokyo)`-shaped outers from the whole function); (2) the CJK fallback now requires the outer to BE a language name (code list, switch-word list, or a new `_LANGUAGE_NAME_ALIASES` set of JP romaji 語-forms and native-script CJK names — the old bare-CJK fallback was a tautology that flagged every flagged-paren shape); (3) whole-outer switcher labels (`Language (中文)` widgets) flag via an exact-label set; (4) a pure parenthetical title flags only when the paren names a language (`(Remote)` is a posting, `(日本語)` is a switcher). Verified against the store's real flagged shapes (13 collateralized shapes flip False — bandai ×6, grackle ×2, siei ×2, gamejobs, plus generic `(Remote)`/`(Unreal Engine)` shapes; 10 switcher shapes stay flagged incl. previously-missed `(日本語)`/`Language (中文)`; negative controls `English Teacher (Tokyo)`/`Language Model Engineer (NLP)` stay False), and the pre-existing `English ( Inglese )` quality-gate pins hold. **Live bandai result: 157 raw jobs (was 146 in the store) with 41/41 hrmos detail codes — the complete live universe, all six en010-family engineering postings including the original 4 trace targets, flowing through the careers funnel for the first time**; the only remaining demoted card is the `jobs?category=…` listing anchor, a genuine container artifact (the demotion working as designed). With remediations 1 (shell skip) and 2 (fallback parity) this closes the bandai killing chain end-to-end; the 20 grackle/greenhouse collateralized rows stop being at-risk. Tests: 4 new test blocks in `tests/test_jobs_exact_category_titles.py` (15 spared shapes, 10 flagged shapes, pure-parenthetical contract, bare-CJK unchanged); the provisional-fallback fixtures switched to a still-flagged switcher shape (the bandai shapes correctly stopped flagging); full suite 5,148 passed / 2 skipped, zero failures. **Guard restoration (2026-09-15 casualty sweep follow-up):** the sweep proved the first narrowing over-rotated — all 13 real switcher titles it drained (`Europe (English)`-class konami rows, kwalee `ไทย (ไทย)`, disneycareers `繁體中文 (CN)`, digitalconfectioners `Português ( Portuguese (Brazil) )`) escaped the narrowed predicate, because region-word outers are not language names and the native-script paren forms (`繁體中文`, `ไทย`) were absent from the alias set. Three targeted extensions, all bounded by the existing contract: (1) `_LANGUAGE_REGION_OUTER_TOKENS` — a region-word outer (`europe`, `asia`, `canada`, `north/south/east/west/central`, CJK `亞洲`/`欧洲`/`北美`…) flags as a switcher ONLY when every outer token is a region word AND the inner paren names a language, so `Europe (Remote)`, `Japan (Marketing)`, and country-qualifier job titles (`…, Fashion (Thailand)`, `LKSG (Germany)`) stay clear; (2) the alias set gains the corpus's native-script forms (`繁體中文`, `简体中文`, `簡体中文`, `台灣中文`, `ไทย`, `한국어`) plus `french` — alias-only, deliberately NOT in the switch-word list, so `French Localization QA (Games)` cannot re-collateralize; (3) a `_latin_fold` NFKD helper so accented inner text (`Português`) recognizes like its ASCII spelling while CJK/Thai runs stay single tokens. Verified: all 14 corpus switcher shapes flag (composite too), 17 posting/neutral controls stay clear (bandai/siei/grackle/grasshopper CJK shapes, `(Remote)`, `Language Model Engineer (NLP)`, `English Teacher (Tokyo)`), region-branch contract pins (`Europe (Remote)` False, `North America (French)` True). Tests: 3 new blocks (16-shape corpus parametrize, 9-shape spared parametrize, alias-scoping + accent-fold units); full suite 5,175 passed / 2 skipped, changed-mode gate exit 0.

- **Provisional-artifact demotion gains card-row fallback parity: demoted cards verify inline against their own detail URL and still emit when verification yields nothing (bandai trace remediation 2, 2026-09-14, evidence `tmp/bandai-trace-20260914/`).** The trace's second mechanism: `_append_rendered_row` demoted provisional rows to detail candidates unconditionally, so when the conversion stage never ran (the budget death remediation 1 fixed), the card's posting evidence died with it — the non-provisional lane has always had the `if row:` card fallback, the provisional lane had none. The rows lane now verifies demoted cards inline (kill switch `BALUFFO_PROVISIONAL_CARD_FALLBACK=0` restores queue-only, default on) gated on probable-detail URLs only (improbable URLs keep queue-only; the candidate fan-out bounds them by cap) and on the shell-skip state (no verification against a host already at 2 strikes); empty verification falls back to the card row stamped `provisionalCardFallback: true` and counted (`provisional_card_fallback_emitted`), and a detail-yielding verification emits the detail rows exactly like the non-provisional arm. **Deliberately no job-like gate**: measured live, the real bandai shape (`家庭用ゲームエンジニア (新規格闘アクション)`) is non-job-like under the same language-shape misclassification this fallback protects, and the non-provisional lane already verifies non-job-like titles first (`needs_lookup = not job_like or …`) — a job-like gate would have excluded exactly the rows that need the fallback (noise-filter ordering unchanged upstream). Two integrity stamps made honest: the rendered `detailFetchRequired: False` meta and the fingerprint `listing_only` cache decision both keyed on `provisional_rows_found == 0`, which now lies when inline verification performed detail fetches — both additionally require `provisional_inline_verified == 0` (a counter the rows lane stamps per inline verification), so a page that did detail work never reports itself as detail-skipped (the exact-category listing-only regression test caught the first one). Live bandai verification: **155 raw jobs (was 151 pre-fallback, 146 in the store), all 38/38 hrmos detail codes present including both remaining `050_0040_en010` targets and the 4 flagged cards inline-verified with detail rows** — every posting the careers page renders now flows through the funnel; the classifier fix (the title-shape misclassification itself) remains the recorded follow-up for gracklehq ×12 + greenhouse ×8. Tests: 7 new in `tests/jobs_static/test_provisional_card_fallback.py` (detail-rows win, empty→fallback emit, improbable-URL queue-only, no-link drop, non-job-like verifies, kill switch queue-only, fetch-failure ratchet semantics); full suite 5,121 passed / 2 skipped, zero failures.

- **Rendered-card detail verification no longer burns the source budget re-fetching JS-shell detail pages it can only reject (bandai trace follow-up, 2026-09-14, evidence `tmp/shell-skip-20260914/`).** The bandainamcostudios careers-page trace proved the killing chain: 38 sibling cards' detail verifications each fetched a ~0.65s JS shell (the hrmos CDN intermittently serves shells to the pipeline's fetch client) that the parser can only reject, exhausting the 25s source budget — and the TimeoutError aborted the listing batch before the queued-candidate plan could ever convert the 5 demoted cards, which is how the 4 directly-linked postings stayed invisible while the source read `ok`. Fix in the rows leaf (`_fetch_rendered_detail_rows`): after two verifications from one detail host produce zero usable rows (post noise-filter) AND the fetched HTML classifies as a JS shell via the shared `detect_js_shell`, the rest of that host's verifications skip fetching for the remainder of the source's run (per-source-run `detail_shell_strikes` tally on the context, naturally reset each pass; `detail_shell_skipped` / `detail_shell_strikes` counters on the stats; kill switch `BALUFFO_STATIC_SHELL_DETAIL_SKIP=0` restores fetch-always, default on). Safety properties: the strike pair is zero-usable-rows + shell-shaped — shell shape alone never strikes (the hrmos listing page itself carries Next.js/`__NEXT_DATA__` tokens yet is fully server-rendered; the whole hrmos platform is shell-marked, so a shell-only predicate would blind every hrmos tenant); cache-hit verifications record no strike (stale HTML is not fresh network evidence); the caller's card-row fallback is unchanged, so skipped verifications still emit their card rows and healthy-run output is byte-equivalent (live A/B: 151 raw jobs before and after; the skipped fetch's document is already in the fetch cache, so no request is duplicated). Live verification: the simulated morning shell-shape run now **completes within budget with all rows emitted** (previously TimeoutError mid-batch) — and the freed budget lets the plan/traversal stage run, so **2 of the 4 original missing postings (`050_0040_en010_*`) land through the careers funnel for the first time**; the other 2 remain blocked by the recorded language-switch classifier bug (separate follow-up, blast radius gracklehq ×12 + greenhouse ×8). The skip is correctness-preserving by construction: a shell cannot carry server-rendered job content, so the fetch was futile; when the CDN serves real HTML again (verified live this session), verifications run and strike nothing. Tests: 7 new in `tests/jobs_static/test_static_shell_detail_skip.py` (two-strikes-then-skip, server-rendered-zero-rows never strikes, cache-hit no strike, per-host scoping, kill switch fetch-always, card-row fallback preserved after skip, shell-detector shape pins); full suite 5,114 passed / 2 skipped, zero failures.

- **Guest-junk guard verification pass exposed and fixed three landed defects: the extraction-lane guards were silent no-ops (registry-form source ids never resolved), queryless company-page rows escaped the shape set, and the drain counter re-counted already-drained stock (2026-09-14, evidence `tmp/guest-junk-drain-20260913/`).** The forced full-universe verification pass proved the drain lane and wire contract end-to-end — `guestJunkDrainedCount: 1541` survived the contracts normalizer (adding the missing `lifecycleSummary` mapping key), and 124 rows landed fresh `guest_junk_provenance`/`unavailable` markers (skybound 62 — including its 63 active search-result junk rows, the audit's frozen stock, plus sidestudio 36, hyperbeard 24, inverge 1, exit-vr 1) — but Fusebox's 66 rows came back `active/available` (`status=ok fetched=86 kept=86`: the Playwright render finally succeeded, the page's embedded LinkedIn widget populated, and the harvest re-entered). Root causes: (1) `source_identity_url`'s regex required the `static_source::` prefix, but extraction ctx carries the registry spelling (`static:listing_url:…`) — empty identity host → `is_junk_provenance_row` fail-opened to False in every rows-flow/detail funnel while all tests used the state spelling, encoding the wrong assumption; (2) the one queryless row my render produced (`/company/fusebox-games/jobs`, the widget's own link) is a company-page path the slug regex never matched; (3) `_apply_guest_junk_lifecycle_entry` returned the entry (success) for already-`unavailable` idempotent re-visits, so the summary counted ~1,443 no-ops (1,541 vs 124 fresh markers). Fixes: the resolver accepts both id spellings (the guard now actually fires in extraction lanes — verified live: identical render conditions now `fetched=0 kept=0` with drops counted); any `/company/…` path classifies as guest junk for non-LinkedIn origins (measured before landing: 209 non-LinkedIn-origin rows enter drain scope, 0 sanctioned-origin rows affected — their 148 company rows are exempt by origin check, not shape); the transition returns None for already-terminal rows so `guestJunkDrainedCount` reflects fresh drains only (already-drained rows route through the preserve accounting, which is truthful — no timestamp or transition-id churn). Fusebox disposition: with the funnel closed the source runs `ok`/0 and banked `consecutiveZeroKept: 2`, so the 66 revived rows drain via the eligible-missing universe path on the next full pass (Big Moxi/Konami precedent — no promotion gate needed). Tests: guard suites 30 cases (7 new: registry-form resolution + sanction both directions, company-page shape + sanctioned-origin pin, fresh-only drain count pin, registry-form funnel drop); full suite 5,076 passed / 2 skipped (the one failure is the pre-existing load-order terminal-rewrite flake, green in isolation).

- **Big Moxi + Inverge browser-fallback adjudications executed (hold-tail repair plan Wave-2 residue; both HOLD, no registry action, floor unchanged at 12).** Big Moxi: the forced targeted pass exercised the fallback pool 3× (`js_shell`, `empty_page` — all got HTML, pool healthy, no relaunches) and an independent real-browser probe (`tmp/holdtail-wave2b-20260912/probe_render.py`, Playwright Chromium, `domcontentloaded` + 6s hydration — `networkidle` never settles on this site) confirmed the automation-visible page **genuinely empty** (200, title rendered, 0 body text, 0 links, 0 console errors, DOM stuck at the 1.1KB shell — no hidden XHR surface, unlike the Dayforce case): **rendered-empty confirmation #1 of the ×2 basis**; one more clean render on a later pass lets the 2 rows drain via the guard (which correctly refused this browser-attempted zero). Inverge: the Sep-6 403 bot wall is **gone** — the listing 200s to plain httpx (143KB server-rendered board, 5 "SEE OFFER" links, no JS render needed) but **every detail URL 404s** (site's Spanish "Página no encontrada" template; trailing-slash and locale shapes all 404; one site-side href malformed `invergestudios.comjobs/…`), the rows flow aborted on the first detail 404, and S1 stamped `details_broken` honestly — the board reads mid-rebuild (Spanish-default locale, stale links), so wait-and-reverify per the plan; the 1 overdue row is the nav junk row and cannot drain while details 404 the rows flow. Records in `docs/plans/hold-tail-repair-plan.md` (per-source adjudications #4, #8).
- **Steer provider-twin drain verified: overdue floor 31 → 21 (Δ −10) on the first full default pass after the Wave-2 promotion — the steer share of the hold-tail floor is gone.** The 10 WP-attributed overdue rows (bamboo-provenance `view.php?id=…` URLs) closed `unavailable` via `source_absent`/`definitive` at 10:13Z together with the WP `careers`/`/internship` rows, exactly the plan's −10 projection; the promoted bamboo row participates normally (excluded/within_freshness_window this pass, trusted zero banked), `preservedBecauseSourceFailedCount: 0`, health baseline re-captured at 21 overdue / 8 sources. Two mechanism records landed in the plan addendum: (1) the drain lane for provider-migrated (tombstoned) sources is the finalize missing-universe path — the `known_missing_evidence_sources` guard requires the full registered-run universe, so targeted passes never drain and the shadow sweep plan alone closes nothing (`enforce_direct=False` records shadow results only; bamboo's 200-redirect shape also classifies `generic_redirect`/`ambiguous`, which deliberately never closes on one strike) — operational rule: **after any provider-twin promotion, run one full default pass to land the drain**; (2) bridge-service direct-evidence commits require an in-process published feed generation ("SQLite jobs feed authority has no published generation" otherwise; the commit path is transactional and the harness rolled back cleanly — no corruption), so scripted bridge commits are not a drain mechanism. Evidence `tmp/holdtail-wave2-20260910/` (`full-pass.log`, `sweep_steer.py`); record in `docs/plans/hold-tail-repair-plan.md` (Wave-2 drain verification).
- **Steer provider-twin migration executed (hold-tail repair plan Wave 2, the plan's single operator-approved registry mutation) — the 31-row overdue floor's highest-value repair.** The steer WP listing's 500 could never produce the trusted zero read that would drain its 9 genuinely-gone bamboo-provenance overdue rows, while the real board (`steerstudios.bamboohr.com/careers`) is live-empty with a structured clean zero (`careers/list` → `totalCount:0`, reconfirmed 2026-09-09 and 2026-09-10). Executed through the sanctioned lanes end-to-end (evidence `tmp/holdtail-wave2-20260910/`, record in `docs/plans/hold-tail-repair-plan.md` Wave-2 execution record): staged `bamboohr:listing_url:https://steerstudios.bamboohr.com/careers` via `transition_registry_to_pending(provider_migration_candidate)` with `migrationSourceIdentity` → the WP row (pending 830 → 831); bamboo-family validation pass with `--include-pending-provider-migration` — exit 0, 0 failed, **Steer Studios (BambooHR) ok fetched 0 / kept 0 with no error** (the clean zero the WP row could never produce); promotion mirroring the 9-row bamboo wave — `transition_registry_to_active` (active 2,167 → 2,167 1:1), pending 831 → 830, WP row tombstoned `superseded_by_provider` (tombstones 158 → 159), `normalize_manual_promotion_rows: 1`, active seed 1,891 → 1,891 (WP row out, promoted row in), full read-back OK; verification pass exit 0 with the availability sweep plan selecting the 9 steer bamboo URLs for re-verification — the −9 drain lands on the sweep/next pass (High-5 precedent). The WP `/internship` row tracks the WP site and re-verifies when it recovers. Also in this wave: **Reflecto adapter disposition recorded** (Dayforce portal catch-all-routes every unknown path — including a fake control path — to the identical SPA shell, so no public JSON API exists at guessable paths; record-only hold, no tombstone, pilot candidate for the iCIMS/Dayforce adapter thread) and **Exit VR wait-and-reverify unchanged** (WP fatal 500, chronic-day 5 of ~28; escalation trigger not met).
- **S3 template-seam rejection extended to the remaining detail-candidate funnels (hold-tail repair plan S3 addendum — Konami was still fetching the seam).** Post-landing live verification caught Konami's `<%= official_site %>` template URL still being fetched (HTTP 400) in the final forced pass: the Wave-1 rejection guarded only the traversal funnel (`_append_detail_candidate`, `static_listing_state.py`), while the heuristic funnel `add_detail_link` (`static_detail_heuristics_filter.py`) — used by every parse/rows flow (`_collect_listing_detail_links`, the parse extractors) — had no template check, and `/jobs/` (a default detail path token) let the seam pass the `probable_job_detail` heuristic to the fetcher. `add_detail_link` now applies the same template-artifact rule to the raw candidate, the joined absolute, and the anchor text (counted as `dead_listing_page`; a relative `<%= %>` href is detectable only pre-join, percent-encoded drift only post-join, so both are checked), and `_nested_detail_candidates` (`static_listing_traversal.py`) applies it to depth+1 links scraped from detail pages — the last direct `StaticDetailCandidate` construction site. No behavior change for healthy URLs: the check fires only on template seams, which always HTTP-400. Tests: `tests/jobs_static/test_detail_link_filtering.py` +5 (Konami exact URL, relative EJS shape, `${jobId}` seam, seam anchor text, nested-candidate filtering); record in `docs/plans/hold-tail-repair-plan.md` (S3 addendum); 383 static tests green; changed-mode gate exit 0.
- **S5 browser-header profile: flag-gated, host-allowlisted header-shape override for edges that 500 the bot shape (hold-tail repair plan, the Mundfish repair; off by default).** Mundfish's Wave-0 all-500 detail shape turned out to be two stacked effects: the origin genuinely broke (it now serves every former-500 path 200 behind a `302 → /en/<path>` locale redirect), and its edge bot handling answers the production default header shape — bot `User-Agent: BaluffoJobsFetcher/1.0`, JSON-first `Accept`, no `Accept-Language` — with **HTTP 500** instead of a 403. Live discrimination pinned the filter precisely: bot-UA → 500, bot-UA + browser Accept → 500, browser-UA alone → 500, browser-UA + browser Accept + `Accept-Language` → 200 — the trio is load-bearing to the last header. New leaf `src/jobs/common/browser_headers.py` owns the gate (`BALUFFO_BROWSER_HEADERS` truthy AND the URL's registrable host on the comma-separated, www-stripped `BALUFFO_BROWSER_HEADERS_HOSTS` allowlist — both required, S4 conventions) and the stateless profile (generic versionless Chrome UA + browser Accept + `Accept-Language`); `transport.build_fetch_headers` applies it per-URL at header construction in both lanes (urllib `default_fetch_text`, httpx `async_fetch_text_httpx` — no retry lane needed; every request carries the profile from the first attempt), while `build_headers` and every non-allowlisted request stay byte-for-byte unchanged. Mundfish activation through the lanes: targeted forced pass (`--ignore-circuit-breaker` at 9 consecutive `details_broken` failures) with the flags → **ok, 32/32 kept, no error**; 6 of 9 real roles re-verified `available` immediately; the 3 roles rotated off the live board plus the `/legal` junk row drained `source_absent`/`definitive` via the finalize missing path on a detached `--force-refresh-all` pass — **Mundfish overdue 10 → 0, floor 22 → 12**, the long-pole block of the hold-tail floor gone. Wave-1's `details_broken` accounting intentionally stays intact — it was the correct diagnosis of a real broken window, and this is the recovery, not a reclassification. `tests/jobs_static/test_browser_header_profile.py` (8 cases: gate matrix, profile application without input mutation, both-lane wiring with captured headers, default invariance, trio shape pin); 396 static tests green; changed-mode gate exit 0.
- **S4 cookie-jar retry for geo-cookie redirect loops (hold-tail repair plan, the Astrum repair; flag-gated, off by default).** The static fetcher's redirect handling (`StaticHtmlFetcher`) raises `Static redirect loop` when an origin bounces the same URL back — the Astrum shape (`astrum-entertainment.ru/en/careers` 200s with 19 fresh server-rendered job links for direct clients, but the cookie-less client sees a geo/consent `Set-Cookie` round-trip and loops forever), leaving its junk row stuck in the overdue floor. Three pieces, all additive: `src/jobs/common/http.py` gains `default_fetch_text_with_response_headers` (identical read-at-most/byte-cap behavior plus lowercased multi-valued response headers; 3xx `HttpStatusError` now carries the response `headers`, while the original `default_fetch_text` contract is byte-for-byte unchanged); new leaf `src/jobs/adapters/static_cookie_retry.py` owns the gates (`BALUFFO_STATIC_COOKIE_RETRY` truthy AND the host on the comma-separated, www-stripped `BALUFFO_STATIC_COOKIE_RETRY_HOSTS` allowlist — both required) and the per-attempt `StaticCookieJar` (stdlib `CookieJar.make_cookies` parsing, per-host storage, per-hop `Cookie` header); and `StaticHtmlFetcher` factors its hop chain into `_fetch_hop_chain` (pre-S4 behavior byte-for-byte) and triggers the single `_cookie_jar_retry_fetch` lane only on a `Static redirect loop` for an allowlisted host under the flag — one attempt per hop, 6-hop budget, same-site redirect safety rules still enforced (cross-host/https-downgrade rejections unchanged), and a cookie that fails to break the loop re-raises the loop error with a `(cookie-jar retry exhausted)` marker so failure classification never changes. Design nuance found while testing: with a same-URL bounce the base lane loops without a second network call (the loop is detected from the redirect target), so the retry lane treats a cookie-less first-hop round-trip as the geo-cookie shape itself and continues with the freshly absorbed cookie. `tests/jobs_static/test_static_cookie_retry.py` (13 cases: gate matrix, cookie-honored recovery, flag-off/unlisted-host loop preservation, exhausted-cookie reclassification, runaway-chain bound, unsafe-redirect rejection, jar unit, primitive contract pins); pre-existing redirect/data-URI suites unchanged. Astrum activation is an operator step: set both env vars, run the forced targeted pass, and the junk row drains via the guard once the fetcher reads the live board — without the flag, behavior is unchanged and Astrum stays record-only. 369 static tests green; changed-mode gate exit 0. **Activation executed 2026-09-11 and exposed an S4 coverage gap with the same shape as the S3 story:** the runner's two listing-fetch funnels (`_fetch_listing_html_sync` sync + async twin in `static_listing_runner.py`) had their own redirect mini-chains raising `Static redirect loop` before any S4 logic — production rides the async batched funnel (`httpx_async`, batch client `follow_redirects=False`), where Astrum's 307 → same-URL `set-cookie: bp_chl=…` bounce died (loop → browser fallback → JS shell → 0 candidates → `needs_review`) while the S4 lane guarded only the traversal/detail funnel. Fix: the async funnel re-issues the identical request through the shared batch client (its jar carries the bounce's `Set-Cookie` — transport-native continuation); the sync funnel delegates to the S4 jar lane; gate, allowlist, and exhausted-retry loop classification unchanged. Activation then landed end-to-end: targeted pass broke the loop (Astrum ok, kept 1, `consecutiveFailures` 8 → 0); the junk row (`/en/legal`) drained `source_absent`/`definitive` via the finalize source-eligible missing path on a detached `--force-refresh-all` full-universe pass (the sanctioned bypass for the incremental empty-source window — targeted passes never drain). Floor 21 → 20 (Δ −1); the transient +2 (Midgar Studio) is an upstream `www.afjv.com` TLS hostname-mismatch break, unrelated. Regression tests +2 in `test_static_cookie_retry.py` (18 cases); full static suite 388 green; changed-mode gate exit 0.
- **Rows-flow detail-failure accounting closes the S1 coverage gap found in live verification (hold-tail repair plan Wave 1, the Mundfish fix).** The forced targeted pass verified the health side (overdue 31/9, Mundfish 10 in `overdueBySource`, Δ0) but stamped Mundfish `js_required` with all-zero detail stats: Mundfish runs the generic runner (no plugin handles `mundfish.com`), and its failure path is the **rows flow** — rendered cards re-verified against their detail URLs — where the first detail 500 aborted the source before any counter landed, so the Wave-1 traversal-only `detail_fetch_failed` hook never saw the evidence; the Wave-1 signal then had nothing to read. Three additive fixes, abort-on-first-failure semantics unchanged: `static_listing_rows.py` counts the detail attempt + failure and appends the error before re-raising (ratchet-safe, expected static fetch fallbacks only) and stamps `listingJobsFound` (the live board's visible card count) before per-row verification — an abort on row 1 means the post-return counters cannot see the other 11 cards; `pipeline_source_results.py` gains `_apply_static_detail_evidence_to_report`, projecting the detail entry's evidence (candidateLinksFound / detailPagesVisited / detailFetchFailedCount / listingJobsFound, nonzero-only) onto the source-level report and stamping `details_broken` when the signal fires (the source-level zero-kept classification runs on the report only, so without this the source-level bucket stayed `js_required` while the detail-level bucket was honestly `details_broken`); `fetch_report_normalization.py` adds `detail_fetch_failed` to the persisted detail stats whitelist and `listingJobsFound` to the detail-item projection. Final forced pass: Mundfish **source- and detail-level `failureBucket=details_broken`**, 3 of 9 hold-tail sources now honestly `details_broken` (Mundfish, Inverge, SNK — all rows-flow detail-abort shapes), health verdict healthy with Δ0 (rows correctly preserved), state `lastFailureBucket=details_broken` with the breaker re-quarantined under the honest bucket. Live-verification record in `docs/plans/hold-tail-repair-plan.md` (Wave-1 verification section); `tests/jobs_static/test_static_rows_detail_error_accounting.py` (9 cases: counter-before-reraise, not-swallowed, unexpected-bug ratchet, evidence stamp before row loop, end-to-end details_broken, no-evidence → js_required separator, projection surface + inertness, normalizer round-trip); 378 static tests + pipeline/taxonomy/enrichment suites green; changed-mode gate exit 0.
- **`details_broken` failure bucket, template-literal href rejection, and marker-promotion guardrails (hold-tail repair plan Wave 1).** Three systemic fixes from `docs/plans/hold-tail-repair-plan.md` (the 9 quarantined sources behind the 31-row overdue floor), designed from the 2026-09-10 Wave-0 re-evidence (`tmp/holdtail-20260910/`). **S1:** a new `FailureBucket.DETAILS_BROKEN` class in `src/jobs/common/taxonomy.py` makes the listing-live/details-dead split visible in monitoring — the Mundfish shape (listing 200 with 12 server-rendered roles, all 10 overdue detail URLs 500, previously stamped `js_required` by the static-manual-no-jobs text rule with zero JS evidence, or `needs_review` by the fall-through) now stamps `failureBucket=details_broken` from a new `detail_fetch_failed` counter (`stats.detail_fetch_failed` / `detailFetchFailedCount`, stamped at the traversal detail-error site). The signal requires actually-visited details, ≥3 candidates or listing links, ≥80% detail-fetch failures, no browser-fallback recommendation, and no empty-confirmation; adapter-stamped evidence classifications (anti-bot, site_changed, timeout, parse, dead listing, empty, ok_no_jobs) keep their own buckets. The zero-kept guard adds `details_broken` to its broken-prior-bucket set (a details-broken prior read is not clean-zero evidence). Deliberate non-change recorded in the plan: adding `needs_review` to the guard's diagnosis refusal set would break every marker promotion (the trusted-empty page's own diagnosis is the needs_review fall-through) — proven by test run and reverted; the bucket-based hardening is the real fix. **S3:** `looks_like_server_template_artifact` (page_gating) now also rejects client-side template seams — EJS/ERB `<%= x %>`/`<% code %>` and JS `${…}` interpolation — at the same two layers (detail-candidate ingestion + noise titles), so Konami's `<%= official_site %>` href (HTTP 400ing run-over-run, the hold-tail Wave-0 finding) can never become a candidate; `$`/`%`-bearing legitimate URLs and salary titles pinned as non-matches. **S2 as guardrail tests only** (Wave 0 found the flagged marker false-positive lived in the probe tooling, not the repo detector): the Astrum shape (script-block JSON no-openings string + 19 real role links → repo visible-text detector False) and the visible-marker-with-19-links guard refusal are pinned in `tests/jobs_static/test_static_zero_kept_guard.py`; `tests/jobs_static/test_taxonomy_details_broken.py` pins the details_broken contract (Mundfish shape, threshold edges at ≥80% failures and ≥3 candidates, strong-classification precedence, context-builder spellings); `tests/jobs_static/test_static_parser_noise_titles.py` pins the client-seam class plus the end-to-end `_append_detail_candidate` Konami rejection. 993 static/jobs/taxonomy tests green; changed-mode gate exit 0.
- **Bridge wire contract goes canonical-only for per-source health counters (alias-collapse Phase 5, the operator-approved contract change).** With persisted state already canonical-only (Phase 4), the alias spellings (`lastJobsKept`, `zeroJobStreak`, `failureCount`) no longer appear on any wire surface: `_source_health_row` (report triage), `_fetch_report_source_state_row` (registry-conflicts fetch-report merge, whose `failureCount` parent read also went canonical), `SOURCE_HEALTH_FIELD_NAMES`, the conflict diff fields/labels, and the shared fetch-report normalizer (`normalize_fetch_report_source_row_base` — alias output keys and their fallback option kwargs deleted) all emit the canonical counters (`lastKeptCount`, `consecutiveZeroKept`, `consecutiveFailures`) only; `frontend/admin/render/registry-conflicts.js` reads canonical; and the registry-conflict join was renamed `_join_source_health_fields`, now normalizing legacy alias-only state rows to canonical at the join via the leaf's `fill_canonical_counters` (display-side copy — persisted state untouched) so pre-Phase-4 state files keep rendering correctly. The policy leaf drops the now-callereless `emit_with_aliases`/`heal_counter_aliases` helpers, leaving one heal direction; the registry-row evidence chains (`registry_conflicts_row_core`, `registry_conflicts_row_audit`) dropped their dead alias fallbacks. Tests updated to the canonical-only contract: the parity test now asserts aliases never survive normalization onto bridge or jobs payloads, the bridge/admin suites pin canonical output keys (source-state fixtures retain alias-only rows as legacy-input exercise), the frontend registry-conflicts render fixtures went canonical, and `tests/test_source_counter_alias_guardrails.py` replaced the Phase-4-era wire-emit alias-carriage guard with a canonical-only grep guard over the emit surfaces (fails if any alias spelling or alias-fill heal reappears on the wire). Record: `docs/plans/source-health-counter-collapse-plan.md` (Phase-5 acceptance evidence); `docs/DATA_CONTRACT.md` notes the canonical-only counter surface.
- **Guarded ok/0 classification: live-200 zero-link static pages read as observed-empty, letting overdue rows drain (systemic classification-gap fix from the 2026-09-09 overdue triage).** A kept==0 read of a live listing page was blanket-classified `status=error` ("no jobs extracted from source pages"), which the availability drain treats as broken evidence — rows sourced from such a page could never be marked missing and persisted as `verification_overdue` forever. New leaf `src/jobs/adapters/static_zero_kept_guard.py`: `promote_clean_zero_kept` promotes a zero-kept read to a clean ok/0 outcome **only** when the read is provably trustworthy — an explicit no-openings marker in the fetched listing HTML (cache-backed probe of ≤3 pages) or a prior clean zero read (`consecutiveZeroKept >= 1` with a non-broken `lastFailureBucket` and prior status ok-with-no-error or the generic extraction-zero error, so transport failures never count). It refuses (keeps the error path) on browser-fallback recommendation/attempt, listing timeouts, dead-listing evidence, broken classifications, js/anti-bot/site-changed diagnoses, canonical-dropped rows, detail candidates or visited detail pages (parser found job-like links it failed to extract — needs review, not emptiness), and unreadable page bodies. The stamped evidence (`empty_confirmed`/`legit_empty`/`no_openings`, `emptyConfirmedEvidence`) is consistent with the taxonomy recompute chain so `update_source_detail_taxonomy` preserves it; both zero-kept funnels consult the guard (`_record_empty_plugin_result`, `_finish_generic_source` promote-else-error without skipping the completion tail). Live verification (record: `docs/snapshots/zero-kept-guard-2026-09-10.md`; evidence `tmp/zeroguard-20260910/`): the targeted forced pass over the 12 hold-tail sources adjudicated all 12 correctly — **3 sources recovered 43 real jobs** (BKOM 6, PlaySimple 36, Brain Up 1; stale failures over live boards, with Brain Up's overdue row re-verifying and draining immediately) and the guard **refused all 9 extraction-trouble cases** (404ing detail candidates → site_changed/needs_review; JS shells → js_required) instead of fabricating empties. Run5's forced pass also exposed and fixed the **ESAPI/velocity template-artifact class**: zoho boards' server HTML leaks literal `'+$ESAPI.encoder().encodeForHTMLAttribute(data['website'])+'` fragments that the parser extracted as detail candidates (HTTP 400 → source error flap) — the same artifact class behind the two March BKOM/PlaySimple junk rows; a new `looks_like_server_template_artifact` detector (page_gating) rejects them at both row and detail-candidate ingestion. Final forced pass with the ingest fix: **overdue 31 rows / 9 sources / Δ −2, verdict healthy** — both junk rows drained (`unavailable`, removedAt stamped), BKOM/PlaySimple fetching clean ok/0 with zero artifact-400 errors, and the 9 remaining extraction-broken sources circuit-breaker quarantined (retrying post-quarantine, rows correctly preserved pending real repairs). 16 guard tests + 7 artifact-regression tests + 923 static/adapter tests green.
- **Bamboo/breezy/workday 9-row promotion wave: every remaining mis-registered provider board migrated, with two zero-failure passes and the second structured-adapter discovery of the week.** The 9 remaining `provider_migration_candidate` rows (6 bamboo tenants, the ILLFONIC breezy board, and the Aristocrat + Tencent-TiMi workday boards — all widget/SPA surfaces mis-registered as static rows on the *same provider URL*) were validated and promoted through the sanctioned flow (record: `docs/snapshots/bamboo-promotion-wave-2026-09-10.md`; evidence `tmp/bamboo-wave-20260910/`). Validation: targeted 3-family pass 0 failed (bamboo 94/91, breezy 25/24, workday 103/103); bounded tenant probe confirmed all 6 bamboo tenants live (wolcen with 1 opening, 5 live-empty). Promotion: `transition_registry_to_active` + `normalize_manual_promotion_rows` for all 9; the 9 static twins tombstoned `superseded_by_provider` (1 active bucket, 8 pending bucket; tombstones 149 → 158); active 2,159 → 2,167, pending 847 → 830, active seed +9 (1,891), metadata map consistent at 2,997. The workday adapters are the win of the wave: **aristocrat 136 and tencent 197 feed rows where the static rows had produced 0 for months** (board sizes workday pagination makes static scraping structurally blind to). Regular-lane verification: output 40,428, **0 failed sources** (second consecutive zero-failure pass), verdict healthy 34/Δ0, per-tenant feed attribution confirmed (aristocrat 136, tencent 197, wolcen 1, dinopoloclub 1, illfonic 1; live-empty tenants at 0 — their structured adapters now poll directly so any future opening lands within one cadence). Registry/seed/definition suites 29 passed.
- **Overdue repair/prune wave executed: chronic overdue floor 100 → 34 rows (Δ −66, target ~35 hit), and the first zero-failure full pass on record.** The evidence-backed triage dispositions (`tmp/overdue-triage-20260909/dispositions.json`) were executed through the sanctioned lanes (record: `docs/snapshots/overdue-repair-prune-wave-2026-09-10.md`; executors `tmp/overdue-wave-20260909/`). **Repair:** High 5 Games' 999-walled LinkedIn row was superseded by the studio's real BambooHR board (`high5games.bamboohr.com/careers`, `careers/list` totalCount 12 re-verified pre-execution) via the staged → validated → promoted provider-migration flow (family pass 94/91, 14 High-5 rows in the feed with live bamboo detail URLs; LinkedIn row retired + tombstoned `superseded_by_provider`). **Prune (9 sources, 46 overdue rows + 3 twins):** notorious.gg (28 junk rows), Crater, ZA/UM, Rainbow Unicorn, Killmonday, all three misattributed tranzfuser rows, Lion Game Lion (stale teamtailor widget over a deleted tenant), the BITKRAFT Joyride shell (+ dead `onjoyride.com` pending twin), and the wave-B Fanatee straggler — all reconfirmed live-200/zero-links or dead on a third consecutive probe before tombstoning via sanctioned `add_tombstone`/`save_tombstones` (136 → 149; active 2,170 → 2,159; active seed 1,890 → 1,882 with the High-5 promotion added; metadata map consistent). **Verification:** the pruned rows drained through the Fix A missing path (overdue 100 → 54, Δ −46, in the incremental pass); failed sources collapsed 72 → 1 → **0** (chronic failure tail gone); the post-promotion regular-lane pass reports overdue **34 / 12 sources / Δ −20, healthy** — the floor is now the HOLD/RECORD-ONLY tail (Mundfish 10, Steer 10, Reflecto 3, Big Moxi 2, Exit VR 2, ≤1-row fetcher-defect cases), pending the systemic live-empty classification fix. Registry/seed/definition suites 29 passed.
- **Alias-collapse Phase-4 live acceptance: the first alias-free state file landed, after two persistence-funnel defects were found and fixed.** The forced full pass (`tmp/alias-collapse-20260910/run1.log`: output 42,114, 72 failed sources, verdict healthy 100 overdue / 22 sources / Δ−1) wrote state at 18:35:26Z that still carried **all three alias keys on all 4,989 rows**. Root causes were the funnel, not the derive: (1) the leaf's fused two-direction heal re-added alias keys on every normalize+save cycle, re-emitting exactly what the derive stopped writing; (2) the state normalizer's canonical-only whitelist coerces absent counters to 0 *before* the heal ran, silently zeroing alias-only legacy rows — the Phase-2 "convergence" test had passed vacuously on all-zero rows and asserted the wrong final shape. Fix: persistence now uses `fill_canonical_counters` (canonical := alias, alias keys **consumed**) applied **before** the whitelist on a copied row; alias-fill (`heal_counter_aliases`) is reserved for the bridge display join and the Phase-5 wire surface. Re-acceptance: in-memory normalize audit of the real payload (4,989 rows: 0 aliases, 0 canonical drift), sanctioned `read_source_state`→`write_source_state` round-trip byte-clean in temp (0 dropped, 0 field diffs), then in place — **0 alias keys, 0 counter drift, metadata intact**. Guardrails pin both heal directions separately (with a values-preserved round-trip and a persisted-text alias guard) and grep-fail if the persistence funnel imports the alias-fill heal; 28 focused tests green. Plan doc records the full audit under Phase-4 acceptance evidence.
- **Alias-collapse migration executed (Phases 1–4 + 6 of the registered plan) — canonical counters are the only stored fact in persisted state.** Per `docs/plans/source-health-counter-collapse-plan.md`: a new shared policy leaf `src/shared/source_counter_aliases.py` now owns the alias map (`lastJobsKept`→`lastKeptCount`, `zeroJobStreak`/`zeroKeptStreak`→`consecutiveZeroKept`, `failureCount`→`consecutiveFailures`), canonical-first `read_counter`, canonical-derived `emit_with_aliases`, and the leaf-owned heal definitions (persistence fills canonical only and consumes aliases; display fills aliases — corrected by the Phase-4 live acceptance audit recorded in the following entry). The derive in `state_source_records.py` reads via the leaf and — the single-writer change — **no longer emits alias keys into persisted state**; the state normalizer heals legacy alias-only rows on load (no bulk migration; the normal read-modify-write cycle converges old files, asserted by a new storage test). In-repo readers converged off hand-rolled alias fallbacks: `source_registry_policy` (three sites), `registry_conflicts_row_core`, `registry_conflicts_automation_provider`, `pipeline_loader_selection`, and `state_incremental` (whose defensive third spelling `zeroKeptStreak` is retired — zero occurrences in real state). The shared fetch-report normalizer's option defaults flipped canonical-preferred for repo-side producers. The bridge emit surface (`_source_health_row`, `_fetch_report_source_state_row`) still carries aliases for the Admin UI — Phase 5 (the operator-approved contract change) was deliberately NOT executed and is pinned by the new guardrail test `tests/test_source_counter_alias_guardrails.py` (alias-map completeness, persistence single-writer grep guard, wire-emit alias carriage). Pre-migration divergence audit against real state: zero divergence across 4,988 rows, so the alias-key drop is information-loss-free. 53 focused + 648 bridge/admin tests green; changed-mode gate exit 0; plan doc updated with the execution state and Phase-4 acceptance evidence.

- **`normalize_availability_health` no longer fabricates the `overdueCount: 0` default for absent input — progress payloads carry no fake health numbers.** The last structural residue of the baseline-poisoning class: absent (or empty/non-dict) input now normalizes to `None` at the shared normalizer (`src/shared/availability_report.py`), so the pipeline payload builder (`src/jobs/common/contracts_fetch_report.py`) and the bridge display normalizer (`src/bridge/report_normalizer.py`) emit **no** `availabilityHealth` field for progress/failed payloads that never observed the lifecycle. Proven through the real artifact round-trip: a normalized progress payload written through `write_fetch_report_summary_artifact` now produces a summary artifact with no `availabilityHealth` key at all (previously: the default shape `status=""`, `overdueCount=0`); terminal payloads pass through verbatim and the terminal-baseline reader returns `None` for mid-run summaries even without the dedicated baseline artifact. Present payloads keep field-level coercion — empty status and zero counts on a *present* payload are honest readings, and the terminal-baseline shape gate (not the normalizer) decides baseline eligibility. Reader-gate fixtures updated to construct the historical poison shape literally (pre-fix artifacts on disk can still carry it); new normalizer contract tests pin the no-fabrication behavior; 57 focused tests green across verdict, baseline-IO, terminal-harness, normalization-parity, and operational-truth suites.

- **Widget-audit 5-repair wave promoted to active with seed swaps and tombstones — first regular-lane fetch confirms the migration end-to-end.** All five staged provider-migration rows were promoted pending → active through the sanctioned flow (wave-B Fanatee/inXile precedent), with the lean-registry gotcha applied (every IO through the `src.source_registry_io` merge load/save on logical `.json` names; metadata map verified intact at 3,018 entries after the write): `transition_registry_to_active` (reason `manual_source_promotion`, actor `ai_widget_wave_promotion_20260909`) set `registryState=active`/`candidateState=live`/`enabledByDefault=true` (pending 853 → 848); the 5 superseded static rows (`grand.gs/careers`, `www.riftgaming.gg/careers`, `www.wetaworkshop.com/about-us/careers`, `tornbanner.com/careers/`, `voldex.com/careers/#jobs`) were retired from active and tombstoned via sanctioned `add_tombstone`/`save_tombstones` (bucket `active`, reason `superseded_by_provider`; tombstones 131 → 136); the tracked active seed carries the swap (4 static rows removed, 5 promoted rows added in promoted shape; 1,889 → 1,890 rows). Every read-back check OK: promoted rows active/live/enabled, statics absent AND tombstone-resolved, zero pending leaks, exact count deltas. Verification ran in the **regular lane without `--include-pending-provider-migration`** — that is the promotion proof: exit 0, 0 failed sources, output 41,295 (slot `jobs-fetch-report-run-20260909-162440`); lever/teamtailor/ashby family fetch+kept are byte-identical to the morning forced run (310/284, 141/131, 99/96), bamboohr's −2 fully attributed to the 6 still-pending bamboo migration rows the forced lane fetched (not a regression); feed rows per promoted studio confirmed (Rift 2, WETA 4, TornBanner 2 matching staged evidence; Grand 43 studio total, Voldex 7). `availabilityHealth` stayed healthy at `overdueCount` 101 / `overdueDelta` 0 through the promotion. Registry/seed consistency suites 21 passed. Evidence: `tmp/widget-wave-20260909/` (`promote_five.py`, `promotion-run.log`, `promotion-result.json`); snapshot section appended in `docs/snapshots/widget-ats-sweep-2026-09-09.md`.

- **Availability-health attribution and baseline poisoning fixed — verdicts trustworthy on every run shape.** Two compound bugs shipped with the Fix C verdict wiring made the health payload lie on real runs. (1) **Attribution:** the finalize call site passed `lifecycle_rows.values()` — the entire ~71k-row lifecycle map (available/unavailable/overdue) — into `_overdue_by_source_counts`, whose contract expects overdue-only rows, so `overdueBySource` attributed every row as overdue (`google_sheets: 61,472`, `overdueSourceCount: 1,607`) against `overdueCount: 101`; the call site now filters to `availabilityStatus == "verification_overdue"` (corrected read-only reconstruction: **101 overdue across 23 sources** summing exactly to the scalar; notorious.gg 28, LinkedIn/High-5 20, Steer 10, Mundfish 10). (2) **Baseline poisoning:** mid-run progress overwrites of the compact summary artifact normalize their payloads through `normalize_fetch_report_payload`, and `normalize_availability_health(None)` returns a default with `overdueCount: 0`, which the compact writer preserves — so every terminal finalize since Fix C read a poisoned baseline of 0 and reported `overdueDelta == overdueCount` (run5's real `-3` was the last honest delta). The baseline now lives in a dedicated artifact `data/jobs-availability-health-baseline.json` written ONLY from the terminal finalize path (`write_availability_health_baseline` in `src/jobs/finalize_availability.py`, wired in `pipeline_finalize.py` after the summary write; `PipelinePaths.availability_health_baseline_path`); `_previous_availability_health` prefers it and falls back to the summary for pre-baseline continuity, and both reads reject non-terminal payloads (status not in {healthy, degraded}, non-int/bool/negative overdueCount) so progress/failed/default shapes can never seed a baseline; failed terminal runs observe no lifecycle and write none. Validated read-only against the real lifecycle state: corrected verdict reads **healthy with delta=0** — the true steady state after the Fix A drain. The next terminal run computes one final delta against the last poisoned-era summary baseline, then deltas are honest run-over-run. Focused tests in `tests/test_availability_health_verdict.py` (attribution contract) and `tests/test_availability_health_baseline_io.py` (baseline preference, fallback, roundtrip, type guards) plus a terminal-harness integration test and a failed-path negative; 39 focused tests green; changed-mode gate exit 0. Evidence: `tmp/verdict-attribution-fix-20260909/`.

- **Five widget-audit recovery candidates staged as provider-migration rows — 23 jobs into the feed on first fetch.** The embedded-ATS-widget audit's Tier-1 adjudication produced five live tenants behind zero-yield static rows, all staged through the sanctioned pending provider-migration lane (`transition_registry_to_pending` with `pendingReason="provider_migration_candidate"`, `candidateState="staged_provider_candidate"`, `createdFromAdvisory=true`, `migrationSourceIdentity` pointing at the superseded static row; pending registry 848 → 853, runtime lane only — the seed intentionally untouched): `lever:account:grand` (Grand Games — lever postings API verified live with 9 postings, Istanbul), `teamtailor:listing_url:https://jobs.riftgaming.gg/jobs` (custom-domain Teamtailor tenant, the yodo1 precedent — one audit correction: the tenant is Teamtailor, not Lever), `teamtailor:listing_url:https://wetaworkshop.teamtailor.com/jobs` (4 live postings), `bamboohr:listing_url:https://tornbanner.bamboohr.com/careers`, and `ashby:board_url:https://jobs.ashbyhq.com/voldex`. Targeted validation pass over the four family loaders with `--include-pending-provider-migration` (validation-only visibility, per the runbook): exit 0, 0 failed sources, all 5 staged rows `status ok` and each kept jobs on the first fetch — Grand 9/9, RiftGaming 2/2, Weta 4/4, TornBanner 4/4, Voldex 4/4 (evidence slot `jobs-fetch-report-run-20260909-093844`); output 41,841 carries all 23 rows (Grand with Istanbul/TR, Rift with Stockholm/SE, Voldex with Remote; TornBanner 4 fetched → 2 in feed after dedup). Promotion through the normal approval flow (Fanatee/inXile precedent) is the separate next step, now fully evidence-backed. Registry/seed/health guardrail suites green; changed-file gate exit 0. Evidence: `tmp/widget-wave-20260909/`.

- **GSC Game World repointed to the live PeopleForce board — 9 jobs recovered.** The failure-tail hold "peopleforce staging candidate" dissolved on probing: `gscgameworld.peopleforce.io/careers` ("GSC Game World - Job openings") is **fully server-rendered** (10 vacancy links, 8 real roles, server-rendered detail pages), so no peopleforce adapter is needed — the correct lever was a static repoint of the 404ing `gsc-game.com/index.php?lang=en&tab=career` row (seed + runtime dual-path identity rewrite via the sanctioned writers, byte-stable seed swap, registry stable at 2,170). Targeted pass: **status ok, 10 fetched / 9 kept** (one pseudo-entry filtered), 0 failed sources; evidence slot `jobs-fetch-report-run-20260909-083248`. No tombstone needed (repoint, not retirement); the three pending gsc-game.com twins stay for the runtime twin machinery. Evidence: `tmp/gsc-peopleforce-repoint-20260909/`.

- **Yodo1 recovered via static-to-teamtailor provider migration — 14 jobs into the feed.** The failure-tail triage's repair-candidate lever (static leaf / list-only plugin) was disproven by deeper probing: the static careers page's 17 server-rendered `/job/<slug>` links are **all 404** (stale Webflow CMS remnants), while the page embeds the public Teamtailor jobs widget (public api key, tenant resolved via `careersite-job-url` links to `careers.yodo1.com` — 10 live jobs through `api.teamtailor.com/v1`, 14 with pagination). Executed as a provider-row migration per the RTL phenom precedent: added `teamtailor:listing_url:https://careers.yodo1.com/jobs`, removed + tombstoned the dead static row (`site_changed` / "HTML contains teamtailor signature — consider adapter reclassification" in run5), dual-path with a byte-stable seed swap (registry 2,170 stable, seed 1,889 stable). Targeted `teamtailor_sources` pass: exit 0, 0 failed, 134 fetched / 124 kept family-wide; **14 yodo1 rows active in the feed**. Locations are genuinely absent in the ATS (0 location includes; only unstructured "Remote"/"China" body text) — family-consistent, not an extraction gap. Evidence: `tmp/yodo1-teamtailor-repair-20260909/`.

- **Per-source health counters no longer split-brain; healthy rows stop being labeled warning/broken.** `derive_source_health_fields` (state persistence, `src/jobs/state_source_records.py`) and `_source_health_row` (report triage, `src/jobs/common/contracts_source_health.py`) read the legacy aliases (`lastJobsKept`, `zeroJobStreak`, `failureCount`) *before* the canonical counters (`lastKeptCount`, `consecutiveZeroKept`, `consecutiveFailures`) that the run appliers update fresh each pass — but the aliases only exist because the derive itself writes them back, so a stale alias (e.g. `zeroJobStreak: 45` on Blizzard, kept 36) overrode the fresh counter on every subsequent run and was then persisted again: a self-perpetuating split brain that mislabeled healthy rows as `warning`/`broken` ("latest fetch kept no jobs"/"repeated zero-job fetches"), fed the registry-conflict merge surfaces, and could trip the incremental empty-source cache skip via the streak readers. Both readers are now canonical-first, so the derive's write-back *heals* stale aliases instead of perpetuating them. Validated read-only against the run-20260908-233220 phApp-family slot: all 15 kept>0 rows derive healthy (12 alias-only-stale heal immediately; 3 rows whose canonical `consecutiveZeroKept` the pre-fix derive had itself corrupted — Blizzard 45, King us/en 10, Activision 2 — heal after one applier pass, which zeroes the streak on a kept>0 success), and the 2 healthy-empty rows stay `warning`. Regression tests cover the phApp shape, the persistence write-back, and the reverse direction (a stale *healthy* alias cannot mask a real zero-streak); 43 focused tests green.

- **Blastworks board repoint executed via pending promotion — 5 jobs recovered.** The failing `blastworksinc.com/careers/open-positions/` row (run5 no-jobs-extracted; path 404 on both 2026-09-08 re-probes) was retired after the cross-bucket save guard revealed the desired URL already existed as a **pending row** (`www.blastworksinc.com/careers/`, auto-demoted in May as the conflict twin of the then-canonical open-positions path, jobsFound 4, medium confidence) — the Miniclip batch-4 pattern. Executed via the real active transition: pending 849 → 848, active count stable at 2,170, tombstones → 130 (the executor's `add_tombstone` result was not persisted — review-8-class fix-up rebuilt it from the pre-mutation backup through the sanctioned save), and the active-seed registration was swapped for the promoted row (byte-stable CRLF rewrite). Targeted pass: source **ok, 5 fetched / 5 kept** through the rendered fallback (js_shell → empty_page → Playwright), per-run evidence slot `jobs-fetch-report-run-20260908-232232`. Notes: the healthy-empty `gamingrealms.com/careers/` sibling row is a different host's board (Gaming Realms owns the brand); the apex pending twin (low confidence, 0 jobs) is left for the runtime twin machinery. Registry/IO guardrail suites green (61 tests); changed-file gate exit 0. Evidence: `tmp/blastworks-repoint-20260908/`.

- **Treyarch phApp twin reconciled via the WP17 path — no new plugin needed.** The failing GameDevMap row `treyarch.com/careers` (run5: no jobs extracted) embeds the same client-rendered phApp widget as the healthy tenant row `careers.treyarch.com/`, which run5 shows **status ok, 8 fetched / 8 kept** through the WP17 shared phApp sitemap adapter (sitemap contract live: 8 `/job/{code}/{slug}` URLs). The marketing host itself serves no phApp sitemap contract (probe 2026-09-08: both sitemap endpoints return non-sitemap 200s with 0 job URLs), so adapter routing would yield 0 — the correct disposition is retirement of the redundant second registration, not a repoint or plugin. Executed via the sanctioned dual path: `add_tombstone`/`save_tombstones` (tombstones 128 -> 129), registry 2,171 -> 2,170, seed 1,890 -> 1,889 (byte-stable CRLF rewrite, pure-deletion diff), healthy twin row verified present on both stores. Registry/seed guardrail suites green (41 tests); changed-file gate exit 0. Evidence: `tmp/treyarch-phapp-20260908/retire-result.json`.

- **Failure-tail prune wave executed: 11 confirmed dead boards demoted with tombstones (registry + seed).** The 13 prune candidates from the 2026-09-08 failure-tail triage were re-probed (bounded read-only, primary + alternate-board sweep per studio host, 41 URLs): all 13 primaries reconfirmed 404 with zero job links; run5 coverage confirmed every candidate carried 0 output rows, so no feed loss. 11 rows demoted via the sanctioned `add_tombstone`/`save_tombstones` path (registry 2,182 -> 2,171; 11 tombstones, total 128) and the 10 seed-member rows removed from `source-registry-active.seed.json` (1,900 -> 1,890; byte-stable CRLF/ensure_ascii rewrite, 641-line pure-deletion diff) so seed restores cannot resurrect them. Two candidates HELD as repoint/repair candidates instead of pruned: **Blastworks** (`/careers/` alive with server-rendered role slugs — repoint target `https://www.blastworksinc.com/careers/`) and **GSC Game World** (`/careers` alive with a live `gscgameworld.peopleforce.io` ATS — peopleforce staging candidate). Shortgun's failing `/career` row pruned while its live `/careers` sibling row stays active. Hasbro follow-up noted: `hasbro.com/en-us/careers` renders zero server-side roles (Workday-class embed). Evidence: `tmp/failtail-20260908/reprobe-results.json`, `reprobe-inspect.json`, `prune-exec-result.json` (count/stale-id/tombstone/seed read-back verification all true). Registry/seed guardrail suites green (56 tests); changed-file gate exit 0.

- **Availability-health verdict now reflects reality, with per-source overdue attribution.** `pipeline_finalize` computed `availabilityHealth.status` as degraded whenever `degradedCoverage` was true — but the 1,000-check sweep budget defers most of a ~46k-row registry on every pass by construction, so the verdict could only ever read degraded, even when 97.96% of active rows were verified within 7 days (target 95%) and the overdue population was falling. The verdict is now produced by the pure builder `evaluate_availability_health` (`src/jobs/availability_schedule.py`): degraded only when the verified-coverage target is missed, overdue rose more than the hysteresis band (`OVERDUE_RISE_ABSOLUTE = 10`) versus the previous terminal run, or identity invariants are unresolved. The previous terminal run's full health payload is read pre-write from the compact fetch-report summary artifact (`_previous_availability_health` in `src/jobs/finalize_availability.py`; failed-run payloads carry no `overdueCount` and are no baseline at all), and `degradedCoverage` is retained as a capacity diagnostic that no longer feeds the verdict. Monitoring: the health payload now carries `overdueBySource` (top-`OVERDUE_BY_SOURCE_LIMIT`=20 task source keys by overdue count, e.g. `static_source::static:listing_url:https://welevel.jobs.personio.com/`: 53), `overdueSourceCount` (full source population, 52 on the 2026-09-08 state), and `overdueBySourceDelta` (run-over-run per-source change, where a source absent from the previous baseline counts as new); the payload is self-contained (includes `overdueCount`) so a verdict can be fed back in as the next run's baseline. New fields are preserved verbatim by the compact summary writer and allowed additively by the bridge normalizer (signed per-source deltas supported). Validated read-only against the real state: the 289 overdue rows attribute across 52 sources matching the failure-tail triage row-for-row, the verdict flips degraded -> healthy with the healthy-coverage evidence, and a simulated post-Fix-A drain reads healthy with a falling delta. Focused tests in `tests/test_availability_health_verdict.py`.

- **Retired-source availability rows now drain instead of stranding as verification_overdue.** Lifecycle rows whose source is absent from the finalize source-evidence universe (eligible + failed + skipped, which includes every registry/cadence/circuit-breaker exclusion report) belong to retired, tombstoned, repointed, or pending registry rows that no loader can re-observe; they previously sat preserved forever as `verification_overdue` (289 rows on the 2026-09-08 full pass, of which 166 belong to already-retired sources). The missing path now finalizes them to `unavailable` (aging to `archived` via the standard 14-day window) with a `retiredSourceDrained` summary counter and `retiredSourceDrainedCount` in the report's lifecycle summary; failed/skipped shields, custom-loader flows, and re-observation resets are unchanged (focused tests in `tests/test_jobs_lifecycle_retired_source_drain.py`).

- **Availability-identity collision repair now resolves residual URL conflicts deterministically.** The preflight diagnostic keeps the full conflicting-row evidence, then preserves the URL-derived owner, reassigns URL-bearing inheritors to their own SHA-256 identity, and quarantines rows that cannot mint a distinct identity because they have no public URL. Primary and fetched-only observed segments now contribute consistent replacement/quarantine metadata without duplicate rejection records. Focused availability verification is green (`17 passed`), and the complete static-plugin suite is green (`165 passed`).

- **Gameberry/Keka and Two Robots now have rendered-card coverage.** `gameberry.keka.com` is routed through the generic rendered-card plugin; its Keka shell is followed to the active-jobs API so the board yields 12 job cards with locations, while `trb.tworobots.com` uses a dedicated server-rendered role-card leaf with query-anchored links for its shared Airtable apply target. Fixtures cover both shapes with 4 Two Robots roles.

- **`detect_js_shell` widened to catch cookie-challenge interstitials (CUPID/slowAES class).** DoubleU's careers page (and IggyMob's before it lapsed) serves a ~780-byte interstitial whose only content is a script that AES-decrypts a cookie, sets it, and reloads with `?ckattempt=1` — invisible to the SPA-token detector, so the source classified as `fetch_ok_extract_zero` and never escalated to the browser fallback, even though a real browser executes the challenge and lands on the actual board. `_heuristics.detect_js_shell` now detects the challenge family via the `cupid.js` script-src token or the inline `slowAES.decrypt` + `document.cookie=` pair (both required inline, so pages that merely bundle a similarly-named helper or discuss cookie APIs in prose stay negative). 3 new tests in `tests/jobs/adapters/plugins/static/test_wp4_js_shell_detection.py`; static suite 164 green. Targeted pass on DoubleU confirms the escalation fires (Playwright attempted, classification `needs_review`); the board itself is currently origin-down (real-browser navigation fails at connection level), so recovery lands when the site returns — the enabler is permanent either way.

- **New Phenom (Jobs2Web) provider adapter — RTL Enterprises recovered into the feed.** The RTL entertainment careers page held since batch 4 because its job links redirect into the Phenom ATS hub (`jobsearch.createyourowncareer.com/RTL/`), which looked JS-only. Live probing found the tenant's `/search/` page is in fact **fully server-rendered** (J2W-era table markup, 50 `/RTL/job/<slug>/<id>` anchors per page pair, `startrow=` pagination). The new `phenom` adapter (`src/jobs/adapters/parsers/phenom.py` parser leaf + `plugins/provider_api/phenom.py` runner, registered as `phenom_sources`) parses the search-results table (title from the anchor, location from the row's `jobLocation` cell, stable ids from the numeric posting id) and pages through `startrow=` links up to a bounded budget; the runner normalizes any deep portal URL to the tenant search root. RTL's registry row migrated from the failing static row to `phenom:listing_url:https://jobsearch.createyourowncareer.com/RTL/` (dual-path, tombstone via `add_tombstone`). Targeted pass: `phenom_sources` **fetched 150 across 3 pages, 25 unique jobs kept into the feed** (output 40,794 → 40,819). 8 new focused tests (`tests/test_provider_phenom_adapter.py`); gate exit 0. The adapter also unlocks the many other J2W/Phenom-tenant careers portals (universities, enterprises) for future staging.

- **Availability-identity preflight failures now dump diagnostics.** The full-pass `AvailabilityIdentityPreflightError` (`post_filter_identity_invariant_failed`) raised without any per-row evidence, making it undiagnosable from the log. `_dump_preflight_diagnostics` now writes `tmp/availability-preflight-diagnostics.json` (missing-identity sample + conflicting availabilityId→token map) before the raise. First use pinpointed the failure: 7 availabilityIds each mapping to 2 distinct URL tokens (rows sharing an id across different URLs after dedup) — 0 missing identities.

- **Batch 4 + the provider-promotion wave are executed and verified.** Batch 4 dispositioned the last 10 unsafe-redirect primaries with live-probe pre-flight + web corroboration: 7 repoints (Reply 301 kept, Welevel 25, Coffee Stain North 3, Poncle 1 via its real `workwithindies` board found by web search, Miniclip resolved by **promoting the pending vacancies row and tombstoning the redirect-blocked corporate row**, RTL to the entertainment jobs page, Wildcard to `studiowildcard.bamboohr.com/careers`), 2 prunes (Andarion lapsed; Kalloc left games for Fuzor AEC), 1 hold (Reflector — Dayforce JS portal). The Fanatee and inXile staged provider rows were promoted through the real active transition with their superseded static rows retired (active seed +1 net; tombstones via `add_tombstone`/`save_tombstones`). Verification caught two promotion mechanics traps now on record: a promotion that loads registry rows via raw gzip instead of `load_json_array` makes the sanctioned re-split save **rebuild the metadata map from lean payload rows** (metadata 3,038 → 2 entries; restored by grafting the pre-mutation backup definitions back through the sanctioned path), and `add_tombstone`/`save_tombstones` is the only tombstone write path — plain `.json` appends land in an invisible fallback candidate the loader never reads. Wave C rendered-probed the 5 `site_changed` rows: IggyMob pruned (hosting-404 lapsed site), Sharkbite held (403 bot-wall), DoubleU held (CUPID JS cookie challenge — `detect_js_shell` widening is the enabler), Gameberry (Keka ATS, renders live cards as a link-less SPA) and Two Robots (JS-injected roles) kept as rendered-card candidates. Targeted passes verified all touched sources `status ok` (output 40,788 → 40,794); registry guardrail group + 52 focused tests green.

- The 2026-09-07 post-batch-3 plan is fully executed. **Wave A** staged the two viable adapter-reclassification candidates from batch 3's notFound cohort as pending provider rows (Fanatee `lever:account:fanatee` — 8 live API postings, projected-registry validation 5 kept; inXile `bamboohr:listing_url:https://inxile.bamboohr.com/careers` — healthy-empty scam-warning-only board; Lion Game Lion not staged — its TeamTailor board is 404-dead, the static signature was a stale embed). **Wave B** ran the consolidation full pass (slot `20260907-123818`, 12:02–12:37 UTC, read the post-batch-3 registry at 2,183 active): the failure tail fell **123 → 92** with the 16 remaining repoint targets all `status ok` (57 kept) and the IPv6 failure class at **zero**; the output delta (42,358 → 40,738) is dominated by `gracklehq` (−1,199), live-probed as an origin-side read-hang (hold, same class as the Roblox tarpit) — not a regression from the registry work. **Wave C** added a registry guardrail that fails definition-less static seed rows (the batch-3 lean-registry trap), wired into the repo gate with runtime merged-view tests. **Wave D** fixed the CarX Technologies data: URI failure class end to end (below) and triaged the no-jobs-extracted tail from the fresh per-run evidence: 80 rows = 5 `site_changed` browser-fallback candidates, 6 already-yielding rows with `js_required`/anti-bot advisories (browser fallback already working), and ~69 `needs_review` chronic-empty boards that stay holds.

- A discovery probe no longer harvests inline-asset URIs as job detail links, and the pipeline refuses to fetch them: the gamedevmap homepage probe skipped `mailto:`/`javascript:` hrefs but not `data:`, so CarX Technologies' 6.5 MB base64 logo href — whose megabyte-long base64 path noise inevitably matches the `/jobs/...` detail-path regex — was persisted into the registry row's `pages`/`detailPagesSample` and fetched as a listing page every pass, failing the source with `URL too long` (Battery Low Interactive carried 3 more `data:img/png` URIs). The probe now requires http(s) schemes after join, `StaticHtmlFetcher.build_request` returns None for non-http(s) fetch URLs (defense in depth), a new seed guardrail (`check_active_seed_no_inline_asset_urls`) fails any active row embedding `data:` URIs in its page lists, and the two contaminated rows were purged in seed + runtime metadata through the sanctioned `load_json_array`/`save_json_atomic` path (metadata 13 MB → 3.1 MB; CarX also dropped its hh.ru single-vacancy detail URL that 406s). Targeted verification: both rows `status ok` — Battery Low yields 2 jobs, CarX healthy-empty on its homepage (`tests/test_data_uri_page_guard.py`).

- **Batch 3 closed out the Wave D follow-up cohorts** (27 redirect rows + 19 notFound rows) with live-probe pre-flight that corrected both labels before execution: the redirect cohort split into **18 repoints** (dual-path seed+runtime; targeted pass 17/18 `status ok` with **75 jobs kept** — ARTE France 24, About Fun 12, Vivid Games 14, Playkot/Mindstorm/Tapnation/Certain Affinity/Guli 4 each) and **9 retirements** (Camel 101, Proton, Rogue Duck — targets are store/marketing pages with no listings; Impact Reality XR — rendered probe shows a VR marketing agency page; 4 Ubisoft geo rows redundant against the group's active rows; Wizards of the Coast duplicate of the already-active `company.wizards.com/en/careers` row), then two more duplicate retirements the duplicate-URL guardrail caught mid-flight (Roblox's `corp.roblox.com/careers` — 301s to `careers.roblox.com`, which already has an active row **and** tarpits non-browser User-Agents; Vivid Games' static row — superseded by the yielding `teamtailor:listing_url:https://jobs.vividgames.com` provider row). The notFound cohort produced **zero prunes**: all 16 listing pages probe 200 with job signals — the failures are detail-page 404s from link-extraction artifacts (mailto-as-path, bare-`&`, `[thrive_page_number]` template placeholders, image-as-detail), and 3 rows are adapter-reclassification candidates (lever/teamtailor/bamboohr signatures). Registry 2,186 → 2,184, tombstones 100 → 111. Execution surfaced and fixed a real mechanics trap: the runtime registry `.json.gz` stores lean core rows while full definitions live in `source-registry-metadata.json.gz` keyed by id — direct `.gz` writes that rename ids leave new ids definition-less (pipeline joins zero pages), so all registry mutations must go through `load_json_array` + `save_json_atomic` which rebuild the metadata map; the repair is recorded in the snapshot doc (`docs/snapshots/chronic-dead-board-decisions-2026-09-06.md`).

- The chronic dead-board work is now fully executed: **Wave A retired the 19 stale-rebrand rows** whose new boards were already covered by other active registry rows (Bungie, the 2K studios, Unity, Supercell, Techland-class; runtime 2,219 → 2,200, seed −15, tombstones with target coverage re-asserted per row); **Wave B executed the remaining 18 repoints** after live-probe pre-flight (RTL Enterprises dropped to review — its target is a soft-404 group-wide careers template, not a games board; targeted pass 18/18 `status ok`, 46 jobs shipped, feed 42,411 → 42,448); and **the review-8 cohort was adjudicated with rendered probes**: Lanterns/Pixowl (Cloudflare 521), Pixel Wizards (redirect loop), The 4 Winds (402 hosting-lapsed), and Mundfish's failing duplicate row pruned with tombstones (registry 2,200 → 2,195, tombstones 95 → 100), VSTEP repointed to `our-company/careers-at-vstep/` (seed + runtime — the old path serves only through a redirect chain the fetch client refuses; the targeted verification pass then showed the new path serves full content behind a broken HTTP 500 status, so the row reclassified to a hold that recovers when the origin fix lands), Astrum held as a `bp_chl` cookie-challenge bot wall and Exit VR held as a site-level WordPress 500. The decision-list artifact now stands at 78 prune / 45 repair / 87 hold with the review bucket empty; a full pipeline pass with all mutations in was launched and its per-run evidence slot records the fresh failure tail (`docs/snapshots/chronic-dead-board-decisions-2026-09-06.md`).

- The chronic dead-board decision list is now executed in its first two waves: **73 rows pruned** (54 dead-404, 12 absorbed-rebrand whose target boards keep active coverage — asserted at execution, 7 lapsed domains; runtime 2,294 → 2,221, seed −58, 73 tombstones carrying per-bucket reasons and a pointer to the decision snapshot) and **repair batch 1 shipped 26 real jobs into the feed** (42,385 → 42,411): the 3-way THQ Nordic Mobile collision resolved by repointing HandyGames to `thqnordicmobile.com/en/jobs` (14 kept) and retiring both Massive Miniteam rows, CCP London repointed to `fenris.com/careers` and renamed Fenris Creations (CCP) after the May 2026 rebrand (3 kept), and Wright Flyer Studios repointed to `wfs.games/recruit` (9 kept) — all three sources `status ok` with per-run report evidence slots recorded.

- `source-discovery-report.json` now has the same per-run evidence protection as `jobs-fetch-report.json`: every changed write snapshots terminal payloads (truthy `finishedAt`) into `data/discovery-report-history/source-discovery-report-<runId>-<UTC-stamp>.json.gz`, one slot per run with same-run dedup and 48-slot retention, so a discovery run can no longer overwrite the previous run's terminal evidence. The slot invariant and mechanics live in a shared leaf (`src/report_history_slots.py`) used by both report writers, registered in the ship-bundle manifest (`src/source_registry_io_save.py`, `src/pipeline_io.py`, `tests/test_report_history_slots.py`).

- The committed Scrapy pin drift is fixed: `test_security_audit.py` asserted `Scrapy==2.16.0` for requirements and lock while commit `9ccf1695` had upgraded both to 2.17.0, so the full test suite failed on a clean checkout. The full suite now runs green with no deselection (4,819 passed).

- The chronic dead-board tail now has an evidence-backed disposition list instead of an ambiguous "fails every pass" status: the 210 sources that failed the 2026-09-04 full pass with zero kept rows (minus everything the post-fix passes recovered) were all live-probed — redirect targets included — and hand-curated into a final decision list of **73 prunes** (54 dead-404 careers paths incl. 2 Twitter/X junk-employer rows, 12 absorbed rebrands whose target boards already have active rows, 7 lapsed domains), **45 repairs** (24 real repoints — CCP Games rebranded to Fenris Creations, THQ Nordic Mobile has no active row despite two studios redirecting to it, Wright Flyer→wfs.games — plus 19 stale rows whose new boards are already in the registry, and 2 already executed), and **84 holds + 8 reviews** with per-bucket reasons. Measurement caveats recorded: CERT_NONE probes cannot clear TLS-dead boards (CipSoft), and Starbreeze recovered post-fix so it is not in this cohort (`docs/snapshots/chronic-dead-board-decisions-2026-09-06.md`, artifacts in `tmp/dead-board-sweep-20260906/` with `final-decision-list.json` as the actionable list).

- The remaining four "Invalid IPv6 URL" static sources (BKOM Studios ×2, PlaySimple Games, Wooster Games) are recovered: the earlier detail-heuristics guard covered only one of two crash paths, and these boards crash through the rendered-card listing path instead — Zoho Recruit careers pages embed an unrendered template href (`https://'+$ESAPI.encoder().encodeForHTMLAttribute(data['website'])+'`, Wooster's board is itself Zoho-backed at `spatial.zohorecruit.com`), and the unguarded `urljoin` on page-controlled hrefs raised a ValueError that was recorded as the source's entire failure. A new `safe_page_urljoin` helper in `static_runtime_support.py` now backs every page-controlled join seam (rendered cards, blizzard, frontier, nintendo_csod, ncsoft, feed postings, runner, detail-heuristics filter): poison hrefs return empty and skip like non-link text instead of crashing. A live sweep of all 2,100 active static listing URLs found exactly one bracket-host carrier (plexonic, already fixed) — no new latent carriers. The BKOM (GameDevMap) row was also repointed to the studio's real careers board `jobs.bkom.com/jobs/Careers` (live-verified same Zoho tenant as the existing BKOM Sheet row; `bkomstudios.com` is now a jobs-less rebranded Wix homepage) through the sanctioned runtime save path. A targeted four-source pass shipped **66 real jobs** into the feed (42,345 → 42,397), all four sources `status ok` — the last of the original 9-member IPv6 failure class is closed (`docs/plans/jobs-coverage-improvement-plan.md`).

- Previous fetch-run evidence no longer disappears when a later pipeline run starts: `jobs-fetch-report.json` is a single-slot artifact, so any run — including a targeted `--only-sources` pass whose progress shell overwrites the file at startup — destroyed the prior run's per-source failure evidence (this cost the Sep 4 full-pass failure breakdown twice in one day and forced a costly reconstruction from side artifacts). Every terminal report now upserts its own per-run evidence slot (`data/fetch-report-history/jobs-fetch-report-run-<runId|timestamp>.json.gz`) at write time, so each run's per-source evidence survives any later run independently: terminal writes finalize an outgoing different-run slot before writing, non-terminal writes only back up a *terminal* existing payload, identical payloads don't rewrite, and retention rotates at 32 slots so concurrent long/short runs never evict each other. Proven end-to-end: a targeted two-source run leaves both the live file updated and its own per-run evidence file (`src/pipeline_io.py`, `tests/test_fetch_report_history_backup.py`).

- Two rebranded studios' careers boards no longer fail behind dead domains: Just Add Water (Developments) (`www.jawltd.com` → `justaddwaterdevelopment.com/careers/`) and JoyBits (`www.joybits.org/jobs` → `joybits.games/jobs/`) were live-verified as same-company rebrands (rendered titles, name matches, same jobs plugin), and their registry rows were repaired in both the tracked seed and runtime state through the sanctioned save path — row identities (`static:listing_url:` ids) rewritten to the new URLs since row identity is the URL. A targeted pipeline pass confirms both sources now fetch with status ok and zero errors; both boards are currently healthy-empty (Jaw's "Open Positions" slider renders 0 positions), so yield resumes when the studios post openings. Neon Play's redirect to `iscoolentertainment.com` was deliberately **not** followed — IsCool Entertainment is a different studio (sister brand; Hachette acquired both in 2016–2017), so the guard's block remains correct there — and Massive Miniteam's rebrand target (`thqnordicmobile.com`) serves expired-TLS errors on both host variants, so it stays blocked until the certificate renews (`docs/plans/jobs-coverage-improvement-plan.md`).

- Job sector classification no longer counts the company name appearing in a job's own ATS/careers URL as game-sector evidence (the dominant classifier was a tautology): greenhouse/lever/workable/ashby/bamboohr job links and own-domain careers sites embed the company slug for every employer, so 7,730 rows — 47.8% of the Game sector, led by Apple (284), Marvell, NVIDIA, Cadence, CyberArk, Qualcomm, Broadcom, NXP, Thales, and Lockheed Martin — were labeled Game with zero game-keyword evidence. The tautological role-word branch is removed from `has_positive_game_evidence` and its frontend mirror; game-company rows are unaffected (keyword and provider-provenance evidence intact), and the change is measured on the full feed: Game 16,177 → 8,503 with reclassifications matching the contamination sweep's noise inventory exactly (`docs/snapshots/sector-signal-contamination-2026-09-06.md`). Regression tests cover Apple/NVIDIA-class reclassification to Tech and keyword/provenance/strict-gate invariants on both surfaces.

- Provider-adapter provenance no longer certifies rows from multi-board static sites it does not belong to: when a job's source bundle mixes provider-board items (greenhouse/lever/…) with static-adapter items — the signature of one careers site aggregating several employers — provenance now counts only if every provider item's studio is employer-consistent with the row's company. Previously one employer's board (e.g. a `Sony Computer Entertainment` greenhouse item) forced Game on a different employer's row from the same aggregated site (Polyphony Digital, Coldwood Interactive rows). Pure provider-board rows are unaffected, employer-consistent mixed bundles (PlayStation Global class) keep Game, and the frontend mirror applies the identical rule.

- The itch.io static source no longer ships parser pollution instead of jobs: the board's real postings live only at `itch.io/j/<numeric-id>/<slug>`, so everything else the parser picked up on itch.io hosts — `/jobs/<skill>/<type>` filter navigation ("Unity", "Windows", "Remote friendly"), `/games/` directory rows ("With Webcam support", Creative-Commons links), `near-*` location filters (place names, street addresses), site pages (`/directory`, `/devlogs`), and `<studio>.itch.io` game/devlog pages — is now gated as `non_job_static_page`, with the pre-existing title-vs-slug disagreement check preserved. Measured on the feed: 48 of the 53 shipped itch.io rows drop (the 5 real postings stay), zero non-itch rows affected.

- Game-keyword matching no longer reads the job's source/URL text: whole-company careers boards carry games-flavored paths for every role they list — WBD's entire site under `careers.wbd.com/.../wb-games-jobs`, Disney's `/search-jobs/game/...` search URL, the `ScientificGamesExternalCareers` Workday site path, `?q=game` query strings — so the board URL classified CNN, HBO Max, Disney, and SciGames rows as Game regardless of the actual job. `has_positive_game_evidence` (and its frontend mirror) now scopes `GAME_KEYWORDS` evidence to company/title only; `classify_company_type`'s employer-token corroboration is scoped identically so `sector` and `companyType` never disagree. Measured on the feed: 458 rows reclassified Tech (Disney 64, WBD 50, SciGames 34, CNN 21, Sonyglobal 13), zero of them game employers losing name-based evidence, and real game-titled rows on those boards keep Game (`docs/snapshots/sector-signal-contamination-2026-09-06.md`).

- The gamejobs.co source no longer ships directory-navigation artifacts instead of jobs: the board's real postings live only in `<div class="job">` cards (title anchor + company/location metadata), but the parser's whole-page anchor-chain regex matched any three consecutive links, so per-company search links (`gamejobs.co/search?c=…`), facet links, and site navigation became rows with directory counts as company/title fields ("Apple 50"/"Wargaming 51", "Digipen 47", "Hasbro 168", "Design 510", the homepage nav mega-blob) — 105 of 109 stored rows, zero real postings among them. The parser is anchored on the card structure (directory links inside cards parse as metadata, never rows), and a targeted live pass shipped 86 real postings (Bungie, teamLFG, Smile-Break, NetEase class) with zero artifacts; the stored artifact rows are no longer re-emitted and age out via the 14-day missing-row archive policy.

- The true game employers whose rows rode URL keywords recover their Game classification through curated employer-name evidence: `has_positive_game_evidence` (and its frontend mirror) now carries a `GAME_EMPLOYER_NAME_HINTS` table — Playrix, Daybreak, Metacore, Avalanche, Square Enix, Lightbulb Crew, Electronic Arts, EA Sports, EA Create — matched against the company field only. These rows were unrecoverable per-source (they share the `google_sheets` aggregator with every other studio, ride adapterless lever links, or sit on static boards excluded from provider provenance), and their names carry no game token ("Electronic Arts", "Square Enix"); hints are deliberately multi-token for short names so unrelated look-alikes (EACH1, Eataly, Eacproductdevelopmentsolutions) cannot match. Measured on the feed: Game 8,042 → 8,382 (+340), zero rows lost, per-company gains exactly equaling the keyword-scoping drops (EA family 271, Avalanche 27, Metacore 11, Playrix 11, Lightbulb Crew 9, Square Enix 7, Daybreak 4). A full `--force-refresh-all` pipeline pass on 2026-09-06 shipped the fixed signal into the stored feed: stored Game **16,177 → 8,192** on 42,254 rows (fresh fetches add/expire rows, so the stored count differs from the 8,382 replay count), with the seven recovered employer families fully Game and the sweep's noise employers (Apple, NVIDIA, Lockheed Martin class) Tech.

- Personio feeds no longer surface cryptic XML parse offsets (e.g. `not well-formed: line 5, column 328658` — an offset deep inside the marketing page's minified CSS) when a board slug is retired: dead slugs serve Personio's 1.7 MB marketing HTML page, and parser, provider runner, and discovery probe now share one marketing-payload predicate so every surface reports `personio feed redirected to marketing site` (or the runner's `site_changed` classification) instead of raw expat noise, while genuinely malformed XML keeps the `invalid personio XML` error. Verified live on the captured InnoGames/Travian payload and against a real sibling feed (traviangames.jobs.personio.de/xml → 3 postings through the unchanged parser).

- Workday CXS fetch no longer hard-fails on `*.myworkdayjobs.com` with "certificate has expired" despite a valid served chain: the OS cert store can hold a stale Workday-chain intermediate that poisons Python's chain building (browsers/curl resolve past it, which masked the failure). The CXS fetcher now anchors verification on certifi (still `CERT_REQUIRED` + hostname-checked, no downgrade) for that host family only, and no longer follows redirects on CXS POSTs — Workday's maintenance 303 now surfaces as a classified HTTP status error with the redirect target instead of a JSON decode failure, and is not retried. Verified live through the fixed path: previously-blocked tenants return real postings (e.g. SciPlay 15 rows); the two staged xboxgaming boards (Activision CentralTech, Beenox/High Moon/Infinity Ward/Sledgehammer External) were pending staging when the platform entered its maintenance window and will recover on the next fetch passes.

- Dedup provider/static auto-safe no longer miscounts digit-bearing studio slugs (e.g. `31stunion` in greenhouse board URLs) as a second concrete job identity: `concreteSharedIdentifierTokens` is now narrowed to job-ID-shaped tokens (numeric identifiers dominated by digits), so bundles whose static parser emits legacy `boards.greenhouse.io` links while the provider uses canonical `job-boards.greenhouse.io` auto-safe to warning instead of blocking the dedup gate. This resolves the 2 current-run blockers from the 2026-09-04 fresh fetch (31st Union), restoring the gate to `warning` / `lifecycleUxReady=true`; regression tests cover the 31st Union shapes and a differing-job-ID negative control that stays blocked.

### Changed

- **Runtime dependency pins updated to current releases — Scrapy 2.17.0 → 2.19.0, psutil 7.0.0 → 7.2.2, pyinstaller 6.19.0 → 6.22.3 — with `requirements-lock.txt` regenerated via the documented `uv pip compile requirements.txt -o requirements-lock.txt --python-platform x86_64-unknown-linux-gnu` command.** Scrapy 2.19's added hard dependencies (aiohttp stack, brotli, zstd, platformdirs) entered the lock as real requirements of the new release; `pyinstaller-hooks-contrib` follows transitively (2026.4 → 2026.7). Toolchain pins deliberately held at ruff 0.15.14 / mypy 1.20.2 (operator decision): ruff 0.16's expanded default rule set + formatter changes would churn 136 findings and reformat 77 files, and mypy 2.x's new defaults surface 46 errors in a zero-suppression repo — both majors are adoption projects, not pin bumps. Verification: full suite 4,848 collected with 4,847 passing (the one failure is a known load-only flake in a timing-sensitive terminal-rewrite test that passes in isolation); `pip-audit` clean including the new Scrapy dep stack; changed-mode precommit gate exit 0. The local env's broken pydantic/pydantic-core pairing from an out-of-band upgrade (pydantic 2.13.5 requires pydantic-core==2.46.5) was repaired to match the pins before verifying. Post-bump live verification: one forced full-universe production pass (1,969 sources, finalize 41,086 jobs) ran clean on the new stack — zero parse-stack tracebacks, same-universe failed-source count 60 → 61 with all 11 ok→fail flips individually attributed (LinkedIn/itch.io HTTP-429/999 upstream churn, one 500, TLS hostname-mismatch vs pre-bump variance, and stale persisted rows re-verified — including the pre-existing `jobs/&` trailing-ampersand GameDevMap rows that 404 identically under Scrapy 2.17 in the prior forced pass); the hold-tail floor rows (Midgar/Exit VR/Inverge/SNK) all delta 0; the overdue rise (+41, Fusebox) is its verification-window cohort aging in while the source keeps failing on an upstream LinkedIn 999 — calendar aging on a pre-existing failure, unrelated to the bump. Evidence `tmp/scrapy219-verify-20260913/`.
- Wave-fallout repairs surfaced by the first full-suite run since the batch-4 wave (all pre-existing on HEAD, none caused by the dependency bumps — the pin diffs are pure version strings): the discovery probe's data:-URI guard skipped pure-relative detail links when no base URL was in play (`src/source_discovery/probe.py` — the `http/https` scheme check now exempts scheme-less relative links; base-ful lanes unchanged); the yodo1 recovery wave's dangling seed pointer healed (the superseded static winner row was deleted without the `superseded_by_provider` tombstone the supersession convention requires — the pending row's `duplicateOfSourceId` now points at the promoted teamtailor board and the tombstones fixture carries the missing record); the qloc recovery test's default-loaders expectation updated for the strengthened missing-universe lifecycle drain (the legacy sheet row now legitimately drains when the sheet family is absent from the synthetic universe; the sibling custom-loader test keeps preserve semantics, untouched); and the security-audit floor assertions in `tests/test_security_audit.py` moved with the pins (they guard the exact versions that closed past PYSEC advisories).

### Fixed

- **Fusebox post-drain overdue rise adjudicated: the 41 newly-overdue rows are LinkedIn guest-view junk provenance on a live-empty HiBob board — trusted-empty drain path, no code and no registry action (hold-tail repair plan Fusebox adjudication, 2026-09-13, evidence `tmp/fusebox-adjudication-20260913/`).** The first post-drain full pass aged in a new overdue cohort (6 → 47, all Fusebox Games (Nazara) (GameDevMap), health degraded/`overdue_rising`). Diagnosis from live evidence: the WP careers page is healthy (200, 332 KB) but contains **zero individual job links** — it is a pure LinkedIn funnel (`linkedin.com/company/fusebox-games/jobs` answers guest/bot requests with HTTP 999), so the static source fails `no jobs extracted` while the 41 overdue rows are the LinkedIn guest-view search URLs (`linkedin.com/jobs/{slug}?trk=…`, the Konami "Community" junk class) harvested by the 09-06 success run, plus one real HiBob ATS detail row already `unavailable`/`source_absent`/`definitive`. The real board is `fuseboxgames.careers.hibob.com` — a HiBob Angular SPA the static lane sees as a 1.3 KB shell — and Playwright renders it trusted-empty (`Current openings — Check again later — New job openings will be added soon.`, 71 visible chars) with the public jobs API confirming: `GET /api/job-ad` → 200 JSON `jobAdDetails: []`. Fusebox is alive (Nazara subsidiary, site actively invites applications) — a hiring pause, not a dead tenant. **Disposition (corrected same day after the restamp attempt): HOLD / record-only — the S6 path is closed.** The stamp passes falsified the trusted-empty premise: the page renders **textful** (570 visible chars of real careers copy — "Keep an eye out on our LinkedIn page for any job openings!" — with 0 rendered job links), so the S6 producer's ≤240-char near-textless cap refuses the stamp deterministically on every run (fail-closed by design: a textful page could hide job surfaces), and the S7 demotion lane never applies (the failure lines are LinkedIn **999**s, non-demotable). Wait lane with three exits: the hibob `/api/job-ad` structured read if Fusebox ever reopens a parseable board, the chronic-overdue tombstone/lapse lane (Exit VR §5 precedent) if the junk rows age out, or a LinkedIn-guest-view provenance junk-class guard if more sources surface (Konami's "Community" row shares the shape). **Recorded for the future hibob adapter thread:** HiBob tenants expose public jobs JSON at `GET https://{tenant}.careers.hibob.com/api/job-ad` (`jobAdDetails[]`; ustwo/Sumo/Torpor also carry hibob detail URLs in the sheets family) — no-build today (single registry tenant, board live-empty, zero yield delta).
- **Deps-verification record correction (same-day): the operator's library upgrade bumped the `playwright` package to browser revision 1234 while the machine had 1208 installed, so every Playwright render in the dependency verification pass silently no-opped (0/3,262 `got_html=True` vs 20 in the pre-bump pass) before `python -m playwright install chromium` restored renders mid-adjudication.** Re-attribution against the pre-bump pass D log: the veom/vertigo zero-extracts and beatshapers/brainup `&`-row 404s stand (pass D shows the same shapes with working renders), so the no-regression verdict holds; the correction is recorded in the hold-tail plan's Fusebox adjudication and the dependency memory note.

- **Big Moxi S6 re-promotion after the deps-verification pass: the ×2 rendered-empty basis survives full-pass state rewrites on the detail row (3 stamps retained through two rewrites), and two targeted no-`--output-dir` stamp passes re-armed and promoted the source without a full pass (hold-tail repair plan Big Moxi re-promotion note, 2026-09-13, evidence `tmp/bigmoxi-restamp-20260913/`).** Stamp #1 appended its confirmation but the promote path's live listing re-read returned no bodies that run (marginal, timing-shaped); stamp #2's guard accepted the accumulated basis and promoted — state `ok`/`no_openings`/`consecutiveZeroKept: 2`, the 2 rows drain on the next full pass with the source eligible. Operational rule refined: targeted runs never erase accumulated stamps, and the promotion can land in the second targeted pass itself — a full pass is only needed for the row drain. Renders worked because the playwright browser binaries were repaired mid-Fusebox adjudication.
- **Origin-aware LinkedIn guest-view junk-class guard landed (hold-tail systemic option 3, exercised early after more non-LinkedIn static origins surfaced with the same harvest shape; Fusebox/Konami adjudications 2026-09-13).** New fail-open leaf `src/jobs/common/origin_junk.py` classifies two junk-provenance shapes for static sources whose registry identity is NOT a `linkedin.com` listing: LinkedIn guest-view search/slug URLs (`linkedin.com/jobs/{slug}?trk=…`, `/jobs/search…`, and decorated `/jobs/view/…?…` rows — Fusebox's 41 stranded `verification_overdue` rows; bare numeric `/jobs/view/{id}` rows stay legitimate) and the nav-anchor self-page row (exact-title match only — "Community", "Privacy Policy", … — same-host, no job-detail path token or job query key; the Konami "Community" → `/pages/sns_account` shape). Sanctioned LinkedIn-origin sources (`linkedin.com/jobs/search/…`, `/company/…` registry listings) are explicitly exempt — their ~1,160 live `/jobs/view/{id}` rows are the product. Enforcement: inbound drops in the static rows flow (`_append_parsed_listing_rows`, `_append_rendered_row`) and detail-candidate accumulation (`_append_detail_candidate` gained an optional ctx, closing the one funnel that lacked the LinkedIn-host check) with a `junkProvenanceRowsDropped` nonzero-only source-report projection; a lifecycle drain (`_apply_guest_junk_lifecycle_entry`) lets a failing source's already-stranded junk rows exit through the failed-source shield to the same terminal state the missing path uses, with dedicated `guest_junk_provenance` evidence kind + `availabilityClosureOrigin` (idempotent once `unavailable`), counted as `guestJunkDrainedCount` in `lifecycleSummary`; kill switch `BALUFFO_GUEST_JUNK_GUARD` (default on, single choke point inside the row predicate so mid-session rollback is immediate). Predicate validated against the live 120,903-row lifecycle store before landing: the drain set is Fusebox's exactly-41 overdue rows plus in-scope shapes that only drain when their sources fail — sanctioned origins, sheets rows, and archived rows untouched. Tests: 23 new (`tests/test_origin_junk_guard.py`, `tests/jobs_static/test_guest_junk_guard_funnels.py`); full suite 4,877 passed / 1 skipped.
## [0.2.147] - 2026-09-04
### Changed

- Source discovery now remembers repeated probe failures per candidate: after three consecutive failures of the same deterministic class (DNS resolution, TLS certificate verification), a candidate is quarantined and skipped on subsequent discovery runs instead of being re-probed every cycle. Quarantines expire with the failure-memory retention window and revive on the next failure, transient failure classes (timeouts, 5xx, connection resets) keep probing as before, and a successful probe clears the record. Failure counters are kept in memory during a run and flushed once at finalize.

- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.146] - 2026-09-03
### Changed

- The Jobs and Admin footer now shows the running app version in container mode too (previously desktop-only): container pages hydrate the same-origin `/app/ready` payload, while desktop keeps reading `/app/update-status`. The version visible in the Umbrel app comes from the running container itself, which makes update verification unambiguous.

- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.145] - 2026-09-03
### Changed

- Upgraded Scrapy to 2.17.0 in the container image and packaged runtime, fixing CVE-2026-84366 (S3DownloadHandler sending signed S3 requests over plaintext HTTP; the codebase has no s3:// usage, but the advisory is now resolved at the source instead of allowlisted).
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.144] - 2026-09-03
### Changed

- "Check availability now" feedback overhaul (jobs + saved pages): results now show human verdicts with meaningful tones instead of raw classifier labels — green "Verified live" only for definitive live evidence, red for closed evidence, neutral "Couldn't verify" (with an Open job page action) for inconclusive outcomes like unverified pages, anti-bot blocks, or careers-page redirects. The checking toast updates in place with an elapsed-seconds counter instead of stacking duplicate toasts, and the frontend polls as long as the backend reports the run as running instead of giving up after 60 seconds.

- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

### Fixed

- Availability check reliability on slow-storage installs (Umbrel): the bridge availability service now caches resolved lifecycle identities with file-stamp validation, so repeated checks skip re-parsing the tens-of-megabytes lifecycle state file, and the worker no longer re-reads the file a second time when nothing changed since target preparation. Custom-saved lookups read the existing priority manifest first and only rebuild it when the identity is absent.
- Packaged desktop runtime now enables `BALUFFO_AVAILABILITY_DIRECT_ENFORCE=1` in the launcher child environment, matching the Umbrel container, so manual availability checks apply evidence immediately instead of running in record-only shadow mode.

## [0.2.143] - 2026-09-02
### Changed

- Jobs page density pass: slim presets row and pagination, merged duplicate quick-actions CSS, active-filters summary hidden when idle, single merged status/sign-in banner, de-chromed NEW badge, de-carded header, accent-styled Update-jobs button. Pipeline CTA caption now shows live GameDevMap audit subtask ticks and stage/target/counter segments, matching the admin page. Bridge hardening: httpx.InvalidURL from malformed redirects is an expected per-page failure instead of crashing the discovery worker; two new pipeline helpers refactored under the C901 threshold.

- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

### Fixed

- Bridge task lifecycle: history mirror no longer creates owner-less active rows.
  `mirror_history_row` mirrored an unfinished history entry via `start_run` without
  owner fields; if the real worker never terminalized it (crash between history write
  and lifecycle finish), the mirrored row was an unreapable zombie of the same shape
  as the event-only stubs. Mirrored running rows now carry `owner_kind='bridge_thread'`,
  which the startup reaper already treats as stale after restart.

## [0.2.142] - 2026-09-02
### Fixed

- Storage: `append_task_event` no longer seeds orphaned active task rows. The
  task_events foreign key required a placeholder `task_runs` row for event-only
  runs, but it was inserted with `status='running'` and no owner_pid/owner_kind;
  when a run's real lifecycle row lived only in JSON (pre-cutover or shadow
  mode), the SQLite placeholder survived as an unreapable "running" zombie that
  blocked subsequent task launches. The placeholder is now terminal
  (`succeeded`, `terminal_reason='event_only'`); a real lifecycle start still
  flips it to running through the normal upsert path.


### Changed

- Bridge task lifecycle: pid-less running rows in the SQLite task_runs projection (rows
  written without an ownerKind or ownerPid, e.g. by pre-migration or crashed bridge
  versions) are now reaped at startup once their heartbeat goes cold (1h threshold,
  far above any real worker cadence). Previously neither the pid check nor the
  owner-kind allowlist could stale them, so they persisted as active tasks forever,
  blocking every later pipeline run behind "Updating local jobs..." (observed live:
  two zombie sync rows, one six days old, disabled the Update-jobs button for hours).

- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.


- Container shipped-code version gate (`tools/repo_health/container_version_policy.py`): the `release` repo guardrail now fails when container-affecting commits land after the last version bump without either advancing the version or declaring explicit release-tag intent (`Release-tag: vX.Y.Z` line, or `release(vX.Y.Z):` / `chore(release):` subject, naming a version newer than the current one). This closes the 0.2.140 reuse trap — code shipped to the Umbrel container channel while the `umbrel-app.yml` version string stayed frozen, so Umbrel's app-store update detection never offered the newer build. The lint CI checkout now fetches full history so the gate evaluates the real commit window.
- Container `paths-ignore` alignment (`.github/workflows/build-container.yml`): the republish trigger's `paths-ignore` now matches the guardrail's shipped-path list exactly (`tools/**`, `.github/**`, `release-notes.md`, `umbrel-app-store.yml` added), so pure-tooling commits stop triggering no-op image republishes; a pinned test keeps the two lists in lockstep.
- Workflow-syntax gate (`tools/repo_health/workflow_syntax_policy.py`): the `workflow` repo guardrail now runs actionlint over every `.github/workflows/*.yml` so workflow YAML is semantically validated in the pre-commit/pre-push gates and CI Lint on every change. The pinned actionlint binary is located on PATH or provisioned as a checksum-verified release into the gitignored `.tmp/actionlint/` cache, and the gate fails — never silently skips — if it cannot be obtained.

## [0.2.141] - 2026-08-31
### Changed

- Runtime↔seed registry reconcile (WP24 runbook, jobs-coverage plan): converges the live
  container's runtime registry (`data/source-registry-active.json.gz` + journal) with the
  twin-reconciled tracked seeds, deferring to the seeds as the source of truth. Applies the
  verified `POST /registry/demote-active` batch demoting **10** real runtime-only twins to
  pending (`static:listing_url` rows for `www.scopely.com/en/join-us`, `www.hugecalf.com/careers`,
  `bandainamcoent.com/careers`, `www.roshkastudios.com/jobs.html`, `www.joinplaygames.com/jobs.php`,
  `www.ninerocksgames.com/careers`, `www.skybound.com/careers`, `sybogames.com/careers/`,
  `www.nocodestudio.com/jobs`, `www.volleygames.com/careers`) — **active 2301 → 2291, pending
  850 → 860**. Deliberately left
  in place: `careers.playstation.com`'s two distinct rows (a marketing redirect + the
  `playstation.com/jobs` Global-website board, 50 jobs — not a twin) and the 16 single-row
  reconcile keys (e.g. bytedance holding `jobs.bytedance.com/en/position`). Every demote is
  reversible via `POST /registry/approve`.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.140] - 2026-08-30

### Changed

- Registry duplicate-careers-URL guardrail (WP20, jobs-coverage plan): `npm run lint:repo-guardrails`
  gained a `registry` group (`source_registry_duplicate_url_policy`) that fails when two
  **active** seed rows share a canonicalized careers URL (www/apex, http/https, trailing-slash,
  fragment), so twin sources like the Scopely `join-us` pair are caught in the tracked seeds
  before they reach the published registry. Known collisions are grandfathered in
  `data/defaults/source-registry-known-url-collisions.json` (34 reviewed entries today) and
  shrink as they are reconciled. `scripts/precommit_gate.py` now watches the two seed files, so
  editing a seed runs the guardrails in changed-file mode. The canonicalization rule now lives in
  `src/source_registry_identity.py` (single authoritative implementation shared with the runtime).
- Runtime URL-twin auto-demotion (WP21, jobs-coverage plan): the source-registry conflict
  automations are gated on the same `canonicalize_careers_url` rule as the commit-time guardrail.
  `duplicate_family_conflict_cards` now also raises `url-twin:` cards for **active** rows that
  share a canonicalized careers URL across different studio families (the Scopely Genjoy/Omnidrone
  class), skipping reviewed collisions from the baseline allowlist, and a safe-automation analyzer
  auto-demotes the non-canonical twin to pending with `registry_conflict_safe_auto_demote` on
  registry load. Winner selection prefers the already-canonical URL form (apex https), matching the
  seed reconciliation precedent. Duplicates introduced by live discovery now demote automatically
  instead of double-emitting jobs.
- First baseline shrink: Activision phApp twin reconciled (WP22, jobs-coverage plan): the two
  active `careers.activision.com` rows (Sheet, trailing slash, 30 jobs vs Manual Website, no
  slash, 6 jobs) both gate the WP17 phApp adapter and double-posted the same board; kept the
  stronger Sheet row in the active seed (2015 → 2014) and demoted the Manual twin to the pending
  seed (48 → 49), removing the `careers.activision.com` entry from the twin-URL baseline
  (34 → 33) — the first shrink of the allowlist, so the phApp adapter now emits Activision jobs
  once.
- All remaining Tier-1 twins reconciled (WP23, jobs-coverage plan): the 14 other www/apex,
  trailing-slash, and http/https true-twin pairs (IO, Sybo, Hello Games, Lightfury, Jyamma,
  CDPR, Singularity 6, Joinplay, Roshka, Nine Rocks, Lil Snack, No Code, Hugecalf, Skybound)
  collapsed to one registration each, keeping the row the runtime duplicate-winner logic ranks
  first (evidence-based; canonical-form preference only on ties) — active seed 2014 → 2000,
  pending 49 → 63. The 14 canonical URLs were pruned from the twin-URL baseline (33 → 19), which
  now holds only genuine same-studio page variants and shared parent boards, and the guardrail
  stays green with the smaller allowlist (no stale entries, no uncovered collisions).
- Live-probe triage of the remaining twin-URL baseline (WP24, jobs-coverage plan): each of the
  19 remaining allowlist entries was redirect-probed live (2026-08-29). 11 same-studio pairs
  provably serve the same careers page (`/positions` → `/en/open-positions/`,
  `jobs.bytedance.com/en/position` → `joinbytedance.com/search`, `volleygames.com/careers` →
  `weekend.com/careers`, gohire `mighty-bear-games-` → `wearemighty-` slug,
  `zenostechnology.com/careers` → `zenosinteractive.com`, and the overwolf/playstation/dsdambuster
  dedicated-hosts redirects) and were reconciled via `transition_registry_to_pending` — active
  seed 2000 → 1989, pending 63 → 74 — pruning the baseline 19 → 8. The 8 remaining entries are
ot twins under the probe: 4 true shared parent boards (amazongames, astragon,
  careers.microsoft, ea.com) and 4 same-studio distinct-page rows (amazongamestudios amazon.jobs
  vs studio careers, Nintendo landing vs list, Romero careers vs home, Waterproof jobs.php vs
  careers). Guardrail and runtime `url-twin` cards both green at 0.
- Stale-baseline guardrail invariant (WP20+, jobs-coverage plan): the `registry` repo guardrail
  now enforces the dual of the existing twin check — every entry in
  `data/defaults/source-registry-known-url-collisions.json` must still be backed by at least two
  active seed rows, or precommit fails. Thus pruning (removing a baseline entry) and
  reconciliation (demoting a twin so its URL drops back to a single active row) stay in
  lockstep: a change that resolves a collision can no longer silently leave a stale allowlist
  entry that would mask future drift for that URL.
- Static coverage recovery (WP2/WP3, jobs-coverage plan): the static fetcher now follows
  bounded multi-hop redirects (up to 4 hops with raw-URL loop detection), fixing the common
  apex → www-with-port → trailing-slash chain that produced false "redirect loop" rejections
  and recovered ~50 jobs across 11 previously zero-kept sources. Four new leaf static plugins
  recover server-rendered listings the generic parser missed: Outerdawn (Webflow `careerrow`),
  Astrid Entertainment (WordPress `job-listing` blocks with Workable apply links), Immersity
  (Webflow `careers_cms_item`), and Perfect Garbage (Squarespace → Work With Indies postings).
- Browser-fallback JS-shell classifier (WP4, jobs-coverage plan): `detect_js_shell` now also
  recognizes jQuery-era / legacy-hydration shells (Ember, AngularJS, Backbone, jQuery SPA)
  that emit no modern React/Next/Angular-2 boot tokens. Detection requires *corroborated*
  evidence (a handlebars/ember/knockout template marker, a client-hydrated `data-href`
  placeholder, or a legacy-SPA boot plus careers/job context), so plain server-rendered pages
  that merely bundle jQuery/handlebars stay negative. Bounded live run on the browser-fallback
  candidates (Twitch, Konami, Konami Gaming, Sandsoft, Upsurge, Optillusion): Konami Gaming
  recovered **45 jobs** via the pool; sandsoft/twitch shell captures now classify
  `blocked_or_challenge` + `browserFallbackRecommended=true` instead of `needs_review`/dead,
  and konami main classifies `empty_confirmed` (genuinely empty today, correct behavior).
- Rendered-empty WP4 board triage (WP5, jobs-coverage plan): Upsurge and Sandsoft were
  misclassified as dead because their listings never reach the generic runner cleanly —
  Upsurge's `/careers/` page server-renders its roles but emits no per-job links, and
  Sandsoft's `/careers/` is a jQuery-era shell while its full posting set lives at the
  server-rendered `/careers/feed/` RSS. Two new leaf static plugins recover them: upsurge
  (6 list-only `CareerSummary` roles) and sandsoft (10 postings from the feed). Optillusion
  was confirmed genuinely closed (only `/job/` exists on the domain, explicitly
  "not actively hiring"; no openings anywhere) and was demoted on the live container
  to review (`/registry/demote-active`, active 2303→2302). Sandsoft's live check-source
  reports `jobsFound: 2` / `weakSignal`, corroborating the plugin recovery.
- WP5-plugin pipeline measurement + list-only anchor fix (WP9, jobs-coverage plan): a
  bounded pipeline pass showed the upsurge plugin kept only 1 of its 6 roles — the shared
  list-only helper anchored rows with `#<slug>` fragments, which URL normalization strips at
  the plugin repair-row dedup, canonicalization, and finalize-fingerprint stages. The helper
  (`static_fragment_link` → `static_listing_anchor_link`) now anchors rows with a
  `?static-role=<slug>` query parameter, which survives normalization end-to-end. Verified on
  a fresh pass: upsurge **6/6** and sandsoft **10/10** roles kept (16 output jobs vs 11),
  matching the plugin unit tests.
- Generic block-title list-only fallback (WP10, jobs-coverage plan): the static listing
  runner now recovers list-only boards without a per-host plugin. When a listing yields no
  rows from the JSON-LD / rendered-card / detail-link paths and has no dead-listing
  evidence, it scans for block-structured headings (`<h2>`–`<h4>`, script/style stripped)
  and emits one query-anchored row (`?static-role=<slug>`) per distinct job-title-looking
  heading (min 2, section-header phrases like "Open Roles"/"We're Hiring" excluded). The
  fallback only fires on otherwise-empty sources, so it cannot change behavior for sources
  the pipeline already handles.
- List-only board sweep (WP11, jobs-coverage plan): swept the 150 zero-kept WP6 sources for
  boards that list roles with no per-role detail URLs and converted three to leaf plugins using
  the shared `static_list_only_job_rows` helper: a4vr (Squarespace `POSITION:` blocks, 3 roles,
  speculative INITIATIVBEWERBUNG block excluded), amrita (SP Page Builder accordion
  `aria-label` titles, 4 roles), and animvs (Elementor desktop tab titles, 5 roles). Bounded
  pass recovers **10 jobs** (a4vr 3 + www.a4vr.com 3 + amrita 4); animvs currently serves an
  expired TLS certificate so it stays zero until the cert is renewed. The duplicate
  `www.a4vr.com` active registry row was demoted to pending (kept the seeded `a4vr.com` row), so
  the feed now carries 3 a4vr jobs instead of 6.
- Full-active-registry list-only sweep (WP12, jobs-coverage plan): extended the WP11 sweep
  to all 2,110 active static URLs (1,961 captured) and converted three more non-heading
  list-only boards to leaf plugins using the shared `static_list_only_job_rows` helper:
  playstack (Astro `dynamic-title` card grid, 21 roles, hero heading filtered), twirlbound
  (WordPress ub-content-toggle accordions, 4 roles), and tatem (Tilda `t-card__title` cards,
  9 roles). Bounded pass recovers **34 jobs** (21+4+9, 0 failed). The shared list-only helper
  (and the WP10 heading fallback) now also unescape HTML entities before slugging, so
  entity-variant duplicate titles ("PC &amp; Console…" vs "PC and Console…") collapse to one row
  instead of two.
- Standalone sub-studio scan (WP16, jobs-coverage plan): checked whether Undead Labs,
  inXile, Compulsion, Smoking Gun, Next Games, Night School and Boss Fight link a
  recoverable parent/ATS board. **None currently yields real jobs**: Undead Labs' Greenhouse
  board (`undeadlabsllc`) holds only a "General Interest Application" catch-all; inXile's
  and Compulsion's BambooHR `/careers/list` (repo adapter) return 0 postings / a
  "Be Careful of Hiring Scams" warning — those three are **adapter-ready but empty today**,
  documented for staging when openings appear. Next Games and Night School route to the
  Netflix custom platform (covered by the WP15 `phApp` adapter decision); Smoking Gun and
  Boss Fight expose no board. No provider staging worthwhile this pass.
- Shared phApp careers-platform adapter (WP17, jobs-coverage plan): reverse-engineered the
  open recovery path for the Phenom People "CareerConnect"/`phApp` platform that WP14/WP15
  flagged as widget-only (the widget-API JSON is tenant+CSRF gated, but every phApp jobsite
  publishes an open per-locale sitemap of `/job/{jobCode}/{slug-title}` URLs whose detail
  pages are server-rendered). The shared plugin
  (`src/jobs/adapters/plugins/static/phapp.py`) derives the sitemap URLs, extracts title /
  location / company from the canonical `<title>` (both the Blizzard "{Title} | {Loc} job in
  … | … jobs at {Co}" and King "{Title} in … | … at {Co}" shapes, plus URL-slug fallback),
  is registered for the widget-only rows (King, Treyarch, Raven, Sledgehammer, WBD, Scopely,
  …), and the dedicated blizzard/activision plugins now fall back to it when their
  server-rendered-card parse yields nothing (the production JS-shell case). Bounded live
  end-to-end pass recovers **103 jobs** (Activision 50, Blizzard 37, King 14, Treyarch 2),
  all previously zero-kept.
- Workday rows for the phApp families (WP18, jobs-coverage plan): measured what `workday_sources`
  would recover from the five boards the phApp families link (xboxgaming.wd1 `/External` = 67 jobs
  covering Beenox/High Moon/Infinity Ward/Sledgehammer, `/CentralTech` = 3 for Activision,
  warnerbros.wd5 `/global` = 356 company-wide). The boards are **live** over verified `curl`, but the
  WS adapter's CXS path uses verified *Python* TLS that rejects these hosts (cert valid through Oct
  5 per `openssl`; WP14's unverified probe is the only working path today), so `workday_sources`
  recovers **0** from all five today — they hard-error rather than return zero. Documented
  ready-to-stage rows (Activision CentralTech + Beenox/High Moon/Infinity Ward External) with the
  verified-TLS/CXS path as the gating fix; `warnerbros/global` set out of scope for a games feed
  without a games filter.  No registry mutation this pass.
- Duplicate-Scopely reconciliation (WP19, jobs-coverage plan): the two GameDevMap rows on the
  same phApp join-us board (`scopely.com/en/join-us` apex vs `www` twin, "Genjoy (Scopely)" and
  "Omnidrone (Scopely)", both `jobsFound: 19` with identical evidence) are now one canonical
  registration in the tracked seeds. Kept the apex row
  (`static:listing_url:https://scopely.com/en/join-us`) per the WP11 a4vr precedent and demoted
  the www twin to the pending seed with the repo's own `transition_registry_to_pending`
  (active seed 2016 → 2015, pending 47 → 48), so a future WP17-phApp recovery of the board
  cannot double-post. The live container still needs the equivalent runtime demotion
  (`POST /registry/demote-active`, active 2301 → 2300) — the runtime registry grew beyond the
  seed; reversible via `/registry/approve`.
- Full-registry phApp platform scan (WP15, jobs-coverage plan): content-scanned the
  1,597 captured active static careers pages for the proprietary `phApp`/`vscdn.net`
  careers platform → **13 active static rows host it directly** (lower bound). **5 also
  expose a real Workday board in their page HTML and are recoverable today via the
  existing `workday_sources` adapter** — Activision, Beenox, High Moon, Infinity Ward
  (all → `xboxgaming.wd1.myworkdayjobs.com`), and Warner Bros. Games
  (→ the large `warnerbros.wd5.myworkdayjobs.com` board). **8 are widget-only** (jobs
  served client-side, no board in HTML): Blizzard, King, Raven, Sledgehammer, Treyarch,
  TT Games, and Scopely/Genjoy + Scopely/Omnidrone. A single shared `phApp` adapter
  is the largest single zero-kept platform lever (13 rows / ~11 studios). No
  code/registry change; evidence in the WP15 artifact.
- ATS-backed shell triage (WP14, jobs-coverage plan): the zero-kept King / Blizzard /
  Microsoft / Netflix / Activision shells all run the proprietary `phApp`/`vscdn.net`
  careers platform (no repo adapter) — jobs are fetched client-side. The one
  existing-adapter path is **Activision**: its careers site delegates to a real Workday
  board (`xboxgaming.wd1.myworkdayjobs.com/CentralTech`), whose CXS API the existing
  `workday_sources` adapter can already pull (verified live, 3 current "Central
  Technology" jobs) → documented as a provider-staging candidate (no registry mutation;
  provider staging is operator-approved). Microsoft resolves to SAP SuccessFactors
  (`position.system_id:successfactors`) and King / Blizzard / Netflix expose no standard
  board — none are provider-staging candidates; a future SuccessFactors or `phApp`
  adapter decision covers them (the `phApp` platform spans all five plus many sub-studios).
  Kept `needs_review`.
- Conservative feed-postings plugins (WP13, jobs-coverage plan): `arsanesia` and
  `petprojectgames` expose their only recoverable job posting as a single blog post mixed into
  the site WordPress news feed. Two leaf plugins (via a shared `_feed_postings` helper) fetch
  the site `/feed/` and gate every item with a conservative `looks_like_feed_role_posting`
  filter — a concrete role keyword **and** a hiring-context signal (looking for / hiring /
  full-time / intern / wanted) with any news term (dev log, trailer, release, teaser, launch,
  introducing, blog) excluded. Requires the hiring signal so team-profile/news posts aren't
  published; false negatives are preferred over non-jobs. Bounded pass recovers **2 jobs**
  ("Game Programmer: Full-Time & Intern", "…Looking for a 3D Animator"), 0 failed.
- German-localized feed gate + `thegoodevil` leaf plugin: the shared
  `looks_like_feed_role_posting` conservative gate gained a minimal German vocabulary
  (`game-design`/`tech-art`/`programmierung` role nouns, `pflichtpraktikum`/`bewerbung`/
  `wir suchen`/`gesucht`/`stellenangebot` hiring signals, plus German news rejects
  `gewonnen`/`nominiert`/`festival`/`messe`/`wettbewerb`/`ausgezeichnet`/`ankündigung`) so
  the persistent, currently-open ``Pflichtpraktikum Game-Design od. Programmierung``
  posting in The Good Evil's mixed Tumblr `/rss/` feed passes while its German news items
  stay rejected. New spec-driven leaf `thegoodevil` (site_rss_url builder, filter on)
  recovers that 1 posting; the German tokens only add acceptance paths, so the English
  arsanesia/petprojectgames pinned titles are unchanged.
- Discovery sweep WordPress-feed probe (source_discovery): before a JS-shell discovery
  candidate is escalated to the browser pool, the page-recovery sweep now probes for a
  server-rendered feed — first the URL advertised via
  `<link rel="alternate" type="application/rss+xml">`, then standard WordPress feed
  paths (`/feed/`, `<page-path>/feed/`, `/feed`). A detected feed wins over the browser
  pool and is emitted as a feed-recovered static candidate (`discoveryStage:
  "wordpress_feed"`, tagged with `feedUrl`/`feedSource`/`feedItemCount`), surfaced under a
  new `feedRecoveryCandidates` summary key. Non-JS-shell pages and pages without a feed
  are unchanged (the latter still land in the browser pool). Shared helpers live in
  `src/source_discovery/wordpress_feed_probe.py`.
- Availability observation fields now reflect the enforcement state instead of a
  hardcoded shadow value: `sweepCoverage.mode` reads `"enforced"` when
  `BALUFFO_AVAILABILITY_DIRECT_ENFORCE` is truthy and `"shadow"` otherwise, and
  `availabilityHealth.shadowClassifier` (including the failed-report path) reads
  `false` while enforcing. Payload shape is unchanged; this removes the need for
  SSH env checks when verifying live enforcement on the container.
- Availability direct enforcement is promoted for the container runtime: the Umbrel
  compose now sets `BALUFFO_AVAILABILITY_DIRECT_ENFORCE=1`, so direct availability
  checks publish lifecycle transitions and reopen rows with definitive live evidence
  instead of only recording shadow results. Promotion followed the reviewed gate
  (healthy seven-day sweep, clean Saved page, reviewed 100-job stratified sample,
  no unresolved high-risk classifier family) recorded in
  `docs/snapshots/availability-direct-promotion-2026-08-27.md`. Desktop runtimes
  stay in shadow mode until separately promoted.
- Jobs page auto-hydrates the complete feed right after the startup snapshot
  renders (idle-deferred, off the boot critical path) in all runtimes, so the
  full list no longer requires pressing Reload. Boot stays bounded: the
  snapshot renders first, the full feed syncs in the background, and explicit
  Reload continues to work as before.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.139] - 2026-08-26

> Container Jobs boot performance fix: stop downloading the full public feed
> and diagnostics on boot, honor the container runtime gates that were
> silently inert, and remove the remaining boot layout shifts.

### Fixed

- Container runtime flag forwarding repaired: `isContainerRuntimeMode` was
  never forwarded into `composeJobsRuntime` or the Jobs feed initializer, so
  every dep-based container gate (task-state polling, dashboard-health,
  cache skip, browser full-feed sync) silently ran browser behavior in
  containers. The flag now flows through composition, boot, and feed init.

### Changed

- Container Jobs boot no longer requests `jobs-unified-light.json`
  (~37 MB decoded at current feed sizes), `jobs-fetch-report.json`,
  `/ops/task-state?view=summary`, or `/ops/dashboard-health?view=summary`.
  Boot renders the bounded startup snapshot directly; the full feed stays
  available through the existing Reload control.
- An unapplied Admin auto-refresh signal discovered during initial container
  boot is acknowledged once and surfaces as a Reload-needed badge with status
  text instead of triggering an immediate full-feed download. Signals arriving
  after the page is interactive still auto-refresh as before, and signal ID
  deduplication is preserved.
- Guest sign-in notice swaps copy between guest/profile states instead of
  hiding the element, eliminating the largest Jobs-boot layout shift.
- Auth meta area and quick actions reserve their populated heights so label
  swaps and data-driven filter chips can no longer reflow the page during
  boot. Measured local-container CLS dropped from ~0.10 to 0.007 warm.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.138] - 2026-08-25

> Follow-up patch restoring status-chip severity coloring and fixing the Ops
> Dedup badge against the live fetch-report shape.

### Fixed

- Status chip severity coloring restored: the shared `--tint-ok/--tint-warning/
  --tint-critical` variables referenced themselves (cyclic definitions), which
  invalidated every `color-mix` usage and left "Warning"/"Succeeded"/
  "Auto-Approvable" chips uncolored. Definitions now use literal colors; a
  cycle guard stops this from shipping again.
- Ops Dedup badge loads from the fetch report shape the bridge actually
  writes (top-level `dedupEvidence`) instead of assuming a `latestRun` wrapper,
  so the badge shows the real review count on first load.
- Admin CSS cache-bust bumped to v15 so browsers fetch the fixed stylesheet.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.137] - 2026-08-25

> Shared Desktop + Umbrel release fixing Admin review-panel interactions and
> styling consistency, repairing Action Center copy over plain HTTP, removing
> the redundant Ops Health refresh button, and eliminating Jobs feed
> first-load jumps.

### Fixed

- Registry Conflicts: the document-level Inspector click delegate no longer
  hijacks native `<details>` toggles, so "Decision details" and per-row
  evidence sections open again; card bodies keep opening the Inspector.
- Dedup tab badge shows a real review count from the jobs fetch report as
  soon as Ops loads (previously stuck on "..." until the tab was opened),
  with fetch-report changes invalidating the counts cache.
- Source Policy Review badge turns warning-toned only when actionable items
  exist; artifact warnings moved into the tooltip.
- Action Center "Copy all diagnostics" works over non-secure HTTP (Umbrel
  LAN) via a clipboard fallback; failure toasts only when every path fails.

### Changed

- Admin panels unify on the elevated card language (12px gradient shells):
  Action Center internals adopt alert-banner severity tints and standard
  buttons; Stored Profiles Overview matches; severity colors consolidated
  into shared `--tint-ok/--tint-warning/--tint-critical` variables, fixing
  the light-theme bulk-busy message contrast.
- Sticky Admin section nav centers its links so they clear the floating
  bridge badge at narrow widths.
- Removed the redundant "Refresh Ops Health" button; Operations Health stays
  auto-refreshed by its existing pollers.
- Conflict cards are more compact: winner-vs-loser summary line plus folded
  decision-signal and adjudication/diff disclosures; hover affordance added.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

### Performance

- Jobs page first-load jumps eliminated: identical list content no longer
  rewrites the DOM when boot/auth/auto-refresh paths re-render; unified feed
  mirrors race instead of chaining sequential timeouts; the guest notice
  renders visible by default so auth resolution no longer shifts layout by
  ~62px (measured CLS 0.094 -> 0.0025 locally).

## [0.2.136] - 2026-08-25

> Shared Desktop + Umbrel Admin review-panels UX release: registry-conflict
> paging and search, dedup evidence readability, source-policy bulk actions,
> discovery lane honesty, and Ops tab/filter URL persistence.

### Added

- `/registry/conflicts` GET supports optional additive paging params
  (`limit`, `offset`, `queue`). When any param is present, conflict cards are
  sorted by `reviewPriority`/`reviewQueue`/`familyKey`, the response gains
  `returnedCount`, and `summary.conflictCount` stays the untouched total;
  without params the payload is unchanged. The Admin Registry Conflicts panel
  now loads 50 cards per page with a "Show 50 more" footer, a family/source
  text search, and P0/P1-only auto-expanded groups.
- Source Policy Review supports bulk acknowledge/snooze: checkbox selection
  persisted across poll re-renders, "Acknowledge selected"/"Snooze selected"
  actions reusing the existing per-pair review-action route, one summary
  toast, and in-flight double-submit protection.
- Discovery Review candidate lanes show honest "showing X of N" counts with
  per-lane "Show 10 more" expansion (Ops panel only; the read-only registry
  page preview stays static).
- Ops tab selection and Registry Conflicts triage/queue/search filters persist
  in the URL hash and restore on page load.
- The Registry Conflicts action strip highlights the first conflict-source
  check as the recommended step when conflicts are queued but no check has
  ever run.

### Changed

- Dedup Lists suppress zero-count buckets across all count summaries, gate
  metric chips, and the merge-reason line ("none" fallback), and raise
  evidence-table/example caps from 5 to 10 rows. The dedup review-queue table
  replaces its single semicolon-joined evidence string with labeled
  per-row evidence disclosures.
- Source Policy Review rows keep the first five metadata fields inline and
  collapse the rest behind "More details" disclosures (pair rows, migration
  candidates, blocked candidates, linked identities, suppression eligibility).
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

### Removed

- Unused plain-text `formatDedupAuditGate` dedup gate formatter and its
  re-export.

## [0.2.135] - 2026-08-24

> Shared Desktop + Umbrel Admin reliability patch: instant schedule-panel
> hydration, JSON-authority source tables fix, /registry/sources legacy-mode
> removal, and dead history-projection/state-reader retirement.

### Fixed

- Admin Ops schedule panel no longer sits on "loading schedule..." for the
  first idle-poll interval (~10s) after opening or refreshing the page: the
  bootstrap payload's schedule section now seeds the panel model directly,
  with the early schedule GET kept as a fallback whenever seeding does not
  yield a hydratable model.
- Pending/Active source tables on JSON-authority deployments (default outside
  SQLite migration) no longer stick on "Source tables refreshing" forever:
  the compact-table payload now serves real limited rows from the normalized
  JSON registry state instead of a degraded-empty stub.
- Stale-report classification and live-task evidence no longer read the frozen
  `admin-task-state.json` artifact; lifecycle rows are the sole liveness
  authority. The packaged desktop also dropped its disk-fallback for conflict
  diagnosis, and the dev supervisor stopped reclaiming PIDs from it.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

### Removed

- `/registry/sources` legacy modes: `view=full`, the `detail=full|summary`
  selection, and the `activeCompact`/`compactActive` aliases now return HTTP
  400 with `removedParams`. The endpoint serves one authority-aware
  compact-table lane (`view=table` or omitted; JSON-authority deployments get
  real rows instead of the previous degraded-empty stub). `/registry/summary`
  no longer accepts the dead `cheap`/`storage` view aliases.
- Dead report-file history projection lane (`sync_history_from_reports`,
  `project_run_history`) and its facade/wiring; `/ops/history` and fetcher
  metrics already read the lifecycle-ledger projection.
- Runtime reads of the frozen `admin-task-state.json` artifact: stale-report
  classification and live-task evidence checks now use lifecycle/report
  signals only. The file is never consulted outside explicit migration
  tooling, and the packaged desktop no longer falls back to it for conflict
  diagnosis.

## [0.2.134] - 2026-08-24

> Shared Desktop + Umbrel Admin performance patch: ops summary TTL caching
> with active-run bypass, alert-state write suppression and locking,
> tab-counts cache key hardening, and mutually exclusive admin poll lanes.

### Changed

- Route-layer TTL caches for `/ops/dashboard-health?view=summary` (10s) and
  `/ops/fetch-kpis?view=summary` (15s) with per-cache single-flight locks;
  entries computed during an active run are never served once idle, and the
  active-run bypass probe also covers standalone fetch/bootstrap runs via the
  hot-task snapshot.
- Alert state: `ops-alert-state.json` is no longer rewritten on every summary
  poll when unchanged, and the alert read-modify-write shares one lock with
  the `/ops/alerts/ack` route to prevent lost acknowledgements under
  concurrent requests.
- Admin Ops tab counts: `jobs-source-state.json` is size-keyed instead of
  mtime-keyed so run-heartbeat rewrites no longer invalidate badges mid-run;
  corrupt cache envelopes recompute instead of failing the route, and cache
  writes use unique temp files for concurrent writers.
- Admin frontend polling lanes are mutually exclusive: while active-run
  evidence exists, idle scheduling routes through the fast active lane instead
  of heavy dashboard-health summaries.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

### Fixed

- Admin pipeline schedule no longer sticks on "loading schedule..." indefinitely:
  a schedule fetch that succeeded but normalized to an unhydratable payload
  (empty/degraded shape) previously recorded neither an error nor a retry, so
  the Ops schedule control and its Enable/interval inputs stayed disabled for
  the whole active run. Such payloads now surface "schedule delayed; retrying"
  and arm the existing backoff retry until a hydratable payload lands.

## [0.2.133] - 2026-08-23

> Shared Desktop + Umbrel performance patch: fetch-stage row streaming,
> lifecycle tree defer, jemalloc container allocator swap, LPT scheduling,
> browser-pool recycling + renderer caps, parser-noise classifier fix, and
> container concurrency profile raise.

### Changed

- Fetch-stage row streaming: seeded and fetched canonical rows deferred to
  finalize handoff via incremental sidecar; lifecycle tree deferred to finalize.
- Copy-on-write lifecycle rows and replace-based dedup renumbering.
- Container allocator swapped from glibc to jemalloc (`LD_PRELOAD`) with
  background page purging and forced mmap for large allocations.
- LPT scheduling: known-slow aggregate loaders (`google_sheets`,
  `scrapy_static_sources`) start first to overlap with fast statics.
- Browser pool recycling every N acquisitions with graceful close + lazy relaunch.
- Chromium renderer-process limit and V8 heap cap for tight cgroups.
- http2 transport attempt with graceful fallback on pooled HTTP clients.
- Compact hot-state JSON writes for task-state and progress reports.
- Heavy-host body caps (2 MiB) and listing-only enforcement for outlier domains.
- Parser-noise classifier tightened: single `{Token}` titles kept as real jobs.
- Container concurrency profile raised: mw=12, max_per_domain=3,
  static_detail_concurrency=6, adapter_http_concurrency cap=32.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.132] - 2026-08-18

> Desktop rollup: the jobs/discovery coverage batch (remote aggregator,
> discovery recovery escalation, provider feed liveness, Gamesmap default-on,
> and new seed coverage) plus the packaged portable EXE fix that restores the
> desktop platform modules the v0.2.131 frozen bundle was missing, and the
> tests/ mypy remediation that turns the type gate on for the whole tree.

### Added

- Remotive community-board loader (`remotive` source) with a game-job filter,
  mirroring the Remote OK loader; registered in the default source loaders and
  compat exports. Live-verified to fetch remote game roles (e.g. Mythwright
  Senior Technical Artist) that were previously missed.
- Discovery recovery escalation for directory rows that fail same-party careers
  recovery: bounded provider-pattern candidates (Workable/Greenhouse/Teamtailor
  etc.) are emitted from the studio name before rejection, and remaining
  `no_careers_evidence` rows are queued for web-search re-staging. Gated by
  `gamedevmap.activeAuditRecoveryEscalation*` settings.
- Personio feed liveness: feed URLs that redirect to the Personio marketing
  homepage now classify as `site_changed` and append the studio to
  `data/discovery-feed-recheck-queue.json` so the next discovery run re-stages
  the studio instead of erroring forever.
- Gamesmap directory adapter is enabled by default (`gamesmap.enabled=true`,
  `websiteOnlyFallback=true`, `activeAuditTtlMinutes=360`) with a new
  `--gamesmap-enabled` CLI flag.
- Seed-catalog coverage for NeoBards and Evolve (neobards static plugin), the
  Crater Studios JS-shell careers site (static plugin deriving titles from URL
  slugs), and a personio 429 recheck path that re-stages rate-limited sources
  on the next discovery run.

### Fixed

- Static/provider empty-source cache decisions require 2 consecutive zero-kept
  runs before skipping a source (`DEFAULT_INCREMENTAL_EMPTY_SOURCE_MIN_ZERO_RUNS`),
  so a single transient bad run no longer parks a parseable source.
- Source-discovery audit tests no longer write fixture artifacts into `data/`
  (all gamesmap/gameprog tests now pin `activeAuditPath` to temp locations);
  polluted `data/gameprog-`/`data/gamesmap-discovery-audit.json` artifacts were
  removed.
- Packaged portable EXE: PyInstaller now statically imports the desktop
  platform modules (`src.ship.desktop_app._windows` / `_linux`) so the frozen
  PYZ bundles them — the v0.2.131 bundle omitted them. Release verify fails
  fast when a required module is missing from the built EXE, and a regression
  test asserts both platform modules are present.
- `BrowserFallbackPool.close()` now captures the live browser/playwright
  handles before dropping pool references and closes the pool event loop, so
  playwright's subprocess pipe transports shut down through asyncio's own
  path instead of emitting unclosed-transport ResourceWarnings at GC time.

### Tooling

- The mypy gate now also type-checks `tests/`: 1,841 errors across 292 files
  remediated with honest annotations/casts and zero new suppressions
  (`files = src, tests` in `mypy.ini`), and the Linux CI typecheck step runs
  this gate for real.
- Native MCP stdio server config (`.agents/mcp.json`) registers Serena and
  Basic Memory with the same commands as `opencode.json`, loaded natively by
  the Freebuff CLI.

### Notes

- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.131] - 2026-08-15

> Jobs-quality rollup: the Track A fixes from the 2026-08-12 entry-validation
> audit (static title noise, country normalization, country acceptance contract
> v3) plus runtime-artifact gitignore hygiene.

### Fixed

- Static parser noise-title classification extended (CSS/JS code payloads, nav/UI
  tokens, zero-width/control characters, country-code-as-title) and wired into the
  static listing append paths; `scripts/jobs_artifact_quality_gate.py` gains
  `parserNoiseTitleLeaks` so the previously shipped raw-title contamination
  classes are gate-visible. Regression tests in
  `test_static_parser_noise_titles.py`.
- Country normalization: `normalize_country` maps non-ISO US state codes to `US`
  and non-Latin garbage to `Unknown`; `sanitize_country_text` 2-letter passthrough
  is ASCII-gated. Regression tests in `test_country_normalization.py`.

### Changed

- `data/contracts/country_acceptance.json` v3: real ISO codes added to the
  acceptance contract (MY, TR, HK, LT, VN, QA, CY, UA, CI, EE, RO, BG, ID, PK, AZ,
  GE, MD, MK, PH, GT, PA, ...); `docs/DATA_CONTRACT.md` documents country
  normalization.
- Gitignore hygiene: `data/*.jsonl.gz` rows sidecar and `data/*.lock` feed
  reconciliation lock are runtime artifacts and stay untracked.

### Notes

- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.130] - 2026-08-12

> Desktop rollup: the public desktop line moves from v0.2.119 straight to
> v0.2.130, folding in the container/Umbrel 0.2.120-0.2.129 patch cycle plus
> the jobs-pipeline memory/stability batch below. Shared fixes (pipeline
> memory, browser-fallback pool, lifecycle preservation, fetch transport)
> apply to desktop and container alike.

### Performance

- Jobs pipeline peak-RSS reduction (pi4-tight seat, 1.5 GiB cap): the finalize pass now runs end-to-end at ~916 MiB peak instead of OOM-killing the fetch child. Three stacked peaks were removed: (1) `read_existing_output` streams rows from a `.rows.jsonl.gz` sidecar instead of `json.loads`-ing the 60+ MB blob (560 → 154 MB parse peak); (2) finalize drops the duplicate `to_dict()` snapshot from tombstone reconciliation and skips the lifecycle-state re-read when the on-disk fingerprint is unchanged; (3) `writing_outputs` streams the unified/light/lifecycle-state/tombstone JSON writes one row at a time (the equivalent `json.dumps` paths peaked 355 + 255 + 178 + ~745 MiB at 40k rows / 111k lifecycle entries), frees ~500-700 MiB of dead identity-preparation references after the lifecycle phase (`gc` + `malloc_trim`), and drops the duplicate pydantic re-validation of every tombstone at write time.
- Browser fallback now pools a single Chromium per fetch stage (`BrowserFallbackPool`) instead of launching a fresh browser per call: measured 43 fallback acquisitions on one browser (559 ms startup, 0 relaunches) vs 43 launches in the subset-50 bench. Fresh `BrowserContext` per call preserves session isolation; `BALUFFO_BROWSER_POOL=0` restores the legacy launch-per-call path; crash recovery stays with the existing circuit-breaker cooldown.
- Fixed an O(N·K) alias-index rebuild in carried lifecycle initialization: `_initialize_carried_lifecycle_rows` rebuilt the full ~71k-entry index after every initialized row (~2 s each), stalling `applying_lifecycle` for ~36 min on ~1,000 fresh-identity rows; incremental index updates make it ~12 s end-to-end with identical output.
- Fetch stage bounds the live future set during source execution (windowed submission instead of all 2k+ loaders at once) and adds stall detection with task-state throttling for long-running child stages.
- `BALUFFO_PROFILE_ALLOC=1` gates per-source tracemalloc capture (`run_profiled_alloc`, `scripts/perf_alloc_top.py`) for allocation-profile diagnostics; findings recorded in the archived `jobs-pipeline-memory-reduction-plan.md` (H1: fetch concurrency count, not per-source body size, drives pi4-tight pressure; mw=10 OOMs at 293/500 sources, mw=4 holds peak).
- Fetch response bodies are now capped at `BALUFFO_FETCH_MAX_BYTES` (default 20 MiB, 1 MiB floor) on both transport paths — urllib read and the httpx async stream — instead of fully materializing unbounded pages. Bounds the H2-class amplification measured for the ~37 MiB playsimple-class peak (`httpx/_models.py` 119.7 MiB cumulative in the allocation profile); a truncated page simply parses fewer rows and is retried on the next run.

### Fixed

- `source_skipped` lifecycle preservation no longer accrues availability failures: the skipped-preserve path called `_apply_unverified_availability_entry`, which incremented `consecutiveAvailabilityFailures` and, after `AVAILABILITY_OVERDUE_FAILURE_COUNT=2` + 7 days, marked jobs `verification_overdue` and hid them from the output — even though their sources were simply not run that cycle (cadence, subset filter, or exclusion). A skipped source provides no availability evidence; only failed sources decay, and eligible-missing retirement is unchanged. Previously collapsed re-run outputs (41k → ~1.2k rows) now project 100%.
- Per-stage RSS logging (`[jobs_fetcher] INFO rssMiB=... phase_enter/exit ...`) in finalize phases plus a `finalizeInputs` size line aid future bench diagnostics.
- `BrowserFallbackPool` close now cancels lingering asyncio tasks (playwright driver `Connection.run`) before stopping its event loop, removing the `Task was destroyed but it is pending!` teardown warning while staying idempotent and join-bounded.
- Atomic writers in `src/pipeline_io.py` sweep same-target `*.tmp` siblings older than one hour before writing, so SIGKILL-interrupted writes (70 MB leftover in the bench seed, 1.3 MB on the live Umbrel volume) no longer accumulate on disk.
- `read_existing_output` is sidecar-only: the legacy `json.loads` fallback on the 60+ MB feed blob is removed (it existed to cover a missing `.rows.jsonl.gz` but re-introduced the ~3x parse peak it was meant to avoid). A missing sidecar cold-seeds the run; the feed rebuilds from the lifecycle carry (the source of truth). Deleting a sidecar is safe but cold-seeds the next run.

### Changed

- Bench harness `scripts/perf_pipeline_stages.py` gains `--only-sources-file` (env-file staged `BALUFFO_CONTAINER_PIPELINE_ONLY_SOURCES` to avoid the Windows 32k command-line cap), `--fetch-max-workers-env` (`BALUFFO_CONTAINER_PIPELINE_FETCH_MAX_WORKERS`), `--browser-fallback-max-workers-env` (`BALUFFO_CONTAINER_PIPELINE_BROWSER_FALLBACK_MAX_WORKERS`, service-capped at 6), and `--profile-alloc`; the pipeline completion timeout default rises to one hour for full-seed runs. Bench evidence and root-cause write-ups live in the archived `jobs-pipeline-memory-reduction-plan.md` and `browser-fallback-pool-plan.md`.

### Tooling

- `scripts/perf_alloc_top.py` aggregates the per-source tracemalloc JSONL (`<data>/perf-profiles/allocations.jsonl`) by cumulative MiB and per-source peak.

### Notes

- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.129] - 2026-08-07

### Added
- Pipeline stage ledger: `PipelineService._mark_stage` now appends `{stage, enteredAt, label}` entries to an in-memory `_stageLedger` (hard cap 64 entries) and flushes the ledger into the persisted lifecycle row's `summary.stageLedger` on terminal status (completed / failed / canceled). `task_lifecycle._compact_lifecycle_summary` whitelists the new field so the ledger survives row compaction without a schema version bump.
- Sub-stage observations: `wait_for_report_completion` now records child-process `taskProgress.phaseKey` transitions into the same ledger as `<taskType>/<phaseKey>` entries (e.g. `fetch/loading_state`, `discovery/probing_candidates`) during long-running discovery and fetch waits. Lets the benchmark and future diagnostics attribute wall clock + memory + CPU to sub-stages without a new thread or extra I/O.
- Fetch prep writer now emits three sub-phases inside `loading_state` — `loading_state/read_source_state`, `loading_state/seed_redirect_cache`, `loading_state/read_lifecycle_state`. The bench harness picks them up via the existing sub-stage ledger splice, exposing the exact read that dominates the stage (lifecycle JSON, on every refresh).

### Tooling
- Added `scripts/perf_pipeline_stages.py`: drives `POST /tasks/run-jobs-pipeline` against a seeded container, samples container process memory + CPU from the host (`docker stats` on Windows hosts, direct `/proc/<pid>/stat` on Linux), then cross-references the captured samples against the persisted `stageLedger` to emit per-stage wall-clock durations, peak/average RSS, CPU seconds, and MiB/s rates. Outputs `stages.json`, `samples.ndjson`, `report.md`, `FINDINGS.md` under `_out/perf-pipeline/<run-token>/`. Supports `--profile pi4-tight` (1.5 CPU / 1.5 GiB — raised from 1 GiB so the production-shaped seed's fetch workload fits), `--preset smoke` (default), `--fresh` to force container rebuild, and reuses a healthy container when one is already running. First findings: on the seeded volume, `fetch/loading_state` consumed 83% of pipeline wall-clock (38.6 s of 46.4 s) at 770 MiB peak RSS before the normalize short-circuit landed; post-fix it sits at ~22 s and ~612 MiB.

### Performance
- `read_job_lifecycle_state` short-circuits when the on-disk payload already matches the writer's normalized shape (schemaVersion marker + spot-check of up to 100 rows for status/list/dict field invariants). Files written by `write_job_lifecycle_state` are normalized by construction, so the previous normalize-on-read was a pure no-op costing the dominant 38 s of `fetch/loading_state` on the seeded dataset at ~770 MiB peak RSS. Legacy or drifted payloads still fall back to full normalization; no on-disk schema or call-site changes.
- `read_existing_output` skips `canonicalize_job` for rows already carrying `availabilityId`+`jobLink` — they're already canonicalized by a previous run, so re-running the normalizer is a pure no-op. On the seeded 5.87 MB `jobs-unified.json.gz` (~40 586 rows) this drops fetch prep cost on the host from ~97 s to ~4.2 s and removes two of the three materializations previously alive simultaneously.
- `canonicalize_existing_output_row` returns `CanonicalJob` directly using `dataclasses.replace` to overlay raw-only fields, eliminating the `to_dict()` → `from_mapping()` double round trip. `read_existing_output` duck-types both dict and CanonicalJob returns; `_merge_concurrent_direct_live_rows` accepts either shape unchanged.

### Changed
- Bench `pi4-tight` container profile moved from `1g` to `1.5g` memory. Production-shaped fetch workloads peak around 1.2 GiB inside `executing_sources`; the previous 1 GiB cap SIGKILLed the fetch child without a terminal report (surfaced as `owner_inactive_without_terminal_report` in the bridge).

### Notes
- This is a container/Umbrel patch only. No `v0.2.129` desktop tag, GitHub desktop release, desktop update, or desktop assets are published.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, and the private community app-store metadata contract.
- wildcard browser CORS allow headers and desktop localhost bridge compatibility remain unchanged from `0.2.128`.

### Fixed
- `/tasks/abort` no longer hangs while the kill + report-repair run. The route now returns `202 aborting` as soon as the lifecycle row flips and runs `process_registry.terminate`, `repair_fetch_canceled_evidence`, and pipeline propagation on a daemon thread. Terminal/canceled branches still answer synchronously (`aborted: true` preserved). Admin "Stop fetch" becomes usable on a running job.
- Container gateway `_handle_abort` no longer double-reads the request body when forwarding non-pipeline aborts to the bridge. Previously the peek at the body + a second `rfile.read` inside `_proxy` made `/tasks/abort {taskType:"fetch",...}` block until the gateway timeout hit, masking genuine async work.
- `/admin/ops-tab-counts?view=summary` cold-open no longer pays full aggregation cost on every Admin refresh. Response body is now cached on disk next to the runtime state, keyed on input-file mtimes plus a 30 s TTL safety net. Second visit on unchanged data drops from ~3 s to tens of ms on Pi-class hardware. Admin "Sources" page and Ops badges feel immediate.
- Container startup now pre-warms `/registry/conflicts?view=full` in the background once the bridge is ready, so the first user click on Admin → Conflicts reads cache instead of paying a 3–4 s on-demand recompute. Warming is opportunistic; failure logs to stderr and never blocks startup.

### Added
- Per-stage abort telemetry on `/ops/performance-profile.operationTimings.operations`: `abort.validate`, `abort.flip_lifecycle_row`, `abort.process_terminate`, `abort.cancel_evidence.fetch`, `abort.cancel_evidence.discovery`, `abort.finalize.pipeline`, `abort.finalize.process_run`, `abort.respond_async`. Captured via the existing `performance_profile.record_operation_duration` ring buffer; no schema change.
- `/tasks/abort` HTTP route now calls `abort_task_async` (added to `BridgeApi`, exposed through `admin_entrypoint_api` + `bridge.bootstrap` with matching default not-implemented fallbacks). Pipeline-child aborts still go through the synchronous `abort_task` to keep serialization against `request_abort_run`.
- `/registry/sources?view=table` now accepts `detail=summary` to skip the auto-approval pending annotation pass. The annotation work walks all active/pending aliases plus the discovery candidates artifact; it isn't needed just to render table rows. Admin startup lane now defaults to `detail=summary`; per-row drill and legacy diagnostics callers can still ask for `detail=full` (default remains `full` for backward compat — external callers see no change unless they opt in to `summary`).

### Tooling
- Added `scripts/perf_admin_seed.py` + `scripts/perf_admin_flows.py` to benchmark every Admin-facing GET route and composite UI flow (bootstrap, sources drill, conflicts drill, fetcher trigger, sync ready) against a container started with Pi-class CPU/memory caps (`pi4-tight`/`pi4-roomy`) on a seeded local `/data` volume. Outputs `routes.json`, `flows.json`, `report.md`, and `meta.json` under `_out/perf-admin-flows/<run-token>/`. Wired `perf:admin:seed`, `perf:admin:flows`, and `perf:admin:flows:roomy` into `package.json`, and added a manual `.github/workflows/perf-admin-flows.yml` lane that accepts an optional seed artifact and a GHCR image. Seed data stays on the host and never lands in git.

## [0.2.128] - 2026-08-01

### Fixed
- Container gateway no longer 504s on heavy Admin routes. Hot summary endpoints keep the 8 s fast-path cutoff; heavy detail/mutation routes (`/registry/sources`, `/registry/conflicts`, `/admin/ops-tab-counts`, `/dedup/review-action`, registry and discovery mutations, fetch/pipeline triggers, `/sources/check`, `/fetcher/*`) now proxy with a 60 s budget, which matches the observed p95 on this production dataset (~2,300 active sources, 37 k jobs).

### Notes
- This is a container/Umbrel patch only. No `v0.2.128` desktop tag, GitHub desktop release, desktop update, or desktop assets are published.
- Direct-link enforcement remains in shadow mode.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.127] - 2026-08-01

### Fixed
- Admin Registry Conflicts detail page: the bridge now caches the expensive full-payload derivation and the container gateway no longer 504s after 8 s when recomputation is slow on large registries (≈2,300 active sources, 37 k jobs). First view computes; follow-up views return the cached payload immediately.

### Notes
- This is a container/Umbrel patch only. No `v0.2.127` desktop tag, GitHub desktop release, desktop update, or desktop assets are published.
- Direct-link enforcement remains in shadow mode.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.126] - 2026-08-01

### Changed
- Multi-wave dead-code and cleanup pass: removed dead bridge routes (`/ops/fetch-report/sources`, `/registry/{active,pending,rejected,rollback,restore-deleted}`), retired the `adapters/api.py` and `adapters/social_parsers.py` compat layers, dropped the orphaned `adapters.run_loader` helper, consolidated storage helpers onto `src.shared`, extracted the packaged-smoke scaffolding helper shared by 11 smoke scripts, and removed the unused IntersectionObserver branch plus dead frontend runtime `security.github_app_enabled_default` exposure.
- CI Python lane now runs tests in parallel (`pytest -n auto --dist=loadfile`).

### Security
- Pinned `serena-agent==1.6.1` install path and bumped `pyasn1` to 0.6.4 plus `brace-expansion`/`js-yaml` via `npm audit fix`, clearing the Dependabot and pip-audit findings.

### Notes
- This is a container/Umbrel patch only. No `v0.2.126` desktop tag, GitHub desktop release, desktop update, or desktop assets are published.
- Direct-link enforcement remains in shadow mode.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.125] - 2026-07-18

### Fixed
- Saved unavailable reports now remain visible with explicit confirmation, a reported-state badge, Clear/Undo recovery, and shared styled tooltips.
- Availability checks now return a run identifier promptly while lifecycle resolution and validation continue asynchronously, with duplicate-run reuse and terminal cleanup on worker failures.
- Admin older-run history now merges bounded refreshes without discarding the loaded history or disclosure state.

### Notes
- This is a container/Umbrel patch only. No `v0.2.125` desktop tag, GitHub desktop release, desktop update, or desktop assets are published.
- Direct-link enforcement remains in shadow mode.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.124] - 2026-07-18

### Fixed
- Fetch-report summary now recovers the newest terminal report when a stale active projection remains after a worker crash, keeping Admin status truthful.
- Jobs and Saved availability actions now use compact accessible icon controls with stable layout sizing, preventing clipped or overlapping refresh, check, report, and link actions.

### Notes
- This is a container/Umbrel patch only. No `v0.2.124` desktop tag, GitHub desktop release, desktop update, or desktop assets are published.
- Direct-link enforcement remains in shadow mode.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.123] - 2026-07-17

### Fixed
- Availability identity preflight now performs a second exact collision pass after deterministic assignments, repairing URL-bearing rows with URL-backed identities and quarantining/excluding only URL-less members of contaminated groups.
- The production pattern where a legacy row ID collided with a newly generated source-backed ID no longer aborts publication through `post_filter_identity_invariant_failed`.

### Tests
- Added single-group and eight-group production collision regressions covering URL repair, URL-less observation exclusion, lifecycle evidence removal, quarantine, and zero post-filter conflicts.

### Notes
- This forward container/Umbrel patch supersedes the failed 0.2.122 live pipeline publication. Direct-link enforcement remains in shadow mode, and no `v0.2.123` desktop tag, GitHub desktop release, desktop update, or desktop assets are published.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.122] - 2026-07-17

### Fixed
- Availability identity preflight now quarantines and excludes candidates that cannot receive an exact collision-safe identity, allowing valid rows to publish atomically without fuzzy recovery or false lifecycle observations.
- Finalization exceptions now write bounded terminal error reports, inactive failed task progress, phase timings, and stable error codes before worker exit, so parent pipelines receive the actual failure instead of orphan fallback diagnostics.

### Changed
- The private availability identity quarantine uses schema v2 with tolerant v1 reads, hashed unresolved-alias evidence, deterministic 30-day/2,000-entry retention, and explicit truncation counts.
- Fetch-report full and summary projections now distinguish accepted, repaired, contaminated, rejected, quarantined, and post-filter identity counts; rejected candidates degrade coverage without becoming feed-integrity failures.

### Tests
- Added exact URL-backed repair and URL-less exclusion regressions, quarantine v1/v2 retention and truncation coverage, terminal failure propagation tests, and a 79,528-candidate synthetic identity audit.
- A fresh full local pipeline published 40,586 monitorable rows with zero missing availability identities, zero cross-URL identity collisions, zero rejected rows, and direct-link classification still in shadow mode.

### Notes
- This is a container/Umbrel patch. No `v0.2.122` desktop tag, GitHub desktop release, desktop update, or desktop assets are published.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.121] - 2026-07-16

### Fixed
- Availability identity preflight now repairs reused source IDs across unrelated canonical URLs with collision-safe URL identities, quarantines ambiguous legacy evidence privately, and rejects incomplete or cross-URL publication.
- Monitorable carried feed rows now receive lifecycle coverage without becoming observations or refreshing `lastSeenAt`; trustworthy source absence and conservative failed/skipped-source ageing continue to apply.
- Non-custom Saved jobs affected by an identity repair now rebind only through an exact unique stored URL match, otherwise becoming unmonitored without changing application tracking or historical activity.
- Fetch-report full and summary paths preserve bounded availability health, identity audit, source/direct conflict, sweep coverage, and shadow classifier evidence; obsolete normalized CSV output keys are removed.

### Changed
- Pipeline finalization now reports truthful indeterminate deduplication, identity reconciliation, lifecycle, quality-audit, and output phases with periodic heartbeats and completed elapsed timings.

### Tests
- Added exact identity/quarantine, carried-seed, Saved migration, report normalization, private-serving, and 5,000-row synthetic feed regressions.

### Notes
- This is a container/Umbrel patch. Direct-link enforcement remains in shadow mode, and no `v0.2.121` desktop tag, GitHub desktop release, or desktop assets are published.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.120] - 2026-07-16

### Added
- Jobs now publish stable availability identities and canonical `available`, `verification_overdue`, and `unavailable` states, with a lazy 30-day history artifact and source-aware health summaries.
- Saved jobs now receive idempotent availability attention, timeline events, acknowledgement controls, profile-local unavailable reports, and backup schema v4 coverage.
- Desktop and container bridges expose bounded background availability checks; direct-link classification starts in shadow mode pending the documented seven-day promotion gate.
- Jobs now expose only light/startup JSON projections publicly; CSV publication is removed and full JSON is retained only as a deprecated private pipeline/rollback handoff.
- Saved availability badges now refresh through a bounded exact-identity overlay, including private custom monitoring without exposing custom URLs.

### Fixed
- Previously published seed rows no longer count as observations in later scans or refresh `lastSeenAt`, so trustworthy source absence can retire stale openings without network failures causing false closure.

### Tests
- Added lifecycle, identity, direct-validator, sweep-planning, bridge-route, local-data, backup, and frontend availability regressions.

### Notes
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.119] - 2026-07-05

### Fixed
- Desktop Admin now hydrates compact active-run KPI and schedule summaries while a pipeline is running, so the overview cards and schedule controls do not stay in Umbrel-style protected placeholders.
- Active pipeline abort recovery now single-flights pipeline-status and task-state summary polling, preventing repeated request storms after a canceled run reports idle.
- Umbrel/container active-run protection remains intact: storage health, full fetch reports, registry summaries/source-table fan-out, and full diagnostics stay out of active polling.

### Tests
- Added desktop active-run Admin regressions for KPI/schedule hydration and abort-shaped request-budget coverage for pipeline-status and task-state polling.

### Notes
- This supersedes `0.2.118`, which shared the Umbrel active-run route budget with desktop validation but left desktop Admin too conservative during local active pipeline runs.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.118] - 2026-07-04

### Fixed
- Task lifecycle writes now compact every retained `admin-task-lifecycle.json` row, so historical hot fetch/discovery payload bloat is cleaned on the next lifecycle mutation while preserving run identity, terminal state, scalar summaries, output paths, and bounded warnings/errors.
- Generic non-fetch lifecycle rows now drop stale nested source, candidate, diagnostic, job, and work-item payloads from `progress` and `summary`; lifecycle remains a shared desktop/container authority for liveness and terminal state, not a hot progress mirror.
- Recent Umbrel active-fetch recovery contracts are now shared desktop package validation requirements: bounded fetch-report summary/live views, visible fetch prep/finalization phases, write coalescing, and active-run route-budget discipline.

### Tests
- Added regressions for next-write historical lifecycle compaction, generic nested payload trimming, read-only no-rewrite behavior, retention at 240 lifecycle rows, and compact SQLite shadow projection parity.

### Notes
- This supersedes `0.2.117`, which fixed drained-source finalization visibility but left old lifecycle rows able to keep historical payload bloat on disk. The desktop package is validated locally for this shared runtime fix, but no public desktop tag or GitHub desktop release is created.
- Container-only pipeline throughput knobs remain container-only; desktop fetch worker defaults are unchanged.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.117] - 2026-07-04

### Fixed
- Active fetch hot summaries now publish `finalizing_sources` as soon as all source rows are terminal and output writing has not started yet, so Admin/JOBS no longer remain stuck on `executing_sources` after the source queue is drained.
- Fetch progress for the drained-source finalization phase keeps complete source counts and running/queued `0` visible without exposing a misleading source-count ETA.
- `writing_outputs` remains the separate output/report write phase and still wins once final output files begin writing.

### Tests
- Added regressions for drained-source finalization phase publication, no fake ETA during finalization, frontend progress rendering, and compact lifecycle payload preservation.

### Notes
- This supersedes `0.2.116`, which fixed aggregate ETA and compact route stability but still left drained-source finalization hidden behind `executing_sources` for several minutes on live Umbrel. No public desktop tag is created.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.116] - 2026-07-04

### Fixed
- Active fetch hot summaries now promote bounded aggregate progress from running sources such as `scrapy_static_sources` into scalar `taskProgress.counts` fields, including aggregate completed/total/running/queued/error counts and aggregate ETA basis.
- Admin/JOBS active fetch rendering now shows aggregate browser-fallback tail progress and uses aggregate ETA when source-count ETA would be misleading.
- Active fetch ETA now omits `estimatedRemainingMs` when aggregate progress exists but no reliable aggregate rate can be computed, avoiding fake near-zero completion estimates.

### Tests
- Added regressions for aggregate-tail ETA selection, unreliable aggregate ETA suppression, existing non-aggregate source ETA behavior, Jobs active aggregate progress rendering, and bounded active fetch write cadence preservation.

### Notes
- This supersedes `0.2.115`, which restored compact route stability, final recovery, and throughput but still left aggregate-tail fetch progress with misleading source-count ETA during `scrapy_static_sources`. No public desktop tag is created.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.115] - 2026-07-04

### Fixed
- Active fetch progress preserves `taskProgress.counts.runningSourceNames` as a bounded `string[]` across hot summaries, task-state, and `/ops/task-live/fetch?view=summary`. Other count fields remain scalar compatibility values.
- Admin/JOBS active fetch rendering now uses bounded hot summaries to show completed/total source counts, running/queued counts, rate, ETA, and capped current source names without calling full fetch-report routes.
- Umbrel pipeline-launched fetch now uses the higher bounded container-only throughput profile: `BALUFFO_CONTAINER_PIPELINE_FETCH_MAX_WORKERS` defaults to `10`, clamps to `1..12`, keeps `maxPerDomain=2`, keeps static detail concurrency at `4`, and caps adapter HTTP concurrency at `24`. Manual fetch and desktop defaults are unchanged.
- Container pipeline fetch adds `BALUFFO_CONTAINER_PIPELINE_BROWSER_FALLBACK_MAX_WORKERS`, defaulting to `4` and clamped to `0..6`, so increased source-worker throughput does not create unbounded Playwright fallback pressure.
- Active schedule fallback now preserves the "after current run completes" shape where Admin can render it, instead of falling back to a blank/loading schedule while a due pipeline is active.

### Tests
- Added regressions for the active fetch progress array contract, Jobs active progress rendering, pipeline-only container profile defaults and clamps, dedicated browser fallback caps, active schedule fallback preservation, and bounded source-execution write behavior.

### Notes
- This supersedes `0.2.114`, which restored compact route stability and bounded write pressure but still left active fetch throughput and Jobs/schedule progress contract gaps. No public desktop tag is created.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.114] - 2026-07-04

### Fixed
- Umbrel pipeline-launched fetch now uses a bounded container-only throughput profile: `BALUFFO_CONTAINER_PIPELINE_FETCH_MAX_WORKERS` defaults to `6`, clamps to `1..8`, keeps `maxPerDomain=2`, keeps static detail concurrency at `4`, and caps adapter HTTP concurrency at `24`. Manual fetch and desktop defaults are unchanged.
- Active fetch hot summaries now include bounded execution timing, completion rate, coarse ETA, and capped running source names so Admin/JOBS can show real source execution progress during long fetches.
- Active source execution no longer forces `jobs-fetch-tasks.json`, `jobs-fetch-report-summary.json`, the active snapshot, or full `jobs-fetch-report.json` writes per source start/finish. Hot task state and summary sidecars update at phase/terminal boundaries and otherwise no faster than a `5s` cadence, while same-phase source execution skips full-report rewrites.
- Fetch lifecycle rows now stay compact: active heartbeats carry only run identity, coarse phase/progress counts, summary scalars, and capped running-source names, and stale oversized fetch lifecycle rows are compacted on the next lifecycle save.

### Tests
- Added regressions for active execution write coalescing, sparse full-report writes, lifecycle compaction, pipeline-only container fetch profile clamping, active fetch rate/ETA payloads, and frontend progress rendering.

### Notes
- This supersedes `0.2.113`, which restored compact route stability under active fetch but still left long-running full pipeline fetches bottlenecked by conservative container throughput and unnecessary active-run artifact write pressure. No public desktop tag is created.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.113] - 2026-07-03

### Fixed
- Umbrel fetch-report live fallback metadata now makes omitted source samples explicit when no compact sidecar is available: `/ops/fetch-report?view=live` returns `sources: []`, preserves `sourceCount`, sets `sourcesTruncated: true`, and points detail callers to `/ops/fetch-report/sources` without parsing the full report body.

### Tests
- Restored green Python CI evidence by aligning the bounded fetch-report route tests with the compact live-report contract and by checking task-state writes separately from the new compact summary sidecar writes.

### Notes
- This supersedes `0.2.112` because the CI repair was first built under the existing `0.2.112` image tag, which leaves Umbrel with no reliable new-version signal. `0.2.113` is the deterministic forward Umbrel/container identity for the same bounded fetch-report recovery line. No public desktop tag is created.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.112] - 2026-07-02

### Fixed
- Umbrel active/final fetch state no longer depends on request-time full fetch-report hydration. Fetch finalization writes a bounded `jobs-fetch-report-summary.json` sidecar on phase changes and terminal closeout, and compact Admin polling surfaces use that sidecar, `jobs-fetch-tasks.json`, and the active snapshot before any full report path.
- `/ops/fetch-report?view=summary` and `/ops/fetch-report?view=live` now return bounded payloads for large reports; live view caps source samples and points detail callers to `/ops/fetch-report/sources`.
- Fetch finalization publishes `writing_outputs` to hot task state before heavy output/report writes, so `/ops/task-live/fetch?view=summary` and `/ops/task-state?view=summary` cannot remain stuck on source execution while terminal report closeout is still in progress.
- The container gateway now gives `/sync/status?view=summary` a bounded summary fallback/cache and rejects active schedule payloads whose `nextRunAt` is already past, preventing 504s, false disabled sync config, and stale schedule triggers during active work.
- Admin fetch completion polling now uses the compact fetch summary route instead of the full `/ops/fetch-report` body.

### Tests
- Added focused regressions for oversized fetch summary/live routes, compact completion polling, `writing_outputs` hot-state propagation, gateway sync timeout fallback/cache, active schedule stale-date rejection, and bounded task-state summary sidecar recovery.

### Notes
- This supersedes `0.2.111`, which exposed fetch preparation phases but still let compact Admin final-state routes depend on full fetch-report hydration and slow gateway paths on real Umbrel reports. No public desktop tag is created.
- Source tables may remain visibly delayed during active fetch, but pipeline final state, schedule, sync readiness, and control-panel state now use bounded hot summaries and gateway fallbacks.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.111] - 2026-07-02

### Fixed
- Umbrel fetch startup now emits bounded preparation progress before source execution begins, so Admin shows phases for loading fetch state, seeding existing output, selecting sources, applying exclusions, and initializing runtime instead of leaving a child fetch stuck on generic `starting`.
- Fetch preparation progress uses the existing `/ops/task-live/fetch?view=summary`, `jobs-fetch-tasks.json`, and active-task snapshot contracts with compact additive `taskProgress` phase/count/timing fields; no new route, JSONL stream, per-row progress stream, storage-health polling, or full diagnostics path is added.
- Fetch lifecycle heartbeats no longer rewrite `admin-task-lifecycle.json` every hot progress tick; lifecycle remains for run identity, terminal state, and bounded coarse phase heartbeats while live UI state stays on `jobs-fetch-tasks.json` and the active snapshot.

### Tests
- Added focused backend coverage for compact fetch-prep task-state writes, setup timing persistence, rate-limited same-phase prep updates, and lifecycle heartbeat throttling.
- Added frontend coverage that active fetch preparation renders phase/count text without regressing into misleading `0 sources resolved` execution progress.

### Notes
- This supersedes `0.2.110`, which fixed active schedule behavior but left fetch preparation silent and allowed avoidable lifecycle write pressure during the pre-source setup window. No public desktop tag is created.
- The existing-output fast path is intentionally not shipped in `0.2.111`; live and synthetic equivalence checks showed canonical payload differences, so the release keeps current canonicalization and ships visibility, timing, and disk-pressure safeguards first.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.110] - 2026-07-01

### Fixed
- Umbrel container `/tasks/jobs-pipeline-schedule` now uses the same schedule-specific bounded bridge timeout as Admin bootstrap/dashboard schedule hydration, so the canonical schedule route does not fall back blank while the bridge can still answer within the allowed schedule window.
- This supersedes `0.2.109`, which fixed one fallback anchor path but still let the direct schedule route use the generic short gateway timeout under live Umbrel load.

### Notes
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.109] - 2026-07-01

### Fixed
- Umbrel container schedule fallback now derives `nextRunAt` from the completed pipeline status control file when lifecycle rows are unavailable, so a degraded `/tasks/jobs-pipeline-schedule` response cannot blank the next trigger after a real pipeline completion.
- This supersedes `0.2.108`, which repaired Admin active-idle recovery but still allowed the container gateway schedule fallback to emit degraded empty schedule data on live Umbrel installs missing a terminal lifecycle row.

### Notes
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.108] - 2026-06-30

### Fixed
- Umbrel Admin now runs a bounded active-idle recovery pass after pipeline/fetch work settles, refreshing final task state, schedule, recent activity, sync summary, and Ops badges before attempting source-table lazy hydration.
- Degraded Admin bootstrap sync stubs no longer overwrite an authoritative ready sync state with disabled/unknown UI; Admin now shows an explicit delayed sync state until the compact sync summary refresh completes.
- Container gateway schedule fallback now waits long enough for the bounded bridge schedule route and preserves computed recurring pipeline schedule data when the bridge route is delayed, so degraded fallback cannot blank the next trigger date.

### Tests
- Added regressions for active-idle Admin recovery sequencing, degraded sync bootstrap rendering, non-empty schedule fallback data, and source-table refresh remaining last and non-blocking.

### Notes
- This supersedes `0.2.107`, which fixed idle startup fan-out but still allowed degraded bootstrap/schedule/sync/control-panel state to win around active pipeline work. No public desktop tag is created.
- Source-table loading may remain visibly delayed while a pipeline is active, but schedule, sync, control-panel final state, and pipeline completion state have their own bounded recovery path.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.107] - 2026-06-30

### Fixed
- Umbrel Admin startup now queues the compact source-table hydration before fallback schedule/history refreshes, so Pending, Active, and Rejected source containers render visible loading states and start the bounded table request without waiting behind slower Ops routes.
- Admin startup heavy reads now run through a single sequential startup lane: the source-table request completes before registry conflict counts, fetch KPIs, and Ops tab counts hydrate, preventing the concurrent bridge fan-out that could produce Umbrel 504s for `/registry/sources` and `/admin/ops-tab-counts`.
- Action Center storage diagnostics no longer run in the first startup window or while Admin startup bridge work is active; storage health remains available on the normal later poll/manual diagnostics path without adding startup pressure.

### Tests
- Admin startup diagnostics and browser hydration smoke now assert source placeholders, single-owner schedule/history fallback loading, no startup storage-health probe, one compact source-table request, and no overlap between source, KPI, tab-count, and registry-conflict startup routes.

### Notes
- This supersedes `0.2.106`, which removed the arbitrary 60-second source-table delay from `0.2.105` but still allowed startup fan-out and late source scheduling under real Umbrel browser timing. No public desktop tag is created.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.106] - 2026-06-29

### Fixed
- Umbrel Admin source tables now render loading placeholders immediately after bootstrap and start the compact `sourceTablesOnly` refresh without the arbitrary 60-second delay from `0.2.105`.
- Admin startup still protects Umbrel by allowing only the bounded `/registry/sources?view=table&limitPerBucket=250` source-table request on idle startup, while keeping full registry, discovery, fetch, and log diagnostics out of the boot path.

### Tests
- Admin startup and hydration smoke coverage now fails on blank source containers, multi-second source-table startup timers, repeated startup source-table requests, or full diagnostics during initial Admin boot.

### Notes
- This supersedes `0.2.105`, which restored green CI/release hygiene but introduced unacceptable blank and delayed Admin source-table hydration. No public desktop tag is created.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.105] - 2026-06-29

### Fixed
- Umbrel `0.2.105` preserves the live-healthy Admin fallback hydration behavior from `0.2.104` while restoring green frontend unit-test evidence for the container release commit.

### Tests
- Admin startup diagnostics coverage now matches the current fallback flow: core summary routes hydrate first, and source-table loading remains delayed with `sourceTablesOnly`.
- Admin run-diagnostics coverage now invokes the copy callback once explicitly instead of making each history render simulate a user copy.

### Notes
- This supersedes the live-validated but CI-incomplete `0.2.104` Umbrel image; no public desktop tag is created.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.104] - 2026-06-29

### Fixed
- Umbrel Admin pipeline schedule now preserves the last valid next-run date when a later degraded schedule fallback lacks `nextRunAt`, preventing the row from flickering back to `schedule details refreshing`.
- Admin source tables now treat degraded empty compact registry payloads as refreshing count placeholders instead of authoritative empty source lists.

### Tests
- Admin smoke and registry controller coverage now catch schedule fallback/date regression and degraded-empty source table payloads before Umbrel test builds are accepted.

### Notes
- This supersedes the failed `0.2.103` Umbrel test image; no public tag is created until live stability is proven.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.103] - 2026-06-29

### Fixed
- Umbrel Admin pipeline schedule now keeps the saved enabled state and interval visible while next-run details are refreshing, preventing transient schedule responses from leaving the row stuck on loading controls.
- Admin schedule rendering now retries delayed next-run status without treating known saved schedule config as a failed authority response.

### Tests
- Bundled Admin smoke now covers transient schedule authority responses, pending-source KPI hydration, Ops tab badges, Operations Activity, and startup route evidence before Umbrel test builds are accepted.

### Notes
- This supersedes the failed `0.2.102` Umbrel test image; no public tag is created until live stability is proven.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.102] - 2026-06-29

### Fixed
- Umbrel Admin degraded startup now hydrates fetch KPI cards and Ops tab badges shortly after authoritative schedule/history load, so the Ops Overview does not remain stuck on loading placeholders when backend authority routes are healthy.
- Admin source-table hydration is moved out of the startup window so it cannot race ahead of schedule/KPI/tab-count authority and recreate first-render timeout pressure.

### Tests
- The bundled Admin hydration smoke now reproduces the live-like degraded Ops Overview and fails when schedule, KPI cards, pending-source count, or tab badges remain stuck after their authoritative routes return.

### Notes
- This supersedes the failed `0.2.101` Umbrel test image; no public tag is created until live stability is proven.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.101] - 2026-06-28

### Fixed
- Umbrel Admin now lets authoritative schedule/history hydration render against the current visible Ops shell instead of stale startup render tokens, preventing successful lightweight route responses from leaving schedule or activity stuck loading.
- Idle Admin startup now defers heavy KPI, tab-count, registry-conflict, and source-table hydration until after schedule/history authority has loaded, reducing browser-visible 504 pressure during first render.

### Tests
- The Admin hydration smoke now builds and serves the hashed container frontend bundle used by Umbrel, and fails if heavy Admin routes are requested during the startup hydration window.

### Notes
- This supersedes the failed `0.2.100` Umbrel test image; no public tag is created until live stability is proven.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.100] - 2026-06-28

### Fixed
- Umbrel Admin pipeline schedule and Operations Activity now hydrate from their authoritative lightweight routes during startup, so degraded bootstrap/dashboard payloads cannot leave schedule stuck loading or activity falsely empty.
- Admin now preserves authoritative schedule/activity state across shell refreshes and rebinds replaced DOM targets before rendering.

### Tests
- Added a browser-based Admin hydration smoke that stubs degraded bootstrap/dashboard payloads plus authoritative schedule/history routes, catching false loading/default/empty states before Umbrel test images are pushed.

### Notes
- This remains an Umbrel/live-stability test build on the current release line; no public tag is created until live stability is proven.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.99] - 2026-06-27

### Fixed
- Umbrel Admin pipeline schedule rendering now uses a dedicated authoritative schedule model hydrated only from `/tasks/jobs-pipeline-schedule` or a successful schedule save, so degraded bootstrap/dashboard payloads cannot reset the row to unchecked `24h` defaults.
- Unknown schedule state now renders disabled loading/retrying controls instead of editable false defaults.

### Notes
- This remains an Umbrel/live-stability test build on the current release line; no public tag is created until live stability is proven.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.98] - 2026-06-27

### Fixed
- Umbrel Admin bootstrap now treats missing schedule data as incomplete even when the shell route succeeds, and waits for the authoritative pipeline schedule route before leaving the first useful render.

### Notes
- This remains an Umbrel/live-stability test build on the current release line; no public tag is created until live stability is proven.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.97] - 2026-06-27

### Fixed
- Umbrel Admin degraded bootstrap now forces an authoritative pipeline schedule refresh after rendering the shell, so the schedule row resolves from `loading` to the real next fetch date.

### Notes
- This remains an Umbrel/live-stability test build on the current release line; no public tag is created until live stability is proven.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.96] - 2026-06-27

### Fixed
- Umbrel Admin degraded bootstrap and dashboard fallbacks no longer publish factual schedule, KPI, registry, sync, or profile data; they now keep the shell usable while authoritative routes refresh the real values.
- Pipeline schedule rendering now ignores stale degraded fallback state, so a valid next scheduled fetch from `/tasks/jobs-pipeline-schedule` cannot be overwritten by `due now`.

### Notes
- This remains an Umbrel/live-stability test build on the current release line; no public tag is created until live stability is proven.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.95] - 2026-06-27

### Fixed
- Runtime SQLite startup now avoids quick-check scans entirely, and storage-health quick-checks are deferred for oversized runtime databases so large Umbrel data stores cannot wedge app readiness.
- Runtime SQLite WAL files now trigger size-based background checkpoint maintenance, and storage health reports database, WAL, SHM, and checkpoint status for diagnosis.
- `storage-metrics.jsonl` now rotates at a bounded size and `/ops/storage-metrics` reads only a tail window, preventing diagnostics growth from becoming a startup or Admin I/O hazard.

### Notes
- This remains an Umbrel/live-stability test build on the current release line; no public tag is created until live stability is proven.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.94] - 2026-06-27

### Fixed
- Admin no longer renders an active scheduled pipeline as `due now`; running pipelines now state that the next scheduled run follows the current pipeline completion when no exact next timestamp is available.
- Admin source tables now use an active-safe compact registry path during running fetch/pipeline work instead of waiting for the entire job or calling the full registry table route.
- Admin KPI cards preserve or lazily hydrate historical values during active jobs instead of remaining indefinitely delayed.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.93] - 2026-06-27

### Fixed
- Pipeline schedules with no prior terminal pipeline run now anchor the next run to the schedule save time plus the configured interval instead of immediately showing `due now`.
- The Umbrel container gateway schedule fallback now uses the same no-history anchor policy as the bridge scheduler, including compatibility fallback to the existing schedule file modification time.

### Notes
- This remains on the current shared release line covering the same-origin Linux container, Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.92] - 2026-06-27

### Fixed
- Umbrel Admin degraded dashboard/bootstrap payloads now prefer the bridge pipeline schedule service when it is available, preventing incomplete fallback evidence from replacing a valid next scheduled fetch with `due now`.
- Admin schedule rendering now prefers a concrete future `nextRunAt` over a stale `due` flag, so enabled schedules show the next date when that date is known.
- Bumped the Admin cache chain so existing Umbrel browser sessions load the 0.2.92 schedule display fix.

### Notes
- This remains on the current shared release line covering the same-origin Linux container, Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.91] - 2026-06-27

### Fixed
- Browser fallback now classifies Playwright `EPIPE` and transport-closed failures as recoverable browser-environment failures, triggering the existing fallback cooldown instead of treating them like source/parser failures.
- Playwright page/browser cleanup is now best-effort so transport-close noise is contained by the fallback path.

### Notes
- This remains on the current shared release line covering the same-origin Linux container, Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.90] - 2026-06-26

### Fixed
- Umbrel Admin schedule fallback now computes the next scheduled pipeline time from the latest terminal pipeline row when the bridge is degraded, so active fetches do not show `next unknown`.
- Admin schedule rendering now avoids `next unknown`; if an exact timestamp is unavailable while a pipeline is active, it states that the next run is after the current pipeline completes.
- Bumped the Admin cache chain so existing Umbrel browser sessions load the 0.2.90 schedule display fix.

### Notes
- This remains on the current shared release line covering the same-origin Linux container, Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.89] - 2026-06-26

### Fixed
- Admin no longer renders a false-empty Stored Profile Overview from degraded Umbrel bootstrap data; missing overview data now appears as delayed and retries the fast local-data overview before reporting an authoritative empty profile list.
- Bumped the Admin cache chain so existing Umbrel browser sessions load the 0.2.89 overview fix.

### Notes
- This remains on the current shared release line covering the same-origin Linux container, Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.88] - 2026-06-26

### Fixed
- Admin source-table startup now requests bounded registry table rows with `limitPerBucket`, reducing idle Umbrel registry payload pressure while keeping the existing full `/registry/sources` compatibility route available.
- The registry table view now supports an additive `limitPerBucket` query for bounded Admin loads and reports truncation metadata in the existing summary envelope.

### Notes
- This remains on the current shared release line covering the same-origin Linux container, Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.87] - 2026-06-25

### Fixed
- The Umbrel container gateway now returns bounded degraded-idle payloads for Admin bootstrap, task-state summary, task-live summary, dashboard summary, and pipeline schedule reads when the internal bridge is slow, preventing idle Admin boot from collapsing into repeated `HTTP 504` errors.
- Admin now treats recent bridge-heavy read timeouts as a degraded-idle state, keeps the shell usable, and delays registry source-table reads with explicit retry placeholders instead of immediately reloading `/registry/summary` and `/registry/sources?view=table`.

### Notes
- This remains on the current shared release line covering the same-origin Linux container, Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.86] - 2026-06-25

### Fixed
- Bounded fetcher and discovery log reads now preserve existing UTF-8 cursor behavior when a log ends with an incomplete multi-byte sequence, keeping `nextOffset` aligned with consumed text instead of raw partial bytes.

### Notes
- This remains on the current shared release line covering the same-origin Linux container, Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.85] - 2026-06-25

### Fixed
- Admin live fetch and discovery log polling now uses bounded log slices instead of unbounded offset reads, preventing large active logs from triggering Umbrel gateway timeouts while preserving the existing log payload shape.
- Fetcher and discovery log routes now enforce bounded offset and tail reads server-side, so stale cursors cannot return multi-megabyte responses.
- The Admin Action Center now delays health, sync, and storage probes while active job updates are known, relying on compact task-status routes until active work returns idle.

### Notes
- This remains on the current shared release line covering the same-origin Linux container, Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.84] - 2026-06-24

### Fixed
- Jobs pipeline progress labels now tolerate browser/server clock skew by using the live server snapshot timestamp when it is newer than the browser clock, preventing active updates from appearing stuck at `Checking sources... 0s`.
- The Jobs update button continues to show the current pipeline stage from `/tasks/run-jobs-pipeline-status`, so active Discovery, Fetch, and Sync work remain visibly distinct while elapsed time advances correctly.
- Aborting a Jobs pipeline now keeps issuing abort requests to the active fetch/discovery child while it remains live, surfaces child abort warnings, and clears or fails the `Aborting...` state after verification instead of leaving the UI stuck.
- Admin active-run polling now defers heavy fetch KPI, dashboard, storage-health, registry-summary, and bootstrap lifecycle reads when compact pipeline/task-state evidence is available, preventing repeated Umbrel `HTTP 504` timeouts during broad job updates.

### Notes
- This is a forward shared desktop and Umbrel patch after `0.2.83`; no existing release tags are moved or recreated.
- Container/Umbrel compatibility from the current public release line remains intact: same-origin Linux container mode, Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, avoidance of wildcard browser CORS allow headers, and desktop localhost bridge compatibility are all preserved.
- Route payloads, SQLite schema, persisted JSON contracts, Umbrel metadata shape, and public CLI surfaces remain compatible.

## [0.2.83] - 2026-06-24

### Fixed
- Admin now truly defers registry source-table loading while a job update or discovery pipeline is active, avoiding repeated `/registry/sources` and `/registry/summary` bridge pressure that could surface as Umbrel `HTTP 504` errors.
- Source tables still recover after the active run returns idle, preserving the delayed-state copy during active work and the existing registry/source-table payload contracts after recovery.

### Notes
- This is a forward shared desktop and Umbrel patch after `0.2.82`; no existing release tags are moved or recreated.
- Container/Umbrel compatibility from the current public release line remains intact: same-origin Linux container mode, Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, avoidance of wildcard browser CORS allow headers, and desktop localhost bridge compatibility are all preserved.
- Route payloads, SQLite schema, persisted JSON contracts, Umbrel metadata shape, and public CLI surfaces remain compatible.

## [0.2.82] - 2026-06-24

### Fixed
- Shared desktop and Umbrel rollup from the post-`v0.2.43` release line, including the Admin/Umbrel active-work responsiveness fixes, bounded task/status routes, compact source-table loading, and Jobs feed refresh recovery.
- Desktop packaged startup and session reliability are hardened: corrupted session JSON no longer breaks active-session recovery, recent invalid lock files are no longer reclaimed too aggressively, lock contention uses bounded backoff, and lock/session failures emit better startup diagnostics.
- Runtime SQLite storage is more resilient under contention: bridge-owned stores can configure busy timeout/retry settings through environment variables, reads reuse a cached connection, and transient read-side busy errors retry before surfacing failure.
- Source discovery fetch retries now use one shared sync/async timing policy with capped exponential backoff and jitter, while preserving existing retry counts, HTTP retry codes, and unexpected-exception propagation.
- Source discovery configuration drift is reduced by centralizing adapter scoring sets and env integer parsing without changing confidence values, concurrency defaults, or compatibility exports.
- Jobs and Admin continue to recover from active pipeline/fetch pressure using hot task snapshots, lightweight task-live summaries, and gateway control-plane fallbacks instead of loading large reports during active work.
- QLOC/Elevato recovery from the Umbrel patch series is included: Elevato boards and comma-style job URLs are parsed, stale Google Sheets evidence is replaced, and runtime registry-backed sources are selected by normal Jobs updates.
- Container/Umbrel compatibility from the current public release line remains intact: same-origin Linux container mode, Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, avoidance of wildcard browser CORS allow headers, and desktop localhost bridge compatibility are all preserved.

### Changed
- Desktop, bridge, updater, source-discovery, storage, and jobs internals have been split into narrower leaves with stronger guardrails and exception-ratchet coverage, reducing broad fallback behavior without changing public API or persisted data contracts.
- Release and packaged smoke coverage now exercises storage health, source-run/job-feed/source-registry SQLite authority, updater handoff/recovery paths, desktop lifecycle behavior, and active-task close/abort scheduling.

### Notes
- This is the next shared desktop and Umbrel release after the long container/Umbrel patch series. `v0.2.43` remains the previous public desktop baseline.
- No existing release tags are moved or recreated for this rollup.
- Route payloads, SQLite schema, persisted JSON contracts, Umbrel metadata shape, and public CLI surfaces remain compatible.

## [0.2.81] - 2026-06-16

### Fixed
- Active Discovery, Fetch, and Sync operator routes now publish a bounded hot task snapshot, so compact Admin/Jobs polling can read current progress without rebuilding lifecycle projections or hydrating large reports during active work.
- `/ops/task-state?view=summary` and `/ops/task-live/<task>?view=summary` now prefer the hot active-task snapshot while it is fresh, preserving existing route shapes while stripping full work items, source lists, registry diagnostics, and unbounded event arrays.
- The Umbrel container gateway now serves compact task-state and task-live summaries directly from the hot snapshot or pipeline control fallback when active work is in progress, reducing exposure to internal bridge slowness and avoiding active-route 504s.
- Runtime startup/cleanup now seeds and clears `admin-active-task-snapshot.json`, preventing stale active rows from surviving restarts while keeping full run history and diagnostics on the existing authoritative idle paths.

### Notes
- This is a forward Umbrel/container release candidate before the public desktop tag. Do not reuse `0.2.80`; use `0.2.81` for the next clean Umbrel smoke and, if that smoke passes, the later public desktop tag.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.80] - 2026-06-16

### Fixed
- Admin remains populated and responsive during active work, with hydrated Stored Profiles, Source Sync, Pipeline schedule, Action Center, source-table placeholders, compact tab badges, bounded Older Runs, and consistent polished controls instead of blank or stale panels.
- Jobs updates, search, and feed publication now recover cleanly after broad fetches: multi-term searches match across job fields, completed updates refresh the visible feed automatically, gzip feed responses preserve their headers, and the Jobs table keeps a stable visual height at narrower desktop widths.
- QLOC/Elevato source recovery is included end to end: Elevato boards and comma-style job URLs are parsed, expired detail pages are filtered, live QLOC `technical-artist,j,240` rows replace stale Google Sheets `j,229` evidence, and active runtime registry sources are selected by normal Jobs updates.
- Desktop packaged runtime fixes cover keyboard reloads, idle liveness, false first-run modal behavior with existing data, packaged Scrapy/lxml metadata, source-sync configuration loading, and bounded portable build-cache retention.
- Umbrel/Admin performance and reliability improvements from the container patch series are included, including compact registry source-table payloads, lightweight task-live summary polling, active-fetch timeout backoff, pipeline abort recovery, and active-run-safe bootstrap behavior.

### Notes
- This is the shared desktop and Umbrel Docker release candidate after the container/Umbrel-only patch series from `0.2.44` through `0.2.79`; `v0.2.43` remains the previous public desktop release.
- The container image preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.
- No existing release tags are moved or recreated for this rollup.

## [0.2.79] - 2026-06-16

### Fixed
- Jobs search now tokenizes multi-term queries across a combined title, company, location, source, and URL search index, so searches such as `QLOC Technical Artist` match the recovered QLOC Technical Artist row.
- Jobs updates now refresh the visible feed automatically after a completed update reports fresh data, replacing stale startup/cache rows without requiring a manual Reload.
- Gzip-backed Jobs feed serving now has regression coverage for the expected `Content-Encoding: gzip` response header.

### Notes
- This is a forward container/Umbrel patch for the post-`0.2.78` Jobs UX gap. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the existing QLOC/Elevato ingestion, registry, sync, and source-selection behavior from `0.2.78`.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.78] - 2026-06-16

### Fixed
- Jobs updates now activate the runtime active source registry from the explicit fetch output directory before default loader selection, so container fetch children consume the live SQLite/JSON registry instead of an import-time packaged fallback.
- Targeted `onlySources` selection now resolves dynamic registry-backed static loaders such as `static_source::static:listing_url:https://qloc.elevato.net/en/` before task launch, avoiding zero-loader targeted runs for valid active sources.
- Dynamic `static_source::...` loaders are classified as `static` for incremental cache decisions and source reports, so source-check-only freshness cannot hide QLOC when the published feed has no QLOC row.

### Notes
- This is a forward container/Umbrel correction after the `0.2.77` live QLOC smoke still excluded QLOC as `cache_within_freshness_window` and left `j,240` out of the feed. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.77] - 2026-06-16

### Fixed
- Normal Jobs updates now build static source loaders from the SQLite-backed active source-registry authority before falling back to JSON exports, so active Source Sync/Admin rows such as QLOC are actually selected by the fetcher child process.
- QLOC feed recovery now covers the live `0.2.76` miss where QLOC was active with `jobsFound: 9` but the full Jobs update selected only built-in provider-family loaders, leaving the published feed on stale Google Sheets `j,229` rows and missing the live Elevato `j,240` opening.

### Notes
- This is a forward container/Umbrel correction after the `0.2.76` live QLOC smoke exposed a source-selection gap. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.76] - 2026-06-16

### Fixed
- Static source freshness now ignores source-check-only `nextEligibleCheckAt` state that has no successful feed-producing fetch history, so active Elevato sources such as QLOC run in the next normal Jobs update instead of being skipped as fresh with `lastJobsKept: 0`.
- QLOC feed recovery now covers the live `0.2.75` failure mode where the active registry row had `jobsFound: 9` but the published feed still carried stale Google Sheets `j,229` evidence and lacked the live Elevato `j,240` opening.

### Notes
- This is a forward container/Umbrel correction after the `0.2.75` live QLOC smoke failed. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.75] - 2026-06-16

### Fixed
- Jobs updates now distinguish source-check freshness from feed-producing freshness, so active static sources such as QLOC are not skipped unless their exact source identity is already represented in the published feed.
- Explicit fetch `onlySources` requests now fail fast when no selector matches and otherwise bypass incremental freshness, cadence, and circuit-breaker skips for the selected source.
- Elevato static rows now win over stale Google Sheets Elevato detail rows for the same opening, keeping the live QLOC Technical Artist `j,240` link as the public primary job and hiding the expired `j,229` detail link from the public bundle sample.
- The locked Python dependency set now carries `cryptography 48.0.1` and compatible `pyOpenSSL 26.2.0`, clearing the current pip-audit advisory for container and desktop package builds.

### Notes
- This is a forward container/Umbrel patch for QLOC/Elevato feed recovery. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.74] - 2026-06-15

### Fixed
- Admin/source-check validation now recognizes Elevato comma-style `,j,<id>` job links, so targeted QLOC checks report live job evidence instead of leaving QLOC pending with `jobsFound: 0`.
- Elevato source-check link extraction filters generic "Join <company>" anchors while preserving real openings such as QLOC Technical Artist.

### Notes
- This is a forward container/Umbrel correction for the `0.2.73` QLOC source-check smoke gap. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.73] - 2026-06-15

### Fixed
- Static source discovery and fetching now support Elevato-hosted job boards such as QLOC, including comma-style `,j,<id>` job URLs so the live QLOC Technical Artist opening is detected from the English board.
- Expired Elevato detail pages are treated as empty/removed evidence, while generic "Join <company>" pages and privacy-policy links are filtered out of static job output.

### Notes
- This is a forward container/Umbrel patch for QLOC/Elevato source recovery. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.72] - 2026-06-15

### Fixed
- Admin Ops startup now avoids false-empty and false-healthy panels: Action Center shows a neutral checking state until required signals complete, Source Sync hydrates from live sync status, source-table delayed states stay visible during active work, tab badges show bounded delayed/unavailable states, and Older Runs uses a contained scroll area without broken inline detail rows.
- Desktop lifecycle handling now keeps idle packaged windows alive by sending regular owner heartbeats, while keyboard reloads continue to bypass close shutdown handling.
- Portable desktop builds now keep Scrapy/lxml package metadata needed by fetch subprocesses and prune `_out/portable-build-cache` to a bounded set of recent bundle caches.

### Notes
- This is the Umbrel Docker release candidate used to validate the current Admin, Jobs, source sync, and desktop rollup before a later public desktop tag. No desktop release tag is created by this container publish; `v0.2.43` remains the latest public desktop release until explicit tag approval.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.71] - 2026-06-14

### Fixed
- Admin active-fetch fallback hydration now keeps Stored Profiles, Source Sync, Pipeline schedule, and source-table delayed placeholders populated when `/admin/bootstrap` is unavailable during active job updates, with a smoke-only fail-once gate and in-app Browser proof helper for visual regression checks.

### Notes
- This is a forward container/Umbrel patch for Admin active-fetch false-empty recovery. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.70] - 2026-06-14

### Fixed
- Admin source tables now request the compact `/registry/sources?view=table` payload, preserving table actions and filters while avoiding full source diagnostic fields that made idle Umbrel source loads too large and slow.

### Notes
- This is a forward container/Umbrel patch for bounded Admin registry source-table payloads. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- The registry source-table view is additive; default `/registry/sources` remains full-fidelity and backward compatible.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.69] - 2026-06-13

### Fixed
- Container Admin and Jobs active-fetch polling now use the additive `/ops/task-live/<task>?view=summary` route, keeping live task progress bounded without hydrating full fetch work-item payloads.
- `/ops/task-live/<task>?view=summary` now returns lightweight task identity, status, progress, counts, timestamps, summary, and bounded recent events while preserving the full default task-live payload for diagnostics.
- `/ops/health` now avoids expensive active-run detail work while a pipeline or fetch is active, keeping the existing route shape responsive during broad Umbrel fetches.
- Source-sync shard pushes now serialize GitHub Contents writes to avoid branch-head conflicts when publishing multiple changed shards to the same remote branch.

### Notes
- This is a forward container/Umbrel patch for active-fetch route performance and source-sync recovery. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- The task-live summary route is additive; default `/ops/task-live/<task>` remains full-fidelity and backward compatible.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.68] - 2026-06-13

### Fixed
- Container Admin source tables now treat terminal pipeline control-plane stages such as `canceled` as idle, so a post-abort refresh loads registry source rows instead of staying stuck on "Source tables delayed while job update is running."
- Source-table recovery now clears the recent active-pipeline marker when the fast pipeline status route reports an inactive terminal state, preventing fresh Admin pages from inheriting stale active-fetch deferral.

### Notes
- This is a forward container/Umbrel recovery patch for the incomplete `0.2.67` live smoke. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.67] - 2026-06-13

### Fixed
- Container Admin now replaces successful-but-partial fetch KPI payloads with terminal "No successful fetch yet" or "Not available" copy instead of leaving "Loading latest fetch KPI..." in KPI cards forever.
- Admin source tables now preflight the fast pipeline status route before starting full `/registry/sources` loads, so active Discovery to Fetch transitions render delayed source-table copy without waiting on heavy registry reads.
- Registry source-table HTTP 504s during active pipeline/fetch work now downgrade to the bounded delayed state instead of logging a blocking Admin registry source-table error.
- Admin now stays on compact active-run polling when pipeline or task-state control routes time out during possible active Fetch, Pipeline, or Abort work, avoiding repeated dashboard, registry conflict, and tab-count route pressure.
- Fetch log polling now backs off after repeated timeouts while preserving the last visible progress and log text.
- Pipeline Abort now renders queued/aborting state immediately and keeps active child Fetch rows visible until backend evidence shows they have actually settled.

### Notes
- This is a forward container/Umbrel recovery patch for the incomplete `0.2.66` live smoke. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- Full registry source tables remain diagnostic/operator data and may be delayed while a job update is running; current pipeline visibility and usable Admin navigation remain prioritized.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.66] - 2026-06-13

### Fixed
- Container Admin now keeps the gateway pipeline status snapshot fresh during active Fetch child waits, so `/tasks/run-jobs-pipeline-status` does not freeze on a stale `snapshotAt` while fetch progress continues.
- Admin current runs now lets fresher pipeline status replace stale Discovery child rows when the pipeline advances to Fetch, while preserving richer matching task-state rows.
- Admin source tables and fetch KPI cards now show bounded delayed copy during active pipeline/fetch work instead of indefinite loading placeholders when registry or summary routes are delayed.
- Admin now suppresses Abort buttons for pipeline-owned child rows and keeps Abort scoped to standalone Fetch/Discovery runs plus the Pipeline parent.
- Admin bridge status checks now use `/app/ready` and accept container-gateway ready/degraded payloads so the badge does not briefly report offline while lightweight gateway routes are healthy.

### Notes
- This is a container/Umbrel active-fetch recovery patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- Full registry source tables remain deferred while a job update is running; active pipeline visibility, current Fetch state, and Pipeline Abort stay prioritized through the gateway control plane.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.65] - 2026-06-12

### Fixed
- Container Admin now uses the gateway pipeline status as the authoritative active-task source during running pipelines, including bounded display-only child rows for Fetch, Discovery, and Sync progress.
- Pipeline status snapshots now expose bounded `activeChildren` rows so the gateway can keep Admin task visibility and Pipeline Abort available even when slow internal Ops routes are delayed.
- Admin active-pipeline polling no longer depends on dashboard-health, task-state, fetch KPI, storage-health, or full diagnostics routes for current task rendering.

### Notes
- This is a container/Umbrel control-plane recovery patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- Full Ops diagnostics may still be delayed during running pipelines; task visibility, navigation, and Pipeline Abort are prioritized through the gateway control plane.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.64] - 2026-06-12

### Fixed
- Container gateway proxied bridge responses now strip the upstream `Content-Length` before writing the gateway response length, avoiding duplicate content-length headers that Umbrel's proxy rejected with `HPE_UNEXPECTED_CONTENT_LENGTH`.

### Notes
- This is a forward fix for the failed live smoke of `0.2.63`, where gateway-native routes were healthy but proxied bridge routes returned Umbrel HTML `502` pages. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.63] - 2026-06-12

### Fixed
- Container gateway readiness now distinguishes an alive internal bridge process from a bridge socket that is actually listening, so `/app/ready` reports degraded until proxied bridge routes can respond.
- Container gateway routing now treats `/admin/*` as API traffic instead of static fallback HTML, restoring `/admin/bootstrap` through the internal bridge.
- Container bridge startup no longer performs source-registry ensure work before binding the internal bridge socket, reducing the chance that live `/data` registry reads leave gateway-only control routes up while bridge APIs refuse connections.

### Notes
- This is a forward fix for the failed live smoke of `0.2.62`, where the public gateway was installed but proxied internal bridge routes returned immediate `504 bridge_degraded` responses. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.62] - 2026-06-12

### Changed
- Container/Umbrel runtime now uses a lightweight public gateway in front of the internal bridge so `/app/ready`, pipeline status, startup static assets, and pipeline abort intake remain responsive while heavier diagnostics or pipeline work are busy.
- Container Jobs startup now keeps the first page on the bounded startup feed and defers the full light-feed refresh until explicit reload or a later safe refresh path, avoiding automatic multi-MiB feed reads before first usable UI.

### Notes
- This is a container/Umbrel control-plane recovery patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- Pipeline status and pipeline abort are now resilient through the public gateway; fetch/discovery child abort still depends on the internal bridge.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.61] - 2026-06-12

### Fixed
- Added a minimal `/app/ready` liveness route and made `/ops/health?view=ready` use the same in-memory readiness payload so bridge badges do not wait on Ops/dashboard/report reads during active pipelines.
- Admin now requests `/tasks/run-jobs-pipeline-status` immediately on boot and keeps a current Pipeline row plus Abort action visible when bootstrap, task-state, health, or dashboard routes are delayed.
- Admin bootstrap and task-state failures no longer clear an active pipeline fallback row or force the bridge badge into a blocking offline state while the lightweight pipeline status route remains responsive.
- Admin and Jobs bridge status checks now degrade gracefully during running-task contention instead of blocking navigation or clearing running/abort controls.

### Notes
- This is a container/Umbrel running-task stability patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.60] - 2026-06-12

### Fixed
- Admin deferred panels now avoid false empty or misleading status values while source tables, registry/sync diagnostics, discovery review, dedup lists, and fetch/discovery logs are still loading.
- Admin Ops health now keeps KPI, warning, badge, and schedule state truthful across automatic summary polling and manual refreshes.
- Jobs and Saved navigation keep the Admin entry point available during transient bridge delays, and Jobs preserves active pipeline/Abort state from the lightweight pipeline status route while optional Ops detail is delayed.
- Desktop packaging now includes `admin.html` in the embedded static payload so Admin navigation does not return the generic packaged 404 page.

### Notes
- This is a container/Umbrel smoke build for the latest Admin truthfulness and running-task stability fixes. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.59] - 2026-06-11

### Fixed
- Jobs no longer loads the full fetch report during normal container page startup or navigation; source metadata is deferred until the Data Sources panel is opened.
- Admin Fetcher and Discovery sections now use bounded summaries and short log tails by default, keeping full diagnostics manual or active-task-only.
- Admin discovery/source-table loading no longer marks task and source action buttons as running work when backend task state is idle.
- Jobs idle pipeline checks and the shared Admin bridge button now avoid overlapping status polling once idle state is confirmed.

### Notes
- This is a container/Umbrel frontend data-flow recovery patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- Chrome DevTools traces on live Umbrel remain the acceptance signal for user-visible Admin and Jobs page-load performance.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.58] - 2026-06-11

### Fixed
- Container Admin now applies active Fetcher and Discovery task progress directly from the bounded bootstrap task rows, so current work is visible immediately while full reports hydrate in the background.
- Frontend smoke coverage now matches the load-on-view Admin contract: full Fetcher diagnostics are verified through explicit manual refresh instead of first-load auto fan-out.

### Notes
- This supersedes the unpublished-to-Umbrel `0.2.57` container image, whose GitHub Tests workflow failed on the old Admin diagnostics smoke expectation. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.57] - 2026-06-11

### Changed
- Container Admin startup now uses one bounded `/admin/bootstrap` control-plane route for first-use data instead of fanning out across Ops, Sync, Registry, Discovery, and dashboard routes during first render.
- Admin boot now renders overview summary, current running tasks, two recent runs, and sync readiness from the bootstrap payload, while full diagnostics remain tab-open or manual-refresh work.
- Task lifecycle current/recent reads now trust SQLite authority without falling back to stale JSON lifecycle rows.

### Notes
- This is a container/Umbrel Admin startup recovery patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- Chrome DevTools traces on live Umbrel remain the acceptance signal for user-visible Admin and Jobs page-load performance.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.56] - 2026-06-07

### Fixed
- Container Jobs startup feed export now writes only the bounded startup preview instead of duplicating the full light feed.
- Container static serving repairs upgraded `/data/jobs-unified-startup.json` artifacts that are malformed or larger than the startup preview contract, so upgraded Umbrel installs recover without waiting for another pipeline.

### Notes
- This is a container/Umbrel startup-feed recovery patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.55] - 2026-06-07

### Changed
- Container/Umbrel bridge-started fetch runs now use conservative default concurrency so Admin, Jobs, and lightweight Ops routes remain responsive while a fetch is active.
- Container/Umbrel fetch defaults are now `--max-workers 4`, `--max-per-domain 2`, `--adapter-http-concurrency 16`, and `--static-detail-concurrency 4`; explicit payload overrides still win.

### Notes
- This is a container/Umbrel runtime-pressure recovery patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- Desktop bridge defaults remain unchanged, and the `uncapped` preset remains intentionally aggressive in container mode.
- Chrome DevTools traces during an active fetch remain the primary acceptance signal for Umbrel page-load performance; backend route profiles remain supporting diagnostics.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.54] - 2026-06-07

### Changed
- Container Admin Fetcher and Discovery sections now load from explicit navigation, hash focus, or manual action instead of near-viewport observation.
- Container Admin Fetcher and Discovery focused sections now request bounded recent log tails before continuing live polling from the returned offset.

### Fixed
- Container Admin Discovery manual refresh now uses the bounded log-tail path instead of rendering full historical log DOM.

### Notes
- This is a container/Umbrel Admin log-tail recovery patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- Chrome DevTools traces remain the primary acceptance signal for Umbrel page-load performance; backend route profiles remain supporting diagnostics.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.53] - 2026-06-07

### Changed
- Container Admin now loads deferred panels on view: Ops recent history, Fetcher output, Discovery/source tables, and Sync diagnostics load when their section is focused or near the viewport instead of relying on delayed full diagnostics.
- Ops history now requests only the two most recent completed runs for the initial Admin view; older run history loads only when the older-runs disclosure is opened while current running tasks remain visible from the task summary.
- Deferred Fetcher, Discovery, Sources, and Sync panels now show truthful animated loading states instead of blank static areas or false empty copy.

### Fixed
- Admin run history no longer shows `No run history yet` before the recent-history request has completed.

### Notes
- This is a container/Umbrel Admin load-on-view recovery patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- Chrome DevTools traces remain the primary acceptance signal for Umbrel page-load performance; backend route profiles remain supporting diagnostics.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.52] - 2026-06-06

### Changed
- Container Admin startup now avoids automatic full diagnostics fan-out after first render, keeps Fetcher and Discovery log DOM bounded and lazy, and deduplicates lightweight summary/ready bridge requests.
- Jobs idle polling now avoids repeated task-state and dashboard-health summary calls after the initial idle check while preserving active pipeline, abort, bootstrap, and completion behavior.

### Fixed
- `/discovery/report?view=summary` now uses a bounded startup projection instead of loading and normalizing the full discovery report or materializing large candidate/failure arrays.

### Notes
- This is a container/Umbrel frontend-pressure recovery patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- Chrome DevTools traces remain the primary acceptance signal for Umbrel page-load performance; backend route profiles remain supporting diagnostics.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.51] - 2026-06-06

### Fixed
- Container static serving now handles upgraded Umbrel installs whose backing light jobs feed is gzip-backed, and still returns a bounded generated startup preview if persisting `data/jobs-unified-startup.json` fails.
- This corrects the live `0.2.50` acceptance failure where `data/jobs-unified-startup.json` could remain `404` after update even though `jobs-unified-light.json` was available.

### Notes
- This is a container/Umbrel startup-feed recovery correction. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- Follow-up Chrome DevTools traces should be captured on live `0.2.51` for Admin cold/warm, Jobs cold/warm, Jobs-to-Admin, and Admin-to-Jobs before choosing the next page-load patch.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.50] - 2026-06-06

### Fixed
- Container static serving now backfills a missing `data/jobs-unified-startup.json` from the existing light jobs feed on upgraded Umbrel installs, so Jobs can render a bounded startup preview before the next pipeline run writes the artifact.
- Existing startup artifacts are preserved, and full `jobs-unified-light.json`, `jobs-unified.json`, and CSV contracts remain unchanged.

### Notes
- This is a container/Umbrel startup-feed recovery patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- Follow-up Chrome DevTools traces should be captured on live `0.2.50` for Admin cold/warm, Jobs cold/warm, Jobs-to-Admin, and Admin-to-Jobs before choosing the next page-load patch.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.49] - 2026-06-06

### Changed
- Admin first-load behavior now keeps the initial route set lightweight, with core panels restored and full diagnostics deferred until tab/manual paths.
- Admin source tables now render large Active, Pending, and Rejected source buckets through virtualized rows so source lists remain usable without thousands of DOM nodes.
- Jobs startup now uses a startup feed path and shared feed loading to reduce repeated large-feed work and avoid missing fallback probes.

### Fixed
- Source sync summary status now preserves the resolved enabled state during Admin boot so saving the form cannot accidentally disable sync from a lightweight summary payload.
- Jobs pipeline starts are no longer blocked solely because source sync is degraded; sync failures remain visible while fetch/discovery pipeline work can proceed.
- Bootstrap tests and release checks now account for the generated startup jobs artifact used by the container startup path.

### Notes
- This is a container/Umbrel page-load recovery patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- Chrome-visible Admin and Jobs behavior remains the primary acceptance signal for future Umbrel page-load performance work; backend route profiles are supporting diagnostics.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.48] - 2026-06-06

### Added
- Added a container-only frontend bundling path for Umbrel images. Docker now builds hashed, minified ESM assets for `admin.html`, `jobs.html`, and `saved.html`, serves gzip sidecars when accepted, and keeps checked-in desktop/local HTML behavior as the fallback.
- Added `GET /ops/dashboard-health?view=summary` for Admin first paint. The default `/ops/dashboard-health` route remains the full compatibility payload.

### Changed
- Admin boot now uses the lightweight dashboard summary first, keeps heavy diagnostics deferred until manual/detail paths, and no longer restores full fetch/discovery reports unconditionally on page load.
- `/ops/task-state?view=summary` now builds a true compact projection instead of compacting the full diagnostic task payload.

### Fixed
- Stale running lifecycle rows with terminal progress, stale heartbeat, and no live task evidence are repaired through the task lifecycle path so old sync rows no longer keep Admin in a fake active state.
- Container static serving now prefers generated container frontend assets when present while preserving no-store behavior for HTML/runtime config and immutable caching for hashed bundles.

### Notes
- This is a container/Umbrel performance recovery patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- Chrome DevTools trace evidence is the release acceptance signal for Umbrel page-load performance; backend route profiles remain supporting diagnostics.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.47] - 2026-06-06

### Added
- Added a Chrome DevTools trace summary tool for `.json` and `.json.gz` Performance exports, and optional `perf:complete` ingestion so LCP elements, slow browser resources, user timing spans, and long main-thread tasks are visible beside backend profiling.

### Fixed
- Rolled back the Umbrel container runtime to the `0.2.44` Admin readiness code path after live Chrome traces showed the later Ops route cache/coalescing stack could leave Admin waiting on slow discovery, registry, sync, and dashboard routes for many seconds.
- Restored the earlier Admin behavior where profile overview and sync status render without being blocked by first-load diagnostics fan-out.
- Stopped the Admin first-load path from automatically loading full discovery source/report data; operators can still load source tables manually or through task-completion refreshes.

### Notes
- This is a container/Umbrel recovery patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.
- `0.2.45` and `0.2.46` remain historical evidence, but should be treated as degraded for the private Umbrel install until the Admin boot path is redesigned around Chrome-trace acceptance criteria.

## [0.2.44] - 2026-06-05

### Added
- `/registry/summary?view=exact` now exposes normalized registry summary counts without source rows for diagnostics, while the default `/registry/summary` remains a lightweight storage snapshot.

### Changed
- Admin registry diagnostics now label storage snapshot counts versus normalized counts so duplicate/pending evidence is not overstated.
- Admin now loads local profile overview summary data first, defers exact attachment-size filesystem work to a background full refresh, and exposes bounded overview performance labels for container/Admin profiling.

### Fixed
- Admin Ops now renders a neutral readiness shell during the first dashboard-health request instead of leaving `Loading operations health...` visible while slower Umbrel containers finish the health snapshot.
- Jobs, Saved Jobs, and Admin now avoid passive first-load placeholder copy such as `Loading jobs...`, `Loading saved jobs...`, `Admin Checking...`, and empty discovery/activity text while background startup data is still settling.

### Notes
- This is a container/Umbrel patch. No desktop release tag is created; `v0.2.43` remains the latest public desktop release.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.43] - 2026-06-04

### Added
- Desktop release rollup from the last public desktop build, bringing the shared task lifecycle hardening, pipeline start-race handling, packaged source-sync config parity, discovery/report diagnostics, and job company repair work into the packaged desktop channel.

### Changed
- Admin now keeps restore, demote, and delete source bulk actions collapsed as advanced actions before runtime JavaScript finishes loading.
- Saved Jobs now hides workspace metrics while guest, restoring, or waiting for profile rows, avoiding prominent zero-value metrics before the local profile has loaded.

### Fixed
- Source-sync shard garbage collection now ignores malformed remote content entries that do not include a path, removing the blank `skipped invalid source-sync shard GC path:` warning while preserving warnings for real invalid shard paths.

### Notes
- This is the next desktop-facing release identity after `v0.2.25`; `0.2.26` through `0.2.42` were primarily Umbrel/container patch identities but included shared fixes that desktop packaging now receives.
- Live Umbrel evidence for `duplicatePendingCount` remains operator registry state, not a deterministic release-blocking code repair. No live registry files were edited.
- This rollup preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.42] - 2026-06-04

### Fixed
- Discovery failure-attempt diagnostics now classify permanent GameDevMap homepage and directory website DNS/404/410 misses as expected negatives, reducing live Umbrel actionable discovery diagnostics without hiding transient, TLS, 403/5xx, parser, or provider-validation failures.

### Notes
- This is a diagnostics-only Umbrel/container patch. Fetcher parsing, provider scoring, source policy, source sync, public job data contracts, same-origin raw-LAN behavior, and desktop packaging behavior are unchanged.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.41] - 2026-06-04

### Fixed
- Discovery failure-attempt diagnostics now separate expected negative GameDevMap recovery and static probe misses from actionable discovery diagnostics, so generated `/careers` or `/jobs` 404s and stale inferred `careers.*` DNS misses no longer inflate the high-priority failure count.
- GameDevMap recovery planning now carries bounded URL-source metadata, uses path-only recovery labels, and skips secondary generated recovery paths when primary generated paths only returned 404/410 for that studio homepage.

### Notes
- Fetcher parsing, provider scoring, source policy, source sync, public job data contracts, same-origin raw-LAN behavior, and desktop packaging behavior are unchanged.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.40] - 2026-06-04

### Fixed
- Task failure-attempt diagnostics now redact URL-like substrings from bounded example labels, closing the live `0.2.39` smoke blocker where GameDevMap recovery example names could expose raw URLs.

### Notes
- This is a corrective Umbrel/container patch for the `0.2.39` diagnostics route. Fetcher parsing, discovery queue policy, provider scoring, source sync, public job data contracts, same-origin raw-LAN behavior, and desktop packaging behavior are unchanged.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.39] - 2026-06-04

### Added
- Admin Ops now exposes bounded task failure-attempt diagnostics through `/ops/task-failure-attempts`, separating expected fetch cache skips and discovery dedupe/queue/static skips from hard fetch failures and actionable discovery diagnostics.
- The Admin Fetcher diagnostics panel now lazy-loads and renders the failure-attempt summary with copy/refresh support, including high-priority discovery buckets without exposing raw artifact bodies or URLs.

### Notes
- This is a diagnostics-only Umbrel/container patch. Fetcher parsing, discovery queue policy, provider scoring, source sync, public job data contracts, same-origin raw-LAN behavior, and desktop packaging behavior are unchanged.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.
- Live `0.2.38` evidence showed no hard fetch failures, one partial static-source warning, and elevated discovery diagnostics in dedupe skips, GameDevMap recovery fetches, and probes; this patch makes those buckets visible before any behavior-changing follow-up.

## [0.2.38] - 2026-06-04

### Fixed
- Google Sheets company repair now recognizes structured LinkedIn detail URLs with numeric job ids and a small set of first-party game-studio career hosts, repairing currently observed `Unknown company` rows for Scopely, Activision, Techland, Wargaming, Rockstar Games, Santa Monica Studio, Believer, and Rovio when the job link itself carries strong company evidence.
- The shipped-artifact quality gate now checks direct structured job-link company evidence before requiring Grackle bundle evidence, so stale feeds with repairable `Unknown company` rows are classified as blockers instead of weak warnings.

### Notes
- Live Umbrel `0.2.37` audit evidence found 135 `Unknown company` rows; 118 are repairable by this patch and 17 remain weak-evidence rows, mostly generic LinkedIn search/expired redirect URLs plus one Jobvite and one Dayforce URL without safe company evidence.
- Fetch attempt audit found no real fetch failures: 22 sources ran successfully and 2,127 were expected `cache_within_freshness_window` exclusions.
- Discovery failure-attempt audit found high diagnostic buckets in dedupe skips, GameDevMap recovery fetches, and static probes, but no queue-policy or provider-scoring change is justified by this patch.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.37] - 2026-06-04

### Fixed
- Jobs pipeline child waits now extend the absolute report wait cap while the discovery/fetch child has live heartbeat or lifecycle evidence, preventing long but healthy Umbrel fetch merges from failing the parent pipeline before the terminal report is written.

### Notes
- This is a corrective container patch for the 0.2.36 Umbrel manual pipeline smoke failure where fetch completed all 555 source tasks and entered merge, but the parent pipeline failed with `fetch_wait: fetch report exceeded absolute safety cap`.
- Terminal child lifecycle rows still fail or cancel the parent promptly when the expected report is missing or unfinished; stale children without live evidence still hit the quiet timeout path.
- Fetcher parsing, provider quality rules, source policy, sync contracts, raw-LAN same-origin behavior, and desktop packaging behavior are unchanged.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.36] - 2026-06-03

### Fixed
- Load-time registry safe-demotion now preserves active rows that were approved by discovery auto-approval, so terminal discovery report reconciliation is not immediately undone by routine registry normalization or auto-sync reads.
- Terminal discovery registry reconciliation now stays durable across the normal registry service load path when completed reports declare auto-approved duplicate candidates as active.

### Notes
- This is a corrective container patch for the 0.2.35 Umbrel verification failure where the registry briefly repaired to the completed report counts and then reverted after load-time safe demotion.
- Manual Admin conflict safe-demotion remains available; this change only protects discovery auto-approved active rows from automatic load-time cleanup.
- Fetcher parsing, provider quality rules, source policy, sync contracts, raw-LAN same-origin behavior, and desktop packaging behavior are unchanged.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.35] - 2026-06-03

### Fixed
- Terminal discovery registry reconciliation now also replays report-declared `discovery_auto_approve` promotions that were already stamped into the completed discovery report, repairing stale active/pending counts when eligibility replay alone cannot reconstruct the worker's final registry state.

### Notes
- This is a corrective container patch for the 0.2.34 Umbrel verification failure where `/discovery/report` still declared `active=2301/pending=811` while registry routes remained at `active=2289/pending=823` after update.
- Fetcher parsing, provider quality rules, source policy, sync contracts, raw-LAN same-origin behavior, and desktop packaging behavior are unchanged.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.34] - 2026-06-03

### Fixed
- Terminal discovery reports now reconcile report-declared auto-approval through the bridge registry authority, repairing stale registry bucket counts before `/discovery/report` is served or a new discovery starts.
- Jobs pipeline child waits now stop promptly when discovery or fetch child lifecycle rows terminalize without a matching terminal report, avoiding long absolute safety-cap waits.

### Notes
- Fetcher parsing, provider quality rules, source policy, sync contracts, raw-LAN same-origin behavior, and desktop packaging behavior are unchanged.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.33] - 2026-06-03

### Fixed
- Discovery auto-sync watching now waits for terminal registry finalization and auto-approval status before processing completed reports, preventing the bridge watcher from overwriting the final discovery report with an intermediate `running` finalization payload.

### Notes
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.32] - 2026-06-03

### Added
- Admin Bridge now exposes a lightweight `/registry/summary` response and a combined `/registry/sources` source-table response so Admin can refresh registry views without three separate full registry loads.

### Fixed
- Discovery completion watching now waits for registry finalization and auto-approval terminal status before refreshing source tables, avoiding misleading post-discovery registry timeout warnings on Umbrel.
- Admin background source-table refreshes now use a longer bounded timeout, preserve existing rows on delayed refreshes, and log delayed refreshes separately from discovery worker failures.

### Notes
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.31] - 2026-06-02

### Added
- Admin Ops now exposes bounded discovery audit artifact diagnostics for known audit files under the active data directory.
- Windows Docker smoke builds can use a clean committed `git archive` context when live workspace reparse points block `docker build .`.

### Fixed
- Jobs pipeline starts now verify live pipeline status before showing a start failure, avoiding a false error toast when the start POST times out after the bridge has accepted the run.
- Published container images now generate the portable encrypted GitHub App source-sync config from BuildKit secrets, matching desktop packaged sync behavior for Umbrel installs.

### Notes
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.30] - 2026-06-02

### Fixed
- Umbrel discovery tasks now write sheet-directory and web-search audit artifacts under the `/data` volume in container mode instead of the unwritable app directory.
- Discovery task reports now self-repair from terminal lifecycle state after child crashes, avoiding stale active `/discovery/report` payloads and long pipeline safety-cap waits.
- POSIX bridge PID checks now reject zombie child processes so container task lifecycle liveness is not falsely extended.

### Notes
- Fetcher parsing, provider quality rules, source policy, and desktop/non-container discovery audit path behavior are unchanged.
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.29] - 2026-06-02

### Fixed
- Umbrel container Admin now preserves the explicit same-origin bridge base, fixing Admin panels that incorrectly called the visitor browser's `127.0.0.1:8877` instead of the LAN app origin.

### Notes
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.28] - 2026-06-02

### Fixed
- Umbrel app metadata now lets `app_proxy` own raw-LAN port `8877` and removes the duplicate `web` container host-port mapping that caused Docker install failures with `port is already allocated`.

### Notes
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.27] - 2026-06-01

### Fixed
- Umbrel container startup now prepares the `/data` bind mount before dropping to the non-root runtime user, fixing first-run seeding on root-owned Umbrel app data directories.

### Notes
- This patch preserves the same-origin Linux container path for Umbrel raw-LAN installs, including GHCR multi-arch image publishing, private community app-store metadata, suppressed wildcard browser CORS allow headers, and desktop localhost bridge compatibility.

## [0.2.26] - 2026-06-01

### Added
- Baluffo can now run as a same-origin Linux container for private Umbrel raw-LAN installs, with GHCR multi-arch image publishing and private community app-store metadata.

### Fixed
- Umbrel app metadata initially added a direct host-port mapping for `8877`; this was corrected in `0.2.28` to avoid conflicting with Umbrel's `app_proxy` port ownership.
- Container mode no longer emits wildcard browser CORS allow headers, while desktop localhost bridge compatibility keeps its existing split-origin behavior.

## [0.2.25] - 2026-06-01

### Added
- Admin Ops can now enable a bridge-owned recurring full Jobs pipeline schedule with a configurable whole-hour interval.
- Jobs and Admin Ops now expose confirmed abort controls for active discovery, fetch/bootstrap, and full Jobs pipeline runs through runId-scoped task cancellation.
- Release preflight now includes a packaged bridge/runtime rehearsal that proves task abort lifecycle evidence and one recurring Jobs pipeline scheduler trigger.
- Source-discovery hardening now includes broader ATS HTML-signature detection, independent `/jobs` Playwright fallback checks, and a jobs artifact quality gate for title/location contamination.

### Changed
- Jobs no longer shows the recent views bar, keeping the page focused on current pipeline and feed state.
- Static source inference now treats dead-listing retries, SPA shell signals, non-English career terms, and custom-domain ATS signatures as first-class discovery evidence.

### Fixed
- Task abort lifecycle closeout now keeps user-canceled evidence sticky across late fetch/discovery reports, watcher cleanup, startup cleanup, and pipeline child terminal races.
- Packaged desktop Jobs-to-Admin navigation no longer sends a regular desktop-close lifecycle signal, and the packaged lifecycle rehearsal now covers the navigation path.
- Desktop close cleanup now keeps packaged shutdown tied to real lifecycle state and avoids lingering active-task close rehearsal failures.
- Packaged desktop updater rehearsal now waits for the updater helper to finish and fails on helper terminal errors instead of reporting a false pass.
- Google Sheets and static-source cleanup now repairs category-style titles, redirect-derived company leaks, and container artifact titles before downstream job output.
- Remote Python CI and packaged rehearsal data-root checks no longer fail on stale runtime assumptions.

## [0.2.18] - 2026-05-25

### Added
- Linux packaged desktop support now includes platform abstraction, credential storage support, launch scripts, AppRun/desktop metadata, AppImage packaging, and Linux smoke tooling.
- Release automation now publishes a Linux AppImage alongside the Windows portable and ship-bundle assets for `v*` release tags.

### Changed
- Windows packaged desktop data now defaults to `%APPDATA%\Baluffo`, with first-launch legacy `ship\data` migration and migration reports.
- Desktop updater handoff, relaunch, rollback, and success-marker paths now preserve the planned external data root instead of deriving state from legacy `ship\data`.

### Fixed
- Linux CI no longer fails Windows desktop compat tests by resolving Windows-specific facade calls through Linux stubs.
- CI complexity checks now keep the Ruff baseline metadata aligned with the pinned Ruff version.

## [0.2.17] - 2026-05-23

### Fixed
- First-run Google Sheets bootstrap now avoids duplicate Retry launches after a feed exists and keeps/rechecks progress during long redirect and title-hydration phases before showing timeout.
- Jobs first-run Retry now loads an already completed runtime feed before trying to start another bootstrap.
- Packaged first-run smoke now exercises the real Jobs UI bootstrap request under a long-active heartbeat mode, catching timeout/recovery regressions without live Google Sheets.

## [0.2.16] - 2026-05-23

### Fixed
- Desktop launcher shutdown now pins post-handoff window liveness to the managed browser PID, so unrelated Baluffo-titled windows cannot keep packaged lifecycle shutdown alive.
- Remote Python CI now preserves carried `sourceBundle` evidence when seeding existing Jobs output and keeps source-policy review candidates blocked when provider validation evidence is explicitly not OK.
- First-run Google Sheets bootstrap no longer live-validates thousands of category rows that would be dropped anyway, and the UI now stays in progress while backend heartbeats remain fresh.
- Google Sheets category-style titles now must repair, hydrate, or drop, with bounded `404`/`410` link validation for suspicious category rows only.
- Google Sheets URL-derived title repair now strips opaque ATS/job ID affixes and skips pure posting-code path segments without hardcoding specific providers.
- Google Sheets provider title hydration now supports Ashby hosted-board pages for `jobs.ashbyhq.com/{board}/{posting_id}` links.
- Google Sheets provider title hydration now supports Workable widget feeds for `apply.workable.com/{account}/j/{shortcode}` links.
- Remote OK now reports a successful empty source when all valid feed rows are filtered out by sanitizer rules.
- Remote OK parser filtering now rejects generic community and open-pool non-job titles such as `Join Our Community`.
- Remote OK parser filtering now ignores description-only game keyword matches, reducing non-game remote job contamination before canonicalization.

## [0.2.15] - 2026-05-20

### Added
- Oracle HCM provider API support, including provider inference, JSON parsing, adapter registration, and fixture-backed coverage.
- Provider coverage migration tooling that stages pending provider candidates, reports validation gaps, and recommends focused next actions without requiring a full discovery rerun.
- Google Sheets and static-source title sanitization evidence, including an audit helper and regression corpus for noisy or source-name-only titles.
- Deterministic first-run Jobs regression coverage for packaged desktop bootstrap, retry, and feed-loading behavior.

### Changed
- Source-policy soak reports now distinguish provider migration staging, pending-provider fetch evidence, unsupported ATS advisories, and provider validation debugging.
- Jobs title normalization now preserves useful role specificity while rejecting source names, location fragments, and non-job boilerplate before rows reach reports or the frontend feed.
- Packaged first-run Jobs startup now uses tighter cache-busting, runtime-state, and bootstrap guards for stale bundled/runtime artifacts.
- Release and testing docs now describe the first-run packaged smoke lane and the Python dependency security audit path.

### Fixed
- Closing the packaged desktop browser window no longer leaves the launcher, site child, or bridge child running because `/ops/health` polling can no longer refresh desktop-window owner activity.
- First-run Jobs regressions after `0.2.1` no longer show stale packaged rows, loop bootstrap retries, or leave the page in a blank no-data state while the starter feed is being prepared.
- Google Sheets and static-source rows with source-name or boilerplate titles are sanitized or dropped consistently before dedup, reports, storage, and frontend rendering.
- Provider migration validation can now fetch explicitly staged pending provider rows without changing default fetch behavior or promoting local registry state.
- Remote CI gates are aligned with the new Oracle HCM provider defaults and the dependency security audit no longer fails on `idna`.

### Security
- Packaged source-sync private keys now use a `v2.` AES-GCM envelope with HKDF-SHA256 machine/embedded derivation and PBKDF2-HMAC-SHA256 passphrase derivation, while legacy no-prefix packaged configs remain decryptable.
- The sync config build helper no longer generates plaintext private-key configs, and sync/package warnings avoid echoing sensitive-looking build inputs or remote snapshot key names.
- Updated the locked Python dependency `idna` to `3.15` to resolve `CVE-2026-45409`.

## [0.2.1] - 2026-05-18

### Added
- Saved Jobs tracking polish, including phase history rendering, clearer action state, activity/timeline refinements, and attachment hardening.
- Previous release-note viewing in the desktop update UI, so users can inspect earlier published release details.
- Windows desktop sessions now flash the Baluffo taskbar button when a long Jobs pipeline run finishes in the background.
- A first-run Jobs notice that explains the starter Google Sheets bootstrap and its expected duration.

### Changed
- Saved Jobs action clarity and phase tracker presentation were tightened for repeated tracking workflows.
- AI/docs routing, Basic Memory closeout policy, and refactoring-analysis guidance were updated for future maintenance sessions.
- First-run Jobs pipeline tooltip and status copy now describe the bootstrap phase instead of the normal refresh cadence.

### Fixed
- First-run Jobs now suppresses stale packaged/runtime rows, starts one Google Sheets bootstrap, serves the promoted feed after success, and avoids the repeated fetch loop.
- Admin and Jobs navigation no longer pay the one-minute cold-start validation cost after first-run bootstrap recovery.
- Jobs rows with empty normalized titles are filtered before render, and the first-run empty state now explains that jobs are still being prepared.
- Saved Jobs attachment, tracking, grouping, and revert edge cases were hardened across browser and desktop local-data paths.

## [0.2.01] - 2026-05-16

### Changed
- Portable ZIP builds now embed only the required `chromium_headless_shell-*` Playwright browser payload, keeping offline browser fallback self-contained while avoiding unrelated browser cache siblings.
- No-openings detection now requires explicit visible empty-state evidence, and source reports keep hidden/script/template text and all-canonical-dropped rows in review instead of treating them as legitimate empty sources.
- Location sanity checks now preserve real city names such as Milan, Tel Aviv, and Frankfurt am Main, and treat `Unknown` country values as missing-country placeholders rather than contamination.

### Fixed
- Windows portable updater handoff confirmation no longer falsely rejects a live launcher when packaged runtimes lack optional `psutil`.
- Updater handoff failures now record non-secret diagnostics and clear stale post-install success markers before a fresh install handoff.
- Desktop update manifests for this release require updater capability `2.0.1`, so affected older clients stop attempting the broken automatic install path for future releases.

## [0.2.0] - 2026-05-15

### Added
- A more polished desktop Jobs experience, with denser job rows, clearer save/open actions, user-facing update controls, and quick-filter presets for common browsing flows.
- A safer Saved Jobs workflow, including contextual phase overrides, clearer remove/undo behavior, and an activity timeline that opens with useful defaults.
- A stronger Admin operations view with clearer run history, selected-run analysis, pipeline diagnostics, warning explanations, and advanced bulk actions kept behind an explicit disclosure.
- Runtime SQLite/WAL storage for task history, sync runs, source runs, jobs feed exports, and source registry rows, while keeping compatibility exports available for existing flows.
- Source-sync v3 with content-addressed shard bundles, changed-shard uploads, pull no-op detection, push progress, bounded cleanup, and stronger validation.
- New source-policy, provider/static, registry-conflict, and dedup review tools that make risky source changes easier to inspect before applying.
- Performance, release-safety, and repo-safety tooling, including startup probes, benchmark reporting, packaged desktop rehearsals, secret scanning, dependency audit wiring, and bridge route inventory checks.

### Changed
- Jobs discovery, fetching, sync, and lifecycle internals were split into smaller, more testable modules without changing the normal user workflow.
- Packaged desktop builds now include the storage/runtime pieces needed for the newer local storage and sync paths.
- Admin startup and heavy review panels now defer more expensive work, improving first-load behavior while preserving access to detailed diagnostics.
- Documentation was reorganized around the active docs index, release guide, storage/sync contracts, source-policy runbook, testing guide, and AI/tooling guardrails.

### Fixed
- Desktop startup, bridge ownership, browser shutdown, updater handoff, and packaged startup readiness are more reliable across Windows desktop sessions.
- Pipeline and fetch lifecycle tracking now uses stronger task authority and better evidence, so Admin progress and diagnostics avoid stale or placeholder state.
- Source-sync writes, retries, snapshot limits, checkpoint tagging, and source-health parity were hardened.
- Source registry conflicts, provider/static overlap, dedup review pressure, Google Sheets role buckets, and static-source conflict handling now produce clearer review evidence.
- Saved Jobs back navigation, activity timeline close behavior, phase override flow, remove action, and scrollbar styling were polished.
- Admin operations rows, completed-run ordering, pipeline summaries, and diagnostics copy now render more consistently.

### Security
- Added gitleaks-based secret scanning and Python dependency audit coverage to the local and release-safety workflow.
- Updated dependency and packaging guardrails used by the desktop release path.

## [0.1.33] - 2026-04-20

### Changed
- The desktop runtime has been modularized into focused `src/ship/desktop_app/` package modules (`launcher`, `startup`, `browser`, `session`, `_windows`, `config`, `process`) behind the existing `src.ship.desktop_app` compatibility facade, and the desktop ownership docs now point editors to those focused boundaries instead of the old monolithic module.
- Windows release-preflight now includes dedicated packaged rehearsal lanes for stale-runtime orphan reclaim and managed Chromium browser-job shutdown propagation, keeping the packaged smoke gate aligned with the hardened desktop supervision path.
- Uncapped fetch now reuses the regular fetch launch/runtime path with a narrower `50 / 5 / 10` overlay, seeds existing output during force-refresh runs, and enables a deeper uncapped static profile instead of maintaining a separate aggressive behavior tree.
- Packaged `scrapy_static_sources` fallback processing now runs as a bounded parallel queue with live heartbeat/progress reporting, and the Admin fetch UI surfaces that tail as an explicit `Browser fallback X/Y` progress badge instead of leaving the last running work item opaque.
- Portable builds now bundle the Scrapy fallback runtime stack needed by packaged child runners, including the `scrapy`, `scrapy_playwright`, and `twisted` runtime path.
- Jobs-page desktop updater install confirmation now falls back cleanly when the richer dialog hook is unavailable, and packaged updater rehearsal now proves `handoff-requested.json` plus an in-flight handoff state before treating launcher exit as a valid install transition.
- Desktop startup probing on the current public release line continues to use the more isolated policy and telemetry path introduced in the recent desktop startup hardening work.
- Packaged desktop smoke and CI release gates on the current public release line continue to isolate Playwright bridge local data from repo-local desktop session state so the bridge-release lane starts from a clean guest profile.

### Fixed
- Windows desktop supervision is now substantially harder to escape: launcher-managed `site`, `bridge`, and managed Chromium processes are attached more strictly to the desktop Job Object, stale runtime children can be reclaimed safely on startup, and detached Chromium handoff no longer leaves the launcher waiting for the bridge's two-minute owner-idle fallback after the Baluffo window is already gone.
- Linux CI desktop-app tests no longer fail spuriously on non-Windows runners by assuming Windows-only `src.ship.desktop_app` globals exist at import time; the Windows helper tests now inject their own shimmed surface instead.
- Desktop bridge/update imports on the current public release line no longer fail across source runtime startup, packaged updater handoff, or release-preflight test collection when `src.ship.desktop_app` and `src.ship.desktop_update` are loaded through different packaged surfaces.
- Packaged static-scrapy runners no longer relaunch `Baluffo.exe` as a second top-level desktop instance in frozen mode; packaged fallback execution now dispatches through the child-script path instead.
- Packaged uncapped fetch no longer leaves `scrapy_static_sources` looking frozen as an opaque final work item while the browser-fallback queue is still advancing.
- Desktop updater status no longer regresses handoff/install-ready state back to `ready` merely because the downloaded ZIP still exists while the updater is already in handoff/install states.
- Desktop update install start now refuses to report success unless durable launcher handoff is confirmed against the live launcher session, so first-click install attempts no longer silently no-op or snap back to `Install and restart` when handoff confirmation fails.
- Startup metrics on the current public release line continue to preserve the authoritative ordering for browser launch, shell-window visibility, and runtime readiness.

## [0.1.32] - 2026-04-19

### Changed
- Desktop update and release-note dialogs now use the newer polished popup presentation layer, and the Saved page received additional UI polish around the activity/workspace flow and local-profile modal presentation.
- Frontend styles now ship as split shared/page-scoped assets under `styles/` (`base.css`, `components.css`, `jobs.css`, `saved.css`, `admin.css`), and release/runtime packaging was updated to include that new asset layout.
- Desktop startup probing on the current public release line continues to use the more isolated policy and telemetry path introduced in the recent desktop startup hardening work.
- Packaged desktop smoke and CI release gates on the current public release line continue to isolate Playwright bridge local data from repo-local desktop session state so the bridge-release lane starts from a clean guest profile.

### Fixed
- Desktop update handoff and recovery no longer get stuck in a stale relaunch state after an install-ready update or updater transition.
- Packaged GitHub HTTPS traffic now shares the same trust fallback across source sync and desktop update flows, including the updater helper, and the preferred PEM override is `BALUFFO_GITHUB_CA_BUNDLE` with sync-only and update-only compatibility envs still supported.
- Packaged source sync no longer bypasses the shared GitHub TLS context on the normal runtime `urlopen` path, so the portable desktop now applies the same certificate trust fallback in real sync requests that desktop update already used.
- Startup metrics on the current public release line continue to preserve the authoritative ordering for browser launch, shell-window visibility, and runtime readiness.
- Jobs-page shared action styling was restored after the stylesheet split, including the `Refresh Jobs` / `Run Discovery + Fetch + Sync` buttons and the bottom `Admin Online` status pill.
- Jobs-page pagination spacing was corrected so the pager no longer sits flush against the end of the jobs table.

## [0.1.31] - 2026-04-19

### Changed
- Desktop release version ordering now follows Baluffo's `0.1.x` scheme across the updater, recovery manager, and release tooling, and `0.1.31` is the compatibility bridge that outranks both legacy semver releases like `0.1.23` and current Baluffo-ordered releases like `0.1.3` and `0.1.29`.
- `v0.1.31` is the first public release intentionally chosen to satisfy both the old semver updater population and the newer Baluffo-specific updater ordering.
- Desktop startup probing still uses the more isolated policy and telemetry path introduced on this release line, and the compatibility bridge keeps that runtime behavior as the current shipped desktop.
- Packaged desktop smoke and CI release gates continue to isolate Playwright bridge local data from repo-local desktop session state so the bridge-release lane starts from a clean guest profile.

### Fixed
- The packaged desktop now reports its intended `0.1.31` app version, and mixed-client update populations can converge on the same release without contradictory `Current` / `Latest` states.
- Startup metrics continue to preserve the authoritative ordering for browser launch, shell-window visibility, and runtime readiness on the current release line.

## [0.1.3] - 2026-04-19

### Changed
- Desktop startup probing now uses a more isolated policy and telemetry path, with tighter readiness checks, faster Chromium launch timing, and lower-overhead paired startup profiling.
- Portable release packaging now trims redundant payload size and hardens updater and runtime recovery behavior around staged startup ordering and launch diagnostics.
- Packaged desktop smoke and CI release gates now isolate Playwright bridge local data from repo-local desktop session state so the release lane starts from a clean guest profile.
- Packaged desktop startup probing, crash coverage, and updater finalize/retry behavior were hardened so release-preflight and smoke lanes stay aligned with the shipped runtime.
- Desktop first-use flow now explains guest-mode persistence, lists existing local desktop profiles before sign-in, shows the installed app version in page chrome, and reframes the initial Admin no-fetch state as guidance instead of an unexpected error.
- Release-notes and desktop update UI wording were tightened around finalize/retry and startup resilience.
- Static listing/detail completeness caps were removed so the fetcher can keep pursuing valid zero-yield and residual detail paths instead of cutting them off early.
- Static traversal now prioritizes recall again without giving up the async transport, capped Playwright, and packaged-runtime throughput improvements that stabilized cold fetches.

### Fixed
- Packaged desktop startup now keeps Jobs, Saved, and Admin navigation state stable during startup handoff and no longer regresses the unload prompt during in-app page switches.
- Startup metrics now preserve the authoritative ordering for browser launch, shell-window visibility, and runtime readiness so packaged startup smoke and profiling report the correct sequence.
- Local CI gate regressions across ship-bundle, runtime, and packaged smoke coverage are resolved so the canonical release-preflight lane stays green on the release commit.
- Desktop startup/update resilience regressions around launch handoff, stale launch retry paths, and packaged crash recovery were removed, including cleanup of the unused desktop launch retry helper.
- Desktop sign-in no longer falls back silently to blind profile-name entry when profile listing fails; it now requires explicit `Retry`, `Create new profile`, or `Cancel`.
- The first-run `fetch_never_run` Admin guidance can no longer be dismissed away before a successful fetch clears the condition.
- Packaged cold fetch validation stayed in the fast runtime class while slightly improving final merged output after the static completeness rollback.

## [0.1.23] - 2026-04-17

### Changed
- Desktop startup probing now uses a more isolated policy and telemetry path, with tighter readiness checks, faster Chromium launch timing, and lower-overhead paired startup profiling.
- Portable release packaging now trims redundant payload size and hardens updater and runtime recovery behavior around staged startup ordering and launch diagnostics.
- Packaged desktop smoke and CI release gates now isolate Playwright bridge local data from repo-local desktop session state so the release lane starts from a clean guest profile.

### Fixed
- Packaged desktop startup now keeps Jobs, Saved, and Admin navigation state stable during startup handoff and no longer regresses the unload prompt during in-app page switches.
- Startup metrics now preserve the authoritative ordering for browser launch, shell-window visibility, and runtime readiness so packaged startup smoke and profiling report the correct sequence.
- Local CI gate regressions across ship-bundle, runtime, and packaged smoke coverage are resolved so the canonical release-preflight lane stays green on the release commit.

## [0.1.22] - 2026-04-16

### Changed
- The desktop Jobs-page updater now surfaces persisted background download failures directly in the update panel instead of falling back to the generic available-update state.
- Release and troubleshooting documentation now describe the explicit failed-download retry path for the portable desktop updater.

### Fixed
- Desktop update downloads that fail in the background now keep the panel open, show the persisted updater error, and offer a direct `Try download again` action.
- Failed portable ZIP downloads now clear stale install-ready state and best-effort delete bad staged artifacts so retry starts from a clean updater state.

## [0.1.21] - 2026-04-16

### Fixed
- Jobs-page desktop job links now open in the default browser again instead of failing when the bridge request path duplicated the local bridge base URL.

## [0.1.2] - 2026-04-15

### Fixed
- Desktop navigation to Admin and Saved no longer prompts to save and closes the app window; the packaged desktop pages now retain the Baluffo window identity token during in-app page switches.

## [0.1.1] - 2026-04-15

### Added
- Desktop in-app update flow in the Jobs desktop UI, backed by a signed GitHub release-manifest pipeline for portable releases.
- Packaged updater rehearsal coverage and release diagnostics for the helper-driven `N -> N+1` install path.
- Shared city-noise and country-acceptance contracts, plus regression coverage for exact junk tokens, country promotion, and backend/frontend location parity.
- Jobs-page pipeline progress reporting, terminal-success packaged smoke coverage, and backend regression coverage for the worker path and bridge wiring.

### Changed
- City parsing now normalizes multi-location strings, dedupes bilingual variants, and rebuilds location summaries from the surviving normalized locations.
- Country-like city values such as `EU & NA` and `UK` are now promoted into the country field instead of being dropped, while valid cities remain untouched.
- Location normalization was consolidated into the canonical parsers path and mirrored in the frontend jobs domain so backend and UI stay aligned.
- Local portable builds now mirror successful `dist\baluffo-portable\Baluffo.exe` outputs to `_out\latest\build\portable\Baluffo.exe` so the latest path does not stay stale.
- Desktop updater install handoff, helper progress tracking, and packaged recovery behavior were hardened so portable releases update more reliably.
- Release tooling and packaged verification docs now reflect the current desktop build, smoke, and update pipeline.

### Fixed
- Exact city garbage, prose bleed, and chrome-like location fragments are now rejected consistently across the audit, canonicalization, and frontend normalization paths.
- The Sega M Electrical Products row no longer gets forced into the `Game` sector classification.
- Country picker dropdown now closes reliably when clicking outside it or pressing `Escape`, matching the shared popup behavior in the Jobs page.
- Source sync can now be pointed at a custom PEM CA bundle via `BALUFFO_SYNC_CA_BUNDLE` for machines with a nonstandard trust store or TLS-inspecting proxy.
- Jobs-page pipeline runs no longer fail at runtime with `'PipelineService' object has no attribute '_load_json_object'`.
- The packaged Jobs-page pipeline smoke now fails on backend worker errors after startup instead of passing once the button briefly enters a busy state.
- Packaged desktop update checks now resolve the correct release repo, avoid relaunch loops, and handle cross-platform release paths correctly.
- Closing the packaged desktop window now tears down the desktop session cleanly instead of leaving stray `Baluffo.exe` processes behind.
- Pre-submit parity and CI gate regressions that blocked the packaged release flow were corrected for the `0.1.1` release line.

## [0.1.0] - 2026-04-10

### Added
- Dedicated Jobs-page packaged smoke lane that proves the pipeline can be launched from Jobs without opening Admin.
- Changelog-backed release-note extraction for tagged releases.
- Shared dead-listing gate for static and generic careers extraction so regular pages reject as `dead_listing_page`
- Provenance-based game-sector normalization instead of a raw source-sector override
- Admin restore hooks for fetch and discovery progress after navigating away and back
- Better public-link rewriting for provider rows that exposed raw API URLs
- Transition-aware source registry sync with per-source merge, schema v2 snapshots, and local tombstone-backed deletes
- Explicit registry restore-deleted flow for locally removed sources

### Changed
- Discovery auto-approval now uses explicit eligibility rules and keeps `weakSignal` as diagnostics only.
- GitHub release notes are generated from the top versioned section of `docs/CHANGELOG.md`.
- Ship-bundle release builds use the canonical `python` entrypoint instead of `py -3.13`.
- Discovery preset semantics swapped in place: `default` now uses the former uncapped-lite behavior, and `uncapped` is the broader exploration preset
- Static plugin fallback metadata is now centralized in a shared helper to reduce duplicated boilerplate across host adapters
- Jobs UI link handling normalizes RemoteOK detail URLs to the safer listing page
- City and country filter normalization was tightened to reject obvious non-location contamination
- k-ID no longer needs a source-specific suppressor plugin; the shared dead-listing gate now handles it
- Source sync now pushes only active and pending rows; rejected stays local and tombstones are never serialized remotely
- Retired `scraping-pipeline-run-notes.md` from the docs archive; use git history for the outdated 2026-03-17 run notes.

### Fixed
- Legacy sync merge comparison no longer prefers stale remote rows when transition metadata is missing on the local side.
- SmartRecruiters API links now rewrite to the public posting URL
- Game-company rows now stay classified as `Game` when provenance or company evidence supports it
- Misclassified regular pages such as About / Contact / Careers landing pages no longer become synthetic job entries
- Static extraction now stops leaking a few repeated metadata payload shapes through copy-pasted per-plugin dict construction

## [0.0.15] - 2026-03-30

### Added
- Full Milestone 1-6 roadmap delivery (health scoring, taxonomy, discovery promotion, static adapter hardening)
- Enhanced static adapter with generic fallback heuristics and location fixes
- Provenance-based game classification
- Discovery promotion pipeline with structured migration
- Browser fallback circuit breaker
- Admin bridge refactoring with improved task lifecycle and busy-state handling
- M4-M6 social experiment reporting
- Complete lint infrastructure (Python + JavaScript/ESLint + pre-commit)
- Fetch artifacts refresh and audit tooling

### Changed
- Various bug fixes and code quality improvements

### Fixed
- Multiple bug fixes from M1-M6 delivery

---

## [0.0.10] — 2026-03-23

### Added
- Release 0.0.10 with sync, pipeline, and discovery fixes

### Notes
- The public app release line is `v0.0.x`.
- Git tags follow `v<app_version>` and, for this historical release entry, the tagged release was `v0.0.10`.

---

## Legacy notes

The notes below were retained from the earlier draft release history and are now treated as historical implementation notes, not separate shipped release lines.

### Admin bridge and runtime rewrite
- Admin bridge extracted to modular services (`src/bridge/`)
- Source check API with Playwright fallback for static sources
- Task history and run history API
- Ops health and alerts system
- Jobs pipeline refactored with separate loader selection and runtime phases
- Static adapter now dispatches to plugins via `AdapterPluginContext`
- Frontend state-hub for cross-module state management
- Browser queue URL collapse by source ID
- Activision canonical listing URL resolution

### Shipping and discovery foundation
- GitHub App-based source sync for multi-PC workflows
- Source discovery package (`src/source_discovery/`) reorganized
- Static adapter plugin system for studio-specific parsing

### Browser-required and initial release work
- Playwright fallback for static source discovery and scraping
- Scrapy-Playwright integration for browser-required sources
- Admin discovery log live tailing
- 403/timeout handling in discovery probe
- Generic static source classification
- Initial release: job aggregation from Google Sheets, Remote OK, provider APIs (Greenhouse, Lever, etc.)
- Static studio page scraping
- Source discovery with web search and probing
- Admin console for source management
- Saved jobs with notes and attachments
- Local-first storage (IndexedDB + file-based)

## Known Issues

| Issue | Status | Workaround |
|-------|--------|------------|
| Some static sources still return 0 jobs | Open | Use browser fallback queue |
| Social sources may miss recent posts | Open | Adjust lookback window |

---

## Version History

- [0.0.10] — 2026-03-23
- [0.0.9] — 2026-03-23
- [0.0.8] — 2026-03-20
- [0.0.7] — 2026-03-20

For older shipped tags, see `v0.0.1` through `v0.0.6`.

*For older releases, see the older versioned sections in this changelog.*
