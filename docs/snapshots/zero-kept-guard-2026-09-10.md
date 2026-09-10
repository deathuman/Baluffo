# Guarded ok/0 classification — live-200 zero-link pages drain (2026-09-10)

Canonical execution record for the systemic classification-gap fix from the
2026-09-09 overdue triage: a kept==0 read of a live listing page was
blanket-classified `status=error` ("no jobs extracted from source pages"),
which the availability drain treats as broken evidence — rows sourced from
such a page could never be marked missing, so they persisted as
`verification_overdue` forever. Executors and evidence:
`tmp/zeroguard-20260910/` (`run1.log`–`run4.log`, `targeted.log`).

## The guard (`src/jobs/adapters/static_zero_kept_guard.py`)

`promote_clean_zero_kept(ctx)` promotes a zero-kept read to a clean ok/0
outcome **only** when the read is provably trustworthy:

- an explicit no-openings marker in the fetched listing HTML
  (`contains_no_openings_marker`, cache-backed probe of ≤3 pages), **or**
- the prior run already recorded a clean zero read (`consecutiveZeroKept >= 1`
  with a non-broken `lastFailureBucket` AND prior status ok-with-no-error or
  the generic extraction-zero error — transport failures don't count), making
  this the second consecutive clean zero read.

It refuses (keeps the error path) on: browser-fallback recommendation or
attempt, listing timeout terminal reasons, dead-listing evidence, broken
classifications, js/anti-bot/site-changed diagnoses, canonical-dropped rows,
detail candidates or visited detail pages (parser found job-like links it
failed to extract — needs review, not emptiness), and unreadable page bodies.

The stamped evidence (`classification=empty_confirmed`, `emptyConfirmed=true`,
`zeroKeptClassification=legit_empty`, `failureBucket=no_openings`,
`emptyConfirmedEvidence`) is consistent with the taxonomy recompute chain
(`assess_zero_extract` → `empty_confirmed` → `legit_empty` → `no_openings`),
so `update_source_detail_taxonomy` preserves it. Both zero-kept funnels consult
the guard: `_record_empty_plugin_result` (plugin fast path) and
`_finish_generic_source` (generic flow, promote-else-error without skipping
the completion tail).

## Live verification (2026-09-09/10, 4 passes)

| Pass | Lane | Output | Failed | Overdue |
|---|---|---|---|---|
| run1 | incremental (freshness-skipped the 12) | 40,428 | 0 | 34 (unchanged) |
| targeted | `--only-sources` over the 12 hold-tail sources (forced) | 40,470 | 9 | 33, Δ −1 |
| run2 | full regular | 40,470 | 0 | 33 |
| run3 | full `--source-ttl-minutes 0` | 40,470 | 0 | 33 (junk rows skipped by the 6h static freshness window — cached 2h earlier by the targeted pass) |
| run4 | full `--force-refresh-all` | 41,509 | 33 | 33 (BKOM/PlaySimple errored on the artifact-400 flap — see below) |
| run5 | full `--force-refresh-all` + artifact-400 fix | 40,629 | 25 | **31 / 9 sources / Δ −2, healthy** — junk rows drained; 9 hold-tail sources circuit-breaker-quarantined (see below) |

Targeted-pass adjudication of the 12 hold-tail sources (the guard's refusal
paths exercised live, each correct):

- **Recovered real jobs (3):** BKOM Studios 6 (zohorecruit via playwright
  fallback), PlaySimple Games 36, Brain Up 1 — these were stale-failing rows
  over live boards, not dead ones. Brain Up's overdue row re-verified and
  drained immediately (34 → 33).
- **Refused, extraction trouble confirmed (9):** Astrum, Reflector, Konami,
  SNK, Inverge (404ing detail candidates → site_changed / needs_review),
  Mundfish, Steer, Big Moxi (JS shells → js_required), Exit VR (needs_review).
  The guard correctly declined to fabricate empties from these.

## The artifact-400 flap and the parser-noise fix (run4's finding)

Run4 exposed why the two BKOM/PlaySimple rows would not drain: their boards
server-render HTML containing literal ESAPI/velocity template fragments
(`'+$ESAPI.encoder().encodeForHTMLAttribute(data['website'])+'`). The static
parser extracted that fragment as a detail-link candidate; fetching it returns
HTTP 400; the source errors run-over-run. The **same artifact class** created
the two March junk rows (titles like
`'+$Esapi.Encoder().Encodeforhtml(Data['Website'])+ '`). Whether a given pass
sees playwright-rendered HTML (clean: BKOM 6, PlaySimple 36 jobs on the
targeted pass) or server HTML (artifact candidate → 400 → error) is what made
these sources flap.

Fix (ingestion-level, two layers): a new
`looks_like_server_template_artifact` detector in `page_gating.py` (ESAPI
encoder calls, encodeForHTML/Attribute(, `$esc.*(`, velocity `'+$` seams —
verified non-matching on salary strings like `$100k+`), extended into
`looks_like_static_parser_noise_title` so template-concat titles can never
form junk rows again, and applied in `_append_detail_candidate` so artifact
URLs/titles are rejected before the 400-ing fetch. With the boards converging
to clean reads, the designed hysteresis (guard → ok/0 → eligible missing
evidence → drain) retires the two rows.

## The residue and the honest floor

Run5 final state: **overdue 31 rows over 9 sources (Δ −2), verdict healthy.**
The two ESAPI junk rows drained (`likely_removed`/`unavailable`,
`removedAt=2026-09-09T22:39:44Z`) after BKOM/PlaySimple converged to clean
ok/0 reads with zero artifact-400 errors remaining in the report. The 9
hold-tail sources (Mundfish 10, Steer 10, Reflector 3, Big Moxi 2, Exit VR 2,
Astrum 1, Konami 1, Inverge 1, SNK 1) were **circuit-breaker quarantined** by
run4's third consecutive errors (`quarantinedUntilAt=2026-09-10T00:02:31Z`),
so run5 excluded them and their lifecycle rows were correctly preserved
(skipped sources provide no availability evidence). They retry after the
quarantine expires; their rows remain overdue until the sources get real
repairs (provider twins or structured adapters). That is the honest floor for
this fix: every drainable row drained, every preserved row preserved for a
correct reason, and zero fabricated empties.

## Tests

`tests/jobs_static/test_static_zero_kept_guard.py` (16): marker/prior-streak
promotion, transport-failure exclusion from the streak, refusal paths
(browser-fallback, detail candidates, terminal timeout, dead listing,
unreadable pages), taxonomy-recompute consistency, drain eligibility
(`_source_report_missing_evidence_kind == "eligible"`), and both funnel
integrations (plugin + generic, promote and error branches).
`tests/jobs_static/test_static_parser_noise_titles.py` (+7): the ESAPI/velocity
template-artifact class rejected at both row and candidate ingestion, with
salary-string and real-URL non-matches pinned. Suites: 923 static/adapter
tests, 42 availability/identity tests green.
