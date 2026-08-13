# Widget-Board Recovery + Provider Zero-Yield Triage — 2026-08-13

> - **Status:** Evidence snapshot; registry mutations applied 2026-08-13 (operator-approved)
> - **Basis:** live probes + bounded pipeline runs on 2026-08-13; registry exports `data/source-registry-*.json.gz`
> - **Canonical for:** Track 3 (browser-fallback/widget boards), Ubisoft link ambiguity, and provider zero-yield triage from `docs/plans/jobs-coverage-improvement-plan.md`
> - **Then inspect:** `docs/source-policy-runbook.md`, `docs/snapshots/provider-coverage-closure-2026-08-12.md`, `_out/source-policy-soak-report.json`

## Registry mutations (via local admin bridge, 127.0.0.1:8877)

| Action | Rows | Evidence |
|---|---|---|
| **Ubisoft migration link** | `smartrecruiters:company_id:ubisoft2` → `static:listing_url:https://www.ubisoft.com/en-us/company/careers/` @ 0.8 | Operator disambiguation: only active canonical EN careers entry; board family matches verified SmartRecruiters API (271 postings live) |
| Reject | `static:listing_url:https://www.ubisoft.com/careers` | Redirects to homepage (`/en-us`), NOT a careers URL — stale Milan Gameprog row |
| Approve | `teamtailor:listing_url:https://jobs.coffeestain.com/jobs` | Server-rendered teamtailor board: 2 job-detail links + working `jobs.rss`; added via JSON state add (bridge `/sources/manual` can't infer custom-domain teamtailor) |
| Approve / reject | sandsoft: approve `sandsoft.com/careers/`, reject stale `sandsoft.com/careers-at-sandsoft/` | `career.sandsoft.com` 404s; `sandsoft.com/careers/` is live WordPress, but archive page is JS-rendered (no server-side job anchors — only `/careers/feed/` RSS with 10 jobs + working detail pages) |
| Demote + reject | `static:listing_url:https://bulkhead.com/careers` | `jobs.bulkheadstudios.com` DNS dead; main site no career links — dead source |
| Approve (8 Ashby) | k-ID (fixed slug `k-id`, 9 jobs), Improbable 7, Joyteractive 3, Rocket Science 8, Second Dinner 2, Sleeper 13, Stellar 10, thatgamecompany 38 | Ashby posting API live probes; bounded run verified 90 fetched / 87 kept, 0 failed |
| Reject (9 Ashby) | `argus`, `argus/jobs`, `intangibleai`, `intangibleai/jobs`, `jagex/jobs`, `kid`, `kid/jobs`, `scopely`, `scopely/jobs` | API 404 (no board; alternates tried: arguslabs, intangible, k-id ✓, scopelyinc…). k-ID pending rows used wrong slug `kid` |
| Reject (5 Ashby) | `improbable/jobs`, `seconddinner/jobs`, `sleeper/jobs`, `thatgamecompany/jobs`, `monsters/jobs` | `/jobs` duplicate variants of approved board rows |
| Reject → restore | `monsters`, `seriesai` | Board exists but 0 jobs (genuinely empty — kept pending per Track 2 precedent) |

## Bounded verification runs (2026-08-13)

| Run | Result |
|---|---|
| `teamtailor_sources` (after coffeestain add) | Coffee Stain **2 fetched / 2 kept** via teamtailor runner |
| Static trio (konami, yodo1, sandsoft) | 0/0/0 kept; konami+sandsoft classify `dead_listing_page` (forces `browserEscalationEligible=false`), yodo1 `needs_review` |
| `ashby_sources` (after approvals) | **90 fetched / 87 kept**, 0 failed, 8/8 sources ok |
| `personio_sources` | Stratosphere 1 kept; Welevel HTTP 429 (upstream rate-limit); Yager genuinely empty (`<workzag-jobs></workzag-jobs>`) |

## Browser-fallback measurement (plan Track 1.2 step)

- **yodo1**: browser fallback DID fire (pool acquisition 1, startup 520 ms, HTML returned) — but the careers page is a **teamtailor widget** whose CDN (`teamtailor-cdn.com`) has **no DNS A record**, so the widget can't load → 0 jobs. Not recoverable via current adapters; would need teamtailor board URL.
- **konami** (`konami.com/games/us/en/jobs/`): 200 but jobs JS-rendered (jQuery-era, not SPA) — `detect_js_shell` only catches React/Next tokens, and the zero-kept path classifies `dead_listing_page` which hard-sets browser eligibility False (`static_listing.py:393-396`). **Classifier gap:** jQuery-era JS shells never reach the browser pool.
- **sandsoft**: archive page JS-rendered; same `dead_listing_page` classification gap. Feed (`/careers/feed/`) exists with 10 jobs but no RSS adapter.
- **Conclusion:** browser-fallback recovery rate 0/3 for this batch; do NOT scale browser eligibility yet. Classifier gap (jQuery-era JS) documented for a future leaf change.

## Folded-in provider zero-yield triage (official plan Track 3)

- **oracle_hcm**: already closed (Glass Egg rejected 2026-08-12; only dead host row remained).
- **personio**: Stratosphere healthy (1 kept); Yager genuinely empty (correct behavior — kept active); Welevel 429 upstream transient; pending InnoGames + Travian feeds also 429 on probe (transient rate-limit, not feed-shape/auth) — **no promotion**; re-probe on next soak refresh.
- **ashby**: 14 boards probed via posting API → 8 live promoted (incl. k-ID slug fix), 5 dead slugs rejected, 2 genuinely-empty kept pending. Pending `/jobs` duplicates cleaned.

## Registry state after mutations

Active **2286** / pending **862** / rejected **22**. Ashby pending 18 → 2 (monsters, seriesai). Migration links 5 → 6 (Ubisoft added).

## Open items

- Konami + sandsoft need browser-eligibility for jQuery-era JS shells (classifier leaf change) or sandsoft needs an RSS/feed adapter — deferred, Track 1.2 expansion.
- Yodo1: teamtailor widget board; the widget CDN is dead — needs the board's own teamtailor URL (unknown) or stays zero-yield.
- Ubisoft: remaining studio subdomain statics (berlin/duesseldorf/mainz/toronto/stockholm/saguenay/winnipeg, massive.se, redlynx, redstorm) untouched — own hosts, own history; suppression now can act on the linked canonical entry.
- personio pending (innogames, travian): re-probe after upstream 429 clears.
- Soak refresh (`scripts/source_policy_soak_report.py`) to confirm `providerCoverageNextAction` clears after the Ubisoft link.
