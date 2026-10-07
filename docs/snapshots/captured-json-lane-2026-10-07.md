# Captured-JSON lane — Evidence Snapshot — 2026-10-07

> - **Status:** the lane works, measured live on a real board, and is **not yet registered**
>   anywhere. Feishu stays unregistered: its payload carries only counts, so the eight tenant
>   boards are still unreachable by anything here. Tencent's board reads today.
> - **Basis:** live Chromium renders through the runtime's own pool
>   (`BrowserFallbackPool.fetch_captured`) on 2026-10-07 — Kurogame (Feishu), Tencent
>   (`careers.tencent.com`), and miHoYo (`job.mihoyo.com`, which does not resolve).
> - **Canonical for:** the design, what the capture reaches, the platform-specific field names
>   this forced, and the two findings that are recorded rather than papered over.
> - **Then inspect:** `src/jobs/browser_fallback_pool.py` (`_fetch_captured`, `fetch_captured`),
>   `src/jobs/browser_fallback.py` (`wrap_capture`), `src/jobs/adapters/plugins/static/rendered_json.py`,
>   `src/jobs/adapters/static_listing_rows.py` (`_append_rendered_json_rows`).

## The idea

A single-page app's openings are not in its markup. They arrive in the XHR the page makes after
load — which is why the existing browser render finds almost nothing on those boards: it returns
on `domcontentloaded`, before the request the board actually renders from has been made.

The browser already fetches that response. So the lane reads it: render, settle, intercept the
JSON responses, and run them through the **same** posting-shape guard the embedded-JSON lane
uses. No per-platform parser, and a board that changes platform keeps working.

## What the capture reaches

| Board | Capture | Rows |
|---|---|---:|
| `careers.tencent.com` | 3 payloads, 45 KB `/tencentcareer/api/post/Query` | **10** |
| `kurogame.jobs.feishu.cn` | 21 payloads, incl. `/api/v1/search/job_post/count` (1.6 KB) | **0** |
| `job.mihoyo.com` | DNS: `ERR_NAME_NOT_RESOLVED` | 0 |

**Tencent end to end, through the runtime's own static path** — not a probe:

```
rows: 1     (of 10 extracted; 9 dropped by the game row filter)
stats: rendered_json_rows_found: 10
       rendered_json_payloads_captured: 3
       non_game_rows_dropped: 9
```

The nine dropped rows are real Tencent roles — `Tencent Cloud – AI & LLM Solution Architect`,
`Product Marketing Manager, PUBG Mobile` — that the game filter correctly excludes. The tenth,
`Senior Marketing Manager JD (AAA Racing Game Title)`, keeps its real Workday posting URL. That
is the boundary working as designed on a board whose HTML has no job links at all.

## Feishu: the capture confirms the earlier finding rather than reversing it

The 0.3.013 probe concluded Feishu was unreachable and parked it. The capture **confirms** that
and adds the reason:

```
/api/v1/search/job_post/count →
{"code": 0, "data": {"job_type_count_map": {15 ids: counts},
                     "job_function_count_map": {14 ids: counts},
                     "job_post_subject_count_map": {},
                     "city_code_count_map": {CT_125: n, ...}}}
```

**It is a count endpoint.** The payload is facet tallies keyed by opaque ids — no titles, no
URLs, no postings. The openings are behind the same POST the probe found answering 405 to an
unauthenticated caller, and the browser's own capture does not include it because the page only
requests counts until someone interacts with a filter.

So Feishu's eight boards and 826 recorded openings remain unreachable, and now for a sharper
reason than "the API refused": the page's own data call is a count, and reaching the listings
means driving the app's search interaction. That is a per-platform client, not a payload read,
and the lane is not that. **Feishu stays unregistered** — registering it would produce exactly
the false green the delivery metric cannot see.

## What the measurement forced

**Field names are platform-specific, and a host-agnostic lane still has to know them.** Tencent's
API answers `RecruitPostName`, `PostURL`, `LocationName`, `CountryName` — no `title`, no `url`.
The key sets in `embedded_json.py` now carry the names boards have actually been measured
sending. This is the honest cost of being generic: not one name per platform, but the union of
every name observed, each earning its place by a real board.

**`Id: 0` on every row.** Tencent answers `Id: 0` for all posts and carries the real identifier
in `PostId` / `RecruitPostId`. Accepting `0` would give every row on the board the same
`sourceJobId` and dedupe the board into one. Pinned by a test.

## Cost and ordering

The lane is **last of all** — after anchors, embedded JSON, rendered cards, detail links and the
block-title fallback — and runs only when a page produced no rows *and* left no detail link to
follow. A board that works without a browser never pays for a settling render. Two tests pin
this, because "the expensive path runs first" is the kind of thing that regresses silently.

Captured payloads are filtered, not whitelisted: telemetry, analytics, config and locale
responses are skipped by URL token; **everything else is walked**, so a board serving openings
from an unremarkable URL is not dropped by a guess. The count of captured-vs-walked goes into
the entry report — that is what distinguishes "this board has no openings" from "we never saw
its data".

The capture is gated by the **same** circuit breaker as the HTML fallback (`wrap_capture`), so it
cannot spend browser time while the breaker is closed, and its demand lands in the same counters.
A render that produced payloads counts as served; one that rendered and captured nothing counts
as an empty *page*, because the browser demonstrably worked.

## Not settled

- **mokahr, recruiterkr, herp, ea-careers** were not re-probed in this pass; the lane is
  available to them and their verdicts come from the next full replay, not from this snapshot.
- Whether the 17 greenhouse `fetched_empty` boards (v8) are dormant, filtered, or were served
  empty because the API was already rate-limiting — still unproven, still needs a healthy
  control.
- Tencent's board paginates (`Count: 2240`, ten per query). This snapshot measured one page;
  paging belongs to the registration decision, not to the lane.
