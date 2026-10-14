# The sync path now trusts what httpx trusts — 2026-10-12

> - **Class:** coverage repair, shipped, with the before/after measured on the same URLs in the
>   same minute and a control arm the change cannot touch
> - **Trigger:** the two 2026-10-12 snapshots that decomposed the audit's biggest stages both
>   tripped over the same thing and left it on the shelf: **507 of 3,000** audited homepages fail
>   `CERTIFICATE_VERIFY_FAILED` on the sync fetch path while the async path reads them, and the
>   homepage bench's sync arm drops **187 of 1,000** against the async arm's 8.
> - **Status:** shipped — the shared sync fetchers anchor verification on certifi **beside** the OS
>   store. The recorded 507-URL failure set reads **0 → 482** (httpx on the same set: **471**); the
>   same 1,000-homepage batch reads **761 → 940** while a control arm the change cannot reach moves
>   **933 → 891**; the whole audit's homepage phase reads **2,326 → 2,763** homepages (+18.8%)
>   and its certificate failures **506 → 16**, for **+104 static candidates** and **+18 active
>   boards** on the same 3,000-studio population. It is a **union** anchor, so nothing that verifies
>   today can start failing.
> - **Verified against:** `9b8901f8` plus the uncommitted board-absence, timeout-ladder and
>   secondary-wave cuts (the tree the retained arms were produced on).
> - **Then inspect:** `src/shared/tls_https.py` (the builder), `src/source_discovery/web_search_fetch.py`
>   and `src/jobs/common/http.py` (the call sites), `tests/test_tls_https.py` (the contract).

## The defect, in one line of trust anchors

On this checkout the OS store holds **84** roots and certifi **121**, and the two disagree on hosts
whose served chain is valid: the OS store's reduced/stale set poisons chain building with
"certificate has expired" for **all 507** URLs in the retained run's trust-failure class, with
`ssl.create_default_context()` (the store, explicitly) failing exactly as the bare `urlopen` does.
Python only calls `load_default_certs()` when no `cafile` is passed, which is why the sync path sees
this and `httpx` — certifi-anchored by default — does not.

## What shipped

One leaf module, `src/shared/tls_https.py`, owns verified-context construction: default certs, then
any operator PEM bundle named by `ca_bundle_envs`, then certifi. `build_verified_ssl_context()` is a
fresh context (the GitHub/update path's existing semantics) and `verified_ssl_context()` is the
process-wide one for hot fetch loops, so the 175-anchor set (84 from the store, 121 from certifi) is
parsed once per lane instead of once per page.

| Call site | What it is |
|---|---|
| `src/source_discovery/web_search_fetch.fetch_text` | the discovery lane's sync fetch, the admin source check's fetch, and every local harness and bench |
| `src/jobs/common/http.default_fetch_text` (+ its `_with_response_headers` twin) | the jobs lane's shared fallback, used by the static adapters |
| `src/bridge/source_check_http.discover_redirect_career_candidates` | the admin source check's alternate-URL probe, where a trust failure was swallowed into "no redirect candidates" |

`src/shared/github_https.build_github_ssl_context` now **delegates** to the same builder and keeps
only its own naming (`BALUFFO_GITHUB_CA_BUNDLE`, the GitHub-specific failure message), so the repo
has one anchor rule rather than two copies of it. `provider_structured_listing._workday_tls_context`
deliberately keeps its **certifi-only** context: there the OS store *poisons* chain building for
`*.myworkdayjobs.com` rather than merely failing it, so the union rule is the wrong rule for that
host family.

The anchor is a union, which is the property that makes this a repair and not a verification
downgrade: `CERT_REQUIRED` and the hostname check are untouched (asserted in
`tests/test_tls_https.py`), the OS store stays loaded, and certifi is added — a superset of trust,
so a host that verifies today cannot start failing.

## The failure set: three arms, one minute, the same 507 URLs

`verify_sync_trust_anchor.py` reads the retained run's trust-failure URLs, then refetches that set
under three arms in one run — so host drift cannot explain the difference between them:

| Arm | What it is | Read | Readable bytes | TLS-trust failures |
|---|---|---|---|---|
| `legacy` | bare `urlopen` — the pre-change sync path, and what the retained run used | **0 / 507 (0.0%)** | 0 | **507** |
| `shipped` | `web_search_fetch.fetch_text`, the anchored production path | **482 / 507 (95.1%)** — 480 and 478 on a second and third run | 57.6 MB | **11** |
| `async` | `async_fetch_text_httpx` + httpx — the path that already read them | **471 / 507 (92.9%)** | 56.5 MB | **11** |

The 11 residual trust failures are the **same 11 hosts in both anchored arms** (set difference
empty in both directions) — genuinely unresolvable chains such as `broxcorp.com`,
`www.clevercg.com` and `littlereddread.com`, which httpx refuses too — so the sync path's anchor is
now neither narrower nor wider than the async path's. Three URLs the async arm read failed on the
shipped arm in this pass (`astrum-entertainment.ru`, `dot9games.com`, `www.cococucumber.co`) and the
shipped arm read 11 the async arm did not; the arms are within noise of each other, which is the
claim.

## The same batch, with a control arm the change cannot reach

`homepage_floor_bench.py` replays one recorded batch of **1,000** homepages through
`fetch_directory_pages`. Its `sync-20` arm passes a custom fetcher and therefore calls the shipped
`fetch_text` — the changed path. Its `async-32` arm passes `fetcher=fetch_text` and is served by the
httpx branch, so `fetch_text` is **never called**: a control that measures host and run drift in the
same minute as the treated arm.

| Arm | Readable | Certificate family | Σ request time | Wall | Peak in-flight |
|---|---|---|---|---|---|
| `sync-20` recorded (pre-change) | 761 | 187 | 763,774 ms | 42,090 ms | 20 |
| **`sync-20` now (anchored)** | **940** | **7** | 707,503 ms | 39,174 ms | 20 |
| `async-32` recorded (control) | 933 | 8 | 566,202 ms | 22,510 ms | 32 |
| `async-32` now (control) | 891 | 8 | 539,921 ms | 21,514 ms | 32 |
| `async-32-r2` recorded (control) | 888 | 8 | 528,296 ms | 21,223 ms | 32 |

**+179 homepages** on the treated arm (+23.5%) against **−42** on the control, which is inside that
arm's own recorded spread (888–933). The sync arm's certificate failures fall to **7** — the residue
the async arms already carry (8). And it is not a wall-clock win: the recovered pages are *cheap*
failures (≤ 200 ms) replacing whole-lane budget, so Σ request time falls slightly (763,774 →
707,503 ms), the 6 s tail thins (15 → 9), and the wall moves 42,090 → 39,174 ms — with the bench's
**20-thread** executor ceiling still the binding constraint (peak in-flight 20 of 32, unchanged).
That is the composition the two earlier snapshots implied: the trust anchor recovers **pages**, the
lane ceiling costs **wall**, and neither substitutes for the other.

## In situ: the whole audit, one arm

`audit_probe_bench.py --arm trust-anchor --batch-size 1000 --max-batches 3 --timeout 6` (the same
instrument and population as the retained `tailfull-shipped` arm), with the homepage phase counted
from the run's own fetch log:

| Arm | Homepages read | Homepage TLS-trust failures | Homepage phase | Static candidates | Recovery candidates | Active boards | Run total |
|---|---|---|---|---|---|---|---|
| `tailfull-shipped` (recorded) | 2,326 (77.5%) | 506 | 138,143 ms | 498 | 98 | 91 | 425,323 ms |
| `secondary-cut-narrow` (control: cut, no anchor) | 2,326 (77.5%) | 506 | 105,744 ms | 494 | 96 | 88 | 362,059 ms |
| **`trust-anchor` (shipped)** | **2,763 (92.1%)** | **16** | 127,198 ms | **598** | **106** | **106** | 443,335 ms |

The two pre-change arms read **the same 2,326** homepages, so this phase is repeatable and the 437
extra pages are the anchor's. The candidates move with the pages: **+104 static** and **+18 active**
against the closest control, while `providerCandidates` is flat (2,434 → 2,435) — the recovered hosts
are the small-studio pages that resolve to a static listing rather than a hosted provider board.

The control is `secondary-cut-narrow` rather than `tailfull-shipped` because the shipped tree also
carries the secondary-wave cut, which is why its wave-2 fetch is 25,651 ms against `tailfull-shipped`'s
48,075 ms; the two differ from this arm only by the anchor (and the probe-timeout bound, which lives
in a later phase). The run total is **not** a like-for-like comparison in any direction — it moved
+18.1 s here and −63.3 s for the cut on the same instruments, because the phases either side of this
change (`recovery_wave1_fetch` 107,697 → 140,846 ms, `probe` 83,934 → 92,512 ms) drift with live
rate limiting. **The page and candidate counts are the durable numbers; the clock is not.**

## What this does not fix

- **11 rescued-class hosts stay unreadable** (above): those chains fail under any anchor available
  here, and they are now visibly *host* failures rather than a lane-wide store problem.
- **The 20-lane fallback ceiling is untouched.** The audit bench's custom fetcher still bottoms out
  in `asyncio.to_thread`'s `min(32, cpu_count + 4)` executor (see
  `discovery-homepage-stage-floor-2026-10-12.md`), which is why this change buys pages and not wall
  on that instrument.
- **Adapter-local `urlopen` calls are not covered.** `jobs/adapters/social.py`,
  `jobs/adapters/html_parsers.py`, `scrapers/providers/jobylon_v1.py`, `jobs/common/url.py` and
  `ashby_registry_refresh.py` each open their own sync request with their own retry/parse contract;
  they are the same defect class but were neither measured nor changed here. The shared fetchers —
  the ones with a population to measure — are.
- **`httpx` is unaffected either way**, which is the point of the control arm; the jobs lane's
  provider paths that already use httpx never had this defect.

## Limits

- **The failure set is a retained run's population**, not a random sample: the 507 URLs are exactly
  the hosts one full audit recorded as trust-failing, so the 0/507 legacy arm is a property of the
  recorded class, not of the internet. The gain is on the population that was actually being lost.
- **Live hosts move between the three arms**, in one minute rather than across days; the 3
  async-only reads against 11 shipped-only reads bound that noise but do not remove it.
- **The control arm's own spread is 888–933** over three recorded runs of the same batch, which is
  why the treated arm's +179 is compared against a control that moved −42 rather than against a
  single baseline number.
- **The anchor is only as good as certifi's bundle**, refreshed when the dependency is; the OS store
  remains loaded beside it, and `BALUFFO_*_CA_BUNDLE`-style overrides still work through the
  `ca_bundle_envs` seam.
- **The in-situ arm was not re-run against a freshly measured control**, so its candidate deltas
  (and especially its run total) carry the same-day drift the two retained arms show between
  themselves; the homepage page count is the one figure both this arm and the homepage batch
  measurement agree on.
- **The homepage bench's sync arm does not reproduce the recorded wall** (39,174 vs 42,090 ms here,
  and the earlier snapshot's 42,090 vs 33,834 ms), so the page counts are the durable number and the
  wall is not.

## Reproducing the numbers

```bash
# the failure set, three arms in one run (~50 s; fails if a stated invariant moves)
python _out/perf-runs/discovery/verify_sync_trust_anchor.py --timeout 6 --lanes 32

# the same 1,000-homepage batch, treated arm plus the control the change cannot reach (~1 min)
python _out/perf-runs/discovery/homepage_floor_bench.py --arms sync-20,async-32 --batches 0 \
    --out _out/perf-runs/discovery/homepage-floor-trust-anchor.json

# the whole audit in situ (~13 min, three batches of 1,000; re-run the control arm the same day
# for a like-for-like comparison, or read the retained secondary-cut-narrow arm beside it)
python _out/perf-runs/discovery/audit_probe_bench.py --arm trust-anchor \
    --batch-size 1000 --max-batches 3 --timeout 6

# the shipped contract
python -m pytest tests/test_tls_https.py tests/test_github_https.py \
    tests/jobs_static/test_static_cookie_retry.py -q
```

The retained arms, this arm's fetch log and the three-arm probe output live under
`_out/perf-runs/discovery/` (untracked). The harnesses stay there on purpose: a `scripts/` edit is
shipped container code and trips the release gate, and these are measurement, not product behaviour.
