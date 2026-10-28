# The probe's ~210 s is one remote check per candidate plus a straggler tail: `api.lever.co` is 53% of lane time — 2026-10-28

> - **Class:** measurement — a repeated-measures pair on the live full population; no shipped
>   behaviour changes.
> - **Trigger:** the post-cut pair left the probe as the audit's largest single phase at **210.2 /
>   211.2 s** (30.6 / 32.5% of the arm), and the phase receipt could say it was 24.7% CPU but not
>   whether the rest was a real per-candidate cost or session weather. The next phase study needed
>   to answer that before any probe change.
> - **Status:** **not CPU-bound, one attempt per candidate, and the mean is weather while the median
>   is real.** On the identical **8,370**-row population (digest `66165df36a10c023`, CSV
>   `896da851…`) the pair reads **194.1 / 218.7 s** (mean **206.4**, inside **2.05%** of the
>   untraced **210.7 s** control) with **9,631 / 9,635** attempts and the same **803** 429s. The
>   phase is **92.4 / 92.7%** HTTP lane time and only **5.9 / 5.8 ms** of process CPU an item
>   (**27.5 / 24.1%** of wall), so **V1** is not CPU-bound and **V2** is one attempt per candidate.
>   **`api.lever.co` carries 53.0 / 53.4%** of the lane time — mostly **1,792 / 1,780** 404s at
>   **0.93 / 1.07 s** and **113 / 141** 6.4 s timeouts — ahead of Greenhouse **11.6 / 10.0%** and
>   SmartRecruiters **6.2 / 10.8%**, whose **1,800 / 1,794** 200s double from **156** to **317 ms**.
>   The paired **3,434** 200s and **4,809** 404s keep their median within **4.5 / 1.1%** (mean
>   **+18.0 / +11.2%**, r **0.68 / 0.85**), so the typical per-URL cost is stable and the mean is
>   weather. The **37.2 / 35.0%** idle lane time is a **straggler tail**: the first two
>   fifths run at **43–97%** occupancy (17 of 18 waves are ≥90% in at least one of their first
>   two fifths), the last two fifths carry **77.7%** of the idle, and the last fifth only **46.4%** — so **V4**'s literal last-fifth rule returns `permit/bucket-bound`
>   even though the early bins are full; the bin curve says tail-bound.
> - **Verified against:** `45220c2e` (`main`) + docs-only working changes; live GameDevMap CSV
>   sha256 `896da851155b2da0b41b96da567134cc63e9eba7765bdba1b4485cbf3168a31a`; 32 lanes, per-host 3,
>   timeout 6, 1,000-row batches, whole eligible population; arms 2026-10-28T15:47:23–16:10:24Z;
>   request trace on both arms; `PROBE_CONCURRENCY_TOTAL=40` (default).
> - **Then inspect:** `_out/perf-runs/discovery/probe-profile-20261028/` — `design.md` (the
>   pre-registration), `run_arms.sh`, `arms/prof{1,2}/arm.json`, each arm's
>   `request-trace.jsonl`, the artifact, `analyze.py`, `analysis.json`, `profile.txt`, and the two
>   arm logs; the instrument check is `_out/perf-runs/discovery/probe-profile-sanity-20261028/`.

## What was run

Two full-population arms, **prof1 → prof2**, at `--lanes 32 --batches 0 --batch-size 1000 --timeout
6`, with the request trace on. Each arm is isolated under its own `BALUFFO_DATA_DIR`. No setting
differs between them, so this is a **repeated-measures pair**, not a counterbalanced A/B: the same
population is measured in two sessions and each URL is its own control. The order repeats the
post-cut pair's, so the traced arms are positionally comparable to the untraced **210.7 s** baseline.

The population is identical (**8,370** rows, same digest and CSV sha256, `remainingUrls` 0). The
traced arms' probe mean is **206.4 s**, **−2.05%** from the untraced baseline, inside the
pre-registered ±5%; the trace is non-perturbing. The sanity arm's first batch also reproduced
post1's first batch within **1.2%** (**23.64** vs **23.92 s**) before the pair ran.

## The decomposition

| Reading | prof1 | prof2 |
| --- | ---: | ---: |
| Arm wall | 690.2 s | 688.2 s |
| Probe wall | **194.1 s** | **218.7 s** |
| Probe share of arm | 28.1% | 31.8% |
| Process CPU | 53.33 s (**27.48%**) | 52.78 s (**24.13%**) |
| CPU per item | 5.86 ms | 5.81 ms |
| Items | 9,097 | 9,078 |
| Lane-busy time | 4,878.8 s | 5,683.9 s |
| Mean lane time per item | 536.3 ms | 626.1 ms |
| Configured lanes / occupancy | 40 / **62.85%** | 40 / **64.97%** |
| Idle lane time | 2,884.0 s (**37.15%**) | 3,064.6 s (**35.03%**) |
| Full-occupancy floor | **122.0 s** | **142.1 s** |
| Trace attempts / items | 9,631 / 1.0587 | 9,635 / 1.0614 |
| Σ HTTP duration / lane-busy | 4,509.1 s / **92.42%** | 5,268.3 s / **92.69%** |

The wall is **busy-equivalent + idle**: **122.0 + 72.1 s** in prof1 and **142.1 + 76.6 s** in
prof2. The busy-equivalent is the lane-busy time divided by 40 lanes; the idle is the rest of the
wall. Of the busy time, **92.4 / 92.7%** is HTTP request duration; the remaining ~7.6% is parse and
inference inside the lane, which is where the 5.8 ms of CPU per item sits. Process CPU is 27.5 /
24.1% of the wall because the 40 probe threads overlap their waits; it is not the wall driver.

## Where the lane time goes

The trace covers **100%** of `probeLifecycle.fetchesStarted` in both arms, so the request mix is the
whole wave, not a sample.

| Status | Requests 1 / 2 | Lane time 1 / 2 | Share 1 / 2 | Latency p50 1 / 2 |
| --- | ---: | ---: | ---: | ---: |
| 404 | 5,089 / 5,065 | 2,220.0 / 2,480.6 s | **49.2 / 47.1%** | 171 / 173 ms |
| 200 | 3,605 / 3,604 | 1,398.8 / 1,726.8 s | 31.0 / 32.8% | 185 / 266 ms |
| none (timeout) | 124 / 152 | 794.7 / 962.9 s | **17.6 / 18.3%** | 6,476 / 6,475 ms |
| 429 | 803 / 803 | 90.0 / 91.4 s | 2.0 / 1.7% | 95 / 98 ms |
| other 4xx / 5xx | 10 / 11 | 5.6 / 6.5 s | 0.1 / 0.1% | 412–621 / 474–855 ms |

| Host | Requests 1 / 2 | Lane time 1 / 2 | Share 1 / 2 | What it answered |
| --- | ---: | ---: | ---: | --- |
| `api.lever.co` | 1,906 / 1,923 | 2,390.0 / 2,814.2 s | **53.0 / 53.4%** | 1,792 / 1,780 × 404 at **0.93 / 1.07 s**; 113 / 141 × timeout at **6.47 s**; 1 / 1 × 200 |
| `boards-api.greenhouse.io` | 3,194 / 3,182 | 523.1 / 525.4 s | 11.6 / 10.0% | 3,183 / 3,171 × 404 at 0.164 s; 11 / 11 × 200 |
| `api.smartrecruiters.com` | 1,800 / 1,794 | 280.2 / 569.1 s | 6.2 / 10.8% | 1,800 / 1,794 × 200, mean **156 → 317 ms** |
| `apply.workable.com` | 848 / 843 | 104.0 / 105.5 s | 2.3 / 2.0% | 762 / 760 × 429 at 0.11 s; 32 / 33 × 200; 54 / 50 × 404 |
| top ten hosts | — | — | 74.9 / 78.4% | — |

The cost is not spread over the long tail of studio homepages: it is concentrated in provider APIs,
and **Lever alone is more than half of it**. Lever's requests are overwhelmingly **negative** (1,792
/ 1,780 of 1,906 / 1,923 are 404s, one 200 in each arm) and its timeouts carry **730.7 / 910.8 s**
(the timeout bucket is 17.6 / 18.3% of all lane time). Workable's 429s are a host policy, not a transient episode:
**762 / 760** of its **848 / 843** requests are 429s in both arms, at **0.11 s** each and only 1.7 /
2.0% of all lane time.

## Weather vs real cost, per paired URL

| Bucket | n | Mean Δ | Median Δ | p5 / p95 Δ | r (log) | Slope |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 200 | 3,434 | **+76.6 ms (+18.0%)** | **+19.0 ms (+4.5%)** | −261 / +588 ms | **0.680** | 0.696 |
| 404 | 4,809 | **+51.5 ms (+11.2%)** | **+5.2 ms (+1.1%)** | −423 / +819 ms | **0.854** | 0.918 |

The median URL is stable; the mean is not. The 404 rank correlation (**0.854**) says the per-URL
404 latency is largely repeatable, and the 200 correlation (**0.680**) is moderate. The slope below
1 in both buckets says the second session's slow URLs were not simply scaled up: the shift is
concentrated in a few hosts and the tail. **SmartRecruiters is the clearest host shift** — the same
1,800 / 1,794 successful reads take **280.2 → 569.1 s** (+103%), mean **156 → 317 ms** — while
Greenhouse moves **523.1 → 525.4 s** and Workable **104.0 → 105.5 s**. The timeout count rises
**124 → 152** (28 more 6.4 s waits, +168 s of lane time), mostly on Lever (**113 → 141**). The 429
count is exactly **803** in both arms.

**V3 returns `weather-affected`** because the pre-registered tripwire `(429 + None) / requests` is
**9.63 / 9.91%** (2% threshold) and the 200s' mean Δ is **18.0%** with r **0.68**; the 404 median
(**1.1%**) and the 200 median (**4.5%**) say the typical candidate is stable. The weather is in the
tail and one host, not in the per-candidate request.

## The idle time is a straggler tail

Both arms produce **9 waves** of ~1,000 attempts each, and every wave reaches **40** in flight. The
first two fifths of a wave run at **43–97%** occupancy (**17 of 18 waves** are ≥90% in at
least one of their first two fifths); the wave then collapses as a handful of slow requests finish.
The first bin below 50% falls in the fourth fifth in **9** of 18 waves, the fifth in **6**, the
third in **2**, and the second in **1**.

The pre-registered last-fifth idle share is **46.4%** (weighted by wave idle), so **V4 as written
returns `permit/bucket-bound`**. The pre-registered bin curve does not support that label: the early
bins are full, the last two fifths carry **77.7%** of the idle, and the last three fifths carry
**89.9%** (post-hoc). The idle is the **straggler tail** — Lever 404s at 0.93 / 1.07 s and 6.4 s
timeouts holding the last lanes — not permit starvation and not a missing lane budget. Both readings are
recorded rather than rewriting the rule after the arms.

## What the profile changes

* **The lever is not CPU.** 5.9 / 5.8 ms of CPU an item, 27.5 / 24.1% of wall, and 92.4 / 92.7% of
  lane time in HTTP. There is no probe-internal CPU hotspot left to cut.
* **The lever is not more lanes.** Occupancy is **62.9 / 65.0%**, but most early bins already run at
  90–97%; the missing lane time is the tail, not the head. A wider budget would idle the same way.
* **The next measurements are the timeout tail and the negative Lever probes.** The 6.4 s timeout
  bucket is **17.6 / 18.3%** of lane time (113 / 141 of them on Lever), and 404s are **46–49%** of
  lane time, with Lever alone carrying **53%** of it for **one** successful Lever API read in each
  arm. Whether a shorter per-call bound,
  a host-level circuit breaker, or a candidate-generation filter is safe is a separate question; the
  profile only prices it.
* **Host latency is a live variable.** SmartRecruiters doubled between sessions with the same
  requests and statuses; a host-level cache or backoff would have to be measured the same way, not
  assumed from one session.
* **The arm wall is not the probe instrument.** The probe moves **24.6 s** between arms while the
  arm wall moves **2.0 s** (690.2 / 688.2 s); the other phases absorb it. Read the phase receipt and
  the trace, not the arm total.

## Limits

* One live session; repeated measures, not a counterbalanced A/B. The pair measures weather; it does
  not remove it.
* The trace wraps the default `fetch_text_httpx` probe path. It covers **100%** of
  `fetchesStarted` here, but structured API probes or browser fallbacks could bypass it on another
  population.
* A successful response is recorded as **200**; **404** and **429** are exact statuses; **None** is
  a transport error or timeout. `probeFailures` is not used: it counts 404 negatives as failures.
* CPU is process-wide across the parent's threads; the homepage-analysis child is a separate process
  and is not counted.
* Wave clustering uses a **15 s** inter-request gap. The observed batch boundaries are **19.2–71.1 s**
  apart and the largest within-wave gap is **5.4 s**, so the split is clean for this pair.
* The 429+None tripwire is a request-rate rule, not a lane-time share: 429s are only 1.7–2.0% of
  lane time. The V3 label is therefore about the presence of weather, not its dominance.
* V4's last-fifth window is narrower than the observed tail; the record keeps the rule's literal
  verdict and the bin reading side by side.
* No source changed; the working tree is docs-only.

## Evidence

Pre-registration `_out/perf-runs/discovery/probe-profile-20261028/design.md`; arms
`arms/prof1/arm.json`, `arms/prof2/arm.json` and their `request-trace.jsonl`; analysis
`analysis.json` and `profile.txt`; contract check `_out/perf-runs/discovery/probe-profile-sanity-20261028/`
(same first batch, −1.2%). The runtime receipt and trace are produced by the shipped tool
`tools/measurements/discovery/lane_parity.py` and `request_trace.py`; no new tool was added.
