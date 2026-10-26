# Fetch pipeline — what probe retirement is worth once one echo is not enough

**Date:** 2026-10-25, run level 2026-10-26 · **Lane:** fetch-pipeline revalidation · **Status:**
measured; rule shipped.
**76 boards** of 941 that hold a validator prove they ignore `If-None-Match`, so a due cycle skips
**77 HEADs** — **37 s** of request time; at run level the unstable rule removes **17** of its own
conditional HEADs and **18 s** of probe time per window, at the price of one flapping board's 304.
Behaviour:
[`scraping-pipeline.md`](../scraping-pipeline.md#a-kept-static-board-asks-before-it-downloads).

Retirement used to write a board off on a single 200 that echoed the tag it was sent. A CDN can
answer a HEAD from a cache miss and echo a tag it holds, so the state update now promotes an echo to
`lastHttpProbeIgnoredAt` only when the **previous** ask echoed too. This re-measures the cohort under
that rule, because the earlier figure (66 confirmed, 81 HEADs) was taken from re-asks that reused
tags captured hours earlier.

## How it was taken

941 live boards with a stored `lastHttpEtag` / `lastHttpLastModified`. For each: one plain HEAD to
capture the validator the board publishes **now** (falling back to the stored one when a HEAD echoes
none), then two conditional HEADs carrying that tag 2 s apart. Each ask is shaped like the shipped
sweep: stop at the first page that answers 200 while holding a validator, continue past a 304.

| | count |
|---|---:|
| echoed on ask 1 | 90 |
| echoed on ask 2 | 81 |
| **echoed on both — retired** | **76 (8.1%)** |
| echoed once, then 304 or a changed tag | 14 |
| echoed only on the second ask | 5 |

Confirmed cohort: 27 kept boards and 49 zero-kept. 75 of the 76 send exactly one HEAD per due cycle;
one sends two, because its page 1 carries no validator and page 2 does. At the measured 486 ms mean
that is 37.4 s of request time.

## Why the earlier estimate moved

- **66 → 76 confirmed.** The old re-ask conditioned on tags captured at 10:11Z. A stale tag turns a
  real echo into a "changed" answer, so doubling the ask with what the fetcher actually carries
  confirms ten more boards.
- **81 → 77 HEADs.** The sweep stops at the first 200 holding a validator, and an ignoring board's
  page 1 always answers one — so a retired board costs one HEAD per cycle, not one per page. The
  earlier count included sibling pages a real sweep never reaches.

## Verification

The predicate alone is not the behaviour, so five confirmed boards were driven through the shipped
`conditional_revalidate_url` → `apply_conditional_revalidate_result` → `update_source_state_rows`
chain. Four went candidate → retirement on the second ask. The fifth, `allpossiblefutures.com`,
answered 304 first and echoed second, so it correctly stayed on the probe — the flip-flop the second
ask exists for. The loopback equivalents are pinned in
[`test_static_read_validator_seed.py`](../../tests/jobs_static/test_static_read_validator_seed.py)
and [`test_conditional_revalidation.py`](../../tests/test_conditional_revalidation.py): run 1 sends
HEAD + GET and stores only the candidate, run 2 retires, run 3 sends GET only.

## The 200-without-echo cohort, classified

A page that answers 200 without echoing its validator has three possible meanings, and only one
of them is a defect. All 84 distinct pages from the sweep (87 rows, three URLs registered twice)
were walked with the tag they last published, then with whatever each answer published in turn;
a page whose answers carried no tag got one GET, and pages that looked stable got two GETs and a
body hash.

| class | pages | what it is |
|---|---:|---|
| honours after seeding | 31 | the stored tag was stale; asking with the tag it just published answers 304 |
| fresh tag every response | 22 | 20 change their bytes; 2 are byte-identical and only the tag moves |
| no validator | 15 | neither HEAD nor GET publishes one, so the seed clock means no per-window HEAD |
| alternating tags | 5 | two or three tags cycle between edges; a stored tag matches about half the time |
| moving `Last-Modified` | 3 | 2 change their bytes; 1 is a stable copy with a moving date |
| HEAD withholds the GET's tag | 4 | 3 answer a conditional **GET** with 304; 1 ignores it on both |
| now echo | 3 | 200 with the tag they were sent; the two-ask rule retires them |
| unreachable | 1 | HTTP 429 during the sweep |

**29 of the 84 could never answer 304 from a HEAD** — 22 fresh-tag, 3 moving-date, 4 HEAD-blind —
and their own probe latency prices the wasted HEADs at **19.9 s per due cycle**. The unstable
retirement covers the 25 that publish a *third* value between the probe and the read (about
17 s of request time; **18 s per due window** measured at run level below); the four HEAD-blind
boards need a conditional read instead. The five alternating boards are deliberately left probing:
their read answers with the tag that was asked about, so they still score 304 from that edge.

## What it does to a run

The 17 s above is a sum of request latencies. A window is different: retirement is written by the
read that follows a conditional 200, so the board still pays the HEAD that produces the
observation and only the *next* window is cheaper. One prior observation (`lastHttpProbeUnstableAt`,
6 h old, no stamp) was therefore seeded for the 25 boards classified as publishing a third value
between probe and read (`fresh_tag_each` / `date_only` in `.tmp/echo-measure/classify-followup.json`),
plus 35 untouched boards as drift controls. Each tree ran a warm window from that seed; the measured
pair starts from the state the warm window left, so the only difference between the two measured
inputs is the retirement the after tree wrote.

| measured window | wall | source work | kept | `notModifiedCount` | conditional HEADs | probe time |
|---|---:|---:|---:|---:|---:|---:|
| pair 1 — before `92c2c125` | 92.2 s | 990.3 s | 1 049 | 9 | **43** | **56.9 s** |
| pair 1 — after `d513dec5` | 93.8 s | 917.6 s | 1 051 | 8 | **26** | **31.2 s** |
| pair 2 — before `92c2c125` | 100.8 s | 1 026.2 s | 1 219 | 8 | **43** | **59.3 s** |
| pair 2 — after `d513dec5` | 95.1 s | 939.7 s | 1 051 | 7 | **26** | **29.3 s** |

`before` is the commit immediately before the unstable rule (the two-ask echo rule is in it);
`after` adds `c41ca750`. Same 60 boards, 12 workers, pairs in opposite order. **15 of the 25**
unstable candidates confirmed a second consecutive third value in the warm window and retired; the
other 10 published a stable pair or a 304 and kept their probe.

- **Conditional HEADs: 43 → 26 in both pairs (−17).** That is the change, and it reproduces
  exactly. `live_run` cannot see a HEAD — the scheduling decision (`revalidate_only /
  static_revalidate_due`) is the same whether the probe is sent or skipped, and the read behind it
  costs 10–100× the probe — so the windows were driven through a wrapper on the one call site the
  static sweep probes from, which records every URL and its own latency.
- **Probe time recovered: 23.6 s and 12.3 s, mean 18.0 s** — the order the per-request price
  predicted. Retired boards' own HEAD latency in the before window: n=16, median 680 ms, mean
  1 391 ms, max 11.1 s.
- **`notModifiedCount` (the all-static count): −1 in both pairs, and it is the same board.**
  `careers.bungie.com/jobs` was retired, then answered a conditional HEAD **304** in the measured
  window, so the before arm skipped its read (977 ms / 356 ms) and the after arm read it in full
  (1 433 ms / 2 490 ms). That is the cost the rule accepts and the docs already state: a board that
  churns most of the time but occasionally revalidates loses that 304 until the weekly recheck. It
  is a ~0.4–2.5 s read against the 12–24 s of HEADs the same window no longer pays, and no other
  board's 304 status differs in either pair.
- **Wall time does not resolve it.** +1.6 s and −5.7 s (mean −2.05 s, −2.1%); the drift control —
  boards whose scheduling reason is identical in both arms — has a median duration ratio of 0.95
  and 1.04, so the after window was ~5% slower in pair 1 and ~4% faster in pair 2. Eighteen
  seconds over 12 workers is ~1.5 s on the critical path, under that floor. Requests and probe
  time are the measurement; the wall delta is not.

**Two of the 17 dropped HEADs are not the cohort.** `synapsegames.com/en/pages/jobs` stopped
probing because it shares a display-name state row with the retired `kongregate.com` board —
existing behaviour, surfaced here, not introduced. `superraregames.com/pages/careers` lost its
validator in the warm window (a 200 with none clears the stored tag), so its full read in the after
arm inflates that arm. Yield of the retired cohort itself is identical: those boards kept 0 in every
window. Pair 1's output parity is 227 vs 228; pair 2's before arm kept 342 only because
`linkedin.com/company/ironbelly-studios` kept 174 rows there — the same flaky redirect seen
elsewhere — while the other three windows failed it.

## Limits

- The 941 are the boards holding a validator in that arm, not the whole population (~2,508 static
  sources); the rest had no tag to ask about.
- The 2 s gap is shorter than a real window. A longer gap lets more pages genuinely change, which
  resets a streak rather than confirming one, so 76 is a snapshot of the day rather than a constant.
- Live hosts, one sweep; the 14 non-repeating boards are the measured justification for the second
  ask, not a rate that generalises to another population.
- The run-level pair is one population, one day: **15 of 25** retired against the 25 of 25 that
  classification predicted, because live boards flap. One measured window per arm; the measured
  window's start is the warm window's output, not a fresh seed.

## Artifacts

Local and gitignored: `.tmp/echo-measure/two_ask.py` (the sweep), `result-two-ask.json` (raw
responses per board and ask). The run-level A/B lives in `.tmp/unstable-run-level/`: `setup.py`
(seed), `cycle.py` (prepare + window), `probe_count.py` (the counted window), `analyse.py` and
`attribution.py` (report), `arms/` (reports and probe lists), `states/`, `RESULTS.md`, and the
`92c2c125` worktree the before arm ran from. The population, cold and steady arms it selects from
are [`fetch-pipeline-revalidation-population-2026-10-25.md`](fetch-pipeline-revalidation-population-2026-10-25.md).
