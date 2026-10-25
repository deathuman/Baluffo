# Fetch pipeline — what probe retirement is worth once one echo is not enough

**Date:** 2026-10-25 · **Lane:** fetch-pipeline revalidation · **Status:** measured; rule shipped.
**76 boards** of 941 that hold a validator prove they ignore `If-None-Match`, and a due cycle then
skips **77 HEADs** — **37 s** of request time. Behaviour:
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
17 s); the four HEAD-blind boards need a conditional read instead. The five alternating boards are
deliberately left probing: their read answers with the tag that was asked about, so they still
score 304 from that edge.

## Limits

- The 941 are the boards holding a validator in that arm, not the whole population (~2,508 static
  sources); the rest had no tag to ask about.
- The 2 s gap is shorter than a real window. A longer gap lets more pages genuinely change, which
  resets a streak rather than confirming one, so 76 is a snapshot of the day rather than a constant.
- Live hosts, one sweep; the 14 non-repeating boards are the measured justification for the second
  ask, not a rate that generalises to another population.

## Artifacts

Local and gitignored: `.tmp/echo-measure/two_ask.py` (the sweep), `result-two-ask.json` (raw
responses per board and ask). The population, cold and steady arms it selects from are
[`fetch-pipeline-revalidation-population-2026-10-25.md`](fetch-pipeline-revalidation-population-2026-10-25.md).
