# The probe phase's timeout ladder is bounded at one retry — 2026-10-12

> - **Class:** one shipped cut, priced deterministically and in situ, with a measured coverage cost of zero
> - **Trigger:** [`snapshots/discovery-probe-board-absence-2026-10-12.md`](discovery-probe-board-absence-2026-10-12.md)
>   removed 39% of the phase's requests and then named the residue: *"the tail is now the lane's next
>   target, not this change's claim"* — the slow candidates that hold the probe phase's clock.
> - **Status:** shipped — a timed-out probe URL is retried **once**, not twice. Worst case per candidate
>   falls from `3 × timeout_s + backoff` (**21.6 s** at `timeout_s=6`) to `2 × timeout_s + backoff`
>   (**13.2 s**); measured on a real loopback socket through the production path, wall **10.40 s →
>   5.45 s** at a 2 s timeout. Six live arm pairs moved **no verdict** by this cut.
>   **Stated up front: the audit's own `probeMs` does not separate** at the two scales measured, and the
>   sections below say why — the cut lowers the *binding* constraint only where the tail is the binding
>   constraint (≈100–2,300 probes per batch, which the audit's ~978-probe batch sits inside), and a run
>   only shows it when a host actually spends the third attempt. Proven: the bound, the worst live
>   candidate (22.0/22.1 s → 13.4/13.5 s) and a zero measured verdict cost — not a phase-level speedup
>   on demand.
> - **Verified against:** `9b8901f8` plus the uncommitted board-absence cut (both changes are in every
>   arm below; the two arms differ only in the retry budget).
> - **Then inspect:** `src/source_discovery/probe.py` (`probe_timeout_attempts`),
>   `src/source_discovery/web_search_fetch.py` (`_attempts_for`, `_is_timeout_failure`),
>   `tests/source_discovery/test_web_search_fetch_exception_ratchet.py`, and
>   `tests/source_discovery/test_probe_failures.py` for the two-attempt contract.

## What the tail actually is, from the retained attempt logs

Every arm the board-absence snapshot kept logs each per-URL attempt, which makes the ladder's value
directly countable. Across the four 2,935-candidate arms (two before that change, two after):

| Arm | attempts per URL | second attempts | third attempts |
|---|---|---|---|
| pre #1 | 4,722 first, 1,112 ok | 4, **1 ok** | 1, 0 ok |
| pre #2 | 4,722 first, 1,122 ok | 30, 0 ok | 2, 0 ok |
| post #1 | 3,045 first, 1,102 ok | 6, 0 ok | 1, 0 ok |
| post #2 | 3,035 first, 1,110 ok | 8, **1 ok** | 1, 0 ok |

53 extra attempts across four arms: **two of them rescued a board (both on the second attempt), and
none of the five third attempts did.** What the third attempt reliably does is set a batch's clock,
because a timeout is the one failure that spends a whole request budget: `3 × 6 s` plus 1.2 s and 2.4 s
of backoff is 21.6 s of wall for one candidate, and both pre-change arms recorded slowest candidates of
21.8 s / 22.2 s.

The failures the ladder is otherwise right for — 429, 5xx, a connection reset, a malformed redirect —
fail in milliseconds, so their retries are nearly free and keep the full ladder.

## The cut

`probe.probe_timeout_attempts()` returns **2** and the probe passes it down to both retry wrappers;
`web_search_fetch._attempts_for` applies it to *timeout-class* failures only (`TimeoutError`,
`httpx.TimeoutException`, or a "timed out"/"timeout" message) and leaves every other failure on the
shared `FETCH_MAX_RETRIES + 1`. Callers outside the discovery probe — the admin source check — pass
nothing and are unchanged.

Two things were deliberately **not** cut:

- **The second attempt.** It is the attempt that has been observed to rescue boards (two of them, one of
  which is a live studio site that answers on the second try). Cutting it would trade a measured
  coverage loss for 6 s.
- **The backoff.** A timed-out retry still waits 1.2 s. The measured rescues happened *after* that wait,
  so removing it would be a change nobody can price from this evidence, for 1.2 s of the 21.6.

`BALUFFO_PROBE_TIMEOUT_RETRY=legacy` restores the shared ladder, so the control arm and the shipped arm
run in one process — which is how every table below was produced.

## The proof on a real socket: a hang costs two waits, not three

Live hosts only hang sometimes (measured: the same board cost 13.1 s on one pair and 22.0 s on the
next), so the pathology is reproduced deterministically: a loopback server with one route that never
answers inside its timeout and 200 that answer immediately, driven through the **production** probe
path (`probe_candidates_async` with the default fetcher).

| Arm | tail attempts | wall | slowest candidate | fast candidates |
|---|---|---|---|---|
| `legacy` | **1, 2, 3** (2,006 / 2,011 / 2,010 ms) | 10,401.5 / 10,364.1 ms | 10,256 / 10,216 ms | 200 / 200 ok |
| shipped | **1, 2** (2,012 / 2,009 ms) | 5,482.8 / 5,408.2 ms | 5,306 / 5,261 ms | 200 / 200 ok |

The wall *is* the tail (97–99% of it): **−47.5%** on the bench, and by the same arithmetic **21.6 s →
13.2 s** at the audit's `timeout_s=6`. The 200 answered candidates are byte-identical in both arms.

**A bound worth knowing about, found while building this bench.** The probe wave already has its own
deadline — `timeout_s × (rows/concurrency) + timeout_s` — and on a *small* batch it clamps far below the
ladder: 21 candidates at a 2 s timeout never exceeded **4,000 ms**, in either arm, because one request
budget per available slot is all the wave allows. A tail measurement has to stay above that line
(`2 × (201/40) + 2 = 12.05 s` for the bench above); the audit's own batches, at ~978 probes per batch,
sit at `6 × 24.5 + 6 = 152 s`, far above both.

## In situ: what the cut changes on the audit's own population

Both arms over the same artifact, production path, two runs each. `Σ/40` is the throughput floor at the
probe's 40-way concurrency, so a wall at the tail line and a wall at the floor line can be told apart.

| Population | Arm | Σ candidate s | Σ/40 | phase wall | slowest candidate |
|---|---|---|---|---|---|
| 244 | legacy | 109.9 | 2.7 s | 13.5 s | **13.2 s** (97% of the wall) |
| 244 | shipped | 112.7 | 2.8 s | 12.3 s | 12.0 s (98%) |
| 1,000 | legacy | 377.3 | 9.4 s | 13.4 s | **13.0 s** (98%) |
| 1,000 | shipped | 385.6 | 9.6 s | 17.7 s ¹ | 13.2 s (75%) |
| 2,935 | legacy | 1,124.9–1,226.3 | 28.1–30.7 s | 37.6 / 43.6 s | **22.0 / 22.1 s** |
| 2,935 | shipped | 1,043.2–1,196.6 | 26.1–29.9 s | 34.4 / 45.1 s | **13.4 / 13.5 s** |

Three things this says, none of them flattering to a single headline number:

1. **The tail is real and it is one candidate.** At 244 and 1,000 candidates the wall is 97–98% of one
   candidate's own duration — a `static` board (`abigames.com.vn/careers`) that no provider endpoint
   answers for. That is the shape the previous snapshot described, unchanged.
2. **The cut removes exactly the third attempt and nothing else.** On the 2,935-candidate arms, per-URL
   attempt ordinals went `#1 3048, #2 15, #3 1` (legacy) and `#1 3048, #2 9, #3 2` (legacy) →
   `#1 3048, #2 3` and `#1 3049, #2 15` (shipped): one to two candidates a batch, and the batch's
   slowest candidate fell from **22.0 / 22.1 s to 13.4 / 13.5 s**.
3. **The phase wall does not separate at 2,935 candidates** (43.6 / 37.6 s legacy against 34.4 / 45.1 s
   shipped): at that size the phase is throughput-bound (Σ/40 ≈ 28–31 s) and an 8.7 s tail saving is
   inside the live-run noise. The cut's effect is a *bound*: it shows up as a whole-batch wall only when
   one candidate sets the clock **and** spends the third attempt. In the 244- and 1,000-candidate rows
   the same host set the clock (97–98% of the wall in four of them) but spent two attempts in both arms,
   so those rows show the shape the cut bounds without collecting the saving on that run.

¹ live-run noise, not the cut: that arm's own slowest candidate was 13.2 s, and its second run of the
same population was 12.8 s. The same spread appears between repeated runs of one arm everywhere in this
snapshot (the two 100-row audit pairs differ by 8% and 16%).

## The audit's own per-phase key

`audit_probe_bench.py` over the pinned CSV, both arms, `--timeout 6`:

| Audit run | probes | network attempts | probe failures | `probeMs` | job total |
|---|---|---|---|---|---|
| one 100-row batch, legacy #1 | 249 | 305 | 173 | 12,442 | 26.7 s |
| one 100-row batch, legacy #2 | 248 | 304 | 173 | 12,585 | 25.5 s |
| one 100-row batch, shipped #1 | 248 | 305 | 173 | 12,932 | 25.7 s |
| one 100-row batch, shipped #2 | 248 | 305 | 174 | 15,059 | 27.8 s |
| three 1,000-row batches, legacy | 2,939 | 3,354 | 1,794 | **81,137** | 419.5 s |
| three 1,000-row batches, shipped | 2,936 | 3,401 | 1,806 | **83,934** | 425.9 s |

**No separation, at either scale, and that is the finding.** The shipped 3-batch arm is 3.4% *higher* on
`probeMs` — inside the noise these two live runs carry (the shipped arm also met 92 rate-limit refusals
against the legacy arm's 79, and a refusal is itself a three-attempt ladder), and inside the spread of
repeated runs of one arm (the two 100-row pairs differ by 8% and 16%). What the audit's own key cannot
show is a cut whose effect depends on a host spending the third attempt, because on these runs none did
in either arm — the same reason the live walls above do not separate.

Where the cut does bind, precisely: the probe wave's own deadline is `timeout_s × (1 + probes/concurrency)`,
so the ladder is the binding constraint only for a batch of more than ~100 probes (`6 × (1 + 104/40) >
21.6 s`), and it stops binding once throughput exceeds it (`Σ durations / 40 > 21.6 s`, measured at ~2,300
probes and above). The audit's real batch is ~978 probes — **inside that window**, with a 9.5 s throughput
floor and a 21.6 s tail, which is exactly the case this cut lowers to 13.2 s. The 3-batch key above was
measured with hosts that took two attempts, so it prices the bound's absence, not its presence.

## Coverage: what the cut costs

Every verdict difference between the arms was inspected against the legacy arm's own attempt log:

| Slice, pair | verdict differences | of which won on attempt 3 in the legacy arm |
|---|---|---|
| 244, pair 1 / 2 | 2 / 1 | **0 / 0** |
| 1,000, pair 1 / 2 | 13 / 1 | **0 / 0** |
| 2,935, pair 1 / 2 | 28 / 15 | **0 / 0** |

Not one difference is a candidate that needed a third attempt: every moved candidate won on its first
or second attempt *in the budget both arms share*. The movement is live noise — the same board answering
one arm and not the other — and 10 of the 13 in the 1,000-candidate pair are `workable` 429s in the arm
that ran second, the same rate-limit artifact quantified in the board-absence snapshot. The two rescues
the retained data does show are on the second attempt, which this cut keeps.

## Limits

- The full 9-batch audit job was **not** re-run. The in-situ measurement is the audit's own bench at one
  100-row batch plus the production path over the audit artifact's own candidates at 244 / 1,000 / 2,935;
  the scale arithmetic for the full job is labelled as arithmetic.
- The deterministic bench's tail is a loopback socket that never answers, which is the *upper* bound of
  what a hanging host does. A host that answers slowly rather than not at all spends the same ladder; a
  host that answers on the second attempt (the live rescue) is unaffected by this cut either way.
- Live arms ran minutes apart against a shared IP, and several boards answer one arm and not the next.
  The verdict claim therefore reads "no difference is attributable to the ladder", not "the arms are
  identical".
- The bound is per **URL**, not per candidate: a candidate with an endpoint and a sibling page can still
  spend a ladder on each. The measured worst case on the production path was 13.4 s (single-URL hangers);
  a multi-URL hanger remains the theoretical worst case and is not covered by this cut.
- The web-search query benchmark is still unusable from this host (`HTTP 403`), untouched here.

## Reproducing the numbers

```bash
# the deterministic tail: one hanging loopback route + 200 answered ones, both arms in one run
python _out/perf-runs/discovery/probe_tail_bench.py --timeout 2 --fast 200 --repeat 2 \
    --out probe-tail-bench-201.json

# the live arms: same artifact, same harness, retry budget as the only difference
ROOT="$(git rev-parse --show-toplevel)"
POP="$ROOT/_out/perf-runs/discovery/population/audit-full-baseline.json"
for limit in 244 1000 0; do
  for arm in legacy shipped; do
    BALUFFO_PROBE_TIMEOUT_RETRY="$([ "$arm" = legacy ] && echo legacy)" \
      python _out/perf-runs/discovery/probe_urls_bench.py \
        --arm "tail-pop$limit-$arm" --candidates "$POP" --limit "$limit" --repeat 1
  done
done

# the audit's own per-phase key, both arms
BALUFFO_PROBE_TIMEOUT_RETRY=legacy python _out/perf-runs/discovery/audit_probe_bench.py \
    --arm tail100-legacy --batch-size 100 --max-batches 1 --timeout 6
python _out/perf-runs/discovery/audit_probe_bench.py \
    --arm tail100-shipped --batch-size 100 --max-batches 1 --timeout 6

# the shipped contract
python -m pytest tests/source_discovery/test_web_search_fetch_exception_ratchet.py \
    tests/source_discovery/test_probe_failures.py -q
```

Raw evidence is under `_out/perf-runs/discovery/` (untracked): `tail-bench/` for the loopback arms,
`probe-runs/probe-tail-*.json` for the live arms, `audit-runs/report-tail*.json` for the audit's own
keys, and `analyze_probe_tail.py` for the per-URL attempt ordinals in this snapshot.
