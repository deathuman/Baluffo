# Full-source corpus epoch — execution evidence, 2026-10-10

Canonical execution record for the **2026-10-10 full-source reference epoch**. Part of the
fetch-pipeline performance work; design and decisions live in
[`docs/plans/fetch-pipeline-performance-instrumentation-plan.md`](../plans/fetch-pipeline-performance-instrumentation-plan.md),
the S2–S7/S0 receipts in
[`fetch-pipeline-corpus-evidence-2026-10-08.md`](fetch-pipeline-corpus-evidence-2026-10-08.md),
the first optimisation pass in
[`fetch-pipeline-normalization-pass-2026-10-08.md`](fetch-pipeline-normalization-pass-2026-10-08.md),
and the first full-source epoch in
[`fetch-pipeline-full-source-epoch-2026-10-09.md`](fetch-pipeline-full-source-epoch-2026-10-09.md).

Everything here is local: epochs live under `_out/perf-runs/`, are never committed, and are cited
by name so a disputed number can be re-read rather than re-argued.

## Why a third reference epoch

The 2026-10-09 epoch could not referee anything. It was recorded with the Google-Sheets redirect
pool at its product default (16), so for the one failing sheet host it captured 47 of 465
candidate detail links; the truncation audit read the 418 uncaptured candidates as *unrequested*
and refused the certification, which left the whole-registry A/B parked behind a knob. This epoch
re-records the same workload with the pool at 64, keeps the 1,800 s static clock that made the
10-09 walk replayable, and — the first full-source epoch to do so — **passes its own audit**.

| | `corpus-full-20261009-114716` | `corpus-full-20261009-224258` | this epoch |
|---|---|---|---|
| sheet pool · static clock | default 16 · 1,800 s | 64 · 1,800 s | 64 · 1,800 s |
| entries · bodies | 13,719 · 6,492 | 14,298 · 6,619 | **14,368 · 6,565** |
| requests offered | 21,361 | 21,816 | **21,954** |
| rows | 40,347 | 40,421 | **40,428** |
| truncation audit | before the audit existed | refused (252 unrequested) | **0 unrequested** |
| delta referee | — | not eligible | **eligible, `blocking: []`** |

## The epoch

`_out/perf-runs/corpus-full-20261010-082830/` — recorded 2026-10-10T09:03:35Z from
`gitSha 4ebcc43a5df2` with a dirty tree (the harness itself is uncommitted), `scope=full`,
`force_refresh_all`, `max_workers=12`, browser fallback disabled (D9).

**Coverage.** All 2,046 registered loaders, `requestsOffered == requestsRecorded == 21,954`,
`unattributedCount == 0`, no misses. Offers by surface: `urlopen` 10,675, `fetch_text` 10,652,
`httpx` 627. The rule that the harness records rather than guesses the clock and the sheet pool
is unchanged: `recordedConfig` states `googleSheetsRedirectConcurrency: 64` and
`staticSourceTimeBudgetS: "1800"`, and the report mentions **zero** sources stopped by a budget.

**Size, measured on disk.** 14,368 entries and 6,565 content-addressed bodies; **223,762,479
compressed bytes** (`gzipRatio` 0.1748) against 2,365,315,717 naive, dedupe saving 1,084,869,791
(`dedupeRatio` 0.4587). Outcomes: 12,503 `ok`, 1,865 failure records — a successes-only corpus
would replay a fictional green run.

**Workload.** Window **08:28:30 → 09:02:27 UTC, 33 min 57 s**. The recorded summary a replay is
graded against:

| unit | value |
|---|---|
| `rowCount` | 40,428 |
| `identityDigest` | `7d28d6ee99b765f689670f0d48be00eb` |
| `rowProjectionDigest` | `a70801a0a0c7a0d9698182501f206601` |
| `sourceStatusCounts` | `{ok: 1711, error: 335}` |

## The audit certified it — and the refusal it fixed was the audit's fault

`corpus-full-20261009-224258` was refused for **252 unrequested candidates**, all from one
source. That source is `gracklehq`, and the product never hands it to the redirect resolver at
all: `REDIRECT_RESOLUTION_SKIP_SOURCES = {"gracklehq"}` in
[`src/jobs/canonicalize.py`](../../src/jobs/canonicalize.py#L90) and
[`src/jobs/canonicalize_locations.py`](../../src/jobs/canonicalize_locations.py#L53). The rows
were in the corpus, the audit's candidate model was wrong, and the record was sound.

[`tools/measurements/corpus/truncation.py`](../../tools/measurements/corpus/truncation.py) now
mirrors the product's own skip gate instead of assuming every row's links are candidates
(`_skip_sources()` reads the constant from the product module; `skip_sources=` exists for tests).
This epoch's audit, from `EPOCH.json`:

| | |
|---|---|
| `candidates` · `unrequestedCandidates` | 443 · **0** |
| `hostsWithUnrequestedCandidates` | 0 |
| `staticSourcesAudited` · `staticSourcesFlagged` | 2,018 · 4 |
| `staticDeclaredDroppedDetails` · `staticUnrequestedUrls` | 1,626 · 0 |
| `deltaReferee` | `eligible: true`, `blocking: []`, notes `detail_cap` |

The four static flags are declared `detail_cap` notes — a count-driven bound the referee reports
as a bound, not a divergence — so this is the first full-source epoch a study may be measured on.

## The birth replay: the guard race it exposed, then the divergence it found

### A process-global allowance that 12 workers raced on (fixed)

The record's own birth replay died before it could judge anything: its static sources failed with
`egress blocked during replay: 127.0.0.1:<port>` on boards the record had fetched fine. The
egress guard has to allow asyncio's loopback `socketpair` self-pipe, and it allowed it with a
process-global flag — so with the pipeline's 12 workers, one thread's connect fell outside
another thread's window. It is now a **per-thread depth** (a socketpair built inside another
closes back to the outer allowance), with the repro in the code's own docstring: in a threaded
socketpair loop 11 of 12 threads were blocked before the fix and none after
([`tools/measurements/corpus/capture.py`](../../tools/measurements/corpus/capture.py), pinned in
[`test_corpus_capture_egress.py`](../../tests/tools/test_corpus_capture_egress.py)).

### `parity-diverged` — 0 misses, and everything else

The re-run attaches cleanly (`verdictSource=recorded`, `eligible: true`, `misses: []`) and then
reports:

| unit | record | replay |
|---|---|---|
| `rowCount` | 40,428 | 39,920 |
| `identityDigest` | `7d28d6ee99b765f689670f0d48be00eb` | `354aaf86a974c4860382f5963d6287ba` |
| `rowProjectionDigest` | `a70801a0a0c7a0d9698182501f206601` | `4e1018b0c6a8544184ee973e7acc3d4c` |
| `sourceStatusCounts` | `{ok: 1711, error: 335}` | `{ok: 1723, error: 323}` |

The corpus served every request the replay made; the pipeline turned the same fetched records into
a different set of rows. Three mechanisms explain it, in size order.

**1. Deduplication is first-wins in arrival order, and arrival order is completion order (the
dominant term).** `deduplicate_jobs` walks `rows` once and merges each into the *earlier* row that
matches ([`src/jobs/dedup.py:120`](../../src/jobs/dedup.py#L120)), so which member of a duplicate
group survives, and under which payload, is decided by who arrives first. Arrival order is source
*completion* order: the source loop drains
`done, pending = wait(pending, return_when=FIRST_COMPLETED)` and iterates `for future in done:`
([`src/jobs/pipeline_source_loop.py:564`](../../src/jobs/pipeline_source_loop.py#L564)) — a set,
so neither live latencies nor hash order reproduce. The sort that would at least make the *order*
stable runs after merging, on the merged rows. Measured against the same corpus:

| | record | replay |
|---|---|---|
| `dedupEvidence.mergedCount` | 5,541 | **6,029** (+488) |
| merges by `primaryUrl` | 3,547 | 4,171 (+624) |
| merges by `secondaryKey` | 1,653 | 1,539 (−114) |
| merges by `sparseIdentity` | 341 | 319 (−22) |
| merge gate tier | `monitor` 5,541 | `monitor` 6,029 |

The merge *reason* moves within one gate tier: the same pairs join, but the label depends on which
side arrived first. Row identities move with them — 1,240 record identities are absent from the
replay and 732 replay identities are absent from the record (so this is not a subset relation;
merged payloads carry the survivor's fields), and 379 of the record's missing rows carry a
`dedupKey` that still exists under a different survivor. **The pipeline is not order-deterministic
above one worker; the plan's S6 determinism gate was measured on a five-source subset and this is
the first whole-registry evidence against it.**

**2. A wall-clock batch deadline that binds live and cannot bind in replay (13 of the flips).**
Thirteen sources are `error` in the record with `total batch deadline exceeded (30s)`
([`src/shared/http_batch.py`](../../src/shared/http_batch.py)) and `ok` in the replay, reporting
the recorded per-request timeouts (`The read operation timed out`, `Network error for …`) because
the recorded bodies are served instantly and the batch clock never reaches 30 s. All thirteen kept
0 rows in both runs, so they move counts and statuses, not jobs. The static per-source clock knob
the 10-09 epoch needed does **not** cover this bound, and the audit's static family looks for the
static lane's own refusal text — this one is invisible to it.

**3. One transport the corpus never intercepted.** `dayforce_sources` is `ok` live and
`error` in replay: `egress blocked during replay: jobs.dayforcehcm.com:443`. Its plugin builds a
private `urllib.request.build_opener` with a cookie jar over stdlib `urllib`
([`src/jobs/adapters/plugins/provider_api/dayforce.py:161`](../../src/jobs/adapters/plugins/provider_api/dayforce.py#L161),
"the transport here is intentionally self-contained"), and `OpenerDirector.open` is not the
patched `urllib.request.urlopen`, so no offer for it ever appeared in `offersBySurface`. The guard
did its job — the corpus model did not see the caller, which is why `unattributedCount == 0` must
not be read as "the record captured all traffic".

## A defect in the record's own bookkeeping

`corpus-full-20261009-114716` — the certified 10-09 reference — is marked
`supersededBy: corpus-full-20261009-224258`, an epoch whose audit **refused** it. A record that can
never referee a delta retired one that could. The cause is the ordering in
[`run_fetch.py`](../../tools/measurements/corpus/run_fetch.py#L748): the birth check runs only
`if verdict["eligible"]`, and only a *failed birth replay* quarantines a record and stops it
superseding older epochs (line 761) — an ineligible record skips the check and supersedes
unconditionally.
Supersession should require an eligible referee (or an explicit override), pinned by a test.

This epoch is eligible, unsuperseded, and therefore the referee listing's **pick**, with its
diverged birth replay carried as a receipt (`referees.py` surfaces `replayVerification` and does
not filter on it, deliberately: a study's own check is the gate). A study may not be measured on it
until the divergence above is either fixed or formally bound.

## The submission-order fix: implemented, measured, and what the re-record still refuses on

The first next step below is implemented and measured. [`_run_selected_loaders`](../../src/jobs/pipeline_source_loop.py#L520)
now drains loader results **in submission order**: a finished future's result is buffered under
its submission index and appended only when the next index is due, so the canonical row order is a
property of the selection, not of which loader finished first. Pinned by
[`test_pipeline_source_loop.py`](../../tests/test_pipeline_source_loop.py): the same loaders
completing in a permuted order produce identical rows and reports.

The cost is bounded memory, not unbounded: at most one window of results is held
(`window = max(8, max_workers * 4)` batches, 48 at `max_workers=12`), so a slow low-index source
defers its successors' batches instead of streaming them straight to the sidecar — this epoch's
largest single source kept 30,949 rows, so the overshoot is tens of MiB rather than the flat-RSS
guarantee the sidecar gives on its own. Ordering is the only thing that changed; no row is dropped
or reordered by a slow source, only delayed.

The re-record is `_out/perf-runs/rejected/corpus-full-20261010-112021` (run dir
`suborder-run-20261010-132021`, recorded 11:20:21 → 11:55:54 UTC): 14,256 entries, 6,561 bodies,
225,768,279 compressed bytes, `rowCount` 40,406, `identityDigest` `dd16af98ef5e3da4f4e583f5fec1dd22`,
`sourceStatusCounts` `{ok: 1712, error: 334}`. Its birth replay refused it — `parity-diverged`,
`misses: []` — and it was quarantined into `rejected/`. The tables below are what that receipt
means, and they are the measurement this snapshot's next-step 1 needed. Both pairs are re-derivable
with `_out/perf-runs/replay-attribution.py`.

| graded pair | 08:28 epoch (no fix) | 13:20 epoch (fix in both arms) |
|---|---|---|
| sources whose `dedupMerged` differs | 28 — **24 of them non-token** | 4 — **0 non-token** |
| clean-row identities differing | 109 (64 + 45) | **9 (5 + 4)** |
| rows differing in total | 508 | 492 |

"Clean" = rows whose source (and source bundle) touches none of the per-render-token sources
(named in the next subsection), so the exclusion is declared rather than the residue hidden. The
24 non-token deltas in the 08:28 pair are the completion-order signature: sibling sources whose
merges *swap* — `careers.king.com/careers` 0↔15 with `careers.king.com/us/en` 15↔0,
`ats.rippling.com/careers` 0↔4 with `…/careers/jobs` 4↔0, `blacksnow.tv/careers` 3↔0 with
`…/vacancies/` 0↔3 — the same jobs under two spellings, decided by which source's rows the merge
saw first. In the 13:20 pair no pair swaps: every differing merge belongs to a source whose crawl
consumed a shared per-render-token page, and outside those sources the record and its replay agree
on all but nine rows.

### One request key, many responses — and the tokens land in the rows

Every company page links a set of shared LinkedIn browse pages
(`https://www.linkedin.com/jobs/<category>-jobs?trk=organization_guest-browse_jobs`), and the
static lane fetches each one **once per consuming source**. LinkedIn renders those pages with
per-fetch `refId`/`trackingId` tokens embedded in the job-card links, and the corpus keeps one
entry per `(method, url, headers)` — the *last* write wins. A page four sources fetch is one body
on disk and four different live responses; replay serves the one body to all four, while the
record's rows carried each source's own render. The card URLs are persisted as the rows'
`jobLink` (the browse cards are rows, not links to fetch — nested links are only enqueued for
pages that yield no rows), so the tokens become `dedupKey`s and merge keys: the same job matches
across sources only when their renders agree.

The evidence, all re-readable in the epoch:

* the record's netease rows and its replay's link the *same jobs* under different tokens — e.g.
  `…captivate-brands-2579001296?pageNum=0&position=31&refId=eRQflubqnxvJOQ…` (record) versus
  `refId=kU6gx2lyZgpCS…` (replay): same job id, same title, different `dedupKey`;
* of all 6,561 bodies the epoch holds, exactly one carries the record's token —
  `bdd93fc6146aa7bb`, 271,678 decoded characters, 60 `refId=eRQflubqnxvJOQ` links — and **no
  entry references it**; exactly one carries the replay's (`6520dc428240af35`, 261,414 decoded
  characters, 60 `refId=kU6gx2lyZgpCS` links, referenced by both the `fetch_text` and the
  `urlopen` entry for `designer-jobs?trk=organization_guest-browse_jobs`);
* 787 of the epoch's bodies (25,191,826 bytes) are referenced by no entry at all — the
  fingerprint of a key fetched more than once with differing results, each earlier response left
  behind when a later one replaced the entry (789 in the 08:28 epoch).

### Why the epoch is still refused

The receipt names four units, and every count in the difference is attributable:

| unit | record | replay | attributed to |
|---|---|---|---|
| `rowCount` | 40,406 | 39,914 | 492 rows of token churn in 4 sources (`netease-games` +393 merges, `nexus-studios` +64, `ironbelly-studios` +34, `koeitecmo.vn` +1) |
| `identityDigest` · `rowProjectionDigest` | `dd16af98…` · `846a2759…` | `53fe9d00…` · `458a6a47…` | the 492 rows, plus 4 Echtra titles differing in text at equal count (`1047games` keeps its merge count but swaps 335 identities) |
| `sourceStatusCounts` | `{ok: 1712, error: 334}` | `{ok: 1723, error: 323}` | the two classes this snapshot already named |

Four of the nine clean rows are one more sighting of the token class at another host: Echtra's
Greenhouse board was fetched twice with different bytes, so four titles read `Marketing Artist` in
the record and `Marketing Artist Tags.` in the replay. The ninth is `koeitecmo.vn`'s
`talent community` row, whose source is one of the 12 the 30 s batch deadline
([`http_batch.py`](../../src/shared/http_batch.py)) fails live and the replay clears — the same
class as the 12 status flips; `dayforce_sources` is the thirteenth
(`egress blocked … jobs.dayforcehcm.com:443`). **Ordering is not one of the causes**, and the 24→0
non-token contrast is what says so. Two independently launched replays of this epoch agree on
**every graded unit** — `rowCount` 39,914, `identityDigest` `53fe9d00…`, `rowProjectionDigest`
`458a6a47…`, `sourceStatusCounts` `{ok: 1723, error: 323}` — and on every per-source `loss`
counter and on the run-level `dedupEvidence` block (the second replay is
`_out/perf-runs/replay2-full-20261010-143546`). Diffing all 2,046 source rows leaves only
wall-clock stamps and duration fields: `lastRunAt`, `lastCheckedAt`, `lastSuccessAt`,
`lastSeenInFetchAt`, `durationMs`, `stageTimingsMs.*`, `details[].stats.*_ms`. The replay side
is deterministic at whole-registry scope; it is the record the corpus cannot re-serve.

### What a record can do about it

A record cannot audit this class the way `truncation.py` audits dropped work — the dropped response
*was* fetched — but it can see it: a key fetched again with a different body or outcome leaves its
predecessor as an orphaned body. Counting orphans *per host* is what the record now does (next step
4, measured below); refusing to certify on a non-zero count is what it deliberately does **not** do,
because every reference epoch carries the class. No ordering change helps here, and no store policy
does either: keep-first is as wrong as keep-last when four callers saw four renders.

### The first record with the replacement log live: 73 keys, 36 orphans, all attributed

The full-source epochs above were recorded before the log existed, so their 776–789 orphaned bodies
have no hosts and never can — nothing in a finished corpus names the URL an orphan came from. The
next record is therefore the measurement, and it needs no re-record: a six-source subset reproduces
the class exactly. Four of the six are the LinkedIn company sheets whose crawls consumed the shared
browse pages (`1047games`, `ironbelly-studios`, `netease-games`, `nexus-studios`), plus
`koeitecmo.vn` and `echtragames.com`; the same knobs as the reference records
(`max_workers=12`, pool 64, `BALUFFO_STATIC_SOURCE_TIME_BUDGET_S=1800`), command and log in
`_out/perf-runs/conflictsub-cmd-20261010-170716.txt` and `…-record-20261010-170716.log`.

The epoch is `corpus-subset-20261010-170723` (recorded 17:07:16 → 17:11:12Z, ~4 min wall; 685
requests offered == recorded, 277 entries, 142 bodies, 5,794,060 compressed bytes, 2,973 rows,
outcomes 270 `ok` / 7 failure). Its audit reads `status: flagged`, **73 replaced keys** across **125
replacement events** (`body-changed` 80, `response-gained` 35, `response-lost` 10), **36 orphaned
bodies**, and `attributedOrphans == 36` — `unattributedOrphans: 0`, i.e. the disk join and the log
agree and nothing is left over. Five hosts:

| host | replaced keys | orphaning replacements | orphaned bodies |
|---|---|---|---|
| `www.linkedin.com` | 62 | 69 | 32 |
| `koeitecmo.vn` | 6 | 9 | 3 |
| `job-boards.greenhouse.io` | 1 | 2 | 1 |
| `sg.linkedin.com` | 2 | 0 | 0 |
| `www.zynga.com` | 2 | 2 | 0 |

The samples name the class rather than describing it: the replaced keys are the shared browse pages
(`https://www.linkedin.com/jobs/artist-jobs?trk=organization_guest-browse_jobs`,
`…/engineer-jobs?trk=…`, `https://www.linkedin.com/top-content?trk=…`), and the orphaned bodies
they left behind each carry **60 `refId=` and 60 `trackingId=`** links — the per-render tokens,
ready to become `dedupKey`s. The two hosts with replaced keys and no orphans are the design, not a
gap: `response-gained` replaces a *failure* with a body, so nothing is orphaned, and a body still
referenced by another entry is nobody's orphan. Counts and objects are therefore different numbers
on purpose — 90 orphaning-eligible events, 36 bodies — which is why the report carries both.

The certification carries all of it: `EPOCH.json.conflicts` holds the summary (5 hosts, status
flagged), `deltaReferee.notes` gains one `key_conflict` note naming the top five hosts with
`replacedKeys 73` / `orphanedBodies 36` / `unattributedOrphans 0`, `conflict-report.json` holds every
host, and stderr gets one warning line per host plus the pointer to the declaration. It changes no
verdict: `eligible: true`, `blocking: []`, the truncation audit `clean`.

**What it measured about the remaining work.** The record's birth replay is `parity-diverged`
(2,973 → 2,493 rows, `identityDigest` `12f278ca…` → `6370d84f…`), so the epoch was quarantined to
`_out/perf-runs/rejected/` and supersedes nothing — as it should be. But every record-only or
replay-only identity and every differing `company`/`source` field belongs to one of those four
LinkedIn sources; outside them the two arms agree row for row. That is precisely the shape the
remaining binding has to accept: a divergence whose **whole** residue is the class the record
declared per host, refused the moment one differing source is outside the declared set.

One defect this first quarantine exposed and that is now fixed rather than noted: the move to
`rejected/` rewrote three paths in the receipt and none of the others it carried, so
`sampleRunDirs` (the pointer a reader follows to the sample run) still named the directory the move
had removed. `_quarantine_epoch` now sweeps **every JSON document the epoch moves with** — receipt,
audit reports, and the sample's own artifacts — with a textual retarget that can only change a path,
and the manifest's birth block by the same walk; `quarantine.originalDir`/`movedTo` still record the
move itself, which is the one place the old path belongs. The three quarantined epochs (this one and
the two full-source re-records) were repaired with the same walk, backed up under
`_out/perf-runs/quarantine-path-repair-backup/` and read back: 30,434 JSON documents parse, none
names a directory that does not exist.

## The opener-interception re-record: one of the three causes survives

The 08:28 birth replay named three causes. Two had fixes by the time of the 13:20 record
(submission order) and of item 3 above (the private-opener transport); this epoch is the first made
with both in place, so its birth replay is the measurement of how much of the divergence the two
fixes actually remove.

The epoch is `_out/perf-runs/rejected/corpus-full-20261010-145103` (run dir
`_out/perf-runs/openrt-run-20261010-165102`; recorded 14:51:03 → 15:25:17Z, birth replay to
15:44:03Z, then quarantined on `parity-diverged`). Same workload and knobs as both earlier records:
`scope=full`, `force_refresh_all`, `max_workers=12`, pool 64, 1,800 s static clock. 15,837 entries,
6,586 bodies, 223,302,105 compressed bytes (`gzipRatio` 0.1744); outcomes 14,206 `ok` / 1,631
failure. `requestsOffered == requestsRecorded == 34,988`, `unattributedCount == 0`.

| | record | replay |
|---|---|---|
| `rowCount` | 40,434 | 39,917 |
| `identityDigest` | `342023f4…` | `aadc065f…` |
| `rowProjectionDigest` | `0eb72171…` | `01599a66…` |
| `sourceStatusCounts` | `{ok: 1714, error: 332}` | `{ok: 1727, error: 319}` |
| egress guard | — | `blocked 0`, `blockedRemote 0`, `targets []` |

The audit is `eligible: true`, `blocking: []`, with notes `detail_cap` (the same 4 sources as both
earlier epochs) and `batch_deadline` (17) — the first epoch to exercise item 2's declaration end to
end, and 0 unrequested candidates. The birth receipt is `parity-diverged`, `misses: []`.

| cause | how it is read | 08:28 (no fix) | this epoch (both fixes) |
|---|---|---|---|
| completion-order dedup | `dedupMerged` deltas that are *not* token sources | 24 of 28 sources | **0 of 4** |
| wall-clock batch deadline | `error` → `ok` flips carrying `total batch deadline exceeded` | 13 | **12** (17 declared by the audit) |
| out-of-lane transport | `dayforce_sources` status, guard targets | `ok` → `error`, `jobs.dayforcehcm.com:443` | **`ok` ↔ `ok`, none** |

**The out-of-lane cause is gone, and not by accident.** The record's corpus now holds the two
requests the dayforce client makes through its own opener
(`/api/auth/csrf`, `/api/geo/ref/jobposting/search`, both `ok`; zero such entries in either earlier
epoch), and the replay's guard never armed a target — those requests were served from the corpus.
**The ordering cause stays gone on a second, freshly recorded pair**: 0 non-token merge deltas, and
the clean-row residue is 9 rows (4 record-only + 5 replay-only) against 109 in the 08:28 pair.
**The deadline cause survives**, as it must — the 30 s batch clock is wall time the replay cannot
reach. 12 of the 17 sources the audit names flip `error` → `ok`; the other five keep `error` with
another message first, and one further flip (`dizzaract`) carries the same 403 chain in both arms
and moves only the status and its failure bucket. Every flipping source kept 0 rows on both sides;
the twelve carrying the deadline text are the replay clearing a live-only bound.

**What still refuses the epoch is the token class, unchanged.** The 517-row difference is entirely
the four LinkedIn sources (`netease-games` −399 final rows, `nexus-studios` −65,
`ironbelly-studios` −34, `1047games` −19), the same per-render `refId`/`trackingId` churn the 13:20
pair attributed, plus the four Echtra titles. Re-derivable with
`python _out/perf-runs/openrt-attribution.py <record-data-dir> <replay-data-dir>`.

One measurement note, because offer counts are not comparable across epochs: this record's ledger
reads `offersBySurface` `{fetch_text: 10,709, urlopen: 23,814, httpx: 465}` against ~10.7k urlopen
offers in both earlier records, and its corpus holds 1,610 URLs that appear under no other surface
(only 68 of them a same-path variant of a `fetch_text` URL). The replay needed 11,142 requests and
missed none, so none of it is part of the graded difference — that reads rows, statuses and
per-source counters — but what this run made ~13k more direct-call attempts through is not yet
attributed, and stays a named observation rather than a cause.

## Next steps, in order

1. ~~Deterministic canonical row order~~ — **done and measured** (section above): drain loader
   results in submission order; 24 non-token merge deltas → 0, 109 clean-row identities → 9.
2. ~~The batch-deadline class~~ — **done**: the audit reads `total batch deadline exceeded (…)` as
   its own reason (`batch_deadline`) and declares it in the referee verdict's notes rather than
   blocking on it. Re-run over this epoch's own artifacts it names **16** sources (not the 13 the
   status flips suggested — two of them carry another message first), `eligible` stays **true**,
   and `staticSourcesFlagged` moves 4 → 20 (21 on the re-record below, 17 in this class). The bound
   has no knob in the product, so declaring it is the whole remedy.
3. ~~Out-of-lane transports~~ — **done, and fixed rather than annotated**: `capture.py` now patches
   `urllib.request.build_opener`, shadowing `open` on each opener a caller builds, so the dayforce
   plugin's own opener is recorded and replayed like any other `urlopen`. The live call is still
   made *through that opener* (its cookie jar and handlers are why the board answers), pinned by a
   test whose marker handler is only reachable through it. `OpenerDirector.open` is deliberately
   not patched — `urlopen` reaches the network through the module-level opener's own `.open`, so a
   class-level patch would re-enter the handler from the live call. **Measured on the re-record
   below**: dayforce is `ok` in both arms, the record's corpus holds its two requests, and the
   replay's guard never armed a target.
4. ~~The record-time conflict audit~~ — **done**: [`conflicts.py`](../../tools/measurements/corpus/conflicts.py)
   counts request keys whose stored response was replaced, joins the store's replacement log
   (`replacements.jsonl`, written by `CorpusStore.record` the moment it overwrites an entry) to the
   bodies no entry references, and reports both per host. It is **declared, never refused**: every
   full-source epoch carries the class, so refusing would refuse every reference epoch forever —
   the dead end the 10-09 pool produced from the other side. An epoch's `EPOCH.json` now carries a
   `conflicts` block and a `key_conflict` note in `deltaReferee.notes` (top five hosts named; the
   report holds them all). Re-run over the finished epochs, the disk half reproduces exactly — 787
   orphans in the 13:20 epoch, 789 in the 08:28, 780 in the re-record above — and reports
   `replacementLog: absent`, hosts unknown, for all of them, because the log only starts with the
   next record — which is the six-source subset measured **above**, the first epoch whose orphans
   have hosts (73 replaced keys, 36 orphans, all attributed). Pinned by
   [`test_corpus_conflicts.py`](../../tests/tools/test_corpus_conflicts.py), including a real
   loopback record where one page is fetched twice: the certification carries the count and still
   certifies.
5. ~~Gate supersession on eligibility~~ — **done**: a record the audit refused no longer supersedes
   anything, which is the inconsistency that retired the certified 10-09 reference above (a failed
   *birth replay* already quarantined instead of superseding; a failed *audit* did not). An
   accepted-but-truncated record still supersedes, and says so in the manifest.

**What still stands between this epoch and the whole-registry A/B.** `ab.py` verifies its referee
before the first pair and refuses on any falsifying verdict, `parity-diverged` included — so the A/B
cannot run until the residue is either gone or **formally bound**. The re-record above is the
measurement of the fixes: ordering and the out-of-lane transport are gone, the wall-clock deadline
is declared as a bound, and what is left is the per-render-token class (one request key, many
responses), which cannot be shrunk at all — keep-first is as wrong as keep-last when four callers
saw four renders. The record now declares that class too: the conflict audit writes the count per
host into `EPOCH.json` and the `key_conflict` note into the verdict. The binding that turns that
declaration into a verdict is now implemented and measured — see
[the binding](#the-binding-the-subset-epoch-pins-2026-10-10) below, where the subset record's
entire residue is attributed to a host it declared and the epoch pins.


## The binding: the subset epoch pins (2026-10-10)

The paragraph above ended on what still stood between this epoch and a study: the declaration was in
place, and what was missing was the join between it and a verdict. `binding.py` is that join. It
reads the record's own rows (through `EPOCH.json.recordedRunDir`) and the replay's, keys them by job
identity, attributes every differing row to the host of the source that carried it — its `source`,
and every `sourceBundle` entry that also published it, falling back to the job link — and
`replay_check` answers **`parity-bound`** only when every one of those hosts is a host the record's
conflict audit declared. `parity-bound` is a labelled pass, never `verified`: the two statements
answer different questions, and the receipt keeps the per-host evidence while the manifest keeps the
compact form.

Measured on the quarantined subset epoch, which is the first record whose residue has hosts:

| Item | Value |
|---|---|
| command | `python tools/measurements/corpus/replay_check.py _out/perf-runs/rejected/corpus-subset-20261010-170723 --out _out/perf-runs/binding-check-<stamp> --samples 1 --timeout-s 1800` |
| verdict | **`parity-bound`**, process exit **0**, `counts {bound: 1, verified: 0, falsified: 0, unjudged: 0}` |
| declared by | `conflict-report.json` — 5 hosts: `www.linkedin.com`, `koeitecmo.vn`, `job-boards.greenhouse.io`, `sg.linkedin.com`, `www.zynga.com` |
| residue | 2,973 → 2,493 rows; **721 record-only, 241 replay-only, 352 with moved fields**; all **1,314 on `www.linkedin.com`**; 0 unattributed, 0 outside the declared set |
| moved fields | `company` 352, `source` 352, `sourceJobId` 352, `sector` 19 |
| repeatability | a second run reproduced the residue exactly (1,314 rows, same host) and exited 0 |
| refusal, same residue | the same epoch with a declaration that omits `www.linkedin.com`: `bound: false`, **1,314 rows outside** the declared set, verdict `parity-diverged`, exit 1, `certification_held` false |

Receipts: `_out/perf-runs/binding-check-20261010-181618/replay-check.{json,md}` and
`_out/perf-runs/binding-check2-20261010-181909/`; the refusal is
`_out/perf-runs/binding-refusal-check-20261010-182851/` — a *copy* of the manifest and a doctored
report, because the refusal had to be measured without editing the evidence epoch. The epoch itself is untouched: its
`replayVerification` block still reads `parity-diverged`, because that is what its birth replay
measured under the code of its own day, and a check re-run today does not edit a record.

What changed is the gate. `ab.py run` accepts the bound verdict and applies the same bind to its
arms, so a pair whose arms both drift inside the declared class **and agree with each other** still
yields a verdict: the arms are graded against each other as well as against the record, a unit the
arms disagree on is refused whatever either arm's residue looks like, and a divergence inside a
declared host that the class does not explain is bound along with it — which is why the block counts
the fields that moved, `sector` included. Three limits are worth stating: the bind needs the
record's rows on disk (an epoch whose recorded run directory has been cleaned away refuses rather
than comparing nothing), it is host-level rather than row-level, and an epoch that declares nothing
— a clean audit, or one recorded before the audit existed — can never be bound.

## Supersession

Nothing was deleted. `corpus-full-20261009-114716` (90 s clock) remains the 10-09 reference;
`corpus-full-20261009-224258` (pool 64, before the audit fix) is the epoch this one replaces
once certified; `corpus-full-20261009-110605` (unreplayable at source 451),
`corpus-full-20261009-103615.crashed-partial` (no manifest), and the `budgetprobe`/`poolprobe`
probes stay as evidence, and the six-source subset that carries the per-host conflict block
(`corpus-subset-20261010-170723`) sits in `rejected/` too, quarantined by its own birth replay for
the very class it declares. The opener-interception re-record
(`corpus-full-20261010-145103`) was quarantined to `rejected/` by its own failed birth replay and
supersedes nothing — the listing's pick is still `corpus-full-20261010-082830`, unchanged.
