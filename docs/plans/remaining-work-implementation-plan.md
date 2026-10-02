# Remaining Work Implementation Plan

> - **Status:** One item blocked and **unsafe to attempt**; four items verified complete; both former flakes are now diagnosed and addressed — 5a fixed at the source, 5b prevented by an enforced session lock.
> - **Class:** cleanup
> - **Trigger:** none. Item 1 is blocked and its method is unsafe to run; items 2-4 are done. Item 1 is the only thing left and it is blocked. Both flakes are closed: 5a was fixed in the test, 5b is now prevented by a session lock rather than left as advice.
> - **Verified against:** 2e039570
> - **Use this when:** someone proposes resuming the 23-board repoint, or asks what remains of this plan
> - **Canonical for:** the do-not-run warning on the repoint method, and the two flake diagnoses (one of which disproved the assumed shared-basetemp cause)
> - **Not canonical for:** measurement method (see [`../measurement-methods.md`](../measurement-methods.md)), registry retirement rules (see [`../scraping-pipeline.md`](../scraping-pipeline.md)), or job-quality policy (see [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md))
> - **Then inspect:** [`../measurement-methods.md`](../measurement-methods.md), [`../scraping-pipeline.md`](../scraping-pipeline.md), and `git log` for the completed items
> - **Last updated:** 2026-10-02 (reduced 489 → 121 lines, then 97; durable lessons extracted; declaration block added; item 5 rewritten after both flakes were diagnosed; 5b's recorded mechanism found to be wrong and corrected)

Every claim below was re-verified against the tree on 2026-10-02 rather than trusted from the
previous revision, which was the sixth plan in this repo whose body had moved on from its status.

## 1. Repoint the 23 moved boards — BLOCKED, and the method in this plan is unsafe

**Status: no repoint was made. Do not run the method below.** The classification is sound; the
*candidate discovery* is not, and it produces confidently wrong answers.

**The rule that must hold:** a studio that is provably alive is never retired. If no replacement
board can be found, that is a finding to record, not a retirement. All 23 rows remain active and
untouched.

### Why the method is unsafe, in one line

The discovery step used a hand-rolled regex that returned **0 postings on Supercell and 0 on
Sega**, where the repo's own detector `static_probe_evidence`
(`src/source_discovery/probe.py:400`) returns **42 and 23**. A method that reports the two
largest boards as empty is not a method worth finishing.

### Why the zeros must not be read as absence

`static_probe_evidence` is a **detail-link** detector, and boards that express postings as cards
score **0 even after Playwright renders them** — proven with three positive controls and
confirmed byte deltas. Eight of seventeen boards known to collect score zero.

So the honest state of the 23: **one** candidate cleared the observed non-zero floor; the rest are
**inconclusive, not absent.** Full method, distribution, and the disproof of the rendering
hypothesis are in git history; the transferable lessons are now in
[`../measurement-methods.md`](../measurement-methods.md) §"Know which instrument produced the
number" and §"Assert the control".

### What resuming this would actually require

Not more discovery work. The remaining split is **3 rows needing adjudication and 4 needing an
isolated fetch** to tell same-board from different-board, plus the safety controls the previous
attempt got wrong (a `status == 200` control, a 3.7 KB "studio root", and a public-suffix
`same_host` false match).

```bash
python src/jobs_fetcher.py --only-sources static_source::<id> \
  --ignore-circuit-breaker --force-refresh-all --output-dir <dir>
```

`--output-dir` is mandatory; omitting it writes stub state into live `data/`.

## 2–4. Completed, verified 2026-10-02

| Item | Commit | Verified how |
|---|---|---|
2. Collapse the duplicate board rows | `6822e3b8` | commit exists; the three `jobs.ea.com` rows are **all still active** in `source-registry-active.seed.json`, confirming the finding that they are complementary, not duplicates. Collapsing them would have deleted 62 job links for nothing. The genuine twin was `romerogames.com`; it was a tracked condition in `source-registry-known-url-collisions.json` and is resolved. |
3. `tmp/` retention | `50c28c1f` | commit exists; `tmp/` holds 1 file and 0 tracked files, and `_out/evidence/` carries 48 relocated evidence directories. |
4. The stashes | — | `git stash list` is empty; all 15 resolved. |

## 5. The two flakes — both diagnosed; one fixed, one deliberately not

`test_transient_get_error_retries_with_backoff` and
`test_bridge_profile_summary_records_external_sample_failure` each failed once in ~8 suite runs.

**This item previously recorded both as the same bug — two pytest processes sharing
`.tmp/pytest/basetemp`. That was half right, and the half that was wrong mattered.**

### 5a. `test_transient_get_error_retries_with_backoff` — PROVEN, and FIXED (`2e039570`)

**It was never the shared basetemp.** It is a `tmp_path` user, so the shared root was a
reasonable guess, but it is not the cause. The real cause is in the test itself:

```python
monkeypatch.setattr(sync._source_sync_snapshot.time, "sleep", recorder)
assert sleep_calls == [1.0]
```

`time` is the **global module object**, so that replaces `time.sleep` for the whole pytest
process. The assertion was therefore measuring process-global sleep traffic, not the retry
helper's own backoff. `src/` has ~38 threaded modules and ~95 `time.sleep` sites, several of
them poll loops, and pytest runs every test in one process — so any background thread still
alive from an earlier test lands in the recording. CI loses the race more often because it is
slower. That is why it never reproduced under a plain re-run.

Reproduced deliberately, with a control, by starting a leaking thread that calls `time.sleep`
via a **dynamic** lookup so it hits the patched attribute:

| Run | Result |
|---|---|
| no leaking thread | 2 passed |
| leaking thread | `assert [0.001, 0.001...1, 0.001, ...] == [1.0]` — **failed** |
| after the fix, leaking thread | 6 passed (and 56.11s → 1.13s) |

Fixed by replacing the `time` attribute **on the module that performs the sleep**
(`source_sync_snapshot_remote`, where `_retry_transient_get` lives) with a proxy that records
`sleep` and delegates everything else to the real module. Production code was never wrong — it
sleeps exactly 1.0s once. Mutation-verified: changing the backoff base 1.0 → 2.0 still fails the
test, so scoping the observation did not blind the assertion.

**Transferable lesson:** a monkeypatch on a stdlib module attribute is process-global. If the
assertion is "this code called sleep once", patch the *module reference*, not `time` itself. And
a flake that never reproduces under a re-run is not thereby innocent — it was reproducing against
a background thread the re-run happened not to have running.

### 5b. `test_bridge_profile_summary_records_external_sample_failure` — PROVEN, and now prevented

Reproduced deliberately: starting the developer suite and launching two more pytest processes
while it was in flight produced exactly this failure and nothing else
(`1 failed, 5482 passed, 1 skipped`):

```
FileNotFoundError: [Errno 2] No such file or directory:
  '...\.tmp\pytest\pytest-tmp-37b1450302ee4b66aa9bd750143738ec\
   bridge-profile\live\performance-profile.json'
```

**The mechanism recorded here before was wrong.** `pytest-tmp-<random>` is not the shared
`--basetemp`; on pytest 9.1.1 it is pytest's own per-session `tmp_path` directory and already
carries a UUID, so concurrent sessions do not collide on its name. What removes a live
directory is that **a pytest session deletes sibling `pytest-tmp-*` directories at startup** —
proven by planting `.tmp/pytest/pytest-tmp-SENTINEL123`, running one test, and finding it
deleted. Two sessions race on the shared `.tmp/pytest` root and one can delete the tree the
other is still writing into.

**Three fixes were measured and all were no-ops** on pytest 9.1.1, because `tmp_path` no longer
derives from any of them:

| Lever | Result |
|---|---|
| `--basetemp=.tmp/pytest/basetemp` (what the npm scripts pin) | ignored; observed root is its *parent* |
| a deeper `--basetemp` (`.../run-<pid>/inner`) | still ignored |
| `PYTEST_DEBUG_TEMPROOT` | ignored |

Setting `option.basetemp` from `pytest_configure` was implemented, measured to change nothing,
and **reverted rather than shipped as a fix that does nothing**. pytest exposes no way to move
that root, so the collision is prevented instead: `tests/conftest.py` takes an exclusive lock at
`.tmp/pytest/session.lock`, and a second concurrent session exits code 4 naming the holding PID.
Verified: the first run completed (7 passed) while the second was refused, and the lock released.
Because it lives in `tests/`, this needs no version bump. Rule and full traceback in
[`../testing.md`](../testing.md).

## Why this plan was reduced rather than deleted

Four of five items were finished, which would normally mean deletion. Item 1 was kept because it
holds a **do-not-run warning** that is the highest-value sentence in the file: without it, a
future session could easily re-run a method that reports Supercell and Sega as empty boards. The
step-by-step archaeology went to git history, which is the provenance.
