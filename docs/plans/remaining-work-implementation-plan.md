# Remaining Work Implementation Plan

> - **Status:** One item blocked and **unsafe to attempt**; four items verified complete; one flake still unproven. No implementation work is live here.
> - **Use this when:** someone proposes resuming the 23-board repoint, or asks what remains of this plan
> - **Canonical for:** the do-not-run warning on the repoint method, and the remaining unproven flake
> - **Not canonical for:** measurement method (see [`../measurement-methods.md`](../measurement-methods.md)), registry retirement rules (see [`../scraping-pipeline.md`](../scraping-pipeline.md)), or job-quality policy (see [`../DATA_CONTRACT.md`](../DATA_CONTRACT.md))
> - **Then inspect:** [`../measurement-methods.md`](../measurement-methods.md), [`../scraping-pipeline.md`](../scraping-pipeline.md), and `git log` for the completed items
> - **Last updated:** 2026-10-02 (reduced 489 → 121 lines; durable lessons extracted)

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

## 5. The two undiagnosed flakes — one proven, one still open

`test_transient_get_error_retries_with_backoff` and
`test_bridge_profile_summary_records_external_sample_failure` each failed once in ~8 suite runs.

**`test_bridge_profile_summary_records_external_sample_failure` — PROVEN.** Reproduced
deliberately: starting the developer suite and launching two more pytest processes while it was in
flight produced exactly this failure and nothing else (`1 failed, 5482 passed, 1 skipped`):

```
FileNotFoundError: [Errno 2] No such file or directory:
  '...\.tmp\pytest\pytest-tmp-37b1450302ee4b66aa9bd750143738ec\
   bridge-profile\live\performance-profile.json'
```

The path is inside the **shared basetemp**, and the write is a plain `Path.write_text` into a
directory the competing pytest removed. The harness is tearing a live test's working directory —
not a product regression, and not something the test can defend against.

**`test_transient_get_error_retries_with_backoff` — STILL NOT PROVEN.** It did not reproduce in
that run. It is a `tmp_path` user exposed to the same shared root, but exposure is not a diagnosis,
and folding it into the proven result would repeat exactly the inference this item originally got
wrong. It stays open until it fails with a captured traceback.

**Not fixed, deliberately.** Both follow from two pytest processes sharing `.tmp/pytest/basetemp`.
Giving a second run its own basetemp would remove the class rather than documenting it, but that
changes how the suites are invoked — a bigger call than a doc correction. The rule and the
traceback are in [`../testing.md`](../testing.md).

## Why this plan was reduced rather than deleted

Four of five items were finished, which would normally mean deletion. Item 1 was kept because it
holds a **do-not-run warning** that is the highest-value sentence in the file: without it, a
future session could easily re-run a method that reports Supercell and Sega as empty boards. The
step-by-step archaeology went to git history, which is the provenance.
