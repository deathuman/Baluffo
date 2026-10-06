# Greenhouse Zeros and Provider Attribution Shadowing — Evidence Snapshot — 2026-10-06

> - **Status:** Hypothesis settled; attribution defect **fixed** in `tools/coverage_drain.py`
>   (tools-only, with regression tests). The discovery-queue collapse below is a shipped-code
>   finding (`src/source_discovery/`), measured and recorded but **not changed** — it needs a
>   decision. The 39 verified greenhouse boards are **not yet registered**: a seed edit is a
>   shipped-data change and rides the next release commit.
> - **Basis:** the isolated drain run `_out/coverage/verify-v7/` (2026-10-05: discovery,
>   fetch, and a 43,221-row output), its `source-discovery-report.json`, and a live
>   per-board re-probe of the 40 unregistered curated greenhouse boards on 2026-10-06.
> - **Canonical for:** the two defects below, the corrected v7 tally (322 → **438** of 688
>   registered boards), and the greenhouse verification table. The broader role-gate replay
>   is in [`dg-role-gate-2026-10-06.md`](dg-role-gate-2026-10-06.md).
> - **Then inspect:** `tools/coverage_drain.py` (`_match_board`, `_source_key_index`,
>   `attribute_collected`), `src/source_discovery/core_identity.py` (`queue_family_key`,
>   `MULTI_TENANT_PROVIDER_DOMAINS`), `src/source_discovery/core_queue.py` (`_process`),
>   `_out/coverage/greenhouse-verify/verified.json`.

## The hypothesis, and what actually produced the zeros

The handoff said: the 41 greenhouse boards reported at zero openings are probably boards the
discovery queue deferred (`summary.deferredByAdapter = {"greenhouse": 43}`), never fetched —
not real zeros. Measured against the v7 run, **both halves were right, and for two different
reasons**:

1. **The candidates were deferred** — 43 of 51 greenhouse candidates in that run carried
   `deferred: true` with `deferReason: "domain_cap"` (and 49 in the later v6/v7 runs). The
   mechanism is in `queue_family_key`: `MULTI_TENANT_PROVIDER_DOMAINS` lists only
   `myworkdayjobs.com` and `bamboohr.com`, so greenhouse falls to the
   `adapter:root_domain` branch and **every greenhouse board shares one queue family**
   (`greenhouse:greenhouse.io`). With `DOMAIN_QUEUE_CAP_DEFAULT = 2` (8 on the uncapped
   preset) that queues 2–8 greenhouse boards per discovery run and defers the rest. A
   multi-tenant provider needs the tenant in the family key; greenhouse's tenant is the
   path slug, which `board_identity_key` already extracts — the map is simply missing the
   platform.
2. **The registered boards still read zero, and that part was an attribution defect in the
   measurement tool, not a fetch result.** In the same v7 run the greenhouse rollup fetched
   **927 jobs**, the output holds **5,833 greenhouse-hosted rows across ~40 tenants**, and
   yet the drain reported `collected = 0` for 50 of the 52 curated greenhouse boards —
   because `_match_board` treated every empty prefix as "this board owns the whole host".
   Every URL-less curated row lands in the prefix index with an empty prefix, so the first
   greenhouse row (**samsungsemiconductor**) absorbed **728** postings and the other 50
   read zero. The same shadowing hit ashby (moonactive 475 / 38 zeros), workable
   (keywords-intl1 467 / 28), lever (amanotes 198 / 21), smartrecruiters (bet3651 287 /
   15): **five shared-host families, 155 false zeros** — the tenant rule added earlier could
   not run because the prefix rule returned first.

## The fix

`_match_board`'s host-root fallback now requires a **single claimant**: with several
empty-prefix rows on one host the host root is ambiguous, and it returns `None` so
`_match_bundle`'s tenant rule resolves each posting by its own path segment. Per-host
ownership behaviour for URL-bearing root rows (one claimant) is unchanged. Regression pins:
`tests/tools/test_coverage_drain_attribution.py` (ambiguous root is not guessed) and
`tests/tools/test_coverage_drain_attribution_index.py` (postings on the canonical host
resolve per tenant, not to the first board).

Replaying the attribution on the saved v7 data (`attribute_collected` over its own output
and report, no re-fetch):

| family | boards | collected > 0 before | after | jobs attributed |
|---|---:|---:|---:|---:|
| greenhouse | 52 | 1 | **48** | 922 |
| ashby | 39 | 1 | **34** | 385 |
| workable | 29 | 1 | **7** | 54 |
| lever | 22 | 1 | **20** | 198 |
| smartrecruiters | 16 | 1 | **12** | 117 |
| **total** | **158** | **5** | **121** | **1,676** |

Corrected tally for the same run: **438 of 688 registered boards keep a non-zero count**
(was 322), and greenhouse's 922 attributed jobs reconcile with the rollup's 927 fetched.
`unmatchedJobs` rose by 479 — those are jobs of provider tenants **not in the curated
report** (anthropic, wargamingen, scopely, …); before the fix they were silently credited
to the first curated board, which is the same defect seen from the other side.

## The unregistered boards, verified live

40 of the 52 curated greenhouse boards are missing from the shipped seed
(`data/defaults/source-registry-active.seed.json`, 1,811 rows; 12 curated greenhouse rows
present). Re-probed individually through `tools/coverage_verify.py --adapter greenhouse`
on 2026-10-06 (`_out/coverage/greenhouse-verify/`):

| result | boards | rows | game roles |
|---|---:|---:|---:|
| collects | **39** | 670 | 215 (32%) |
| empty | 1 (`firesprite`) | 0 | 0 |
| unknown | 0 | — | — |

Rockstar 36/36, NetEase 30/30, Loonshot 20/20, Tangogameworks 12/12 are all-game boards;
the mixed ones show why registration shape matters: **the New York Times board is 10 game
roles of 138** (its Games division beside its newsroom), Samsung Semiconductor 1 of 65,
Twitch 3 of 49, Take-Two 0 of 31. Registering all 39 as-is delivers 670 rows of which 215
are game roles — and because the greenhouse parser applies no row filter (see the role-gate
snapshot), those rows arrive unfiltered. The registration itself waits for the release
commit (seed edits carry the version bump); whether it should land before or after a
provider-parser filter is the open decision, not the board set.

## Reproduction

```bash
# the deferral: candidates deferred by the family collapse
python - <<'PY'
import json; r = json.load(open("_out/coverage/verify-collected-1/source-discovery-report.json"))
print(r["summary"]["deferredByAdapter"], r["summary"]["deferredReasons"])
PY

# the shadowing: replay attribution over the saved run (no re-fetch)
python - <<'PY'
import importlib.util, json
spec = importlib.util.spec_from_file_location("cd", "tools/coverage_drain.py")
cd = importlib.util.module_from_spec(spec); spec.loader.exec_module(cd)
from pathlib import Path
base = Path("_out/coverage/verify-v7")
report = json.loads((base / "result.json").read_text())["curated"]
counts, unmatched = cd.attribute_collected(base, report)
print(len(counts), unmatched)
PY

# the live per-board verification (network)
python tools/coverage_verify.py --boards _out/coverage/greenhouse-verify/candidates.json \
  --adapter greenhouse --out _out/coverage/greenhouse-verify/verified.json
```
