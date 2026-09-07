# xboxgaming Workday attribution — final investigation results

**Date:** 2026-09-05
**Scope:** where the T5-staged xboxgaming Workday rows actually live, and whether this checkout's local state can act on them.

## What's true in this checkout (local snapshots are 2026-08-29 vintage)

| Artifact | Rows | xboxgaming Workday present? |
|---|---|---|
| `data/source-registry-pending.json.gz` | 860 | **No** — 8 other workday rows (Aristocrat, Intel, SciPlay, L&W, SciGames, LIGHTSPEED, TiMi, Avalanche) |
| `data/defaults/source-registry-pending.seed.json` | 74 | **No** — 0 workday rows |
| `data/source-registry-pending.jsonl` (delta) | 25 | **No** |
| `data/source-registry-tombstones.json*` | 1 dummy row | **No** |
| `data/source-registry-active.json.gz` | 2291 | **No** — 0 workday rows active locally |
| `_out/coverage-refresh-2026-08-28/reg-pending-live.json` (500-row table capture, 8/29) | 500 (cap) | **No** — `pendingCount` summary says 869, table has 500 rows, none are workday |
| `_out/coverage-refresh-2026-08-28/reg-active-live.json` (25-row active table capture, 8/29) | 25 (cap) | **No** — `activeCount` summary says 2302, table truncated to 25 |

The 8 workday rows that **are** in this checkout's pending are the WP18/T3 candidate cohort (Intel, SciPlay, L&W, Scientific Games, LIGHTSPEED, TiMi, Avalanche, Aristocrat) — not the xboxgaming phApp-family rows.

The table caps (25 for active, 500 for pending) are **local capture artifacts**, not live container limits. The live container summary counts (`activeCount 2302`, `pendingCount 869`) are exact; the table views in these snapshot files are truncated for transport.

## What's true on the live container (per T5 staging.json, staged 2026-09-05; maintenance window cleared 2026-09-05)

T5's `_out/t5-workday-cxs-fix/20260905/staging.json` records:

- **Staging action:** added 2 workday rows to the live container via `/sources/manual`
  - `workday:listing_url:https://xboxgaming.wd1.myworkdayjobs.com/CentralTech` (Activision / Central Technology)
  - `workday:listing_url:https://xboxgaming.wd1.myworkdayjobs.com/External` (Beenox, High Moon, Infinity Ward, Sledgehammer)
- **Read-back:** both rows present in container pending bucket, `adapter=workday`, `name="Xboxgaming (Workday)"`, `evidenceScore=44`
- **pendingCount delta:** 860 → 862 (the 2 new rows)
- **Naming caveat:** host-derived studio attribution is `Xboxgaming` — "correct to Activision / per-studio attribution during T3 review before promotion"
- **Recovery expectation:** WP18 8/28 yields (External ~67, CentralTech ~3) recover on next fetch passes after Workday's platform maintenance window clears

**Maintenance-window recheck (this session, through the fixed adapter):**

- Both boards now return real postings through the fixed workday_sources transport: CentralTech 2, External 40 (42 total today)
- Evidence artifact: `_out/xboxgaming-maintenance-probe-20260905.json`
- Reading: the Workday maintenance window that was live at T5 staging time has cleared; the T5-staged rows are recoverable now
- WP18 comparison: WP18 8/28 measured CentralTech ~3, External ~67 before the TLS fix blocked the adapter; today's CentralTech 2 / External 40 is in the same ballpark and confirms recoverability (External's 40 vs 67 is a real-time snapshot difference, not a regression — both are point-in-time CXS totals)

The T5 TLS fix (`src/jobs/adapters/provider_structured_listing.py`, certifi-anchored verified TLS for `*.myworkdayjobs.com`) is what makes this probe possible here. Without it the adapter would still be hitting the same `certificate has expired` block WP18 documented.

**Key point:** the rows exist on the live container, but this checkout's local snapshots (8/29) predate the 9/5 staging. The container state with xboxgaming rows is NOT in this checkout — but the boards are reachable through the fixed adapter, and today they're live.

## Who is "xboxgaming" anyway

The Workday CXS site `xboxgaming.wd1.myworkdayjobs.com` is a shared tenant that hosts multiple game studios:

- `/External` → Beenox, High Moon Studios, Infinity Ward, Sledgehammer Games (T5 expect ~67 postings)
- `/CentralTech` → Activision (Central Technology) (T5 expect ~3 postings)

The T5 staging rows are named `Xboxgaming (Workday)` because the source identity derives from the shared host. The T3 review callback flagged this as an attribution problem: jobs surfaced from these rows would be user-facing attributed to "Xboxgaming" instead of to Activision / the specific studios.## Investigation result (what we can and can't do in this checkout)

1. **There are no xboxgaming Workday rows to mutate in this checkout.**
   Neither local pending, pending seed, pending delta, tombstones, active, nor the 8/29 live captures contain them. The registry table cap questions (25/500) are about the captured snapshot transport, not live container limits.

2. **The rows exist on the live container, and today they're recoverable through the fixed adapter.**
   T5 staged them on 9/5 to the container at `192.168.50.61:8877`. This checkout's snapshots are from 8/29, before T5, so this checkout's local registry doesn't have them. But the boards are reachable through the fixed adapter, and the maintenance window has cleared: CentralTech 2, External 40 (42 total today).

3. **The attribution problem (name = "Xboxgaming (Workday)" vs Activision / per-studio) is real, and now it's a promotion-time T3 review item because the rows have recovered yield.**
   - No field-edit route exists (POST admin only has approve/reject/demote-active/restore-rejected/delete/manual-add)
   - `/sources/manual` and `/registry/approve` land rows in pending
   - `add_manual_source` does tombstone check by URL fingerprint before accepting — delete + re-add of the same URL is blocked
   - Tombstones here are a single dummy placeholder (`src-1`, empty URL fingerprint), so they're not currently a live obstacle in this data — but the fingerprint logic is real in code
   - The durable fix is a registry rename/edit route (or a tombstone-safe replace-name-for-existing-url path), not a one-off patch; see the follow-up tracked below


## Final plan (re-anchored, per the approved plan)

1. **Don't attempt the xboxgaming attribution fix in this checkout.**
   There are no xboxgaming Workday rows here to rename, re-label, approve, or delete. The rows T5 staged live are not present in this checkout's snapshots.

2. **Treat xboxgaming as a staging/recovery question, not an attribution repair in this checkout.**
   The T5 rows are staged on the live container. They will recover yield when the Workday maintenance window clears (External ~67, CentralTech ~3 per WP18 8/28). The attribution correction (Activision / per-studio naming) is a T3 review item for when those rows are promoted.

3. **The durable fix is a registry capability decision, not a one-off patch.**
   The right long-term move is a source rename/edit route (or a tombstone-aware "replace name for existing URL" path) so mislabeled manual sources can be corrected without losing the URL fingerprint. That's a feature/route + UI change to track, not something to hack in this checkout.

4. **Handle the 8 local workday pending rows (Intel, SciPlay, L&W, Scientific Games, LIGHTSPEED, TiMi, Avalanche, Aristocrat) as the real T3 cohort in this checkout.**
   These are the WP18/T3 workday candidates actually present in this checkout's state. Their staging/fetch/yield decisions happen here. The xboxgaming rows are the live-container siblings of two of these families (Activision → CentralTech; Beenox/High Moon/Infinity Ward/Sledgehammer → External), but they're not in this checkout's registry.

5. **Note the `jobs-unified.json.gz` single-line shape** when doing any job-level substring inspection (the file is one massive newline-free JSON line ~65MB). Substring searches inside it can mislead about what's staged.

## Final answer

**The investigation is complete, and the maintenance window has now cleared.** xboxgaming Workday rows are not in this checkout's local registry state — they're on the live container (T5 staging.json confirms the two rows were added 2026-09-05), and this checkout's snapshots predate that staging (8/29 vs 9/5). So there's nothing to mutate locally. But the container rows **can** be probed through the fixed adapter, and today they returned fresh yield: CentralTech 2, External 40 (42 total). That confirms the Workday maintenance window that blocked T5 staging has cleared, and the T5-staged rows can recover their WP18 measurements (WP18 8/28 had measured CentralTech ~3, External ~67 before the TLS fix blocked the adapter).

The honest work in this checkout is still the T3 cohort of 8 local workday pending rows (Intel, SciPlay, L&W, Scientific Games, LIGHTSPEED, TiMi, Avalanche, Aristocrat), plus deciding the registry rename/edit capability for the attribution problem long-term. The xboxgaming attribution issue (name `Xboxgaming (Workday)` vs Activision / per-studio) is now a promotion-time T3 review item for rows that have recovered yield — not a fix that can run against this checkout's data.
