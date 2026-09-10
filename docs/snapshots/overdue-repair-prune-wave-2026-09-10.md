# Overdue repair/prune wave — 2026-09-09/10

Canonical execution record for the 23-source chronic-overdue triage
(`tmp/overdue-triage-20260909/dispositions.json`, read-only). Executors:
`tmp/overdue-wave-20260909/` (`wave.py`, `promote_high5.py`, `reprobe.py`,
`run1.log`, `run2.log`, `high5-validate.log`, backups).

## What executed

### Repair — High 5 Games (20 overdue rows recovered)

The LinkedIn `jobs/` row (999 bot-wall, wrong surface) is replaced by the
studio's real BambooHR board. Pre-execution re-probe: `careers/list` JSON
**200, totalCount 12**; `high5games.bamboohr.com/careers` 200 (98KB, bamboo
job refs); `high5games.com/about/careers/` 200. TornBanner precedent.

1. Staged `bamboohr:listing_url:https://high5games.bamboohr.com/careers`
   pending (`provider_migration_candidate`, `migrationSourceIdentity` → the
   LinkedIn row, actor `provider_migration_advisory`).
2. Validation fetch (targeted `bamboohr_sources` +
   `--include-pending-provider-migration`): family **94 fetched / 91 kept,
   0 failed** — +11 over the staged-wave baseline, exactly the High-5 board
   yield. 14 High-5 rows verified in the feed with live bamboo detail URLs.
3. Promoted via `transition_registry_to_active` +
   `normalize_manual_promotion_rows` (active/live/enabled); LinkedIn row
   retired + tombstoned (`superseded_by_provider`, bucket active). Active
   net-stable 2,159; pending 848 → 847; tombstones 148 → 149; active seed
   1,881 → 1,882 (LinkedIn was never a seed member).

### Prune — 9 triage sources (46 overdue rows) + 3 twin rows

All 9 primaries reconfirmed live-200/zero-links (or dead tenant) on a third
consecutive probe immediately before execution. Tombstoned via sanctioned
`add_tombstone`/`save_tombstones` (actor `ai_overdue_wave_20260910`):

| Row | Reason |
| --- | --- |
| `static:listing_url:https://www.notorious.gg/careers` (28 rows) | chronic_live_empty_confirmed_3x |
| `static:listing_url:https://craterstudiosgames.com/careers` (6) | chronic_live_empty_confirmed_3x |
| `static:listing_url:https://zaumstudio.com` (2) | junk_rows_live_empty |
| `static:listing_url:https://www.rainbowunicorngames.com/jobs` | chronic_live_empty_confirmed_3x |
| `static:listing_url:https://killmondaygames.com/career/` | chronic_live_empty_confirmed_3x |
| `static:listing_url:https://tranzfuser.com/careers` + `/about/careers` + `/company/careers` (3 rows, 1 overdue) | misattributed_junk_source |
| `static:listing_url:https://www.liongamelion.com/careers.html` | stale_widget_dead_tenant |
| `static:listing_url:https://careers.bitkraft.vc/companies/joyride-games` | portfolio_shell_dead_twin |
| `static:listing_url:https://www.fanatee.com/careers/marketing` | superseded_by_provider (completes wave-B set) |
| pending `static:listing_url:https://onjoyride.com` | dead_twin_404 (bucket pending) |

Counts: active 2,170 → 2,159; tombstones 136 → 149; active seed 1,890 → 1,881
(9 seed members dropped; byte-normalized to LF). Registry/seed/definition
suites: 29 passed. Metadata map verified consistent (3,007 after prune wave,
3,006 after promotion — no clobber).

## Verification (three passes)

1. **Incremental full pass** (`run1.log`): output 40,377, **failed sources 72
   → 1** — the chronic failure tail dissolved with the prunes; overdue
   **100 → 54 (Δ −46)**: pruned rows drained through the Fix A missing path.
2. **Targeted bamboo validation** (`high5-validate.log`): exit 0, 0 failed,
   94/91; High-5 rows in feed.
3. **Post-promotion regular-lane pass** (`run2.log`): output 40,389,
   **0 failed sources** (first zero-failure full pass on record), overdue
   **34 / 12 sources / Δ −20, healthy** — the LinkedIn row absent from the
   report (drained), High-5's 14 bamboo rows present.

Floor composition (34): Mundfish 10, Steer 10 (both HOLD: bamboo-empty /
detail-500 with server-rendered listings), Reflecto 3, Big Moxi 2, Exit VR 2,
plus the ≤1-row record-only and fetcher-defect tail (Astrum redirect-loop,
Brain Up `&`, Konami EJS literals) — all adjudicated HOLD/RECORD-ONLY in the
triage, pending the systemic zero-link classification fix.
