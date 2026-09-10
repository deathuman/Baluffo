# Bamboo/breezy/workday 9-row promotion wave — 2026-09-10

Canonical execution record for the 9 remaining provider-migration candidates
(the widget-audit mis-registered boards). Executors and evidence:
`tmp/bamboo-wave-20260910/` (`promote_nine.py`, `validate.log`,
`tenant_probe.py`, `tenant-probe.json`, `run1.log`, backups).

## The candidates

All 9 were staged `provider_migration_candidate` rows whose
`migrationSourceIdentity` static twins sat on the **same provider URL** —
boards registered as static rows that a static scraper cannot parse (widget
or SPA). Buckets at start: 1 static twin in active (BeamDog), 8 in pending
(5 `sheet_directory`, 3 `repeated_zero_jobs`).

| Promoted row | Static twin retired | Evidence at adjudication |
| --- | --- | --- |
| `bamboohr:listing_url:https://beamdog.bamboohr.com/careers` | active static same URL | careers/list 200, **live-empty** |
| `bamboohr:listing_url:https://dinopoloclub.bamboohr.com/careers` | pending static (sheet_directory) | 200, live-empty; 1 feed row from window |
| `bamboohr:listing_url:https://eleventhhourgames.bamboohr.com/careers` | pending static (repeated_zero_jobs) | 200, live-empty |
| `bamboohr:listing_url:https://expressiongames.bamboohr.com/careers` | pending static (repeated_zero_jobs) | 200, live-empty |
| `bamboohr:listing_url:https://reforgedstudios.bamboohr.com/careers` | pending static (sheet_directory) | 200, live-empty |
| `bamboohr:listing_url:https://wolcenstudio.bamboohr.com/careers` | pending static (sheet_directory) | 200, **1 live opening**; 1 feed row |
| `breezy:board_url:https://illfonic.breezy.hr/` | pending static (sheet_directory) | live board, 1 real opening; static's "5 kept" was stale 03-30 |
| `workday:listing_url:https://aristocrat.wd3.myworkdayjobs.com/aristocratexternalcareerssite` | pending static (repeated_zero_jobs) | **136 feed rows** vs static 0 |
| `workday:listing_url:https://tencent.wd1.myworkdayjobs.com/timi_careers` | pending static (sheet_directory) | **197 feed rows** vs static 0 |

## Execution (sanctioned lanes end-to-end)

Validation pass (`validate.log`, `--only-sources
bamboohr_sources,breezy_sources,workday_sources --include-pending-provider-migration`):
exit 0, 0 failed; families bamboo 94/91, breezy 25/24, workday 103/103.
Live-vs-dead tenant probe: all 6 bamboo tenants 200 (wolcen count=1).
Promotion (`promote_nine.py`): `transition_registry_to_active` +
`normalize_manual_promotion_rows` for all 9; static twins tombstoned via
`add_tombstone`/`save_tombstones` (1 active bucket, 8 pending bucket;
`superseded_by_provider`), actor `ai_bamboo_wave_20260910`.

Counts: active 2,159 → 2,167; pending 847 → 830; tombstones 149 → 158;
active seed 1,882 → 1,891 (+9, LF-normalized). Metadata map 2,997 entries
(consistent: −9 statics, net registry −1). Read-back: all 9 promoted
active/live/enabled, all 9 statics absent AND tombstone-resolved, zero
pending leaks. Registry/seed/definition suites: 29 passed.

## Verification (regular lane, no pending flag)

Post-promotion full pass (`run1.log`): output **40,428**, **0 failed
sources** — second consecutive zero-failure pass; verdict **healthy, overdue
34 / 12 sources / Δ 0**. Feed attribution per promoted tenant:
aristocrat 136, tencent 197, High-5 12, tornbanner 7, wolcen 1,
dinopoloclub 1, illfonic 1; live-empty bamboo tenants correctly at 0.

## Notes

- The five live-empty bamboo tenants were still promoted deliberately: the
  structured adapter reads the tenant directly, so a future opening appears
  in the next cadence pass with zero extra latency — the exact property the
  static scraper could never offer on these widget boards.
- The ILLFONIC adjudication records a stale-counter lesson: its static row
  showed "kept 5" from 2026-03-30 while the live board has 1 opening; the
  adapter matched live truth.
