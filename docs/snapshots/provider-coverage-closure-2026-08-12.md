# Provider Coverage Closure — 2026-08-12 (Track 2)

> - **Status:** Evidence snapshot; registry mutations applied 2026-08-12 (operator-approved)
> - **Basis:** fresh soak report (generated 2026-08-12 after provider staging refresh + provider-only pipeline run with `--include-pending-provider-migration`), live probes on 2026-08-12
> - **Canonical for:** Track 2 closure evidence of `docs/plans/jobs-coverage-improvement-plan.md`
> - **Then inspect:** `docs/source-policy-runbook.md`, `_out/source-policy-soak-report.json`, `docs/snapshots/static-regression-triage-2026-08-12.md`

## Mutations applied (via local admin bridge, 127.0.0.1:8877)

### Migration links applied (2, high-confidence 0.95)

| Provider row | Static target | Evidence |
|---|---|---|
| `lever:account:xsolla` (Xsolla) | `static:listing_url:https://xsolla.com/careers` | redundant_static_rule_exact_match, registry_static_disambiguation |
| `smartrecruiters:company_id:cdprojektred` (CD PROJEKT RED) | `static:listing_url:https://cdprojektred.com/en/jobs` | active_static_canonical_url_disambiguation, redundant_static_rule_exact_match |

`alreadyLinkedCount` 3 → 5; `reviewCandidates` 0 (no high/medium-confidence candidates remain).

### Provider rows approved active (9, repeated validation evidence)

BambooHR (consecutive successes 5, kept > 0): `activategames` (45), `blazinggriffin` (5), `catface` (7), `flyingbark` (25), `relicentertainment` (2), `streamlinestudios` (16).
Breezy (consec 5): `fugo-games` (13), `flowplay-llc` (1), `warhorsestudios` (9).

Active 2268 → 2277; pending 894 → 885. Verified present in `source-registry-active.json.gz` with `registryState: active`.

### Dead provider rows rejected (3)

| Row | Evidence |
|---|---|
| `bamboohr:listing_url:https://lemonskystudios.bamboohr.com/careers` (provider) | careers URL redirects to generic BambooHR marketing page — no board |
| `static:listing_url:https://lemonskystudios.bamboohr.com/careers` (static) | same dead board |
| `oracle_hcm:site_path:/hcmui/candidateexperience/en/sites/cx_1/jobs` (Glass Egg) | `glass-egg.oraclecloud.com` DNS NXDOMAIN — defunct |

Rejected 0 → 3.

## Triage findings (9 fetched-but-not-validated rows)

| Row | Live probe | Disposition |
|---|---|---|
| `bamboohr:beamdog` | `/careers/list` → `[]` | genuinely empty — correct behavior |
| `bamboohr:eleventhhourgames` | `[]` | empty |
| `bamboohr:expressiongames` | `[]` | empty |
| `bamboohr:reforgedstudios` | list → 1 job | recovery on refresh (validated consec 1) |
| `bamboohr:wolcenstudio` | list → 1 job | recovery on refresh |
| `bamboohr:lemonskystudios` | careers → BambooHR marketing page | dead — rejected |
| `breezy:illfonic` | `/json` → `[]` | empty |
| `oracle_hcm:glass-egg` | DNS dead | dead — rejected |
| workday (4 rows: aristocrat/intel/sciplay/light&wonder) | SSL CERTIFICATE_VERIFY_FAILED (expired) | upstream transient — hold; re-validate after cert renewal |

Note: Lemon Sky source-state still shows stale `succ 1 / latestKept 20` counters from before the board died; the rows are rejected and will not refetch.

## Validated provider count

20 → **15** after refresh (workday SSL + empty boards reclassified). Next soak action: `resolve_link_ambiguity` (Ubisoft `smartrecruiters:company_id:ubisoft2` vs 6 static candidates, confidence 0.65 — below the 0.75 apply threshold, so the guard correctly requires human judgment).

## Verification commands

```powershell
python scripts/provider_migration_staging_refresh.py --data-dir data --out-dir _out --apply-pending
python scripts/source_policy_soak_report.py --data-dir data --out-dir _out
python -m src.jobs.pipeline --only-sources bamboohr_sources,breezy_sources,workday_sources,oracle_hcm_sources --include-pending-provider-migration --no-seed-existing-output --no-preserve-previous-on-empty --force-refresh-all --ignore-circuit-breaker --timeout 15 --quiet
```

Runtime registry exports are gitignored; the only repo change is the documented evidence.
