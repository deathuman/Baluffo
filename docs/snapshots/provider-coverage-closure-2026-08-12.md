# Provider Coverage Closure — 2026-08-12 (Track 2)

> - **Status:** Evidence snapshot; registry mutations applied 2026-08-12 (operator-approved). **ATS-migration staging added 2026-08-12 (Nintendo greenhouse).**
> - **Basis:** fresh soak report (generated 2026-08-12 after provider staging refresh + provider-only pipeline run with `--include-pending-provider-migration`), live probes on 2026-08-12
> - **Canonical for:** Track 2 closure evidence of `docs/plans/jobs-coverage-improvement-plan.md`
> - **Then inspect:** `docs/source-policy-runbook.md`, `_out/source-policy-soak-report.json`, `docs/snapshots/static-regression-triage-2026-08-12.md`

## ATS-migration staging (Track 1 leftover, 2026-08-12)

| Studio | Board | Outcome |
|---|---|---|
| **Nintendo** | `boards.greenhouse.io/nintendo` (greenhouse API, 51 jobs live) | **Added + approved active** `greenhouse:slug:nintendo`; bounded run verified **51 rows fetched/kept** |
| Coffee Stain | `jobs.coffeestain.com` (teamtailor custom domain, 2 raw-HTML links) | Not provider-stageable via inference (custom domain); static variant rejected; defer to browser-fallback track |
| Bulkhead / Yodo1 / Lion Game Lion / Sandsoft | teamtailor **widget-only** pages (no raw-HTML `/jobs/` links; liongamelion subdomain 404s) | Not provider rows — need browser fallback (Track 3); liongamelion provider row added then rejected (dead board) |
| Fenris (CCP) | `careers.fenriscreations.com` custom site + jobs.rss | Not provider-supported; static coverage only |
| Welevel | `welevel.jobs.personio.de/xml` | **personio row already active** — only its stale static twin remains |

Nintendo static rows (`careers.nintendo.com/job-openings/`, `careers.nintendo.com/jobs`) remain active alongside the provider row — `staticStillActiveDespiteValidatedProvider` warning pattern, acceptable; migration-link apply deferred (not a high-confidence backfill candidate).

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
