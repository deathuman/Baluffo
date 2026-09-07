# Chronic Dead-Board Sweep — Prune/Reject Decision List — 2026-09-06

> - **Status:** Evidence snapshot with a curated, programmatically-final decision list (`tmp/dead-board-sweep-20260906/final-decision-list.json`). No registry mutations performed by this sweep; execution is queued for the next maintenance pass, and per-run fetch-report snapshots now preserve each pass's evidence automatically.
> - **Use this when:** deciding prune/repair/hold outcomes for the chronic failed-source tail (sources that failed the Sep 4 full pass with zero kept rows), or when planning the next registry-maintenance batch.
> - **Canonical for:** the 2026-09-06 candidate rebuild (Sep 4 baseline + success cache), the 210 live probes, redirect-target evidence, and the final decision buckets below.
> - **Not canonical for:** the fetch-report overwrite mechanism (see `docs/plans/jobs-coverage-improvement-plan.md`), sector-signal classification (`sector-signal-contamination-2026-09-06.md`), or provider migration staging.
> - **Then inspect:** `docs/plans/jobs-coverage-improvement-plan.md` (maintenance-class notes), `docs/snapshots/jobs-dead-source-evidence-2026-04-29.md` (two-pass deletion-batch precedent), artifacts in `tmp/dead-board-sweep-20260906/` (`candidates.json`, `live-probes.json`, `decision-list.json`, `repair-target-probes.json`, `finalize_decisions.py`).
> - **Last updated:** 2026-09-06

## Method

1. **Candidate rebuild:** all sources present in the Sep 4 full-pass fetch report with a failed per-source status and `kept: 0`, minus every source successful in the post-fix full pass — **210 chronic rows**.
2. **Error classification:** segment-aware parsing of the multi-attempt error aggregates (the report strings `;`-join per-attempt errors); redirect targets extracted from `unsafe_static_redirect` messages.
3. **Live probes:** every candidate URL (and each extracted redirect target) probed with a bounded plain-HTTP client — 12 workers, 12 s timeouts, 300 KB reads.
4. **Hand curation:** the heuristic output was adjudicated row-by-row against redirect-target probes, current active-registry state (URL coverage attribution per row ID), and web corroboration where identity was ambiguous (CCP→Fenris, No Code→Screen Burn, Uncommon→Animalkind, Merge Games closure). The final buckets were then produced programmatically by `finalize_decisions.py`.

**Measurement caveats (both matter for reuse):**
- The prober sets `ssl.CERT_NONE`, so probe success **cannot clear a TLS-dead board**: CipSoft stays TLS-dead until a validating client confirms cert renewal. (Starbreeze is *not* in the chronic cohort — it recovered after the IPv6-class fixes; the earlier "two TLS-dead" framing applied to the IPv6 class, not this sweep.)
- The prober follows redirects; the pipeline correctly refuses cross-host redirects. Probe `finalUrl` therefore shows where a board *went* — decision evidence, not a fetch success.

## Final decision summary

| Final decision | Rows | Execution action |
|---|---:|---|
| `prune_dead_404` | 54 | Retire row (careers path gone, HTTP 404) |
| `prune_absorbed_rebrand` | 12 | Retire row (genuine rebrand, but target board already covered by another active row) |
| `prune_lapsed_domain` | 7 | Retire row (domain lapsed to an unrelated site/employer) |
| `repair_rebrand` | 45 | 24 repoint URL · 19 retire (new-board row already exists) · 2 already executed today |
| `hold_recheck` | 58 | None — alive with job signals; next full pass heals or the per-run report preserves evidence |
| `hold_live_empty` | 14 | None — live, healthy, no visible openings |
| `review` | 0 | ~~None~~ — all 8 adjudicated 2026-09-07 (see Review-8 section below) |
| `hold_rebrand_no_jobs` | 5 | None today — rebrand confirmed, target board has no postings |
| `hold_botwall` | 3 | None — alive but blocks plain clients (429/403) |
| `hold_timeout` | 3 | None — timeouts; revisit after next full pass |
| `hold_tls_dead` | 1 | None — CipSoft, self-heals at cert renewal |
| **Total** | **210** | |

## Prune cohorts

### `prune_dead_404` (54)

A-Game Studios, AHEARTFULOFGAMES, ASBO Interactive, Adhoc Studio, Afterverse, Athena Worlds, Atomontage, Baryonix (actually a Twitter/X careers URL — junk-employer class), Bigpoint, Blackbird Interactive, Bolverk Games, Bonus Level Entertainment, Brightline Interactive, Burning Planet Digital, Digital Hero Games, Donuts, Emerald City Games, Ever Curious Entertainment, Everywhen Games, Far Far Games, Fatshark Games (`fatsharkgames.com/career` 404s — coverage is intact via the separate `jobs.fatsharkgames.com` TeamTailor row), Flipstar Studios, Free Range Games, Games Farm, HiDef, Koolhaus Games, KraiSoft Entertainment, Lucky VR (Breezy board gone), Lullabyte Games (an X.com careers URL — same junk class), Maschinen-Mensch, Megu Mobile Gaming, Might & Delight, Mighty Bear, Nine Dots Studio, Obelisk Studio, Penrose, Petricore Games, Raikiri, Simutronics Corp (4 `play.net` path variants, all 404), Snowbright Studio, Spoilz, Stark Learning, Third Pie Studios, TiMi Bellevue (`timistudios.com` root 404; single row, no sibling), Tricore, Unit9, Usiku Games, Zepetto, alchemie, exA-Arcadia.

### `prune_absorbed_rebrand` (12)

| Studio | Redirects to | Covered by existing row |
|---|---|---|
| Amazon Game Publishing Service | amazongamestudios.com/en-us/careers | Amazon Games Studios row (same target URL) |
| Everplay Group | apply.workable.com/team-17-digital | Team17 workable row |
| Everywear Games | metacoregames.com | Metacore ×2 |
| ILMxLAB | ilm.com/immersive | (ILM family; Lucasfilm Games row active) |
| Neon Play | iscoolentertainment.com | isCool Entertainment |
| No Code Studios | screenburn.com/jobs | Screen Burn (No Code rebranded to Screen Burn, Jul 2025) |
| Relentless Studios | amazongamestudios.com/en-us/careers | Amazon Games Studios row |
| RisingWings | krafton.com/studios/risingwings | Six KRAFTON careers rows |
| SD Studio | sonysandiegostudio.games/careers | Sony Interactive Entertainment San Diego Studio row |
| Square Enix London Studios | eidosmontreal.com/careers | Eidos Montreal |
| Super Lucky Casino | stillfront.com | 8 Stillfront-family rows |
| Volley | weekend.com/careers | Volley's own weekend.com row (same-studio duplicate) |

### `prune_lapsed_domain` (7)

| Studio | Sep 4 URL | Target (live) | Evidence |
|---|---|---|---|
| Batterystaple Games | batterystaplegames.com/jobs-at-batterystaple | store.steampowered.com (Clowntown app page) | Domain lapsed to its Steam page |
| Clay Token Game Studio | claytoken.net/work-with-us | store.steampowered.com (Steel Swarm page) | Same lapsed pattern |
| Crowdpark | crowdpark.com/careers | bw-gaming.net slots page | Casino-site takeover |
| Immersed Games | immersedgames.com/careers | tytoonline.com/about | Founder's unrelated education venture |
| Infinite Reality | theinfinitereality.com/careers | napster.com/careers | Napster rebrand — unrelated employer |
| Merge Games | mergegames.com/careers | silverliningint.com/merge-games-info | Merge Games closed 2024; successors founded Silver Lining Interactive — a *different* employer (web-corroborated) |
| Talespin | talespin.company/careers | cornerstoneondemand.com/careers | Pivot; Cornerstone row already active |

## Repair cohort (45)

### 24 repoint targets (real URL fixes; target not covered today)

| Studio | Sep 4 URL → new board (live-verified) |
|---|---|
| CCP London | ccpgames.com/careers → **fenris.com/careers** (CCP Games rebranded to Fenris Creations, May 2026 — web-corroborated) |
| HandyGames | handy-games.com/en/jobs → thqnordicmobile.com/en/jobs (13 job signals; *no* active THQ Nordic Mobile row today) |
| Massive Miniteam ×2 | massiveminiteam.com/(en/)jobs → thqnordicmobile.com/en/jobs (same target as HandyGames — dedupe to ONE row at execution) |
| Highwire Games | highwiregames.com/careers → sixdays.com/careers (Six Days in Fallujah is Highwire's own game) |
| Synapse Games | synapsegames.com/en/pages/jobs → kongregate.com/en/pages/jobs |
| Uncommon Games | uncommon.gg/careers → animalkind.gg/careers (Animalkind is Uncommon's own game) |
| Wright Flyer Studios | wrightflyer.net/recruit → wfs.games/recruit (49 job signals) |
| Dark Slope Studios | darkslopestudios.com/careers → darkslope.com/careers |
| Discord Inc. | discordapp.com/careers → discord.com/careers |
| FUNLabs | funlabs.com/jobs → fun-labs.wixsite.com/website |
| Future Mark | futuremark.com/careers → benchmarks.ul.com/careers |
| Gameberry Labs | gameberrylabs.com/jobs → gameberry.keka.com/careers |
| Gun Media Holdings | fearthegun.com/careers → guninteractive.com |
| Kano Apps | kanoapps.com/careers → kano.ca/careers |
| KeokeN Interactive | keoken.nl/jobs → keokeninteractive.com/jobs |
| Letibus Design | blazgracar.com/about/careers → letibus.com/about/careers |
| Pixel Federation | portal.pixelfederation.com/en/career → career.pixelfederation.com |
| Playful | playfulcorp.com/careers → playfulstudios.com/careers |
| RTL Enterprises | rtl-interactive.de/de/career → company.rtl.com business-units (parent-org page — identity check at execution) |
| Rock Pocket | rockpocketgames.com/jobs → rockpocket.games |
| Streamline Games | streamline-games.com/en/careers → streamline-studios.com/en/careers |
| Wanted 5 Games | wanted5games.com/jobs → wantedgamestudio.com/jobs |
| Wildcard Games | wildcardstudios.com → wildcardmobile.com |

### 19 retire-because-covered

The studio's *new* board already exists as an active registry row — the stale row is retired rather than repointed: 2K Madrid, 2K Vegas, Bungie Studios (greenhouse `bungie` + careers.bungie.com rows), Bright Future, Cloud Chamber, Gearbox Software, Hangar 13, Hinterland Studio, Klei Entertainment, NAMCO BANDAI Games America, Red Storm Entertainment (Ubisoft careers family), SOFTGAMES (playsoftgames.com row), Say Games, Sea Monster, Sports Interactive, Supercell London (supercell.com/en/careers row), Techland (techland.net/job-offers row), Travian Publishing, Unity Technologies. (Full per-row attribution in `final-decision-list.json` → `targetCoveredByRows`.)

### 2 already executed

Just Add Water and JoyBits — repaired earlier today; their Sep 4 URLs no longer exist in the registry.

## Hold buckets (no action)

- `hold_recheck` (58): boards alive with job signals — includes every already-repaired Sep 4 row (Jaw, JoyBits, BKOM, plexonic, Take-Two, Zwift) whose old URLs have left the registry, and the botwall-victim pair from earlier sweeps.
- `hold_live_empty` (14), `hold_botwall` (3: Akupara, Devolver Digital, +1), `hold_timeout` (3: Future Club, MadSword, SUNFOX), `hold_rebrand_no_jobs` (5: Impact Reality XR, Proton Studio, Rogue Duck — Steam work-with pages, Rookery Interactive, BKOM's superseded Sep 4 URL), `review` (8: Astrum 307, Exit VR 500, Lanterns 521, Mundfish 500, Pixel Wizards 307, Pixowl 521, The 4 Winds 402, VSTEP 500), `hold_tls_dead` (1: CipSoft — CERT_NONE probes cannot clear it; treat as dead until a validating probe passes).

## Review-8 adjudication (executed 2026-09-07)

All 8 rows re-probed with rendered Playwright (`review8-probes.json` in the sweep artifact dir; follows redirects + JS):

| Studio | Probe outcome | Final decision |
|---|---|---|
| Astrum Entertainment | 200, 57 job links — behind a `bp_chl` cookie-challenge bot wall; plain fetches see the 307 handshake loop | `hold_botwall` (no action) |
| Exit VR | WordPress fatal 500 site-level; board content absent | `hold_transient` (no action) |
| Lanterns Studio | Cloudflare 521, origin down — persistent with Sep 4 | `prune_dead` (executed) |
| Mundfish | failing row is a **duplicate**: the registry's Sheet row `mundfish.com/en/careers` renders 200 with 74 job links | `prune_absorbed_duplicate` (executed) |
| Pixel Wizards | redirect loop (`ERR_TOO_MANY_REDIRECTS`) — persistent with Sep 4 | `prune_dead` (executed) |
| Pixowl | Cloudflare 521, origin down — persistent with Sep 4 | `prune_dead` (executed) |
| The 4 Winds | 402 payment-required (hosting lapsed) — persistent with Sep 4 | `prune_dead` (executed) |
| VSTEP | old path serves the full careers page via redirect to `our-company/careers-at-vstep/`; the redirect chain is what breaks the fetch client | repoint executed (seed+runtime), then **reclassified `hold_soft_error_status`**: the new path serves full content but with a broken HTTP 500 status (WordPress misconfiguration) — recovers when the origin fix lands |

Post-adjudication artifact tally: **78 prune / 45 repair / 87 hold** (210 total); review bucket empty.

## Registry-state corrections discovered during curation

- Dambuster Studios' decision row was stale: the registry already points at `careers.dsdambuster.com/vacancies` — no action needed.
- Baryonix and Lullabyte Games rows are Twitter/X careers URLs — a junk-employer class that should never have been sourced; pruned with the dead cohort.
- BKOM's Sep 4 URL (`bkomstudios.com`) no longer exists in the registry — repaired earlier today to `jobs.bkom.com/jobs/Careers`.
- Fatshark's 404 row is safely prunable: the `jobs.fatsharkgames.com` TeamTailor row provides coverage.
- Neon Play and Massive Miniteam still carry their lapsed Sep 4 URLs — the Jaw/JoyBits/BKOM repair batch descoped them; they are in this list's repair cohort.

## Execution policy

1. **Prunes** (73 rows) execute as registry row retirement with tombstones (the 2026-04-29 batch precedent), backed by per-run fetch-report snapshots.
2. **Repairs** execute through the sanctioned seed+runtime path, one studio at a time, with read-back verification and a targeted `--only-sources` pass; the 3-way THQ Nordic Mobile collision (HandyGames + Massive Miniteam ×2 → one target) must dedupe to a single row.
3. **Holds** stay untouched; re-evaluate after the next full pass using the per-run reports, not by re-deriving failure sets from side artifacts.
4. ~~The 8 `review` rows get one manual browser check each before any decision; none show evidence of death.~~ **Executed 2026-09-07:** rendered probes adjudicated all 8 — 5 pruned (tombstones, seed-aware), 1 repointed (VSTEP, seed+runtime), 2 held (Astrum botwall, Exit VR transient). Post-adjudication tallies: 78 prune / 46 repair / 86 hold.

## Batch-3 execution (2026-09-07): Wave D follow-up cohorts

The Wave D redirect cohort (27) and notFound cohort (19) were executed as **batch 3**, with live-probe pre-flight that corrected both cohorts' labels:

**Redirect cohort → 18 repoints + 9 retirements.** Pre-flight probes confirmed 23 extracted targets live with job signals; four edge cases reclassified: Camel 101 (no careers presence anywhere on site), Proton Studio and Rogue Duck (targets are Steam store pages with no listings), Impact Reality XR (rendered probe: VR marketing agency page, no careers content). Four Ubisoft geo rows retired as redundant against the group's active rows (SmartRecruiters + Manual Website; their extracted targets are redirector gateways). Wizards of the Coast retired as duplicate — `company.wizards.com/en/careers` was already active. Roblox's repoint target `corp.roblox.com/careers` 301s to `careers.roblox.com`, which already has an active yielding row (`careers.roblox.com/jobs`) **and** tarpits non-browser User-Agents (httpx ReadTimeout vs instant browser-UA 200 — a UA-based bot wall); the corp row retired as duplicate-covered rather than chasing the bot wall. Vivid Games' static row retired post-repoint: the duplicate-URL guard caught that `teamtailor:listing_url:https://jobs.vividgames.com` (provider adapter, 108 kept in the full pass) already covers the board — provider preferred over static.

**notFound cohort → 0 prunes, 15 holds.** The "fresh-404 prune candidates" label was wrong: all 16 listing pages probed 200 with job signals. The failures are **detail-page 404s** from link-extraction artifacts (mailto parsed as a path, bare-`&` URLs, a `[thrive_page_number]` template placeholder, an image fetched as a detail, relative-join mistakes) plus stale postings — not dead boards. Three rows (Fanatee, Lion Game Lion, inXile) carry ATS signatures (lever/teamtailor/bamboohr) and are adapter-reclassification candidates, held for the provider-migration path.

**Batch-3 tallies:** 18 repoints (dual-path, 75 jobs kept in the targeted pass: ARTE 24, Vivid→superseded by provider, About Fun 12, Playkot 4, Mindstorm 4, Tapnation 4, Certain Affinity 4, Guli 4, + singles), 11 retirements with tombstones (9 + Roblox dup + Vivid dup; tombstones 100 → 111), 0 notFound prunes. Registry 2,186 → 2,184 runtime, seed → 1,900.

**Mechanics lesson recorded:** the runtime registry `.json.gz` stores lean core rows only; full definitions live in `source-registry-metadata.json.gz` keyed by source id and merged at load. Direct `.gz` writes that rename ids leave the new ids definition-less (pipeline joins zero pages → fetched=0). All registry mutations must go through `load_json_array` + `save_json_atomic`, which re-splits and rebuilds the metadata map — this is now the standing executor pattern (Wave B's sanctioned path was correct; batch 3's first executor wasn't, and was repaired in place via `repair_batch3_metadata.py`).
