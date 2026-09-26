# Registry Hygiene Follow-Up Plan

> - **Status:** Code work complete (P0, P1, P5, P6). Data reconciliation applied: the 13 lost rows, P3's 31 duplicate groups, P4's 102-row seed prune, the stranded-family repair (37 promotions), the non-page `pages` cleanup, and the RTL non-game rejection are all in the store and the seed. The preflight is clean — 0 uncovered collisions, 0 stale baseline, 0 boards emptied. Remaining open items are the residue of the probe under-count, the `sector` provenance signal in `game_detection.py` (recorded, needs product sign-off), and Optional D. Continuity handoff in Basic Memory (`baluffo/registry-hygiene-p3-p4-closeout-2026-09-26`).
> - **Use this when:** recording or applying a registry repair decision, reconciling live-registry drift from the committed seed, or extending the advisory/guard surfaces
> - **Canonical for:** the human-approval gate's boundaries, the live-vs-seed authority question, and the ordered follow-up work
> - **Not canonical for:** the advisory report shape (see `docs/DATA_CONTRACT.md`) or registry transition mechanics (see `src/source_registry_state.py`)
> - **Last updated:** 2026-09-26

## What landed

| Commit | Surface |
|---|---|
| `2e8fc780` | Advisory `registryHygieneAudit` report (asset pages, duplicate candidates, host drift, unreachable). |
| `180c3036` | Two hard commit-time guards: duplicate registry ids, malformed page refs. |
| `9c9e5cd1` | `repair_candidate` — dead-domain signal under a repeated-evidence rule. |
| `eec27403` | `data/registry-repair-review.json` — the human-approval gate. |
| `12dbe58a` | `POST /registry/repair-review-action` — the gate's writer (P0+P1), plus the two `typecheck:py` fixes (P6). |
| `fec4e443` | `tools/repo_health/source_registry_preflight.py` — the live-registry surface (P5). |
| `6f564a6b` | `--worksheet` — the per-row adjudication inventory. |
| `62724469` | Stranded-family guard: safe auto-demote can no longer empty a board. |
| `4502437d` | P3 (31 duplicate groups demoted) and P4 (102-row seed prune). |
| `9a69e33a` | Board-coverage preflight finding, 8 tests, and four hard stops for the trap classes that caused four wrong answers in this programme. |
| this change | Phenom pagination decode; probe R1 singular/plural siblings; preflight id-case + split consistency; 227 non-page `pages` entries stripped; `hiddenFromDefault` cleared on promotion; conflict-demote provenance on inert fields plus `winnerId` in the run response. |
| `57eef38e` | Probe R3: `vacancies?` could not match `/vacancy/`, so singular-path boards counted zero. |
| pending | Probe R4: keyword-headed posting sections, 117 roles on 13 boards with 0 noise boards falsely gaining. |

## The authority question: answered

**The live registry is authoritative; the committed seed is stale.** This was the fork gating P3/P4
and it is now settled by evidence rather than assumption:

- The changelog records the invariant directly. Sanctioned mutations perform a **dual-path seed
  swap** (`save_registry_state_atomic`), e.g. "runtime 2,167 → 2,168, seed 1,892 → 1,893" and "active
  seed 1,890 → 1,882 with the High-5 promotion added". The seed is meant to *track* live, not to lag
  it.
- 426 of the 517 live-only rows are `static:` — the class the seed is dominated by and is supposed to
  carry.
- 21 of the 31 uncovered duplicate groups are **partly seeded**: one twin in the seed, its twin live
  only. That is the signature of a row added live without the seed swap, not of a design where the
  seed holds a subset.
- Zero duplicate groups exist only in the seed.

So drift is a real defect and the seed is behind. What that does *not* settle is how to close it, and
that remains a policy decision — see P2/P3/P4 below.

## Delivered: the adjudication inventory

`python tools/repo_health/source_registry_preflight.py --worksheet <path>` writes 717 individually
adjudicable items on the current tree:

| Kind | Count | Why it is a human decision |
|---|---|---|
| `live_only_row` | 517 | Should the seed adopt it (a fresh install lacks it), or is it live-only pollution? |
| `seed_only_row` | 165 | Prune (a seed restore would resurrect a retired source) or restore to live? |
| `uncovered_duplicate_url` | 31 | Baseline the twin or retire one? Most are `www`/apex pairs of one board. |
| `stale_baseline_entry` | 3 | Prune the entry or restore the twin? |
| `definitionless_static_row` | 1 | The Miniclip row (P2). |

Every item carries its evidence, suggested dispositions, and an explicit
`disposition: "undecided"`. The tool does not decide any of them.

## Also fixed en route

`test_batch_scoped_recovery_cache_preserves_fixture_outcomes` failed intermittently in the
source-discovery lane — on `HEAD` as well as on this branch, roughly two runs in three, and it
aborted `npm run test:py:linux` twice. Root cause found by diffing the two results field by field
rather than reading the truncated pytest output: the parity helper stripped timestamp fields but not
`probeDurationMs`, a wall-clock measurement that jitters by a millisecond or two between otherwise
identical runs, and it compared the candidate lists positionally while discovery emits them in
nondeterministic probe-completion order. Both are test defects, not product defects; the stripper now
removes `*DurationMs`/`*ElapsedMs`/`*Ms` alongside the timestamps, sorts lists canonically, and still
reports a genuine content change. Ten consecutive runs green afterwards.

## Completed

### P0 — Push blocker: two dead functions in `src/` — **done**

`vulture --min-confidence=60 src whitelist.py` returned exactly two findings repo-wide, both from
`eec27403`. Both are now resolved: `apply_registry_repair_review_action` is live via the new route,
and `find_registry_repair_review_row` was **deleted** rather than whitelisted, because
`registry_repair_review_status` already owns the fingerprint match and returns a superset. Vulture is
back to **0** findings.

Correction on my own earlier reporting: the `dead-code` guardrail group in
`tools/repo_health/repo_guardrails.py` covers dead *branches* and leaked loop bindings. It does not
run vulture and says nothing about unused functions. I previously treated "repo guardrails: dead-code
passed" as if it covered this class; it does not.

### P1 — Admin write route — **done**

`POST /registry/repair-review-action`, following the dedup pair review precedent. It records and never
applies: there is no branch that writes, demotes, deletes, or repoints a row, the response carries
`"applied": false`, and a test asserts the active registry file is byte-identical across a recorded
`retire` approval. `REGISTRY_REPAIR_REVIEW_PATH` is threaded through `api.py`, `bootstrap.py`, the
entrypoint api/runtime rebind, and `admin_bridge` — required rather than incidental, because the
`api.py` class default is a relative path and the packaged app would otherwise read a stray file from
the CWD.

### P5 — Live-registry coverage gap — **done**

`tools/repo_health/source_registry_preflight.py` re-runs the existing, already-tested predicates
against the live rows and adds a live-versus-seed drift comparison, so the two sides cannot disagree
about what counts as a defect. Advisory by default with `--strict` to fail, because a gate that fails
on every invocation until 31 collisions are adjudicated gets ignored.

Two runtime behaviors it surfaces that nothing else did:

- A **missing** live registry resolves to the committed seed (correct for a fresh install).
- A **corrupt** live registry *also* silently resolves to the seed. Right at runtime, wrong to hide:
  the operator's live rows vanish while every count still looks plausible.

### P6 — `typecheck:py` — **done**

Green for the first time on this branch: 1381 source files, 0 errors. Both were pre-existing on
`origin/main`. `npm run lint:repo-guardrails` 15/15 and `vulture` 0.

## Resolved by operator decision

### P2 — The Miniclip row — **repaired, and the original reading was wrong**

`static:listing_url:https://www.miniclip.com/careers/vacancies` was active with no `listing_url` and no
`pages`, so it fetched nothing while reporting ok. The first reading called it a superseded
intermediate left unretired. That was wrong. The tombstone for `corporate.miniclip.com/careers`
(`deletedBy repair_batch4_20260907`) records that batch 4 **promoted** this row from pending as the
live-verified replacement, and the row's own `lastPromotedAt`/`stateChangedBy` carry the same batch-4
stamp. So the promotion is what corrupted it: the store copy had 48 of 58 fields stripped, leaving a
row with no definition. The seed held the complete definition and was used as the graft source.
Repaired with the backup-and-verify procedure; the preflight now reports the seed/store split
divergence rather than a definitionless row.

The lesson generalises: the write path has a documented corruption mode (a raw-gzip or
non-`load_json_array` read makes the sanctioned save rebuild the metadata map from lean rows —
"metadata 3,038 → 2 entries"). Every store write since must be read back, not assumed.

### The 13 `seed_row_lost_from_store` rows — **resolved, 1 deliberately held**

Thirteen seed rows were absent from the store with no tombstone. Reported to the operator, who asked
for the live boards to be checked rather than the seed metadata to be trusted. Probing the boards
through the same endpoints the pipeline's own adapters use — validated against two working controls
(`activategames` 38 jobs, `streamlinestudios` 16) — changed the answer completely:

| Group | Count | Probe | Disposition |
|---|---:|---|---|
| Six BambooHR tenants + IllFonic (Breezy) | 7 | HTTP 200, structured zero | Restored |
| TiMi Studio Group (Workday) | 1 | HTTP 200 **via certifi** | Restored |
| Lucky VR (Breezy) | 1 | **HTTP 404 at the board root** (seed `jobsFound: 3`) | Retired |
| Vivid Games (Teamtailor) | 1 | board root HTTP 200, ~93 KB | Restored |
| RTL Enterprises (Phenom) | 1 | 200, but renders client-side | Held |
| inXile (static) | 1 | already tombstoned 2026-09-07 | Seed pruned only |
| `greenhouse:slug:examplestudio` | 1 | literal placeholder | Seed pruned only |

**Restored (9).** Their absence was a *bug*, not a decision — the same store write that stripped
Miniclip dropped them. The 2026-09-09 bamboo/breezy/workday wave promoted and deliberately kept these
rows even when live-empty, and an empty board that answers with a clean structured zero is a
legitimate row under the existing Steer precedent. Seed rows were grafted verbatim so the promotion
metadata stays the truthful history. Active 2,246 → 2,255, metadata 3,086 → 3,095 (+9 each), zero
unrelated rows changed. Vivid Games needed its store row grafted too, after the probe correction
below — its seed row alone would have left it flagged as still lost.

**Retired (1).** `/registry/delete` only tombstones rows it finds in a bucket, and Lucky VR is not in
the store, so it would have been a no-op. The tombstone was written from the seed row — the only
surviving definition — with `reason: dead_board_http_404`. The tombstone is what stops a later
discovery pass resurrecting the board; the seed prune stops a fresh install shipping a dead row.

### Three probe traps, each of which nearly caused a wrong retirement

Every one of these produced a confident wrong answer first. This is the most important lesson from
the pass:

1. **Bare `urllib` vs certifi.** TiMi returned `CERTIFICATE_VERIFY_FAILED`, which reads exactly like
   a dead board. It is alive, and the documented certifi-anchored fix for `*.myworkdayjobs.com` makes
   it return 200. It was historically the group's largest contributor (197 feed rows), so retiring it
   on that one probe would have been the most expensive available error.
2. **The wrong endpoint 404s on healthy rows.** BambooHR's `/api/listed_jobs` 404s on *known-good*
   tenants; the adapter's real endpoint is `/careers/list`. The Teamtailor `embed` endpoint 404s on a
   live board, and `teamtailor_runner` never uses it — it parses listing links out of the board HTML.
   **Vivid Games was retired on exactly this error** and had to be walked back: its board root
   answers HTTP 200. The seed row was re-inserted at its original index and its tombstone removed,
   leaving only the three genuinely-stale prunes. It is listed as "Restored" above.
3. **An empty board is not a failure.** A 200 with a clean structured zero is a legitimate row under
   the Steer precedent, not grounds for retirement.

The rule: **probe the board root as well as the adapter endpoint, and always run a known-good
control before believing a 404.** The control is what exposed trap 2 immediately.

`seed_row_lost_from_store` is now **1** (RTL, held pending a rendered probe).

### Two golden-seed tests had to be re-pinned

`tests/test_source_registry_p1_operational_noise.py` uses the *committed* seed as its fixture, so
retiring Lucky VR broke two tests that named it. Both were re-pinned to rows that are live today
(`greenhouse:slug:bungie` for the static-residual pair, and the four remaining `antiBotBrowserRetry`
rows), which is the correct fix: the tests were asserting seed history, not behaviour. Standing
fragility — any future legitimate retirement of a pinned row will break them again.

## Awaiting human disposition

Both remaining items are registry-policy calls about rows that are live and being fetched today, so
neither is mechanical and neither is made unilaterally here.

### P3 — The 31 uncovered duplicate groups

Almost all are `www`/apex twins of one board — exactly what
`data/defaults/source-registry-known-url-collisions.json` exists for. But they are heterogeneous (some
are the same path, some are genuinely different windows on one host), so a blanket baseline would
grandfather real duplication. Decide per group: baseline the twin, or retire one.

Retiring is not free: these rows are being fetched today, so each decision trades duplicate output
against losing a board.

**Resolved 2026-09-26: all 31 are the same board, and all 31 were reconciled by demotion.**

Live evidence, with three working controls and certifi-anchored TLS:

- **31/31 groups resolve to one board.** Compared on the registry's *own*
  `canonicalize_careers_url` applied to the post-redirect final URL, not on raw URL strings. Raw
  comparison invents differences that are not there — `http` vs `https`, an explicit `:443`, a
  trailing slash and `www` vs apex are exactly what the canonicalizer collapses for the twin rule.
- A first pass using a **content hash** called the scopely pair "different boards". Wrong: the page
  embeds dynamic content. Both URLs redirect to the identical `careers.scopely.com/us/en` with the
  same declared canonical. The fingerprint was the unreliable signal, not the boards.
- The last 3 apparent differences (krafton ×2, rollicgames) were confirmed to be self-referential
  links: each page links to its own alternate URL form and locale variants.

Survivor authority, in order: **the row the committed seed already keeps** (19 groups — the shipped
default is the deliberate shape, which is why the seed has zero collisions), else the row the other
one redirects to (5), else the row matching the page's declared canonical (3), else the apex
spelling (3). Every demoted row moves to pending, hidden, carrying `duplicateOfSourceId` back to an
active survivor — not tombstoned, so the board stays covered and the change is reversible.

Result: active 2,255 → 2,224, pending 840 → 871, `uncovered collisions 31 → 0`,
`seed_row_demoted_in_store` unchanged at 152 (confirming no loser was a seed row).

**Why not `demote_duplicate_active_variants` and why not the baseline file:**

- The sanctioned bulk function sweeps the whole registry — **193 conflict cards** against 62 rows
  under review — and its family grouping does not match the verified collision boundaries. For
  `infinityward` and `mindstormstudios` it marks *both* twins as losers, because the family winner
  is a third row outside the reviewed set; running it would have stripped those studios of their
  board entirely. The per-row primitive it uses internally (`_demote_duplicate_variant`) was applied
  to exactly the 31 verified losers instead.
- Baselining was **not available**: the commit-time stale check runs against the *seed*
  (`check_active_seed_stale_baseline`), so an entry needs 2+ active seed rows. The seed
  deliberately keeps one row per board, so every one of these 31 URLs has ≤1 seed row. Baselining
  them would have failed the guard immediately. Reconciling to one active row is the resolution the
  seed's own shape implies.

### P4 — The 3 stale baseline entries and the 153 seed-only rows

Backed by only one live row each, though the seed still has two:
`amazongamestudios.com/en-us/careers`, `careers.nintendo.com`, `waterproofstudios.com/careers`.

The 153 seed-only rows are the larger half of this. Their provenance is now fully decomposed:
**152 `seed_row_demoted_in_store`** (the store demoted them, so the seed copy is stale) and **1
`seed_row_lost_from_store`** (RTL, resolved below). The earlier "163 unknown provenance" figure is
resolved — it was these two buckets plus the lost rows, not a third mystery class. The
`greenhouse:slug:examplestudio` placeholder and the tombstoned inXile row are pruned.

**Resolved 2026-09-26: 102 pruned, 50 deliberately kept.** The 152 were classified by *why* the
store demoted them, because the store's reason decides whether the seed copy is stale or the store
is merely retrying:

| Bucket | Count | Disposition |
|---|---:|---|
| `registry_conflict_*_auto_demote` (parked losers) | 127 | prune where a live replacement is verified |
| `sheet_directory` / `seed` / `provider_migration_candidate` (superseded) | 8 | prune |
| `fetch_failure_demote` (retry state) | 13 | **keep** — a failed fetch is not a retirement |
| `hidden:repeated_zero_jobs` (retry state) | 4 | **keep** — parked, deliberately retryable |
| tombstoned | 0 | — |

Of the 135 prune candidates, only **102** had a verified live replacement: 31 superseded by an active
row on the same canonical board, 3 with an explicit `duplicateOfSourceId` winner, and 68 superseded
by a provider-backed row for the same studio under a different adapter (whose canonical URL is the
provider endpoint, not the careers page — which is why a same-board check alone undercounts).

**The other 33 must not be pruned, and this is the important finding of P4.** They have no active
row on any adapter: `registry_conflict_*_auto_demote` demoted their *entire studio family*, leaving
the studio with zero coverage. Several are whole clusters of one board's URL variants
(Studio IGGYMOB ×5, Rogue Duck Interactive ×5, Proton Studio ×5, Clay Token ×5 — all `?l=thai` /
`?ckattempt=1` variants of a single page). Affected studios include k-ID, Unknown Worlds
Entertainment, Vertigo Games (PLAION), 4A Games, Frogwares, Double Eleven, Ever Curious
Entertainment, Magnopus, Behaviour Interactive, Black Beach Studio, GigXR.

**Correction (2026-09-26): that figure was 33 _rows_, not 33 studios**, drawn from a subset — the
authoritative count over all pending rows is **17 studios / 33 rows**. Pruning those seed rows would
have silently retired real boards, so they stay and a fresh install still activates them.

Also pruned in the same change: the 3 stale collision-baseline entries. A baseline entry no longer
backed by 2+ active rows would otherwise mask the URL permanently.

Result: seed 1,890 → 1,788, collision baseline 7 → 4, every non-pruned row byte-identical and in
order.

### Stranded families: guard added, then repaired

`62724469` added a post-condition to the safe auto-demote: after the demotion steps, any family left
with no active row gets its best remaining registration put back. Root cause was that
`_apply_safe_demotion_targets` moved rows with no post-condition, and the demoted rows came back with
`duplicateOfSourceId` **unset** — no recorded winner, and nothing in the response saying a board had
been emptied.

The guard prevents recurrence but cannot repair the damage: verified that **0** of the stranded
families still have a safe-eligible conflict card, and most have no card at all, because with no
active row there is no active-side conflict left to detect.

So the repair was a data pass: one registration promoted per stranded studio, chosen by the same
rule the guard uses (rankScore, then jobsFound, then id), siblings left parked. **16 studios
repaired** — active 2,224 → 2,240, `seed_row_demoted_in_store` 52 → 40, uncovered collisions still 0.

Two derivations had to be corrected on the way, and both are worth recording:

- **"No active row + has a pending row" is not a defect.** It is the normal state for a parked
  source. 523 pending rows have no active row, but they are 305 `gamedevmap` candidate backlog, 107
  `sheet_directory`, 18 fetch-failure, 12 repeated-zero, 9 operator-`manual`, and 14
  `duplicate_family_weaker_variant` (including the rows P3 deliberately demoted). Promoting on that
  test alone would have activated **452** rows — a draft of that script did exactly that and was
  rolled back from backup.
- **Studio labels are not a safe identity.** The pass promoted a `static` row for Lost Boys
  Interactive next to an already-active `jazzhr` row, because the labels differ
  (`Lost Boys Interactive` vs `Lost Boys Interactive (Embracer Group)`) and the normalizer does not
  strip that suffix. The preflight's uncovered-collision count caught it; the static row was reverted
  and the jazzhr row kept. **Match on host, not on studio label.**

### Latent same-studio duplicates the canonicalizer cannot see

Auditing the repairs surfaced two pairs that are the same board twice but do not collide, because
`canonicalize_careers_url` does not collapse these path variants:

- `unknownworlds.com/careers` (Unknown Worlds (Krafton)) vs `unknownworlds.com/en/careers`
- `double11.com/join-us` vs `double11.com/vacancies`

Both are one studio registered at two paths. The twin rule keys purely on canonical URL, so it will
never flag them, and the advisory audit inherits that blindness. Resolving them needs a per-studio
path decision plus a probe, so they are recorded here rather than folded into the P3 reconciliation.

### RTL Enterprises — the row was never lost, and it works (correction)

An earlier revision of this document claimed the RTL row was misconfigured, could not extract jobs,
and was correctly left out of the store. **All three claims were wrong**, and they rested on two
mistakes worth recording:

- **The row was never absent from the store.** It was reported as
  `seed_row_lost_from_store` only because the seed spells the tenant `.../RTL/` and the store
  `.../rtl/`, and the preflight compared ids case-sensitively. The row is active and has been
  collecting. The preflight now folds case and reports the disagreement as its own
  `seed_row_id_case_mismatch` finding, and the seed id has been aligned to the store's canonical
  form so a seed restore cannot add a second row for one board.
- **The probe used the wrong URL.** A browser render of `/RTL/` and `/RTL/content/search/` shows the
  new jobs2web front end, which genuinely has no server-rendered postings. But the phenom runner
  rewrites the registry row to `/RTL/search/` — a separate, still-live legacy j2w endpoint serving
  the classic `searchresults` table. Probing the URL the registry lists rather than the URL the
  runner fetches is what produced the wrong answer.

The board is a jobs2web tenant, and the phenom parser's `^/[^/]+/job/[^/]+/\d+/?$` **does** match its
legacy listing. No new adapter is needed and no re-point is warranted: RTL has no greenhouse, lever,
teamtailor, breezy or workable board, and the one Workday host that resolved rejected the request
unauthenticated (inconclusive).

What the board *did* have wrong was pagination — see the phenom fix below.

### Phenom pagination never advanced — 42% of a tenant's jobs uncollected

`src/jobs/adapters/parsers/phenom.py`. The next-page href on a search-results page carries
HTML-escaped separators (`&amp;startrow=25`). `_phenom_pagination_url` unescaped for its own
`startrow` test but returned the **escaped** string, and `parse_phenom_jobs_html` appended the raw
`absolute` rather than the helper's return value. The server therefore read the parameter as
`amp;startrow`, fell back to page 1, and the adapter re-fetched the first page instead of advancing.

Measured on RTL with the repo's own parser:

| | |
|---|---|
| page 1 | 25 jobs |
| emitted next page, as-is | 25 rows, **overlap 25, new 0** — page 1 again |
| emitted next page, de-escaped | 18 rows, **overlap 0, new 18** |
| unique jobs before / after | 25 → **43** |

`lastKeptCount` read 100 for 25 unique jobs, which is the fingerprint of the same page being fetched
repeatedly. The existing tests could not catch it: the fixture hrefs *are* escaped, but the
assertion was `all("startrow=" in page for page in next_pages)`, and the escaped string contains
that substring. The new test asserts the decoded form and that the parsed query has a real `startrow`
key and no `amp;`-prefixed names.

This affects every phenom tenant, not just RTL.

## Open items for the operator

1. **`static_probe_evidence` under-reports, and discovery acts on that number — converged. It is a
   discovery-side *counting* bug, never a collection bug, and most of it is not fixable in the probe.**

   **The central correction:** the jobs are already collected. About Fun is the clean proof — the board
   is `www.about-fun.com` (not `aboutfun.com`, which is a different dead host), and
   `data/jobs-fetcher.log` records `DONE source=static_source::static:listing_url:https://www.about-fun.com/jobs
   status=ok fetched=12 kept=12`. All 12 titles match the feed exactly, they share one `fetchedAt`, and
   their descriptions are synthetic (`"<title> at About Fun"`), i.e. **no detail page was ever
   fetched**. They arrive through `extract_rendered_card_jobs` (the rendered-card lane), which emits a
   job from a job-like anchor *without consulting the detail predicate at all*. So the probe counting
   zero is harmless to collection. Only the discovery-side number is wrong.

   **The mechanism I had assumed does not exist.** There are four lanes, not one, and the per-source
   opt-in reaches only two of them:
   - `add_detail_link` (`src/jobs/adapters/static_detail_heuristics_filter.py:261`) is the predicate
     gate for the detail-traversal lane, but it is only a *narrower*: `static_listing_runner.py:851-855`
     narrows to probable links **only when that set is non-empty**, so an empty set means every link
     passes through unfiltered.
   - The traversal lane (`_append_detail_candidate`) never calls the predicate at all.
   - The rendered-card lane calls it only for *provisional* rows.
   - `static_probe_evidence(text, base_url)` has **no source-row parameter**, so per-source config
     cannot reach the probe without changing the signature and all six call sites.

   And `detailPathTokens` structurally cannot express the shape anyway: tokens are slash-wrapped
   (`src/jobs/adapters/static_detail_heuristics_filter.py:240-242`), so the best any token can produce
   is `/jobs-/` — which never occurs in `/jobs-junior-creative-video-creator`. Verified for
   `['jobs-']`, `['jobs']`, `['/jobs-/']` and `['jobs-freelance-concept-artist']`: all `False`. **The
   recommendation this section used to make was not implementable.**

   **Landed, R1:** `_listing_path_variants` accepts the singular/plural sibling of a recognised
   listing path inside `_is_same_listing_detail_link` — not by widening `_STATIC_DETAIL_PATH_RE`,
   because every widening of that pattern broke a pinned test. Bandai Namco MY 0 → 20 roles.
   R2, the slug-shaped variant, recovered nothing measurable (`freelanceconceptartist` has no
   separator) and was dropped rather than kept on a hopeful theory.

   **Landed, R3:** `_STATIC_DETAIL_PATH_RE` and `_STATIC_LISTING_PATH_RE` said `vacancies?`, which
   expands to "vacanc" + "ie" + "s" + an optional "s" — so they accepted `/vacancies/<slug>` and
   `/vacanciess/<slug>` but **not** `/vacancy/<slug>`. Two-token change to `vacanc(?:y|ies)`, with the
   detail segment left untouched. Measured live against a control: **gismart 0 → 26** and
   **playground-games 0 → 12**, both entirely own-host; sega holds at 23 on the plural path. 170 probe
   tests and 1,369 static/discovery/admin tests pass, plus 8 new ones.

   **Landed, R4:** `_is_keyword_headed_detail_link` admits a same-host posting whose *head* segment is a
   postings keyword, including the compound spellings boards use (`job-listing`, `career_listing`,
   `open_positions`, `job-details`). The sibling rule requires a posting to sit under the board's own
   base path, which misses two measured shapes: a landing page that is not a listing path at all
   (Jyamma serves its master list at `/careersjyamma`, so the base check bails before any link is
   examined), and a listing section spelled differently from the landing page (`/join-us` boards whose
   roles live under `/career/<slug>`).

   Measured live, control first, **both directions**:

   | | boards gaining | roles | noise boards falsely gaining |
   |---|---:|---:|---:|
   | R4 as proposed (admit on a multi-word label) | ~17 rows | ~120 | **~19** |
   | R4 as landed (keyed on the last path segment) | **13/15** | **117** | **0/10** |

   The whole safety of the branch is in refusing section pages, so the leaf is tested rather than the
   label: a posting leaf is hyphenated and none of its hyphen parts is a section word. Counting hyphens
   is the wrong test — `/careers/benefits-and-perks` has two of them and is a section, while
   `/careers/vfx-artist/` has one and is a role. A category path (`/careers/all-openings/job-category/<dept>/`)
   is refused on the segment above the leaf.

   This also *strengthened* an existing pin rather than weakening it. The `listing-navigation-only`
   fixture protected Krafton's `/careers/jobs/` and `/careers/people/` only because their labels were
   single words; the landed rule refuses them on the **path**, and new multi-word-labelled section-tab
   fixtures plus two end-to-end cases (a navigation-only board and a department-index board must still
   report `no_jobs`) close the gap that fixture left open.

   **Measured shortfall:** Buffalo Buffalo stays at 0 — its `/careers/3denvironmentartist` has no hyphen
   and no job-like label — against the 2 a delegated analysis predicted. Left alone deliberately:
   forcing it means loosening the leaf rule that is doing all the safety work.

   ### The scale of the zero-count population, corrected twice

   Both figures this section previously carried came from a **5% stride sample** and were wrong by large
   factors:

   | | previously stated | actual |
   |---|---:|---:|
   | active static rows probing zero | 85 | **1,600 of 2,039** |
   | of those, JS-rendered | ~10 | **716 of the 1,522 that return 200** |
   | boards needing Playwright and not covered | "10 boards" | **0 — all 716 are already covered** |

   The Playwright fallback triggers automatically on `detect_js_shell` (`static_listing_runner.py:690-696`)
   plus a second independent trigger at `:698-704`. It is not per-source opt-in, which is how this
   section used to describe it.

   **The caveat that matters more than the count:** `lastStatus` is `None` on 1,598 of those 1,600 rows.
   Per the `excluded ≠ empty` rule, a probe count of 0 on a never-fetched row says nothing about yield,
   so none of this establishes lost jobs. What R3 and R4 fix is a *counting* defect that misleads
   discovery ranking — not collection.


   **What is deliberately still not fixed, and why:**

   - **carx-online is not a recovery.** Its probe count moves 0 → 1, but that one "detail link" is
     `krasnodar.hh.ru/vacancy/137323128` — an hh.ru embed on a 7.4 MB page, not a carx posting. It is
     recorded as a false positive rather than claimed as a win.
   - **The probe has no off-host filter on this branch at all**, and never did: `hh.ru/jobs/123` was
     already admitted before R3, because the netloc checks live only in the same-host listing
     predicates. R3 extends a pre-existing weakness to one more spelling rather than introducing it.
     Fixing it properly means changing probe counting semantics and shifting many boards' counts at
     once — a much larger change than adding one spelling, and not something to do unvalidated. The
     fetch adapter's `KNOWN_NON_JOB_DETAIL_HOSTS` plus per-source curation is the real defence against
     off-host junk, not the probe estimate.
   - **Root-level slugs are split:** `/senior-programmer/` and `/senior-game-designer/` (futurats,
     dynamicnext) are reachable by `detailPathTokens` because the trailing slash makes the token match;
     `/2d-artist` and a bare `/26072` are not, having no slash-delimited segment.
   - **`jyammagames.com/careersjyamma` is not a malformed URL and the row is correct.** An earlier
     revision of this section called it a data defect; measurement contradicted that. The registered
     URL is the site's *master* list and its HTML carries all four role paths, and the row's
     `jobsFound: 8` is direct evidence the fetch collects from it. The probe was reporting
     `no_jobs` at `confidence=high` on a page with four real roles — a confident false negative, the
     worst failure mode — purely because `/careersjyamma` fails `_STATIC_LISTING_PATH_RE` and
     `_is_same_listing_detail_link` bails at `probe.py:218` before examining a link. R4 fixes it on the
     probe side (0 → 4).

     Repointing the row to `/careers` was measured and rejected: that URL 302s to `/careers-art/`,
     whose HTML mentions only 1 of the 4 role paths, so the probe would report 4 while the board
     yields 1 — trading a false negative for a false positive on the exact number discovery acts on.
   - **About Fun's `/jobs-<slug>` needs nothing.** Its 12 jobs are collected. Only its probe count is
     wrong, and fixing that needs a source-row parameter on `static_probe_evidence` plus six call-site
     changes — a separate change from any registry field, and correctly not started here. Note the
     latent risk if it ever is: `/jobs-open-application` is a real 200 on that site and is inseparable
     from a role by shape, and `add_detail_link` never consults `_GENERIC_APPLICATION_TOKENS`.

2. **Non-game jobs carried `sector: "Game"` — resolved as a rejection, and the cause was not the
   parser.** `docs/notes/t3-workday-promotion-2026-09-05.md:62` had already settled the policy for a
   board whose `has_positive_game_evidence` is contaminated: measure how many rows a *correct* game
   filter would keep, and if it is ~nothing, reject the whole company as `non_game_employer_scope`
   rather than promote it with a filter. Intel kept 0/36, SciGames 0/34, L&W 1/23 — all three
   rejected.

   Measured against that test, on the **live** board and through the repo's own parser
   (`parse_phenom_jobs_html` over the 185,358-byte response from the search URL the runner rewrites
   to): **25 rows, 0 of them a game role.** Controlling, accounting, editorial, media production, HR,
   legal, retail, advertising, SAP support, backend IT. Not one borderline title. The row is now
   rejected in the live store and removed from the committed seed, exactly as the precedent was
   applied — and the seed removal is the part that matters, since all three t3 boards are absent from
   the seed and a fresh install would otherwise resurrect the row.

   Two corrections this forced, both against the earlier reading in this plan:

   - The hard-coded `"sector": "Game"` at `phenom.py:147` is **inert**. `normalize_sector`
     (`src/jobs/normalizers.py:129-139`) discards its `value` argument and returns `"Game"` iff
     `has_positive_game_evidence(...)`; `canonicalize_locations.py:577` and the frontend mirror
     `frontend/jobs/domain/feed.js:151` both pass the parser value in and it is ignored. The real
     cause is `has_game_source_provenance` (`src/jobs/game_detection.py:161-165`), where any bundle
     item whose adapter is not in `STATIC_ADAPTERS` counts as games-industry provenance with no games
     check on the source or studio — so a multi-tenant *platform* adapter lends game provenance to
     every tenant. Deleting that branch is too blunt: it is also the sole provenance for ~133
     legitimately-game rows.
   - The rows are **not** location-broken. They carry `country: 'DE'` and `city: 'Hamburg'` live. The
     empty locations were stale stored-feed data, so the "these rows are also low-quality" claim was
     wrong.

   **The signal fix is still open, and deliberately so.** A `PLATFORM_ADAPTERS` concept in
   `game_detection.py` requiring corroboration before a platform adapter alone counts is the narrow
   fix, and it stays recorded rather than applied — the same way t3 recorded the filter concept in T13
   for future genuinely mixed boards. It is not a one-liner: it changes a public field in
   `REQUIRED_FIELDS`, and `frontend/jobs/domain/feed.js` is *already* stale against Python (it lacks
   the `NON_GAME_INDUSTRY_HINTS` veto and uses a broader keyword regex), so a backend-only change
   would desynchronize backend and UI sector. It also needs the job-data-quality skill's approval,
   which requires explicit product sign-off for any new sector gate. No tombstone was written for the
   rejection: the t3 precedent did not use one, and `TOMBSTONES_PATH` resolves to a gitignored
   `data/*.json` that does not exist while the real store is `.json.gz`/`.jsonl`, so a default-path
   write would be a silent no-op that looks like protection. **Recurrence is therefore possible** —
   discovery could re-add the tenant — and the honest guard is a preflight finding, not an artifact
   nobody reads.

3. **Optional D — a frontend surface for `POST /registry/repair-review-action` — closed as out of
   scope, with the reasoning recorded.** The route exists (`12dbe58a`) and is the sanctioned way to
   record a repair decision; what does not exist is Admin UI for it. Two reasons to leave it there.
   First, this programme's own non-goal is that the gate *records* decisions while applying one stays a
   human action through the existing transition paths — P1 deliberately built a way to record, not a
   way to apply, and a one-click UI would erode exactly that boundary. Second, the need it was meant
   to serve is demonstrably already met: the advisory monitor plus per-row repair scripts applied
   several hundred row changes during this programme without any UI, each with a plan assertion, a
   backup, and a read-back check. An Admin surface is a product-surface decision that belongs with
   whoever owns Admin, and it should be scoped against real usage of the route rather than invented
   here. Revisit if the route starts being driven from scripts in a way that needs operator visibility
   in the product.

   If it is ever built, two constraints carry over from this work: a repair view must key rows by
   **host**, not studio label (`Lost Boys Interactive` vs `Lost Boys Interactive (Embracer Group)` are
   one board), and must never match by truncated public suffix (that would collapse every `*.co.uk`).

## Resolved since the last update

### A test fixture was registered as a live source

Reconciling the preflight's live-row headline against the store showed they disagreed by one
(2,260 vs 2,259). The preflight deliberately delegates to the runtime loader so it cannot disagree
with the fetcher, so the difference was a row the pipeline cannot see — and it was
`greenhouse:slug:examplestudio`, studio **"Example Studio GmbH"**: present only in the live active
store (the P4 seed prune had already removed it from the seed), `boards-api.greenhouse.com` 404 for
that board, `enabledByDefault: true`, credited with a fabricated `jobsFound: 2`, and pointing at a
gamesmap *example* page. Retired as `test_fixture_row_never_a_real_board`.

The lesson generalises past this row: a summary count I read without reconciling against its source is
a number I had not actually checked, and it hid a fictional employer in the default fetch set.

### The two latent same-studio duplicates — only one of them was real

Probed both pairs with a known-good control first (supercell and playstation, both 200), so a 404
could not be mistaken for a dead board:

| Pair | Content | Declared canonical | Verdict |
|---|---|---|---|
| `www.unknownworlds.com/careers` → `/en/careers` | identical (93,828 b, same fingerprint) | `/careers` | **one board, registered twice** |
| `double11.com/join-us` (203,067 b) vs `/vacancies` (105,038 b) | different | `/join-us/` vs `/vacancies/` | **two different pages** |

`/careers` redirects to `/en/careers` and serves the same bytes, so one registration is redundant.
The survivor is the pre-existing `www.unknownworlds.com/careers` row (rank 59, 6 jobs) over the
stranded-family promotion `unknownworlds.com/en/careers` (rank 0, 4 jobs); the latter is now pending
via `_demote_duplicate_variant`, so it carries `duplicateOfSourceId` and the family key.

Double Eleven is **not** a duplicate and was left alone: "Join us" and "Vacancies" are different
pages with different canonicals and different sizes. The genuine Double Eleven redundancy is a
different pair — `www.double11.com/vacancies/` is pending from a conflict demote while
`double11.com/vacancies/` is active, which is the correct end state, not a defect. Note the twin rule
inherently cannot see *path* variants, so this class stays a per-row judgement rather than something
the advisory audit can automate.

### Conflict-demoted rows now record what they lost to — on inert fields

Open item 4 is closed, and the obvious implementation was rejected on evidence. Stamping
`duplicateOfSourceId` would have:

- moved the documented `duplicatePendingCount` KPI (`registry_service.py:623`,
  `registry_sync_summary.py:173`, `source_registry_io_load.py:247`) from 32 real duplicates to ~194,
  a 6× jump attributed to a change nobody asked for;
- subtracted 25 `evidenceScore` and appended a `duplicate_static_row` blocker per row in
  `scripts/source_policy_soak_report_links.py:226`;
- carried a deferred regression, because `transition_registry_to_active` does not clear the field, so
  a stamped row that was later re-promoted would become an *active* row every one of those readers
  now treats as a duplicate.

So provenance lands on `conflictFamilyKey` and `supersededBySourceId`, which **nothing reads** —
verified across `src/`, `scripts/` and `tools/`. Durable, offline-joinable, and behaviour-neutral.
`hiddenFromDefault` was ruled out for the same reason and harder: it is a live gate, so a stamped row
would drop out of the admin pending listings and the pending provider-migration fetch lane.

The winner also belongs in the run response, where it costs one key: `applied[]` entries now carry
`winnerId`, which is where an operator reviewing a run learns what each row moved behind. Both are
visible in the admin conflict diff via `CONFLICT_DIFF_FIELDS` + `_FIELD_LABELS`, so no frontend change
was needed. `DATA_CONTRACT.md` documents the new fields and, more usefully, now says explicitly that
`duplicateOfSourceId` is the duplicate-policy field and must not be reused for other demotions.

### 227 registry `pages` entries were not pages

`pages` is the list of URLs the fetcher requests, and 227 of them could never yield a posting: 135
asset extensions, 46 `wp-json`/`oembed` REST endpoints, 46 CDN-host entries — across 63 rows.
Bungie's row listed `favicon.ico` and four social SVGs, EA's listed `favicon.png`, King and TT Games
listed dozens of `cdn.phenompeople.com` resources. Stripped, with one guard: a row is never left
without pages *and* without a `listing_url` (the `definitionless_row` defect), and rows that would
have been are left untouched and reported. Row counts unchanged; 0 survivors; 0 new definitionless
rows; every kept page byte-identical to a page that was there before.

**Counter-check worth keeping:** the first framing of this audit was "493 off-site page entries" and
it was mostly *legitimate* — Bethesda→`jobs.zenimax.com`, BKOM→its own Zoho tenant, Kakao→
`careers.kakao.com`, JoyBits→`joybits.games`, Coldwood→its parent's board. "Off-site" is the wrong
discriminator: a studio's jobs often live on a different domain than its homepage. A naive
registrable-domain helper made it worse by calling `activision.com` and `careers.activision.com`
different sites. Only the asset/REST class is unambiguous, which is exactly the class fixed.

### `hiddenFromDefault` was not cleared on promotion — a bug in the stranded-family repair

`transition_registry_to_active` set `enabledByDefault: True` but left `hiddenFromDefault` alone, and
`is_hidden_from_default` reads that flag. So a row promoted out of a demotion was *active, enabled,
and hidden at once* — dropped from admin pending listings and from the pending provider-migration
fetch lane. **34 active rows were in that state**, including one of the 37 stranded-family
promotions (`glera-games.com/jobs/`), i.e. the repair restored the row and left it inert.

The cause was verified rather than assumed: only two places set the flag
(`_demote_duplicate_variant` and the repeated-zero-jobs auto-demote) and both are demotion paths,
neither of which can leave a row active; and every one of the 34 has `lastPromotedAt` after
`lastDemotedAt` with an empty `pendingReason`. So the flag was inherited 34 times over.

Fixed in two parts, because the code fix alone would not have helped rows already stranded:
promotion now clears the flag (3 tests), and the 34 active rows plus 2 seed rows were cleaned up
under an asserted plan with a backup and a read-back check. The cleanup adds `hiddenFlagClearedBy` /
`hiddenFlagClearedAt` to each repaired row so the change is attributable. Row counts unchanged, 0
unrelated rows touched.

### Correction: the "static parser is losing two-thirds of these boards" claim was wrong

An earlier sweep of 40 GameDevMap-sourced static boards reported 26 of 39 reachable boards at zero
postings and claimed 122 lost. **That did not survive validation**, and the reason is worth keeping:

- The sweep's posting counter was a heuristic that counts same-host job-ish links. It cannot tell a
  posting from a category page, so it over-counts — Novaquark's 85 "links" are mostly not postings.
- `lastStatus: excluded` on those rows does **not** mean "fetched and found nothing". In
  `static_listing_flow._handle_skip_and_revalidation` it means *not fetched this run*: a cache
  `skip_fresh`/`cooldown_skip`, an HTTP 304, or `structured_migration_promoted`. Their state entries
  corroborate it — `lastDurationMs: 0`, `lastFetchedCount: 0`, and `lastFingerprint` equal to
  `da39a3ee…`, the SHA-1 of the empty string.
- So `lastKeptCount: 0` on those rows is **not evidence of collection failure**, and neither is a
  probe count of 0. Running the actual fetcher is the only thing that settles it, and when I did,
  the boards collected fine.

The one durable takeaway is narrower than the original claim: **probe count and actual collection
disagree, and only a real fetch tells you which is right.** The yield columns in
`_out/registry-repair-20260925/duplicate-classification.json` should be read with that caveat — though
the P3 demotions themselves are unaffected, since every one of those 31 was a same-board
reconciliation established by redirect and content evidence, not by yield.



### Release state — no gate work needed

The commits carrying the store/seed repair touch `data/defaults/` and
`data/source-registry-tombstones.json.gz`, which are shipped paths, so they republish the container.
That does **not** need a new `Release-tag` line: the window since anchor `f704cdae` (v0.2.152) already
carries `Release-tag: v0.2.153` intent on `fa573231` and `13a11206`, and the gate passes if *any*
commit in the window declares valid intent. A decorative tag on a data repair would misdeclare a
release, so none was added.


## Non-goals

- **No automatic URL or host mutation.** The gate records decisions; applying one is a human action
  through the existing sanctioned transition paths. P1 added a way to *record*, not a way to *apply*.
- **No blanket suppression.** Every finding stays reportable; review state annotates a finding rather
  than hiding it, and changed evidence reappears as `stale`.
- **No bulk registry reconciliation without per-row evidence.**
- **No new dependency.**

## Verification gates

Each item: focused tests for the touched module, then `npm run test:py:extended` and
`npm run test:py:linux`, plus `npm run lint:repo-guardrails`, `npm run typecheck:py`, and
`python -m vulture --min-confidence=60 src whitelist.py` returning zero findings.
