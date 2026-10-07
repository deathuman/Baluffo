# Shadow cleanup and the country filter — Evidence Snapshot — 2026-10-07

> - **Status:** the planned shadow-row cleanup is **mostly wrong**, and the check that found out
>   why is now recorded so nobody repeats it. Three rows are genuinely redundant; seven serve
>   openings nothing else serves, and the cleanup would have deleted them. The country filter
>   defect is **fixed** and shipped.
> - **Basis:** the v9 replay (`_out/coverage/verify-v9/`), host+tenant attribution over its
>   output, and the frontend's own `matchesCountrySelection` run against the real stored
>   values.
> - **Canonical for:** the shadow-row decision, the GB/UK filter defect, and the blank-country
>   audit. Delivery detail: [`delivery-v9-2026-10-07.md`](delivery-v9-2026-10-07.md).

## The shadow rows are not shadows

The ledger carries "Retire redundant `static` rows shadowing working provider paths — 34 boards
/ 128 openings, **already collecting** — cleanup". The plan's own note says shadow rows should be
*demoted*, not retired. Measuring it, by the repo's identity rule (host + tenant), gives:

| static board | rows it alone serves | v9 state | decision |
|---|---:|---|---|
| `remotecontrol.jobs.personio.com/` | 2 | rollup_only | keep — demoting loses them |
| `chimera-entertainment.jobs.personio.com/` | 2 | rollup_only | keep |
| `studiowildcard.bamboohr.com/careers` | 0 | not_selected | **demote** |
| `www.redkitegames.co.uk/jobs` | 3 | collected | keep |
| `careers.lionbridge.com` | 0 | unknown | **demote** |
| `jobs.starstableentertainment.com` | 1 | error | keep |
| `jobs.fatsharkgames.com` | 4 | error | keep |
| `jobs.eu.lever.co` | 0 | unknown | **demote** |
| `www.frontier.co.uk/careers` | 0 | fetched_empty | **demote** |
| `careers.sunandmoonstudios.co.uk` | 0 | fetched_empty | **demote** |

**Five of ten candidates, 3 distinct boards at zero, and 18 openings that only those rows serve.**
The "already collecting" in the ledger row was read off registry presence — the rows are watched,
not that anything comes of them.

### The check that nearly got this wrong

The first pass compared **source-name strings**: for each shadow host, count the rows under
`static_source::<id>` and the rows under the provider's rollup on the same site. It reported
**8 of 10 as serving nothing** — because their postings arrive under a differently-spelled
source name, and `personio_sources` carries 42 rows on `*.jobs.personio.com` while the static
row's own name matches none of them.

That is the same class of error the whole coverage effort keeps recording: a join on something
that is not the thing being measured, with a confident number attached. The registry's rule is
host + tenant, and under that rule the answer is the opposite. **Nothing was demoted on the
string comparison**, because the identity check ran first.

One genuine duplicate did turn up, and it is a duplicate by URL rather than by family:
`https://jobs.fatsharkgames.com` is carried by **2** rows.

## The country filter: Europe excluded its own country

`REGION_COUNTRY_TOKEN_LOOKUP` canonicalised only the region list's own labels before indexing.
The list says `United Kingdom`, which the country contract's alias map resolves to `UK` (token
`uk`); a stored row of `GB` or `UK` tokenises to `unitedkingdom`. So the region index held one
spelling of a member and not the spelling the rows carry:

| stored value | rows | token | Europe region, before | after |
|---|---:|---|---|---|
| `GB` | 1,770 | `unitedkingdom` | no match | match |
| `UK` | 20 | `unitedkingdom` | no match | match |
| `England` | 356 | `england` | no match | match |

**2,146 rows were invisible to a Europe selection** — a filter that exists, is offered, and
returns a count. Fixed in `frontend/jobs/app/countries.js` by indexing every spelling a member
can arrive as, and by naming `England` explicitly (`REGION_EXTRA_MEMBER_TOKENS`) since the
contract deliberately keeps it distinct from GB while the region still contains it. Shipped in
0.3.014; pinned by `tests/frontend/unit/jobs-country-region.test.mjs`, which also asserts the
inverse (US/CA/JP/Remote stay out, DE/FR/NL/PL/SE/ES stay in).

## The blank-country half is not a derivability problem

Of **4,947** rows with a blank or `Unknown` country in the v9 feed:

| | rows | |
|---|---:|---|
| derivable from the city's own text | **7** | all Stockholm → SE, all `bamboohr_sources` |
| remote / anywhere — `Unknown` is correct | 1,006 | 945 of them from the Google Sheet |
| carry no location text at all | **3,934** | 828 from the Sheet, 343 hrmos Cygames, Japanese titles with an empty location field |

The candidates that *looked* promising are noise, not cities: `Category` (27), `width` (18),
`stroke` (9), `Previous` (9), `group` (9), `Division` (8), `from QA to HR` (8). Real cities are
there too — Osaka 21, Pangyo 11, Bengaluru 11, Limassol 7, Belgrade 7, Ho Chi Minh City 7,
Wroclaw 6, Leamington Spa 6, Poznań 5, Prague 5 — and adding those to `_CITY_COUNTRY_HINTS` is
the whole of the honest work.

**The studio's country is not used to fill the rest.** A headquarters says where the company is,
not where the role is, and inferring 4,000 countries that way would put confident wrong values
into a public feed and into every country filter. That is the line the plan drew when it said
"no new dependencies without approval" and "an unfilled country is honest, a guessed one is not".
A country gazetteer is the missing input, and it is a dependency decision, not a code change.

## What this leaves

- The 351-board Phase 5 probe is still running; its verdicts decide the registration wave.
- Three shadow rows are ready to demote (`studiowildcard.bamboohr.com`, `careers.lionbridge.com`,
  `jobs.eu.lever.co`, plus the two at `fetched_empty`), pending the probe's read on whether any
  of them collects on a fresh fetch.
- `https://jobs.fatsharkgames.com` carried by 2 rows is the one duplicate worth collapsing.
