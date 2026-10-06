#!/usr/bin/env python3
"""Compare a Baluffo job feed against a Games Jobs Index snapshot.

Baluffo and gamesjobsindex.com draw from different universes, so a raw row count
says nothing about coverage. Baluffo's feed is mostly community Google Sheets and
scraped career pages; GJI indexes studios' own ATS boards. The useful question is
which GJI openings Baluffo lacks, and *why* -- so every miss is classified into an
actionable bucket rather than reported as a number.

The GJI filters are replicated from the site's own client code rather than
guessed, because a comparison built on the wrong filter is meaningless:

  location=EUROPE  -> country in EU27 + IS/LI/NO + CH. GB and TR are excluded.
  q=<text>         -> raw-lowercase substring OR alnum-normalised substring,
                      matched against title and company.

Buckets:

  label_mismatch      A studio matches under a different label (GJI and Baluffo both
                      publish "welevel GmbH" and "welevel Studios"). Not a gap.
  unregistered_board  The board is live and serves the role, but Baluffo has no
                      registry row for it. Registering it is the fix.
  registered_no_role  A registry row exists for the board and Baluffo collects
                      jobs from it, but this specific role is absent. Either the
                      board changed or an adapter dropped it.
  role_not_on_board   GJI lists the role but it is no longer on the board. Stale
                      upstream; re-check before acting.
  unknown             Could not decide without a live probe.

Every miss also carries the role gate: ``gameRole`` is whether the
production row filter (``looks_like_game_job``, the same call the parsers
make) would keep the role, applied to the fields a GJI record carries.
A raw miss count is not recoverable coverage -- the 2026-10-06
re-measurement sized 24 absent boards at 486 listings, of which 149
(31%) were game roles -- so the split is made where the numbers are
produced, not after the fact.

`role_not_on_board` is deliberately PROVISIONAL. A fetchable detail page does not
prove a job is still open, so this bucket needs a list-API or live confirmation
before anyone acts on it.

Usage:
  python tools/coverage_audit.py --feed data/jobs-unified-light.json \\
      --gji gji-jobs.json --out _out/coverage
  python tools/coverage_audit.py ... --registry data/source-registry-active.json.gz
  python tools/coverage_audit.py ... --query "Technical Artist" --region EU
"""

from __future__ import annotations

import argparse
import gzip
import json
import re
import sys
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

# tools/ is flat and has no package init, so a sibling import needs the directory on
# the path. Guarded so repeated loads (tests import these by path) do not grow it.
_TOOLS_DIR = str(Path(__file__).resolve().parent)
if _TOOLS_DIR not in sys.path:
    sys.path.insert(0, _TOOLS_DIR)

from coverage_board_identity import (  # noqa: E402, I001
    build_candidate,
    candidate_identity,
    registry_identity,
)

# The repo root must be importable too: the role gate reuses the production
# row filter rather than a second copy of the rule, so `src` has to resolve.
_ROOT_DIR = str(Path(__file__).resolve().parents[1])
if _ROOT_DIR not in sys.path:
    sys.path.insert(0, _ROOT_DIR)

from src.jobs.game_detection import looks_like_game_job  # noqa: E402

# EU 27 + EEA non-EU (IS, LI, NO) + CH. Mirrors gamesjobsindex.com
# assets/js/jobs-page-utils.js EUROPEAN_COUNTRIES exactly. GB and TR are
# deliberately absent: the site excludes both, so including them here would
# manufacture phantom gaps on every UK role.
EUROPEAN_COUNTRIES = frozenset(
    {
        "AT",
        "BE",
        "BG",
        "HR",
        "CY",
        "CZ",
        "DK",
        "EE",
        "FI",
        "FR",
        "DE",
        "GR",
        "HU",
        "IE",
        "IT",
        "LV",
        "LT",
        "LU",
        "MT",
        "NL",
        "PL",
        "PT",
        "RO",
        "SK",
        "SI",
        "ES",
        "SE",
        "IS",
        "LI",
        "NO",
        "CH",
    }
)

# Country names the feed stores alongside ISO codes. The feed mixes both, and
# 73 distinct values do not resolve to ISO-2, so a code-only region filter
# silently under-counts.
COUNTRY_NAME_TO_ISO = {
    "germany": "DE",
    "poland": "PL",
    "spain": "ES",
    "netherlands": "NL",
    "sweden": "SE",
    "romania": "RO",
    "ireland": "IE",
    "italy": "IT",
    "belgium": "BE",
    "czechia": "CZ",
    "czech republic": "CZ",
    "portugal": "PT",
    "cyprus": "CY",
    "bulgaria": "BG",
    "finland": "FI",
    "denmark": "DK",
    "austria": "AT",
    "hungary": "HU",
    "greece": "GR",
    "croatia": "HR",
    "latvia": "LV",
    "lithuania": "LT",
    "luxembourg": "LU",
    "malta": "MT",
    "slovakia": "SK",
    "slovenia": "SI",
    "estonia": "EE",
    "iceland": "IS",
    "liechtenstein": "LI",
    "norway": "NO",
    "switzerland": "CH",
    "united kingdom": "GB",
    "england": "GB",
    "scotland": "GB",
    "wales": "GB",
    "ukraine": "UA",
    "serbia": "RS",
}

BUCKET_LABEL_MISMATCH = "label_mismatch"
BUCKET_UNREGISTERED = "unregistered_board"
BUCKET_REGISTERED_NO_ROLE = "registered_no_role"
BUCKET_STUDIO_COVERED = "studio_covered_role_absent"
BUCKET_NOT_ON_BOARD = "role_not_on_board"
BUCKET_UNKNOWN = "unknown"

# Ordered most-actionable first so a truncated summary still leads with real work.
BUCKET_ORDER = (
    BUCKET_UNREGISTERED,
    BUCKET_STUDIO_COVERED,
    BUCKET_REGISTERED_NO_ROLE,
    BUCKET_NOT_ON_BOARD,
    BUCKET_LABEL_MISMATCH,
    BUCKET_UNKNOWN,
)


def normalize_token(value: Any) -> str:
    """Lowercase alnum-only, matching the site's ``normalizeSearchText``."""
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def strip_bracket_prefix(value: Any) -> str:
    """Drop GJI's ``[Studio] Title`` prefix before comparing titles."""
    return re.sub(r"^\[[^\]]+\]\s*", "", str(value or ""))


def to_iso2(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) == 2 and text.isalpha():
        return text.upper()
    return COUNTRY_NAME_TO_ISO.get(text.lower(), "")


def in_europe(row: Mapping[str, Any]) -> bool:
    """Region test over a GJI record, falling back to location text for the feed.

    GJI records carry a clean ISO-2 ``country``. Feed rows carry ``country`` that
    is sometimes a name, sometimes blank, and sometimes only implied by an entry
    in ``locations``, so all three are consulted rather than silently dropping
    the row.
    """
    iso = to_iso2(row.get("country"))
    if iso:
        return iso in EUROPEAN_COUNTRIES
    for item in row.get("locations") or []:
        if isinstance(item, Mapping) and to_iso2(item.get("country")) in EUROPEAN_COUNTRIES:
            return True
    parts = [row.get("locationSummary"), row.get("city"), row.get("location")]
    blob = " ".join(str(p) for p in parts if p).lower()
    if not blob:
        return False
    return any(to_iso2(name) in EUROPEAN_COUNTRIES and name in blob for name in COUNTRY_NAME_TO_ISO)


def matches_query(row: Mapping[str, Any], query: str, *, company_key: str = "company") -> bool:
    """The site's search semantics: raw substring OR normalised substring."""
    lowered = query.lower().strip()
    if not lowered:
        return True
    normalized = normalize_token(lowered)
    if len(normalized) < 2:
        normalized = ""
    for field in ("title", company_key):
        text = str(row.get(field) or "")
        if lowered in text.lower():
            return True
        if normalized and normalized in normalize_token(text):
            return True
    return False


def read_json(path: Path) -> Any:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as handle:  # type: ignore[operator]
        return json.load(handle)


def load_gji_records(path: Path) -> list[dict[str, Any]]:
    payload = read_json(path)
    if isinstance(payload, Mapping):
        payload = payload.get("records") or []
    return [row for row in payload if isinstance(row, Mapping)]


def load_feed_rows(path: Path) -> list[dict[str, Any]]:
    payload = read_json(path)
    if isinstance(payload, Mapping):
        payload = payload.get("jobs") or payload.get("records") or []
    return [row for row in payload if isinstance(row, Mapping)]


def load_registry_ids(path: Path | None) -> set[str]:
    """Registry source ids, if a registry snapshot is supplied."""
    if path is None or not path.exists():
        return set()
    payload = read_json(path)
    rows: Iterable[Any] = payload if isinstance(payload, list) else []
    return {
        str(row.get("id") or "").strip().lower()
        for row in rows
        if isinstance(row, Mapping) and row.get("id")
    }


def _title_variants(row: Mapping[str, Any], *, company_key: str) -> set[str]:
    return {
        normalize_token(strip_bracket_prefix(row.get("title"))),
        normalize_token(row.get("title")),
    }


def _labels_match(left: str, right: str) -> bool:
    return bool(left) and left == right


def _labels_related(left: str, right: str) -> bool:
    """True when two studio labels plausibly name the same company.

    GJI and Baluffo routinely publish the same studio under different legal
    suffixes ("welevel GmbH" vs "welevel Studios", "2K Czech" vs "hangar13"), so a
    shared distinctive stem must not be reported as a coverage gap. Requiring the
    stem to open the longer label keeps "SEGA" from swallowing "SEGA Europe".
    """
    if not left or not right:
        return False
    if left == right:
        return True
    shorter, longer = sorted((left, right), key=len)
    return len(shorter) >= 6 and longer.startswith(shorter)


def normalize_url(value: Any) -> str:
    """Normalised job URL for exact cross-feed matching.

    The query string is **kept**: for several vendors it is the job identifier
    (``?ashby_jid=``), so dropping it collapses every opening at a studio onto one
    URL -- measured at 218 distinct jobs sharing one base URL on a single board.
    Only the scheme, a leading ``www.``, and the fragment are dropped.
    """
    text = str(value or "").strip().lower()
    if not text:
        return ""
    text = re.sub(r"^https?://", "", text).split("#", 1)[0].rstrip("/")
    return re.sub(r"^www\.", "", text)


def build_url_index(rows: Sequence[Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    index: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        key = normalize_url(row.get("jobLink") or row.get("url"))
        if key:
            index.setdefault(key, row)
    return index


def find_match(
    gji_row: Mapping[str, Any],
    feed_index: Mapping[str, list[Mapping[str, Any]]],
    *,
    company_key: str,
    prefix_index: Mapping[str, set[str]] | None = None,
    url_index: Mapping[str, Mapping[str, Any]] | None = None,
) -> tuple[Mapping[str, Any] | None, bool, bool, str]:
    """Match one GJI row against the feed.

    Returns ``(row, fuzzy_label, studio_known, basis)``.

    URL first, because it is the only definitive signal available: the two boards
    publish the same posting URL, so a hit is the same opening rather than a
    similar title. On the full catalogue 3,368 of 15,332 rows match this way.

    ``basis`` is ``"url"``, ``"title"``, or ``""``, and the caller reports the two
    separately. Title matching alone over-claims -- "HR Business Partner - US
    Operations (West)" and "(East)" collapse to one title -- so an unlabelled
    headline rate would hide how much of the coverage is actually proven.

    Otherwise:
    * ``row`` set, ``fuzzy_label`` True -> same role under a different studio
      label. Not a gap, so it is kept out of the miss list entirely.
    * ``row`` None, ``studio_known`` True -> the studio IS in the feed but this
      role is not. A real gap, and the studio being covered is precisely what
      separates it from an unregistered board.
    """
    if url_index is not None:
        by_url = url_index.get(normalize_url(gji_row.get("source_url")))
        if by_url is not None:
            gji_label = normalize_token(strip_bracket_prefix(gji_row.get(company_key)))
            feed_label = normalize_token(strip_bracket_prefix(by_url.get(company_key)))
            return by_url, not _labels_match(gji_label, feed_label), True, "url"
    gji_label = normalize_token(strip_bracket_prefix(gji_row.get(company_key)))
    gji_titles = _title_variants(gji_row, company_key=company_key)
    studio_known = False
    if prefix_index is not None and len(gji_label) >= _LABEL_PREFIX_LEN:
        labels: Iterable[str] = prefix_index.get(gji_label[:_LABEL_PREFIX_LEN], set())
    else:
        labels = feed_index
    for label in labels:
        if not _labels_related(gji_label, label):
            continue
        studio_known = True
        for candidate in feed_index[label]:
            if _title_variants(candidate, company_key=company_key) & gji_titles:
                return candidate, not _labels_match(gji_label, label), True, "title"
    return None, False, studio_known, ""


def build_feed_index(
    rows: Sequence[Mapping[str, Any]], *, company_key: str
) -> dict[str, list[Mapping[str, Any]]]:
    """Index feed rows by normalised studio label.

    Rows are indexed by reference, not copied: ``audit`` subtracts matched rows from
    the feed-only set by identity, and a ``dict(row)`` copy would make every matched
    row look feed-only too.
    """
    index: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        index.setdefault(normalize_token(strip_bracket_prefix(row.get(company_key))), []).append(
            row
        )
    return index


# `_labels_related` only ever relates labels where the shorter is a prefix of the
# longer and at least 6 characters, so bucketing on the first 6 characters makes
# candidate lookup near-constant instead of a scan of every studio in the feed.
_LABEL_PREFIX_LEN = 6


def build_label_prefix_index(
    feed_index: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, set[str]]:
    prefixes: dict[str, set[str]] = {}
    for label in feed_index:
        if len(label) >= _LABEL_PREFIX_LEN:
            prefixes.setdefault(label[:_LABEL_PREFIX_LEN], set()).add(label)
    return prefixes


def registry_ids_for_board(registry_ids: set[str], source_url: str) -> list[str]:
    """Registry rows that actually cover this board.

    Identity is ``(adapter, host, tenant)``, resolved the same way
    ``coverage_boards`` resolves it for a candidate. The previous version matched
    any path segment as a substring of any registry id, which was loose enough to
    call most things registered: measured on the catalogue, 4,564 of 4,589
    ``registered_no_role`` rows matched *only* that way. So the bucket named a
    collection failure where the real cause was an unregistered board, and phase 2
    would have gone looking for a collection bug that did not exist.

    Host alone is not enough either -- ids like ``greenhouse:slug:example`` never
    contain ``job-boards.greenhouse.io`` -- and neither is the id string, which is
    inconsistent about trailing slashes across 2,030 active static rows.
    """
    if not source_url or not registry_ids:
        return []
    try:
        candidate = build_candidate(source_url)
    except Exception:
        return []
    if not candidate.get("tenant"):
        return []
    identity = candidate_identity(candidate)
    hits: list[str] = []
    for rid in registry_ids:
        if registry_identity(rid) == identity:
            hits.append(rid)
    return sorted(hits)


def classify_miss(
    gji_row: Mapping[str, Any],
    *,
    studio_known: bool,
    registry_ids: set[str],
    live_boards: Mapping[str, bool] | None = None,
    company_key: str = "company",
) -> str:
    board = str(gji_row.get("source_url") or "")
    board_registered = bool(registry_ids_for_board(registry_ids, board))
    if live_boards is not None and live_boards.get(board) is False:
        return BUCKET_NOT_ON_BOARD
    if live_boards is not None and live_boards.get(board) is True and not board_registered:
        return BUCKET_UNREGISTERED
    if studio_known:
        # The studio is present in the feed, so this is a per-role gap rather than
        # a missing board: either the board changed or an adapter dropped it.
        return BUCKET_STUDIO_COVERED
    if board_registered or registry_ids:
        return BUCKET_REGISTERED_NO_ROLE if board_registered else BUCKET_UNREGISTERED
    return BUCKET_UNKNOWN


def audit(
    gji_rows: Sequence[Mapping[str, Any]],
    feed_rows: Sequence[Mapping[str, Any]],
    *,
    query: str = "",
    region: str = "EU",
    registry_ids: set[str] | None = None,
    live_boards: Mapping[str, bool] | None = None,
    gji_company_key: str = "company",
    feed_company_key: str = "company",
    categories: set[str] | None = None,
) -> dict[str, Any]:
    ids = registry_ids or set()
    feed_index = build_feed_index(feed_rows, company_key=feed_company_key)
    prefix_index = build_label_prefix_index(feed_index)
    url_index = build_url_index(feed_rows)
    matched_feed: list[Mapping[str, Any]] = []
    matched_gji: list[Mapping[str, Any]] = []
    misses: list[dict[str, Any]] = []
    label_mismatches: list[dict[str, Any]] = []
    buckets: dict[str, int] = {}
    match_basis: dict[str, int] = {}
    missing_game_roles = 0
    missing_non_game_roles = 0

    for gji_row in gji_rows:
        if region.upper() == "EU" and not in_europe(gji_row):
            continue
        if categories is not None and str(gji_row.get("category")) not in categories:
            continue
        if not matches_query(gji_row, query, company_key=gji_company_key):
            continue
        found, fuzzy_label, studio_known, basis = find_match(
            gji_row,
            feed_index,
            company_key=feed_company_key,
            prefix_index=prefix_index,
            url_index=url_index,
        )
        if found is not None:
            match_basis[basis or "title"] = match_basis.get(basis or "title", 0) + 1
            if fuzzy_label:
                # Same role under a different studio label: a naming artefact, not
                # a gap, so it is recorded but kept out of the miss list.
                label_mismatches.append(
                    {
                        "gjiCompany": gji_row.get(gji_company_key),
                        "feedCompany": found.get(feed_company_key),
                    }
                )
            matched_gji.append(dict(gji_row))
            matched_feed.append(found)
            continue
        bucket = classify_miss(
            gji_row,
            studio_known=studio_known,
            registry_ids=ids,
            live_boards=live_boards,
            company_key=gji_company_key,
        )
        buckets[bucket] = buckets.get(bucket, 0) + 1
        # The role gate. The predicate is the production row filter --
        # the same `looks_like_game_job` call the parsers make -- applied
        # to the fields a GJI record carries. The pipeline also consults
        # tags, which the index does not publish, so a title-only match
        # is monotone in the pipeline's inputs: everything the gate calls
        # a game role is a role the pipeline keeps, and the gate's
        # non-game remainder may still contain roles the pipeline keeps
        # through tags. The game-role count is therefore a floor, never
        # an over-promise of recoverable coverage.
        game_role = looks_like_game_job(
            strip_bracket_prefix(gji_row.get("title")), gji_row.get(gji_company_key)
        )
        if game_role:
            missing_game_roles += 1
        else:
            missing_non_game_roles += 1
        misses.append(
            {
                "bucket": bucket,
                "gameRole": game_role,
                "title": gji_row.get("title"),
                "company": gji_row.get(gji_company_key),
                "ats": gji_row.get("source_ats"),
                "country": gji_row.get("country"),
                "location": gji_row.get("location"),
                "sourceUrl": gji_row.get("source_url"),
                "postedAt": gji_row.get("posted_at"),
                "firstSeen": gji_row.get("first_seen"),
            }
        )

    feed_hits = {id(row) for row in matched_feed}
    feed_only = [
        {
            "title": row.get("title"),
            "company": row.get(feed_company_key),
            "country": row.get("country"),
            "source": row.get("source"),
        }
        for row in feed_rows
        if id(row) not in feed_hits
        and (region.upper() != "EU" or in_europe(row))
        and matches_query(row, query, company_key=feed_company_key)
    ]
    return {
        "query": query,
        "region": region.upper(),
        "gjiConsidered": len(matched_gji) + len(misses),
        "gjiMatched": len(matched_gji),
        "gjiMatchedByUrl": match_basis.get("url", 0),
        "gjiMatchedByTitleOnly": match_basis.get("title", 0),
        "gjiMissing": len(misses),
        "gjiMissingGameRoles": missing_game_roles,
        "gjiMissingNonGameRoles": missing_non_game_roles,
        "feedOnlyCount": len(feed_only),
        "buckets": {name: buckets.get(name, 0) for name in BUCKET_ORDER if buckets.get(name)},
        "misses": sorted(misses, key=lambda row: (row["bucket"], str(row["company"]))),
        "labelMismatches": label_mismatches,
        "feedOnly": sorted(feed_only, key=lambda row: str(row["company"]))[:200],
        "caveats": [
            "role_not_on_board comes from an explicit live probe, never from a "
            "detail page resolving. Boards the probe could not decide are left "
            "out of the probe map entirely, so they fall through to the "
            "studio/registry buckets instead of being called delisted.",
            "The feed mixes ISO codes and country names and 73 values resolve to "
            "neither, so region counts are a lower bound.",
            "label_mismatches are counted as matched, not reported as gaps: GJI "
            "and Baluffo publish the same studio under different labels.",
            "The game-role split consults title and company only: GJI does not "
            "publish the tags the pipeline also reads, so the split is a floor "
            "on roles the pipeline would keep, never an over-count.",
        ],
    }


def render_summary(result: Mapping[str, Any]) -> str:
    lines = [
        f"query={result['query'] or '(any)'} region={result['region']}",
        f"GJI considered {result['gjiConsidered']}, matched {result['gjiMatched']}, "
        f"missing {result['gjiMissing']}, feed-only {result['feedOnlyCount']}",
        f"  matched by URL (definitive) : {result.get('gjiMatchedByUrl', 0)}",
        f"  matched by title only       : {result.get('gjiMatchedByTitleOnly', 0)}",
        f"  missing game roles          : {result.get('gjiMissingGameRoles', 0)}",
        f"  missing non-game roles      : {result.get('gjiMissingNonGameRoles', 0)}",
        "",
        "misses by bucket:",
    ]
    for name, count in (result.get("buckets") or {}).items():
        lines.append(f"  {count:>4}  {name}")
    lines.append("")
    lines.append("caveats:")
    for caveat in result.get("caveats") or []:
        lines.append(f"  - {caveat}")
    return "\n".join(lines)


def sweep(
    gji_rows: Sequence[Mapping[str, Any]],
    feed_rows: Sequence[Mapping[str, Any]],
    *,
    region: str = "EU",
    registry_ids: set[str] | None = None,
) -> dict[str, Any]:
    """Audit every discipline the index publishes, not one query at a time.

    One query at a time hides the shape of the gap. Sweeping the categories shows
    whether a discipline is thin because its boards are unregistered or because the
    whole discipline is served differently, which is the difference between
    registering boards and doing nothing.
    """
    categories = sorted({str(row.get("category")) for row in gji_rows if row.get("category")})
    per_category: dict[str, Any] = {}
    for category in categories:
        result = audit(
            gji_rows,
            feed_rows,
            region=region,
            registry_ids=registry_ids,
            categories={category},
        )
        considered = result["gjiConsidered"]
        per_category[category] = {
            "gjiConsidered": considered,
            "gjiMatched": result["gjiMatched"],
            "gjiMissing": result["gjiMissing"],
            "gjiMissingGameRoles": result["gjiMissingGameRoles"],
            "gjiMissingNonGameRoles": result["gjiMissingNonGameRoles"],
            "matchRatePct": round(result["gjiMatched"] * 100 / considered, 1)
            if considered
            else None,
            "buckets": result["buckets"],
        }
    overall = audit(gji_rows, feed_rows, region=region, registry_ids=registry_ids)
    # Worst coverage first: the point of a sweep is to lead with the thin disciplines.
    ranked = sorted(
        (c for c in per_category.items() if c[1]["gjiConsidered"]),
        key=lambda item: (item[1]["matchRatePct"] or 0.0, -item[1]["gjiConsidered"]),
    )
    return {
        "region": region.upper(),
        "overall": {
            "gjiConsidered": overall["gjiConsidered"],
            "gjiMatched": overall["gjiMatched"],
            "gjiMissing": overall["gjiMissing"],
            "matchRatePct": round(overall["gjiMatched"] * 100 / overall["gjiConsidered"], 1)
            if overall["gjiConsidered"]
            else None,
            "buckets": overall["buckets"],
        },
        "byCategory": dict(ranked),
        "caveats": overall["caveats"],
    }


def render_sweep(result: Mapping[str, Any]) -> str:
    overall = result["overall"]
    lines = [
        f"region={result['region']}  GJI considered {overall['gjiConsidered']}, "
        f"matched {overall['gjiMatched']} ({overall['matchRatePct']}%), "
        f"missing {overall['gjiMissing']}",
        "",
        f"{'discipline':<24} {'considered':>10} {'matched':>8} {'rate':>7}  top bucket",
        "-" * 84,
    ]
    for category, stats in result["byCategory"].items():
        buckets = stats["buckets"] or {}
        top = max(buckets.items(), key=lambda kv: kv[1])[0] if buckets else "-"
        lines.append(
            f"{category[:24]:<24} {stats['gjiConsidered']:>10} {stats['gjiMatched']:>8} "
            f"{str(stats['matchRatePct']):>7}  {top}"
        )
    lines.append("")
    for caveat in result.get("caveats") or []:
        lines.append(f"  - {caveat}")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--feed", required=True, type=Path, help="Baluffo job feed JSON")
    parser.add_argument("--gji", required=True, type=Path, help="gamesjobsindex jobs.json")
    parser.add_argument("--out", type=Path, help="directory for report.json + summary.txt")
    parser.add_argument("--registry", type=Path, help="source-registry-active.json[.gz]")
    parser.add_argument("--query", default="", help="title/company search text")
    parser.add_argument("--region", default="EU", choices=["EU", "ANY"])
    parser.add_argument(
        "--sweep",
        action="store_true",
        help="audit every discipline the index publishes instead of one query",
    )
    parser.add_argument(
        "--live-probe",
        type=Path,
        help="JSON mapping source_url -> bool (does the board still serve the role?). "
        "Probing stays a separate, reviewable step; supplying this is what lets the "
        "tool separate a live gap from a delisted upstream listing.",
    )
    args = parser.parse_args(argv)

    live_boards: dict[str, bool] | None = None
    if args.live_probe:
        payload = read_json(args.live_probe)
        if isinstance(payload, Mapping):
            live_boards = {str(key): bool(value) for key, value in payload.items()}

    gji_rows = load_gji_records(args.gji)
    feed_rows = load_feed_rows(args.feed)
    registry_ids = load_registry_ids(args.registry)

    if args.sweep:
        result = sweep(gji_rows, feed_rows, region=args.region, registry_ids=registry_ids)
        text = render_sweep(result)
        if args.out:
            args.out.mkdir(parents=True, exist_ok=True)
            (args.out / "sweep.json").write_text(
                json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8"
            )
            (args.out / "sweep.txt").write_text(text + "\n", encoding="utf-8")
        print(text)
        return 0

    result = audit(
        gji_rows,
        feed_rows,
        query=args.query,
        region=args.region,
        registry_ids=registry_ids,
        live_boards=live_boards,
    )
    summary = render_summary(result)
    if args.out:
        args.out.mkdir(parents=True, exist_ok=True)
        (args.out / "report.json").write_text(
            json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        (args.out / "summary.txt").write_text(summary + "\n", encoding="utf-8")
    print(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
