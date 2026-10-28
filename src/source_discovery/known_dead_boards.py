"""A durable memo of provider boards whose own listing endpoint answered 404/410.

AI boundary owns: the memo's identity rule, its TTL, and the probe wrapper that skips a memoized board.
AI boundary implement in: this file for the rule and the wrapper; the audit call site owns the path and
the config/env read.
AI boundary search before contracts: GameDevMap active dry-run probe entry, `probe._settles_the_board`.
AI boundary verify: `python -m pytest tests/source_discovery/test_known_dead_boards.py -q`.

Why this exists: `probe._settles_the_board` already treats a 404/410 from a provider's own endpoint as
settling the board — but only for that candidate, inside that run. Measured on the 2026-10-28 probe
profile, the audit's population carries **1,802** Lever and **3,194** Greenhouse candidates of which
**1,801 / 3,183** answer 404, all of them distinct. Every full re-audit (a reset, or any config-signature
change) re-probes every one of them: ~2,322 s of lane time, about a third of the probe phase. The
artifact's rejection rows cannot help, because a reset replaces the artifact.

Identity is **provider host + tenant** — never a studio label, a row URL or a registry id. The host is
the one that answered authoritatively for the board, and the tenant is the candidate's own account/slug,
which `probe` already requires to appear in the probed URL before it settles anything.

The memo only ever **skips**: a cold file, a disabled TTL or an unknown key leaves the candidate list
exactly as the shipped code probes it.
"""

from __future__ import annotations

import json
import os
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from src.shared.json_io import read_json, write_json_text
from src.shared.utils import now_iso, parse_iso

from .probe_runtime import ProbeResult

KNOWN_DEAD_BOARDS_FILENAME = "gamedevmap-known-dead-boards.json"
KNOWN_DEAD_BOARDS_SCHEMA_VERSION = 1
KNOWN_DEAD_BOARDS_TTL_ENV = "BALUFFO_DISCOVERY_KNOWN_DEAD_DAYS"
DEFAULT_TTL_DAYS = 7
# Bounds the file, not the saving: the audited population contributes ~5k entries and the cap is an
# order of magnitude above that, so it only ever bites on a directory-wide re-audit deficit.
MAX_ENTRIES = 50_000

# Only the adapters whose own listing endpoint answers authoritatively for the board it names. A
# provider whose endpoint can 404 while the board is served elsewhere must not be here: Workable's
# widget API answers 404 for a slug `apply.workable.com` serves, measured in the board-absence study.
AUTHORITATIVE_BOARD_HOSTS = {
    "greenhouse": "boards-api.greenhouse.io",
    "lever": "api.lever.co",
}
_BOARD_TOKEN_FIELDS = {"greenhouse": "slug", "lever": "account"}
_DEFINITIVE_NOT_FOUND_CODES = frozenset({404, 410})


def memo_ttl_days() -> int:
    """The memo's staleness bound in days; 0 disables the memo entirely."""
    raw = str(os.getenv(KNOWN_DEAD_BOARDS_TTL_ENV) or "").strip()
    if not raw:
        return DEFAULT_TTL_DAYS
    try:
        return max(0, int(raw))
    except ValueError:
        return DEFAULT_TTL_DAYS


def board_key(candidate: dict[str, Any]) -> str:
    """The board's durable identity (``<provider host>/<tenant>``), or "" when it has none.

    Empty for every adapter that is not an authoritative provider endpoint, for a candidate without a
    tenant token, and for a candidate whose own API URL does not address that provider — the same
    evidence `probe._settles_the_board` requires before it treats a 404 as settling the board.
    """
    if not isinstance(candidate, dict):
        return ""
    adapter = str(candidate.get("adapter") or "").strip().lower()
    host = AUTHORITATIVE_BOARD_HOSTS.get(adapter)
    if not host:
        return ""
    token = str(candidate.get(_BOARD_TOKEN_FIELDS[adapter]) or "").strip().lower()
    if not token or not _is_usable_token(token):
        return ""
    if host not in str(candidate.get("api_url") or "").strip().lower():
        return ""
    return f"{host}/{token}"


def _is_usable_token(token: str) -> bool:
    return all(char.isalnum() or char in "-_." for char in token)


def definitive_not_found(candidate: dict[str, Any], error: str) -> bool:
    """True when this failure says the board itself does not exist, from its own endpoint.

    The status is read back from the stored error text with the same parser the live probe path uses,
    because a result row outlives the exception that produced it.
    """
    if not board_key(candidate):
        return False
    from .probe import parse_probe_status

    return parse_probe_status(error) in _DEFINITIVE_NOT_FOUND_CODES


def _seen_at(entry: dict[str, Any]) -> str:
    """The entry's own clock: when it was last confirmed, else when it was first seen.

    The fallback matters on the write path: an entry that only carries `firstSeenAt` is knowledge this
    module did not date, and dropping it in `save_memo` would silently forget a board it had already
    learned.
    """
    return str(entry.get("lastSeenAt") or entry.get("firstSeenAt") or "")


def _entry_is_fresh(entry: dict[str, Any], cutoff: datetime) -> bool:
    seen = parse_iso(_seen_at(entry))
    return bool(seen and seen >= cutoff)


def load_memo(path: Path, *, now: datetime | None = None) -> dict[str, dict[str, Any]]:
    """Read the memo, dropping entries past the TTL and anything unparseable.

    Read-only and total: a corrupt or half-written file yields an empty memo rather than an error,
    because the memo is an optimization and must never be able to fail a run.
    """
    ttl_days = memo_ttl_days()
    if ttl_days <= 0:
        return {}
    cutoff = (now or datetime.now(UTC)) - timedelta(days=ttl_days)
    payload = read_json(Path(path), {})
    boards = payload.get("boards") if isinstance(payload, dict) else None
    if not isinstance(boards, dict):
        return {}
    memo: dict[str, dict[str, Any]] = {}
    for key, entry in boards.items():
        if not isinstance(entry, dict):
            continue
        board = str(key or "").strip()
        if board and _entry_is_fresh(entry, cutoff):
            memo[board] = dict(entry)
    return memo


def save_memo(path: Path, memo: dict[str, dict[str, Any]], *, now: datetime | None = None) -> None:
    """Write the memo, pruned to the TTL and the entry cap, most recently seen first."""
    ttl_days = memo_ttl_days()
    if ttl_days <= 0:
        return
    as_of = now or datetime.now(UTC)
    cutoff = as_of - timedelta(days=ttl_days)
    live = [
        (board, dict(entry))
        for board, entry in memo.items()
        if board and _entry_is_fresh(entry, cutoff)
    ]
    live.sort(key=lambda item: _seen_at(item[1]), reverse=True)
    payload = {
        "schemaVersion": KNOWN_DEAD_BOARDS_SCHEMA_VERSION,
        "updatedAt": now_iso(),
        "ttlDays": ttl_days,
        "boards": dict(live[:MAX_ENTRIES]),
    }
    write_json_text(Path(path), json.dumps(payload, indent=0, sort_keys=True) + "\n")


def split_known_dead(
    candidates: list[dict[str, Any]],
    memo: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[tuple[dict[str, Any], str, dict[str, Any]]]]:
    """Split into (to probe, memo hits), preserving the input order in both."""
    if not memo:
        return list(candidates), []
    alive: list[dict[str, Any]] = []
    skipped: list[tuple[dict[str, Any], str, dict[str, Any]]] = []
    for candidate in candidates:
        key = board_key(candidate)
        entry = memo.get(key) if key else None
        if key and entry is not None:
            skipped.append((candidate, key, entry))
        else:
            alive.append(candidate)
    return alive, skipped


def record_definitive_not_founds(
    results: list[ProbeResult],
    memo: dict[str, dict[str, Any]],
    *,
    now: datetime | None = None,
) -> int:
    """Add this batch's dead boards to the memo; returns the count of newly learned ones."""
    seen_at = now_iso()
    learned = 0
    for row, ok, _jobs, error, _duration_ms in results:
        if ok or not definitive_not_found(row, error):
            continue
        key = board_key(row)
        entry = memo.get(key)
        if entry is None:
            memo[key] = {"firstSeenAt": seen_at, "lastSeenAt": seen_at, "hits": 1}
            learned += 1
        else:
            entry["lastSeenAt"] = seen_at
            entry["hits"] = int(entry.get("hits") or 0) + 1
    return learned


def memo_hit_result(
    row: dict[str, Any],
    key: str,
    entry: dict[str, Any],
) -> ProbeResult:
    """The result a skipped candidate reports, shaped exactly like the probe failure it stands for."""
    memoized = str(entry.get("firstSeenAt") or _seen_at(entry))
    return (row, False, 0, f"known dead board (memoized {memoized}): {key}", 0)


async def probe_with_known_dead(
    candidates: list[dict[str, Any]],
    memo: dict[str, dict[str, Any]],
    *,
    probe: Callable[[list[dict[str, Any]]], Awaitable[list[ProbeResult]]],
    memo_path: Path | None,
    now: datetime | None = None,
) -> list[ProbeResult]:
    """Probe every candidate the memo has not already settled, then learn this batch's dead boards.

    `memo_path=None` is the memo switched off: one `probe(candidates)` and no learning, so a disabled
    run cannot start skipping boards part-way through it.

    A **cold** memo (empty dict, real path) still probes everything — and still learns, which is the
    run that populates it. An early return on `not memo` would have made the first run learn nothing
    and every later run learn from nothing, so the empty-memo case deliberately falls through.
    """
    if memo_path is None:
        return await probe(candidates)
    alive, skipped = split_known_dead(candidates, memo)
    results = await probe(alive) if alive else []
    if record_definitive_not_founds(results, memo, now=now):
        save_memo(memo_path, memo, now=now)
    return [*results, *(memo_hit_result(row, key, entry) for row, key, entry in skipped)]


__all__ = [
    "AUTHORITATIVE_BOARD_HOSTS",
    "DEFAULT_TTL_DAYS",
    "KNOWN_DEAD_BOARDS_FILENAME",
    "KNOWN_DEAD_BOARDS_SCHEMA_VERSION",
    "KNOWN_DEAD_BOARDS_TTL_ENV",
    "board_key",
    "definitive_not_found",
    "load_memo",
    "memo_hit_result",
    "memo_ttl_days",
    "probe_with_known_dead",
    "record_definitive_not_founds",
    "save_memo",
    "split_known_dead",
]
