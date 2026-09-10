"""Single policy leaf for the source-health counter alias pairs.

One fact — per-source fetch counters — used to be stored under six names:
three canonical fields maintained by the run appliers (`lastKeptCount`,
`consecutiveZeroKept`, `consecutiveFailures`) plus three legacy aliases
(`lastJobsKept`, `zeroJobStreak`, `failureCount`) written back by the health
derive. The alias-first reader ordering caused a self-perpetuating split brain
(stale alias overriding the fresh counter; healthy rows labeled warning) fixed
in commit `ca778258`; this leaf makes the canonical-first precedence the single
definition so no consumer hand-rolls alias fallbacks again.

Importable from `src/jobs`, `src/bridge`, `src/shared`, and root-level scripts —
no composition-root imports (per AGENTS.md code boundaries).

The migration plan lives in `docs/plans/source-health-counter-collapse-plan.md`.
"""

from __future__ import annotations

from typing import Any

#: Alias -> canonical. `zeroKeptStreak` is a defensive third spelling for the
#: zero-kept streak; audited 2026-09-09 with zero occurrences in real state,
#: kept only so readers that encounter legacy rows keep converging.
COUNTER_ALIASES: dict[str, str] = {
    "lastJobsKept": "lastKeptCount",
    "zeroJobStreak": "consecutiveZeroKept",
    "zeroKeptStreak": "consecutiveZeroKept",
    "failureCount": "consecutiveFailures",
}

CANONICAL_COUNTERS: tuple[str, ...] = (
    "lastKeptCount",
    "consecutiveZeroKept",
    "consecutiveFailures",
)


def read_counter(row: dict[str, Any], canonical_name: str) -> Any:
    """First present value for a counter: canonical, then its aliases.

    Returns the raw value (caller owns coercion); missing keys yield ``None``
    so callers can distinguish "absent" from a stored zero.
    """

    value = row.get(canonical_name)
    if value is not None:
        return value
    for alias, canonical in COUNTER_ALIASES.items():
        if canonical != canonical_name:
            continue
        value = row.get(alias)
        if value is not None:
            return value
    return None


def emit_with_aliases(row: dict[str, Any]) -> dict[str, Any]:
    """Wire-side emission: fill alias keys from their canonical counters.

    Aliases are always canonical-derived so the two names can never diverge on
    an emitted payload. Missing canonical counters are left untouched (no
    fabricated zeros).
    """

    for alias, canonical in COUNTER_ALIASES.items():
        if canonical in row:
            row[alias] = row[canonical]
    return row


def _absent(value: Any) -> bool:
    """Absent for heal purposes: missing, None, or blank-string."""

    return value is None or (isinstance(value, str) and not value.strip())


def fill_canonical_counters(entry: dict[str, Any]) -> dict[str, Any]:
    """Persistence-side heal: canonical counters := aliases; alias keys consumed.

    This is the only direction the persistence funnel (state normalizer, save
    path) may use: legacy alias-only rows gain the canonical name so the
    whitelist coercion never zeroes a legacy counter, and the alias spellings
    are consumed (removed) — persisted state must never carry them. An earlier
    combined heal instead re-added alias keys whenever a canonical counter was
    present, which is how aliases re-entered persisted state through every
    normalize+save funnel even after the derive stopped emitting them (found
    in the Phase-4 live acceptance audit, 2026-09-09). Alias spellings on
    payloads are wire-side (Phase 5 surface); they are re-added at emit time
    via emit_with_aliases, never persisted.
    """

    for canonical in CANONICAL_COUNTERS:
        if _absent(entry.get(canonical)):
            alias_value = read_counter(entry, canonical)
            if not _absent(alias_value):
                entry[canonical] = alias_value
    for alias in COUNTER_ALIASES:
        entry.pop(alias, None)
    return entry


def heal_counter_aliases(entry: dict[str, Any]) -> dict[str, Any]:
    """Display/wire-side heal: aliases := canonical; canonical := alias if absent.

    For joined display rows (bridge registry conflicts) and other Phase-5
    wire surfaces where the alias names are still part of the visible
    contract: a canonical counter wins over a stale alias, and an alias-only
    legacy row gains the canonical name. Persistence funnels must use
    fill_canonical_counters instead — writing alias keys into persisted state
    is the Phase-4 single-writer violation this leaf guards against. Blank
    strings count as absent (legacy wire rows use "" for unset).
    """

    fill_canonical_counters(entry)  # canonical := alias; input aliases consumed
    for canonical in CANONICAL_COUNTERS:
        if _absent(entry.get(canonical)):
            continue
        for alias, target in COUNTER_ALIASES.items():
            if target == canonical:
                entry[alias] = entry[canonical]
    return entry
