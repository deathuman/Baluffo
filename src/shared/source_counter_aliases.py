"""Single policy leaf for the source-health counter alias pairs.

One fact — per-source fetch counters — used to be stored under six names:
three canonical fields maintained by the run appliers (`lastKeptCount`,
`consecutiveZeroKept`, `consecutiveFailures`) plus three legacy aliases
(`lastJobsKept`, `zeroJobStreak`, `failureCount`) written back by the health
derive. The alias-first reader ordering caused a self-perpetuating split brain
(stale alias overriding the fresh counter; healthy rows labeled warning) fixed
in commit `ca778258`; this leaf makes the canonical-first precedence the single
definition so no consumer hand-rolls alias fallbacks again.

Migration state (docs/plans/source-health-counter-collapse-plan.md): Phases
1–4 + 6 collapsed persisted state to canonical counters only (2026-09-09), and
Phase 5 (2026-09-10, operator-approved) dropped the alias spellings from the
bridge wire contract — no emit surface carries them anymore. The alias map
remains for reading legacy rows and for the persistence heal; new code must
never write an alias spelling anywhere.

Importable from `src/jobs`, `src/bridge`, `src/shared`, and root-level scripts —
no composition-root imports (per AGENTS.md code boundaries).
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


def _absent(value: Any) -> bool:
    """Absent for heal purposes: missing, None, or blank-string."""

    return value is None or (isinstance(value, str) and not value.strip())


def fill_canonical_counters(entry: dict[str, Any]) -> dict[str, Any]:
    """Persistence-side heal: canonical counters := aliases; alias keys consumed.

    This is the only heal direction in the repo: legacy alias-only rows gain
    the canonical name so the whitelist coercion never zeroes a legacy counter,
    and the alias spellings are consumed (removed) — persisted state must never
    carry them. An earlier combined heal instead re-added alias keys whenever a
    canonical counter was present, which is how aliases re-entered persisted
    state through every normalize+save funnel even after the derive stopped
    emitting them (found in the Phase-4 live acceptance audit, 2026-09-09).
    Since Phase 5 (2026-09-10) no wire surface re-adds them either: the bridge
    contract is canonical-only.
    """

    for canonical in CANONICAL_COUNTERS:
        if _absent(entry.get(canonical)):
            alias_value = read_counter(entry, canonical)
            if not _absent(alias_value):
                entry[canonical] = alias_value
    for alias in COUNTER_ALIASES:
        entry.pop(alias, None)
    return entry
