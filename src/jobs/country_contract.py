"""Country acceptance contract helpers (leaf, pure).

AI boundary owns: locating a contract file relative to a caller-supplied anchor,
and deriving country label lookups from an already-parsed
``country_acceptance.json`` payload.
AI boundary implement in: this file for the derivations; caching and file IO stay
with the loader in ``src.jobs.text_utils``.
AI boundary search before contracts: country normalisation, location parsing, and
the packaged-contract fallback tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused country tests.

This is a leaf by necessity: ``src.jobs.normalizers`` and ``src.jobs.text_utils``
import each other, so neither can host shared country logic. It deliberately holds
no cached state and no module-level ``__file__`` lookup of its own — the anchor is
always passed in, because the contract loaders are redirected in tests by
monkeypatching ``text_utils.__file__`` to simulate a packaged ship layout, and a
resolver anchored on this module would silently read the repo copy instead.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

COUNTRY_ACCEPTANCE_CONTRACT_NAME = "country_acceptance.json"


def resolve_contract_path(filename: str, *, anchor: str | Path) -> Path:
    """Find ``data/contracts/<filename>`` by walking up from ``anchor``.

    Walking rather than assuming a fixed depth is what makes the packaged layout
    work: from ``app/versions/<v>/src/jobs/<module>.py`` the first match is the
    version-local contract, and a layout without one falls through to the shared
    ``data/contracts`` above it.
    """
    base = Path(anchor).resolve()
    for parent in base.parents:
        candidate = parent / "data" / "contracts" / filename
        if candidate.exists():
            return candidate
    return base.parents[2] / "data" / "contracts" / filename


def country_code_to_name(raw: Mapping[str, Any]) -> dict[str, str]:
    """ISO 3166-1 alpha-2 to the contract's country name, verbatim.

    Ten of these names never appear in ``acceptedExactLabels`` — Anguilla,
    Bermuda, European Union, Gibraltar, Greenland, Hong Kong, Isle of Man, Macau,
    Montserrat and Puerto Rico — so a loader reading only that list could not
    resolve territories the contract itself lists.
    """
    return {
        str(code or "").strip().upper(): str(name or "").strip()
        for code, name in (raw.get("countryNameByCode") or {}).items()
        if str(code or "").strip() and str(name or "").strip()
    }


def country_name_to_code(raw: Mapping[str, Any]) -> dict[str, str]:
    """Country name (lowercase) to ISO 3166-1 alpha-2, unambiguous entries only.

    A name claimed by more than one code is skipped rather than guessed.
    ``United Kingdom`` is the live case: the contract carries both ``GB`` and
    ``UK``, so silently picking either would make the persisted value depend on
    dict ordering.
    """
    claimed: dict[str, set[str]] = {}
    for code, name in (raw.get("countryNameByCode") or {}).items():
        label = str(name or "").strip().lower()
        if label:
            claimed.setdefault(label, set()).add(str(code or "").strip().upper())
    return {
        label: next(iter(codes))
        for label, codes in claimed.items()
        if len(codes) == 1 and next(iter(codes))
    }


__all__ = [
    "COUNTRY_ACCEPTANCE_CONTRACT_NAME",
    "country_code_to_name",
    "country_name_to_code",
    "resolve_contract_path",
]
