"""Read-only payload access for canonical rows.

``CanonicalJob.to_dict()`` is the row's public payload, but most callers want one or two
fields of it. A replay of the reference subset epoch measured **2,076,173** conversions for
30,053 rows -- ~69 per row, **86% of them redundant** -- with 71 caller frames each reading a
field or two out of a full deep copy. Those reads ask this view instead:

    source = clean_text(row_payload(job).get("source"))

The view answers ``get``/``[]``/``in``/``items`` from the row itself and allocates nothing, so
the payload is built only where it is actually needed: a site that rebinds keys, mutates
nested containers, calls ``.copy()`` or serializes it keeps calling ``to_dict()``, which is
what such a site needs. That split is enforced rather than trusted -- the payload view is a
``Mapping`` and not a ``dict``, so ``mypy`` rejects a write through it
(``Unsupported target for indexed assignment``) and a missing ``.copy()``.

A plain ``dict`` row is still copied, exactly as the ``row.to_dict() if isinstance(row,
CanonicalJob) else dict(row)`` idiom it replaces did, so callers that hand in payload rows
see no change at all -- and a row-like duck type (a test double, an adapter shim) keeps
answering its own ``to_dict``, the way the bare ``row.to_dict()`` call it replaces did.

``BALUFFO_ROW_PAYLOAD=legacy`` restores that whole-row conversion in both this view and
``models.CanonicalJob.to_dict``, so the corpus A/B compares the change against the code it
replaced while ``src/`` stays byte-identical across the arms.

``mutable_payload`` is the writer's half of the same split. A site that has to *write* cannot use
a view, so it takes one shallow copy here -- and only here -- which is what lets the three sites
that were kept as writers (the observed and projected lifecycle lists, and finalize's published
output rows) read through the view and copy on write instead of building a whole-row payload up
front.
"""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping
from typing import Any

from src.jobs.models import CANONICAL_JOB_FIELDS, ROW_PAYLOAD_LEGACY, CanonicalJob

__all__ = [
    "LEGACY_ROW_COPY_ON_WRITE",
    "ROW_COPY_ON_WRITE_ENV",
    "RowPayload",
    "mutable_payload",
    "row_payload",
]


class RowPayload(Mapping[str, Any]):
    """A canonical row's fields read on demand, instead of materialised into a payload."""

    __slots__ = ("_row",)

    def __init__(self, row: CanonicalJob) -> None:
        self._row = row

    def __getitem__(self, key: str) -> Any:
        try:
            return getattr(self._row, key)
        except AttributeError as exc:
            raise KeyError(key) from exc

    def __iter__(self) -> Iterator[str]:
        return iter(CANONICAL_JOB_FIELDS)

    def __len__(self) -> int:
        return len(CANONICAL_JOB_FIELDS)

    def get(self, key: str, default: Any = None) -> Any:
        # Overridden rather than inherited: ``Mapping.get`` is ``__getitem__`` plus a caught
        # KeyError, and this is the call the hot paths make millions of times.
        return getattr(self._row, key, default)

    def __repr__(self) -> str:
        return f"RowPayload({self._row.id!r})"


def row_payload(row: CanonicalJob | Mapping[str, Any]) -> Mapping[str, Any]:
    """The row's field->value payload, read-only: a canonical row is viewed, a dict copied."""

    if isinstance(row, CanonicalJob):
        if ROW_PAYLOAD_LEGACY:
            return row.to_dict()
        return RowPayload(row)
    if isinstance(row, Mapping):
        return dict(row)
    # Row-like duck types: the test doubles and adapter shims the pipeline is handed answer their
    # own ``to_dict``, exactly as the bare ``row.to_dict()`` call this function replaces did.
    return _row_like_payload(row)


ROW_COPY_ON_WRITE_ENV = "BALUFFO_ROW_COPY_ON_WRITE"
# A third measurement switch, for the three sites that used to build a whole-row payload up front
# and are now read through the view: ``legacy`` restores that up-front build at those sites only,
# so they can be priced on their own instead of riding on ``BALUFFO_ROW_PAYLOAD``. It is read as a
# module global at call time, so the equivalence test can exercise both paths in one process.
LEGACY_ROW_COPY_ON_WRITE = os.environ.get(ROW_COPY_ON_WRITE_ENV, "").strip().lower() == "legacy"


def mutable_payload(row: CanonicalJob | Mapping[str, Any]) -> dict[str, Any]:
    """The row's payload as a dict a caller may write to: the copy-on-write half of the split.

    Built on ``row_payload``, so the switch governs both halves: the legacy arm hands back the
    whole-row conversion it always did (a dict, which the caller then writes into, exactly as
    before), and the shipped arm pays **one shallow copy** for a row it means to change.

    Only the top level is copied: the nested containers are shared with the row, so a site that
    has to change one assigns a *fresh* container rather than mutating the one it was handed.
    That is what the converted sites do, and it is why a payload built this way still owns every
    key it publishes.
    """

    if isinstance(row, dict):
        # Already a payload the caller owns -- the legacy arm's ``to_dict()`` copy, a payload dict
        # a caller is holding, a row-like double's own payload. Copying it again would only make
        # the control arm pay for work the change is meant to remove.
        return row
    payload = row_payload(row)
    return payload if isinstance(payload, dict) else dict(payload)


def _row_like_payload(row: object) -> Mapping[str, Any]:
    """A duck-typed row's own payload, or a named failure instead of an opaque ``TypeError``."""

    to_dict = getattr(row, "to_dict", None)
    if not callable(to_dict):
        raise TypeError(
            f"row_payload expects a CanonicalJob or a mapping, got {type(row).__name__}"
        )
    payload = to_dict()
    return payload if isinstance(payload, Mapping) else dict(payload)
