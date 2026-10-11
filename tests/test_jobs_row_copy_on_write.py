"""The copy-on-write contract for the three sites that used to build payloads up front.

``BALUFFO_ROW_PAYLOAD`` converted 43 reader sites to the payload view. Three sites stayed writers
because they write to what they were handed: the observed lifecycle list (whose entry builder wrote
into the row), the projected lifecycle list, and finalize's published output rows (which the
location guardrail rewrites and the serializers read). Those three now read through the view and
copy on write -- a shallow copy built only where a row is actually written -- which is what lets
them stop building ~90k whole-row payloads before anything needs one.

Two properties carry that, and both are pinned here rather than trusted:

* ``mutable_payload`` is the writer's half: one shallow copy for a canonical row, containers still
  the row's, and nothing copied for a dict the caller already owns;
* the sites read the row *before* deciding to copy it, so a carried row is the object handed in and
  a dropped row costs no payload at all.

``BALUFFO_ROW_COPY_ON_WRITE=legacy`` restores the up-front build at exactly those three sites, so
the corpus A/B can price them on their own instead of riding on the payload switch's -23%.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pytest

from src.jobs.models import CANONICAL_JOB_CONTAINER_FIELDS, CanonicalJob
from src.jobs.row_payload import mutable_payload, row_payload


def _rich_row() -> CanonicalJob:
    """A row that exercises the container fields, unicode text and empty containers."""

    return CanonicalJob.from_mapping(
        {
            "id": "row-1",
            "title": "Senior ゲーム Engineer",
            "company": "Studio Ltd.",
            "city": "東京",
            "country": "Japan",
            "jobLink": "https://example.com/jobs/row-1",
            "description": "A description long enough to be worth copying: " + ("x" * 500),
            "source": "static_source::demo",
            "sourceJobId": "demo-1",
            "availabilityId": "avail-1",
            "availabilityEvidence": {"checked": ["2026-10-10"], "code": 200},
            "sourceBundle": [{"source": "static_source::demo", "url": "https://example.com/a"}],
            "locations": [{"city": "東京", "country": "Japan"}],
        }
    )


@contextmanager
def _copy_on_write_mode(*, legacy: bool) -> Iterator[None]:
    """Flip the third switch in the two modules that bound it at import time."""

    from src.jobs import row_payload as row_payload_module
    from src.jobs import state_lifecycle_orchestration as orchestration

    previous = (
        row_payload_module.LEGACY_ROW_COPY_ON_WRITE,
        orchestration.LEGACY_ROW_COPY_ON_WRITE,
    )
    row_payload_module.LEGACY_ROW_COPY_ON_WRITE = legacy
    orchestration.LEGACY_ROW_COPY_ON_WRITE = legacy
    try:
        yield
    finally:
        (
            row_payload_module.LEGACY_ROW_COPY_ON_WRITE,
            orchestration.LEGACY_ROW_COPY_ON_WRITE,
        ) = previous


def test_mutable_payload_copies_a_canonical_row_once_and_only_at_the_top_level() -> None:
    """The writer's half of the split: one shallow copy, containers still the row's.

    A site that reads can use the view; a site that writes needs a dict, and this is the only
    place that pays for one. Copying only the top level is what keeps the copy cheap, and it is
    why the converted sites assign fresh containers instead of mutating inherited ones.
    """

    row = _rich_row()
    payload = mutable_payload(row)
    assert isinstance(payload, dict)
    assert payload == row.to_dict()
    for name in CANONICAL_JOB_CONTAINER_FIELDS:
        assert payload[name] is getattr(row, name), "copy-on-write must not deep-copy containers"

    payload["title"] = "written by the site"
    payload["locations"] = [{"city": "nowhere"}]
    assert row.title == "Senior ゲーム Engineer"
    assert row.locations == [{"city": "東京", "country": "Japan"}]


def test_mutable_payload_hands_back_what_a_writer_already_owns() -> None:
    """A dict row -- the legacy arm, and any caller holding a payload -- is not copied again."""

    plain = {"title": "dict row", "locations": [{"city": "Remote"}]}
    assert mutable_payload(plain) is plain

    class RowLike:
        def __init__(self) -> None:
            self.payload: dict[str, Any] = {"title": "duck row"}

        def to_dict(self) -> dict[str, Any]:
            return self.payload

    duck = RowLike()
    # A row-like double is deliberately outside the declared input type: that escape hatch is
    # what the pipeline's own doubles rely on, so it is exercised with the annotation set aside.
    assert mutable_payload(duck) is duck.payload  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="expects a CanonicalJob or a mapping"):
        mutable_payload(object())  # type: ignore[arg-type]


def test_projection_carries_read_only_rows_through_and_copies_only_what_it_writes() -> None:
    """The projector is the copy-on-write site: a dropped or carried row costs no payload."""

    from src.jobs.state_lifecycle_orchestration import _project_available_payload_rows

    row = _rich_row()
    view = row_payload(row)

    # No lifecycle entry: carried on as the view itself, so nothing is copied.
    projected = _project_available_payload_rows([row], {})
    assert len(projected) == 1
    assert not isinstance(projected[0], dict)
    assert projected[0]["title"] == row.title
    # A view handed in comes back identical -- a hit here is proof the row was not copied.
    assert _project_available_payload_rows([view], {})[0] is view

    # An entry that is not available drops the row *before* any payload is built for it.
    unavailable = {"status": "active", "availabilityStatus": "unavailable"}
    assert _project_available_payload_rows([row], {"availability:avail-1": unavailable}) == []

    # Kept and projected: exactly one writable payload, with the row's containers still shared.
    entry = {
        "status": "active",
        "availabilityId": "avail-1",
        "firstSeenAt": "2026-04-01T00:00:00+00:00",
        "lastSeenAt": "2026-04-02T00:00:00+00:00",
        "availabilityEvidence": {
            "kind": "source_present",
            "checkedAt": "2026-04-02T00:00:00+00:00",
        },
    }
    projected = _project_available_payload_rows([row], {"availability:avail-1": entry})
    assert len(projected) == 1
    payload = projected[0]
    assert isinstance(payload, dict)
    assert payload["firstSeenAt"] == "2026-04-01T00:00:00+00:00"
    assert payload["status"] == "active"
    assert payload["locations"] is row.locations
    assert row.firstSeenAt != "2026-04-01T00:00:00+00:00", "the projector wrote into the row"

    # The control arm builds a payload for a row it will not use, which is the behaviour the
    # switch exists to price: the same carried row comes back as a dict there, not as the view.
    with _copy_on_write_mode(legacy=True):
        control = _project_available_payload_rows([row], {})
    assert isinstance(control[0], dict)
    with _copy_on_write_mode(legacy=False):
        assert not isinstance(_project_available_payload_rows([row], {})[0], dict)


def test_the_published_row_guardrail_assigns_fresh_containers_instead_of_mutating_shared_ones() -> (
    None
):
    """Site C's one writer: the published payload shares the row's containers.

    The final location guardrail is the only thing downstream of the sector gate that changes a
    published row, so with a shallow payload it must *replace* the container it fixes. An in-place
    edit would reach back into the row the view points at -- and the deep copies these rows used to
    be made of would have hidden that, which is why the property is pinned rather than assumed.
    """

    from src.jobs.finalize_locations import _apply_final_location_quality_guardrail

    row = CanonicalJob.from_mapping(
        {
            "id": "row-dup",
            "title": "Role",
            "city": "Berlin",
            "country": "DE",
            # A duplicate entry, which the guardrail dedupes: a fix it is certain to make.
            "locations": [{"city": "Berlin", "country": "DE"}, {"city": "Berlin", "country": "DE"}],
        }
    )
    payload = mutable_payload(row)
    before_locations = row.locations
    before_entries = [dict(item) for item in row.locations]

    _apply_final_location_quality_guardrail([payload])

    assert payload["locations"] == [{"city": "Berlin", "country": "DE"}], "the duplicate survived"
    assert payload["locations"] is not before_locations, "the fix edited the shared list in place"
    assert row.locations is before_locations
    assert [dict(item) for item in row.locations] == before_entries, (
        "the row was reached through the view"
    )


def test_lifecycle_state_leaves_the_rows_it_reads_untouched() -> None:
    """The two lifecycle lists are read through the view now, so no row may be written to."""

    from src.jobs.state_lifecycle import apply_job_lifecycle_state

    row = _rich_row()
    before_evidence = row.availabilityEvidence
    before_locations = row.locations
    before_state = (row.status, row.firstSeenAt, row.lastSeenAt, row.removedAt, row.title)
    entry = {
        "status": "active",
        "title": "Role from the entry",
        "availabilityId": "avail-1",
        "firstSeenAt": "2026-04-01T00:00:00+00:00",
        "lastSeenAt": "2026-04-02T00:00:00+00:00",
    }

    rows, _next_rows, _archive, _summary = apply_job_lifecycle_state(
        deduped_rows=[row],
        lifecycle_rows={"availability:avail-1": entry},
        finished_at="2026-04-02T00:00:00+00:00",
        allow_mark_missing=False,
        observed_rows=[row],
    )

    assert len(rows) == 1
    assert rows[0].firstSeenAt == "2026-04-01T00:00:00+00:00", "the projection did not run"
    assert before_state[1] != "2026-04-01T00:00:00+00:00", "the row already looked projected"
    assert row.availabilityEvidence is before_evidence
    assert row.locations is before_locations
    assert (row.status, row.firstSeenAt, row.lastSeenAt, row.removedAt, row.title) == before_state
