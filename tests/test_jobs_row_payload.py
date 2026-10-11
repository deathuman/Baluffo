"""The payload contracts the row-payload conversion rests on.

``CanonicalJob.to_dict`` is read by tests and by callers that rebind a key of the payload they
were handed, and the conversion sites read fields through ``row_payload``'s view instead of
building that payload at all. Three properties carry the change:

* the cheap build is equal in value (and key order) to the ``dataclasses.asdict`` it replaced,
  and ``CANONICAL_JOB_CONTAINER_FIELDS`` covers exactly the mutable fields ``asdict`` copied;
* every caller still gets its own payload, with its own nested containers;
* ``BALUFFO_ROW_PAYLOAD=legacy`` restores the pre-change build on both sides -- which is what
  makes the corpus A/B a paired study on one tree rather than two trees.

The view's one intentional difference is pinned too: it shares the row's containers, which is
why a site that mutates a container keeps calling ``to_dict()``.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import asdict, fields
from pathlib import Path
from typing import Any

import pytest

from src.jobs.models import (
    CANONICAL_JOB_CONTAINER_FIELDS,
    CANONICAL_JOB_FIELDS,
    CanonicalJob,
)
from src.jobs.row_payload import RowPayload, row_payload

REPO_ROOT = Path(__file__).resolve().parents[1]


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


def _empty_row() -> CanonicalJob:
    return CanonicalJob()


def test_to_dict_matches_asdict_in_value_and_key_order() -> None:
    for row in (_rich_row(), _empty_row()):
        payload = row.to_dict()
        reference = asdict(row)
        assert payload == reference
        assert list(payload) == list(reference)


def test_container_fields_cover_exactly_the_mutable_fields() -> None:
    """The cheap build copies what ``asdict`` deep-copied, and nothing else needs copying."""

    row = _rich_row()
    mutable = sorted(
        field.name
        for field in fields(CanonicalJob)
        if isinstance(getattr(row, field.name), (dict, list, set))
    )
    assert mutable == sorted(CANONICAL_JOB_CONTAINER_FIELDS)
    payload = row.to_dict()
    for name in CANONICAL_JOB_CONTAINER_FIELDS:
        assert payload[name] is not getattr(row, name)
    assert len(payload) == len(CANONICAL_JOB_FIELDS)


def test_to_dict_gives_every_caller_a_fresh_payload() -> None:
    row = _rich_row()
    first = row.to_dict()
    second = row.to_dict()
    assert first is not second

    first["title"] = "rebound by one caller"
    first["locations"].append({"city": "Elsewhere"})
    first["availabilityEvidence"]["checked"].append("mutated")
    first["sourceBundle"].clear()

    assert row.title == "Senior ゲーム Engineer"
    assert len(row.locations) == 1
    assert row.availabilityEvidence["checked"] == ["2026-10-10"]
    assert len(row.sourceBundle) == 1
    assert second["title"] == row.title
    assert second["locations"] == row.locations


def test_row_payload_view_answers_the_payload_values() -> None:
    row = _rich_row()
    view = row_payload(row)
    payload = row.to_dict()
    assert isinstance(view, RowPayload)
    assert dict(view) == payload
    assert view.get("title") == row.title
    assert view["source"] == row.source
    assert list(view) == list(CANONICAL_JOB_FIELDS)
    assert len(view) == len(payload)
    assert "jobLink" in view
    assert "notAField" not in view
    assert view.get("notAField", "fallback") == "fallback"
    with pytest.raises(KeyError):
        view["notAField"]


def test_row_payload_view_is_read_only() -> None:
    view: Any = row_payload(_rich_row())
    with pytest.raises(TypeError):
        view["title"] = "a view is not a payload"


def test_row_payload_view_shares_the_row_containers() -> None:
    """The documented boundary: reads through the view are the row's own containers.

    This is why a site that rebinds a key, mutates a container, copies or serializes the
    payload keeps calling ``to_dict()``; the conversion sites are the ones that only read.
    """

    row = _rich_row()
    view = row_payload(row)
    assert view["locations"] is row.locations
    assert view.get("availabilityEvidence") is row.availabilityEvidence


def test_row_payload_copies_a_plain_dict_row() -> None:
    """A dict row is copied as shallowly as the ``dict(row)`` idiom it replaces did."""

    plain = {"title": "dict row", "locations": [{"city": "Remote"}]}
    copied = row_payload(plain)
    assert isinstance(copied, dict)
    assert copied == plain
    assert copied is not plain
    copied["title"] = "mutated copy"
    assert plain["title"] == "dict row"
    assert copied["locations"] is plain["locations"]


def test_row_payload_keeps_a_row_like_objects_own_payload() -> None:
    """Row-like duck types (test doubles, adapter shims) answer their own ``to_dict``.

    That is what the bare ``row.to_dict()`` call this replaces did, and the pipeline's own test
    doubles depend on it, so it is contract rather than an implementation detail.
    """

    class RowLike:
        def __init__(self) -> None:
            self.payload: dict[str, Any] = {"title": "duck row", "locations": [{"city": "Remote"}]}

        def to_dict(self) -> dict[str, Any]:
            return self.payload

    duck = RowLike()
    assert row_payload(duck) is duck.payload  # type: ignore[arg-type]
    assert row_payload(duck) == duck.payload  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="expects a CanonicalJob or a mapping"):
        row_payload(object())  # type: ignore[arg-type]


def _switch_probe(value: str | None, *, copy_on_write: str | None = None) -> dict[str, Any]:
    """One fresh interpreter, reporting which build each switch selected and what it returns."""

    program = (
        "import json, sys\n"
        "from dataclasses import asdict\n"
        "from src.jobs import models\n"
        "from src.jobs import row_payload as row_payload_module\n"
        "from src.jobs.row_payload import mutable_payload, row_payload\n"
        "row = models.CanonicalJob.from_mapping(\n"
        "    {'title': 't', 'locations': [{'city': 'Remote'}],\n"
        "     'availabilityEvidence': {'code': 200}, 'sourceBundle': [{'source': 's'}]}\n"
        ")\n"
        "view = row_payload(row)\n"
        "plain = {'title': 'dict row'}\n"
        "writable = mutable_payload(row)\n"
        "print(json.dumps({\n"
        "    'legacy': models.ROW_PAYLOAD_LEGACY,\n"
        "    'viewType': type(view).__name__,\n"
        "    'viewIsDict': isinstance(view, dict),\n"
        "    'viewEqualsAsdict': dict(view) == asdict(row),\n"
        "    'toDictEqualsAsdict': row.to_dict() == asdict(row),\n"
        "    'toDictSharesContainer': row.to_dict()['locations'] is row.locations,\n"
        "    'writableIsDict': isinstance(writable, dict),\n"
        "    'writableIsView': writable is view,\n"
        "    'writableSharesContainer': writable['locations'] is row.locations,\n"
        "    'writableDictRowIsSameObject': mutable_payload(plain) is plain,\n"
        "    'copyOnWriteLegacy': row_payload_module.LEGACY_ROW_COPY_ON_WRITE,\n"
        "}))\n"
    )
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in {"BALUFFO_ROW_PAYLOAD", "BALUFFO_ROW_COPY_ON_WRITE"}
    }
    env["PYTHONPATH"] = os.pathsep.join(
        [str(REPO_ROOT), *([env["PYTHONPATH"]] if env.get("PYTHONPATH") else [])]
    )
    if value is not None:
        env["BALUFFO_ROW_PAYLOAD"] = value
    if copy_on_write is not None:
        env["BALUFFO_ROW_COPY_ON_WRITE"] = copy_on_write
    completed = subprocess.run(  # noqa: S603 - a fixed program in this interpreter, no shell
        [sys.executable, "-c", program],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    report = json.loads(completed.stdout.strip().splitlines()[-1])
    assert isinstance(report, dict), report
    return report


def test_payload_switch_selects_the_build_in_a_fresh_interpreter() -> None:
    """``BALUFFO_ROW_PAYLOAD=legacy`` restores the old work; unset takes the cheap build."""

    default = _switch_probe(None)
    assert default["legacy"] is False
    assert default["viewType"] == "RowPayload"
    assert default["viewIsDict"] is False
    assert default["viewEqualsAsdict"] is True
    assert default["toDictEqualsAsdict"] is True
    assert default["toDictSharesContainer"] is False
    assert default["writableIsDict"] is True
    assert default["writableIsView"] is False
    assert default["writableSharesContainer"] is True
    assert default["writableDictRowIsSameObject"] is True

    legacy = _switch_probe("legacy")
    assert legacy["legacy"] is True
    assert legacy["viewType"] == "dict"
    assert legacy["viewIsDict"] is True
    assert legacy["viewEqualsAsdict"] is True
    assert legacy["toDictEqualsAsdict"] is True
    assert legacy["toDictSharesContainer"] is False
    assert legacy["writableIsDict"] is True
    assert legacy["writableIsView"] is False
    assert legacy["writableSharesContainer"] is False
    assert legacy["writableDictRowIsSameObject"] is True

    copy_on_write = _switch_probe(None, copy_on_write="legacy")
    assert copy_on_write["copyOnWriteLegacy"] is True
    assert copy_on_write["legacy"] is False, "the two switches must be independent"
    assert _switch_probe(None, copy_on_write="VIEW")["copyOnWriteLegacy"] is False

    for value in ("LEGACY", "legacy "):
        assert _switch_probe(value)["legacy"] is True
    assert _switch_probe("view")["legacy"] is False
