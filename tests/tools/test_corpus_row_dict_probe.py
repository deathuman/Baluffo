"""The priced-ablation payload patches the row conversion, and says so when it cannot.

The payload exists so a candidate change can be measured on the corpus harness *before* it is
committed, which means ``src/`` must stay byte-identical in both arms and the only thing that
differs is one environment variable. Three properties carry that, and each is pinned here: the
cheap build returns the same values ``asdict`` would (so a delta is the only difference between
the arms), each caller gets its own top-level dict (so a caller that rebinds a key cannot hand a
different row to the next reader), and an unset variable leaves the product untouched -- while a
*supported* variable that cannot be applied raises instead of quietly measuring nothing.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pytest

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.jobs.models import CanonicalJob  # noqa: E402

_PROBE_PATH = (
    _ROOT / "tools" / "measurements" / "corpus" / "probes" / "row_dict" / "sitecustomize.py"
)


def _payload_module() -> Any:
    """Load the payload by path: its name is ``sitecustomize``, which must not be importable here."""

    spec = importlib.util.spec_from_file_location("_row_dict_probe_under_test", _PROBE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def probe() -> Any:
    module = _payload_module()
    original = CanonicalJob.to_dict
    yield module
    CanonicalJob.to_dict = original  # type: ignore[method-assign]


def _rows() -> list[CanonicalJob]:
    """A battery that exercises every field shape the payload has to reproduce."""

    return [
        CanonicalJob(),
        CanonicalJob.from_mapping(
            {
                "id": 7,
                "title": "Senior Gameplay Engineer",
                "company": "Example Studios",
                "city": "Montréal",
                "country": "Canada",
                "jobLink": "https://boards.example.com/jobs/1",
                "source": "static_source::example",
                "sourceJobId": "1",
                "sector": "Games",
                "qualityScore": 71,
                "focusScore": 12,
                "sourceBundleCount": 2,
                "sourceBundle": [
                    {"source": "a", "jobLink": "https://a.example.com/1"},
                    {"source": "b", "jobLink": "https://b.example.com/1", "sector": "Games"},
                ],
                "locations": [
                    {"city": "Tokyo", "country": "Japan"},
                    {"city": "大阪", "country": "Japan"},
                ],
                "availabilityEvidence": {"checkedAt": "2026-10-01", "status": "available"},
                "locationSummary": "Tokyo, Japan",
            }
        ),
        CanonicalJob.from_mapping(
            {
                "title": "  Artiste 3D  ",
                "company": "Ünïcode Ltée",
                "description": "<p>desc &amp; more</p>\n\n\tCJK: 東京",
                "locations": [],
                "sourceBundle": [],
                "availabilityEvidence": {},
            }
        ),
    ]


def test_the_cheap_build_returns_the_values_asdict_would(probe: Any) -> None:
    field_names = tuple(CanonicalJob.__dataclass_fields__)
    for row in _rows():
        assert probe.cheap_payload(row, field_names) == asdict(row)
        assert list(probe.cheap_payload(row, field_names)) == list(asdict(row))


def test_installing_the_ablation_replaces_the_conversion_and_counts_it(probe: Any) -> None:
    original = CanonicalJob.to_dict
    counters = probe.install(ablation=probe.ABLATION_CACHED)
    assert CanonicalJob.to_dict is not original
    row = _rows()[1]
    first = row.to_dict()
    second = row.to_dict()
    assert first == asdict(row)
    # Each caller owns its top level, because callers rebind payload keys before reading again.
    assert first is not second
    assert counters["calls"] == 2
    assert counters["builds"] == 1
    assert counters["distinctInstances"] == 1
    assert counters["payloadMismatches"] == 0
    json.dumps(counters)  # the receipt has to be writable


def test_the_counters_distinguish_distinct_rows(probe: Any) -> None:
    counters = probe.install(ablation=probe.ABLATION_CACHED)
    rows = _rows()
    for row in rows:
        row.to_dict()
        row.to_dict()
    assert counters["calls"] == 2 * len(rows)
    assert counters["builds"] == len(rows)
    assert counters["distinctInstances"] == len(rows)


def test_an_unset_variable_leaves_the_product_untouched(probe: Any) -> None:
    original = CanonicalJob.to_dict
    assert probe.activate(environ={}) is None
    assert probe.activate(environ={probe.MARKER_ENV: "/tmp/whatever"}) is None
    assert CanonicalJob.to_dict is original


def test_an_unknown_ablation_refuses_instead_of_measuring_nothing(probe: Any) -> None:
    with pytest.raises(ValueError):
        probe.activate(environ={probe.ABLATION_ENV: "wishful-thinking"})


def test_a_payload_that_moved_fails_the_arm_instead_of_reporting_a_zero(
    probe: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An arm whose probe changed the payload's values must fail, not report a cheaper no-op."""

    monkeypatch.setattr(probe, "cheap_payload", lambda instance, field_names: {})
    counters = probe.install(ablation=probe.ABLATION_CACHED)
    with pytest.raises(RuntimeError):
        _rows()[0].to_dict()
    assert counters["payloadMismatches"] == 1


def test_installing_twice_keeps_one_patch(probe: Any) -> None:
    counters = probe.install(ablation=probe.ABLATION_CACHED)
    again = probe.install(ablation=probe.ABLATION_CACHED)
    assert again == counters
    _rows()[0].to_dict()
    assert counters["calls"] == 1


def test_the_marker_is_a_readable_receipt(probe: Any, tmp_path: Path) -> None:
    counters = probe.install(ablation=probe.ABLATION_CACHED)
    marker = tmp_path / "probe-123.json"
    _rows()[0].to_dict()
    probe.write_marker(marker, counters)
    written = json.loads(marker.read_text(encoding="utf-8"))
    assert written["ablation"] == probe.ABLATION_CACHED
    assert written["calls"] == counters["calls"] == 1
    assert written["pid"] == counters["pid"]
