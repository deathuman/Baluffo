"""The tagless cohort probe: three questions per page, and the net it reports.

The probe exists to answer one question before any code changes: does a board that publishes
no validator honour ``If-Modified-Since``? The ancient-date arm is the control, so a 304 in the
read-date arm is evidence the header was asked with the clock we mean. The net is a
subtraction -- unchanged reads saved minus probes spent -- so both terms are pinned here.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tests.tools._revalidation_fixtures import (  # noqa: E402
    A_NAME,
    B_NAME,
    B_URL,
    REGISTRY,
    row,
)
from tools.measurements.pipeline import revalidation_study as study_mod  # noqa: E402

ANCIENT = "Thu, 01 Jan 2015 00:00:00 GMT"


def test_probe_asks_three_questions_and_prices_only_the_boards_that_answer_304(
    monkeypatch,
) -> None:
    def fake_probe(url: str, *, since: str = "", timeout_s: int = 10) -> dict[str, Any]:
        # Only the read-date question may answer 304 here; the ancient-date control must not.
        answered = 304 if since and since != ANCIENT else 200
        return {"url": url, "status": answered, "notModified": answered == 304, "durationMs": 500}

    monkeypatch.setattr(study_mod, "head_probe", fake_probe)
    rows = {A_NAME: row(kept=0, streak=2)}
    run = {"sources": [{"name": A_NAME, "durationMs": 5000}]}

    payload = study_mod.probe_tagless(
        rows, registry=[REGISTRY[0]], run=run, concurrency=1, timeout_s=5
    )

    assert payload["cohort"] == 1
    assert payload["since"]["notModified"] == 1
    assert payload["since"]["allUnchangedBoards"] == 1
    assert payload["plain"]["notModified"] == 0
    assert payload["old"]["notModified"] == 0
    assert payload["unchangedReadSeconds"] == 5.0
    assert payload["netSecondsPerEmptyWindow"] == round(
        5.0 - payload["since"]["probeSeconds"], 1
    ), "the net is the reads a 304 replaced minus the probes it cost"


def test_probe_asks_every_configured_page_of_a_multi_page_board(monkeypatch) -> None:
    seen: list[tuple[str, bool]] = []

    def fake_probe(url: str, *, since: str = "", timeout_s: int = 10) -> dict[str, Any]:
        seen.append((url, bool(since)))
        return {"url": url, "status": 304, "notModified": True, "durationMs": 100}

    monkeypatch.setattr(study_mod, "head_probe", fake_probe)
    rows = {B_NAME: row(kept=0, streak=2)}

    study_mod.probe_tagless(
        rows,
        registry=[REGISTRY[1]],
        run={"sources": [{"name": B_NAME, "durationMs": 8000}]},
        concurrency=1,
    )

    urls = {url for url, _conditional in seen}
    assert urls == {B_URL, "https://b.test/jobs"}
    assert all(any(conditional for url, conditional in seen if url == page) for page in urls), (
        "every page is asked with the read clock, not only the first"
    )
