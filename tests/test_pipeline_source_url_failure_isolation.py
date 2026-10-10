"""A single source's malformed URL must fail that source, not the whole fetch run.

Found while recording the full-source corpus epoch (2,046 loaders): a static board's templated
detail link -- ``/careers/" + root + "/moonactive-position?uid=%22+++key+++%22`` -- reached
``urllib`` and raised ``http.client.InvalidURL``. That class was not in the pipeline's
"expected source operation" set, so it escaped the worker future and killed the run at source
1,579 of 2,046. A URL that can never be fetched is a source-level failure with a message,
exactly like a timeout, and the remaining boards still have to run.
"""

from __future__ import annotations

import sys
from http.client import InvalidURL
from pathlib import Path
from typing import Any

import pytest

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.jobs import pipeline_source_results  # noqa: E402


def test_a_malformed_url_is_a_source_failure_not_a_run_failure() -> None:
    """The loader boundary converts it to ``_SourceExecutionFailure``, which ``execute_loader``
    reports as ``status == "error"`` with the message; the failure bucket machinery then sees a
    normal error instead of the run dying inside a ``future.result()``."""

    def loader(**_kwargs: Any) -> list[Any]:
        raise InvalidURL("URL can't contain control characters")

    with pytest.raises(pipeline_source_results._SourceExecutionFailure) as raised:
        pipeline_source_results._run_loader_and_record_fetch(
            name="static_source::static:listing_url:https://boards.example.com/jobs",
            loader=loader,
            accepted_kwargs={},
            report={},
            report_loss={},
            progress_callback=lambda **_kwargs: None,
        )

    assert "control characters" in str(raised.value)
