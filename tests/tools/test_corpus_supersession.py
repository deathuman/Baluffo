"""Supersession is a claim about the current reference, so only a usable referee may retire one.

A corpus cannot be regenerated, only re-recorded, which is why nothing is ever deleted and a newer
epoch replaces an older one by marking it ``supersededBy``. That makes the replacement itself a
decision worth gating: an epoch that cannot referee a delta must not retire the epoch a study would
otherwise be measured on.

Measured on 2026-10-09: an epoch the audit refused for 252 unrequested candidates -- every one of
them an audit defect, fixed the next day in ``truncation.py`` -- retired the certified reference, and
the listing was left holding only epochs no study could be measured on. The gate was inconsistent
rather than deliberate: a failed *birth replay* already quarantines instead of superseding, while a
failed *audit*, decided earlier on the same artifact, superseded unconditionally.

The refused record is still written where it was recorded -- it is a coverage record, and deleting it
would lose the only evidence of what that run saw -- and the certified sibling stays the parent's
current epoch.

The driver-level harness (``_record_args``, ``_manifest``, ``_checked_row``) lives with the birth
suite, and is imported rather than re-stated so the two cannot drift into testing different records.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pytest

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tests.tools.test_corpus_birth_verification import (  # noqa: E402
    _checked_row,
    _manifest,
    _record_args,
)
from tests.tools.test_corpus_capture import _surface_loader, local_http_server  # noqa: E402
from tools.measurements.corpus import referees, run_fetch, truncation  # noqa: E402
from tools.measurements.corpus import replay_check as check  # noqa: E402


def test_a_refused_record_supersedes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A record that may not referee a delta must not retire one that can."""

    good_dir = tmp_path / "corpus-good"
    corpus_dir = tmp_path / "corpus-bad"
    real_verdict = truncation.referee_verdict
    refused = {
        "eligible": False,
        "accepted": False,
        "blocking": [
            {
                "reason": "redirect_pool",
                "detail": "redirect candidates the record never requested",
                "knob": truncation.KNOB_REDIRECT_POOL,
            }
        ],
        "knobs": [truncation.KNOB_REDIRECT_POOL],
        "notes": [],
    }
    # Flipped between the two records rather than counted: ``referee_verdict`` is read by more than
    # one caller per record (the driver and its own ``referee_lines``), so a call count would pin how
    # many times it is asked rather than which record is being judged.
    refuse = {"on": False}

    def verdict_for(report: dict[str, Any], *, accepted: bool = False) -> dict[str, Any]:
        return refused if refuse["on"] else real_verdict(report, accepted=accepted)

    monkeypatch.setattr(truncation, "referee_verdict", verdict_for)
    monkeypatch.setattr(check, "check_epoch", lambda row, **kw: _checked_row(row, kw["out_dir"]))

    with local_http_server() as base:
        loader = _surface_loader(base)
        monkeypatch.setattr(run_fetch, "_loaders", lambda: [("loopback", loader)])
        good = run_fetch.main(_record_args(good_dir, tmp_path / "record-good"))
        refuse["on"] = True
        exit_code = run_fetch.main(_record_args(corpus_dir, tmp_path / "record-bad"))

    stderr = capsys.readouterr().err
    assert good == run_fetch.EXIT_OK
    assert exit_code == run_fetch.EXIT_REFEREE_REFUSED
    assert corpus_dir.exists()
    assert _manifest(corpus_dir)["deltaReferee"]["eligible"] is False
    assert _manifest(corpus_dir)["supersededBy"] == ""
    assert _manifest(good_dir)["supersededBy"] == ""
    assert "superseding nothing" in stderr

    # The listing still picks the certified epoch: a refusal is recorded, not promoted.
    pick = referees.pick_referee(referees.epochs_in_scope(corpus_parent=tmp_path))
    assert pick is not None and pick["epochId"] == good_dir.name
