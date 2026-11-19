"""Registry cleanup dry-run and host-matching behavior."""

from __future__ import annotations

import json
from pathlib import Path

from tests.tools.registry_cleanup_plan_helpers import (
    _active_bytes,
    _fixture,
    _run,
    _verdict,
    _write_registry,
)


def test_dry_run_prints_the_plan_and_writes_nothing(tmp_path: Path) -> None:
    data, verdicts, repoint_map = _fixture(tmp_path)
    before = _active_bytes(data)
    result = _run(
        "--data-dir",
        str(data),
        "--verdicts",
        str(verdicts),
        "--repoint-map",
        str(repoint_map),
    )
    assert result.returncode == 0, result.stderr
    assert "RETIRE BOARD (host root and URL both gone) - 1" in result.stdout
    assert "RETIRE SEED (host alive, registered URL never worked) - 1" in result.stdout
    assert "REPOINT (host alive, URL drifted) - 1" in result.stdout
    assert "https://moved.example/careers" in result.stdout
    assert "retire 2" in result.stdout
    assert _active_bytes(data) == before


def test_last_source_guard_groups_canonical_studios_and_flags_emptied_groups(
    tmp_path: Path,
) -> None:
    """A retire is flagged only when its canonical studio/tenant group is emptied.

    Twin Studio's parenthetical and case variants are one family, so retiring two of
    three rows is not a last-source event. Two rows both retiring from Both Studio is
    one, even though neither row is the last row in the unmodified registry. The
    generic ``jobs`` label falls back to host, so jobs-a is not kept alive by an
    unrelated jobs-b row.
    """
    data = tmp_path / "data"
    _write_registry(
        data,
        [
            {
                "id": "static:listing_url:https://solo.example/careers",
                "studio": "Solo Studio",
                "pages": ["https://solo.example/careers"],
            },
            {
                "id": "static:listing_url:https://twin-a.example/careers",
                "studio": "Twin Studio",
                "pages": ["https://twin-a.example/careers"],
            },
            {
                "id": "static:listing_url:https://twin-b.example/careers",
                "studio": "Twin Studio (Parent)",
                "pages": ["https://twin-b.example/careers"],
            },
            {
                "id": "static:listing_url:https://twin-c.example/careers",
                "studio": "twin studio",
                "pages": ["https://twin-c.example/careers"],
            },
            {
                "id": "static:listing_url:https://jobs-a.example/jobs",
                "studio": "jobs",
                "name": "jobs",
                "pages": ["https://jobs-a.example/jobs"],
            },
            {
                "id": "static:listing_url:https://jobs-b.example/jobs",
                "studio": "jobs",
                "name": "jobs",
                "pages": ["https://jobs-b.example/jobs"],
            },
            {
                "id": "static:listing_url:https://both-a.example/careers",
                "studio": "Both Studio",
                "pages": ["https://both-a.example/careers"],
            },
            {
                "id": "static:listing_url:https://both-b.example/careers",
                "studio": "both studio",
                "pages": ["https://both-b.example/careers"],
            },
        ],
    )
    verdicts = tmp_path / "verdicts.json"
    verdicts.write_text(
        json.dumps(
            [
                _verdict("https://solo.example/careers", verdict="retire_stale_seed"),
                _verdict("https://twin-a.example/careers", verdict="retire_stale_seed"),
                _verdict("https://twin-c.example/careers", verdict="retire_stale_seed"),
                _verdict("https://jobs-a.example/jobs", verdict="retire_stale_seed"),
                _verdict("https://both-a.example/careers", verdict="retire_stale_seed"),
                _verdict("https://both-b.example/careers", verdict="retire_stale_seed"),
            ],
            indent=2,
        ),
        encoding="utf-8",
    )

    result = _run("--data-dir", str(data), "--verdicts", str(verdicts))
    assert result.returncode == 0, result.stderr
    marker = "LAST SOURCE (studio/tenant loses its last active source) - 3 groups / 4 retires"
    assert marker in result.stdout
    section = result.stdout.split(marker, 1)[1].split("\n\n", 1)[0]
    assert "Solo Studio" in section
    assert "Both Studio" in section
    assert "both-a.example/careers" in section
    assert "both-b.example/careers" in section
    assert "jobs-a.example" in section
    assert "Twin Studio" not in section
    assert "jobs-b.example" not in section


def test_host_matching_beats_the_registered_path(tmp_path: Path) -> None:
    # The dead row is registered at /old-careers and the verdict names /careers: only a
    # host match can pair them.
    data, verdicts, _ = _fixture(tmp_path)
    result = _run("--data-dir", str(data), "--verdicts", str(verdicts))
    assert result.returncode == 0, result.stderr
    assert "static:name:gone" in result.stdout
    assert "unmatched: 0" in result.stdout


def test_registered_url_in_the_name_disambiguates_a_shared_host(tmp_path: Path) -> None:
    """A failing URL on a shared ATS host must not make every row on that host a match."""
    data = tmp_path / "data"
    _write_registry(
        data,
        [
            {
                "id": "static:name:chronicler",
                "studio": "Chronicler",
                "adapter": "static",
                "registryState": "active",
                "pages": ["https://www.chroniclersoftware.com/careers"],
            },
            {
                "id": "static:name:rippling-a",
                "studio": "Rippling A",
                "adapter": "static",
                "registryState": "active",
                "pages": ["https://ats.rippling.com/careers"],
            },
            {
                "id": "static:name:rippling-b",
                "studio": "Rippling B",
                "adapter": "static",
                "registryState": "active",
                "pages": ["https://ats.rippling.com/careers/jobs"],
            },
        ],
    )
    verdicts = tmp_path / "verdicts.json"
    verdicts.write_text(
        json.dumps(
            [
                _verdict(
                    "https://www.chroniclersoftware.com/careers",
                    failing="https://ats.rippling.com/careers/123",
                )
            ],
            indent=2,
        ),
        encoding="utf-8",
    )
    result = _run("--data-dir", str(data), "--verdicts", str(verdicts))
    assert result.returncode == 0, result.stderr
    assert "ambiguous: 0" in result.stdout
    assert "static:name:chronicler" in result.stdout
    assert "static:name:rippling" not in result.stdout
