"""Contracts for the durable known-dead board memo.

The memo is an optimization that sits on a delivery path, so its two failure directions are pinned
here: it must never invent a skip (a cold, disabled or foreign candidate list probes exactly what the
shipped code probes), and it must never learn from anything weaker than a 404/410 from the board's own
authoritative endpoint. Identity is provider host + tenant — the repo's board rule — so a label change,
a row URL change or a second row for the same dead board still hits the same entry.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from src.source_discovery import known_dead_boards as memo_module
from src.source_discovery.probe_runtime import ProbeResult


def _lever(account: str) -> dict[str, object]:
    return {
        "adapter": "lever",
        "account": account,
        "api_url": f"https://api.lever.co/v0/postings/{account}",
        "listing_url": f"https://jobs.lever.co/{account}",
        "studio": f"{account} studio",
    }


def _greenhouse(slug: str) -> dict[str, object]:
    return {
        "adapter": "greenhouse",
        "slug": slug,
        "api_url": f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs",
        "listing_url": f"https://boards.greenhouse.io/{slug}",
    }


def _result(row: dict, *, ok: bool, jobs: int, error: str) -> ProbeResult:
    return (row, ok, jobs, error, 12)


def _e404(account: str) -> str:
    return (
        f"https://api.lever.co/v0/postings/{account}: Client error '404 Not Found' for url "
        f"'https://api.lever.co/v0/postings/{account}'"
    )


def test_board_identity_is_provider_host_and_tenant() -> None:
    assert memo_module.board_key(_lever("Acme")) == "api.lever.co/acme"
    assert memo_module.board_key(_greenhouse("Acme")) == "boards-api.greenhouse.io/acme"


@pytest.mark.parametrize(
    "candidate",
    [
        {"adapter": "static", "careersUrl": "https://acme.example/careers"},
        {"adapter": "workable", "account": "acme", "api_url": "https://apply.workable.com/acme"},
        {"adapter": "lever", "account": "", "api_url": "https://api.lever.co/v0/postings/"},
        {
            "adapter": "lever",
            "account": "acme",
            "api_url": "https://jobs.lever.co/acme",
        },
        {
            "adapter": "lever",
            "account": "ac me",
            "api_url": "https://api.lever.co/v0/postings/ac me",
        },
    ],
)
def test_board_identity_refuses_what_is_not_an_authoritative_endpoint(
    candidate: dict[str, object],
) -> None:
    assert memo_module.board_key(candidate) == ""


def test_only_a_definitive_not_found_from_its_own_endpoint_is_learned() -> None:
    assert memo_module.definitive_not_found(_lever("acme"), _e404("acme"))
    assert memo_module.definitive_not_found(
        _greenhouse("acme"),
        "https://boards-api.greenhouse.io/v1/boards/acme/jobs: Client error '410 Gone' for url",
    )
    assert not memo_module.definitive_not_found(
        _lever("acme"),
        "https://api.lever.co/v0/postings/acme: Client error '429 Too Many Requests' for url",
    )
    assert not memo_module.definitive_not_found(_lever("acme"), "fetch timed out after 6s"), (
        "a timeout says nothing about whether the board exists"
    )
    assert not memo_module.definitive_not_found(
        {"adapter": "static", "careersUrl": "https://acme.example"},
        "https://acme.example: Client error '404 Not Found' for url",
    )


def test_load_memo_drops_entries_past_the_ttl(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(memo_module.KNOWN_DEAD_BOARDS_TTL_ENV, "7")
    now = datetime(2026, 10, 28, 12, 0, tzinfo=UTC)
    path = tmp_path / memo_module.KNOWN_DEAD_BOARDS_FILENAME
    path.write_text(
        json.dumps(
            {
                "schemaVersion": 1,
                "boards": {
                    "api.lever.co/fresh": {"firstSeenAt": "", "lastSeenAt": now.isoformat()},
                    "api.lever.co/stale": {"lastSeenAt": (now - timedelta(days=8)).isoformat()},
                    "api.lever.co/broken": {"lastSeenAt": "not-a-date"},
                },
            }
        ),
        encoding="utf-8",
    )

    memo = memo_module.load_memo(path, now=now)

    assert set(memo) == {"api.lever.co/fresh"}


def test_load_memo_is_total_on_a_corrupt_file(tmp_path: Path) -> None:
    path = tmp_path / memo_module.KNOWN_DEAD_BOARDS_FILENAME
    path.write_text("{not json", encoding="utf-8")

    assert memo_module.load_memo(path) == {}


def test_save_memo_round_trips_and_prunes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(memo_module.KNOWN_DEAD_BOARDS_TTL_ENV, "7")
    now = datetime(2026, 10, 28, 12, 0, tzinfo=UTC)
    path = tmp_path / memo_module.KNOWN_DEAD_BOARDS_FILENAME
    memo = {
        "api.lever.co/fresh": {"firstSeenAt": now.isoformat(), "lastSeenAt": now.isoformat()},
        "api.lever.co/stale": {
            "lastSeenAt": (now - timedelta(days=30)).isoformat(),
        },
    }

    memo_module.save_memo(path, memo, now=now)

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["schemaVersion"] == 1
    assert payload["ttlDays"] == 7
    assert set(payload["boards"]) == {"api.lever.co/fresh"}


def test_split_keeps_order_and_only_skips_memoized_boards() -> None:
    dead, alive = _lever("dead"), _lever("alive")
    candidates = [dead, _greenhouse("other"), alive]
    memo = {"api.lever.co/dead": {"lastSeenAt": "2026-10-28T00:00:00+00:00"}}

    kept, skipped = memo_module.split_known_dead(candidates, memo)

    assert kept == [_greenhouse("other"), alive]
    assert [(row, key) for row, key, _entry in skipped] == [(dead, "api.lever.co/dead")]


def test_split_with_an_empty_memo_returns_the_same_list() -> None:
    candidates = [_lever("a"), _lever("b")]
    kept, skipped = memo_module.split_known_dead(candidates, {})
    assert kept == candidates
    assert skipped == []


def test_record_learns_only_definitive_not_founds() -> None:
    memo: dict[str, dict] = {}
    results = [
        _result(_lever("dead"), ok=False, jobs=0, error=_e404("dead")),
        _result(
            _lever("throttled"), ok=False, jobs=0, error="Client error '429 Too Many Requests'"
        ),
        _result(_lever("live"), ok=True, jobs=4, error=""),
    ]

    learned = memo_module.record_definitive_not_founds(results, memo)
    again = memo_module.record_definitive_not_founds(
        [_result(_lever("dead"), ok=False, jobs=0, error=_e404("dead"))], memo
    )

    assert learned == 1
    assert again == 0, "a repeated sighting updates the entry, it does not add one"
    assert set(memo) == {"api.lever.co/dead"}
    assert memo["api.lever.co/dead"]["hits"] == 2


def test_disabled_memo_probes_everything_and_writes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(memo_module.KNOWN_DEAD_BOARDS_TTL_ENV, "0")
    calls: list[list[dict]] = []

    async def _probe(rows: list[dict]) -> list[ProbeResult]:
        calls.append(rows)
        return [_result(row, ok=False, jobs=0, error=_e404("dead")) for row in rows]

    candidates = [_lever("dead"), _lever("other")]
    results = asyncio.run(
        memo_module.probe_with_known_dead(candidates, {}, probe=_probe, memo_path=None)
    )

    assert calls == [candidates], "a disabled memo is the shipped probe call"
    assert len(results) == 2
    assert list(tmp_path.iterdir()) == [], "a disabled memo writes no file"


def test_a_cold_memo_probes_everything_and_learns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The first run is the one that teaches the memo; it must not return early and learn nothing."""
    monkeypatch.setenv(memo_module.KNOWN_DEAD_BOARDS_TTL_ENV, "7")
    path = tmp_path / memo_module.KNOWN_DEAD_BOARDS_FILENAME
    candidates = [_lever("dead"), _lever("live")]

    async def _probe(rows: list[dict]) -> list[ProbeResult]:
        assert rows == candidates, "a cold memo probes the whole batch"
        return [
            _result(rows[0], ok=False, jobs=0, error=_e404("dead")),
            _result(rows[1], ok=True, jobs=2, error=""),
        ]

    results = asyncio.run(
        memo_module.probe_with_known_dead(candidates, {}, probe=_probe, memo_path=path)
    )

    assert len(results) == 2
    assert not any("known dead board" in result[3] for result in results)
    written = json.loads(path.read_text(encoding="utf-8"))["boards"]
    assert set(written) == {"api.lever.co/dead"}


def test_warm_memo_skips_memoized_boards_and_still_learns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(memo_module.KNOWN_DEAD_BOARDS_TTL_ENV, "7")
    path = tmp_path / memo_module.KNOWN_DEAD_BOARDS_FILENAME
    # Dated now: the write path prunes past the TTL, and this test is about the skip, not staleness.
    memo = {
        "api.lever.co/gone": {"firstSeenAt": datetime.now(UTC).isoformat(), "hits": 1},
    }
    gone, gone_again, fresh_dead, live = (
        _lever("gone"),
        _lever("gone"),
        _greenhouse("gone"),
        _greenhouse("live"),
    )
    probed: list[list[dict]] = []

    async def _probe(rows: list[dict]) -> list[ProbeResult]:
        probed.append(rows)
        out: list[ProbeResult] = []
        for row in rows:
            if row is live:
                out.append(_result(row, ok=True, jobs=3, error=""))
            else:
                out.append(
                    _result(
                        row,
                        ok=False,
                        jobs=0,
                        error=(
                            "https://boards-api.greenhouse.io/v1/boards/gone/jobs: "
                            "Client error '404 Not Found' for url"
                        ),
                    )
                )
        return out

    results = asyncio.run(
        memo_module.probe_with_known_dead(
            [gone, gone_again, fresh_dead, live], memo, probe=_probe, memo_path=path
        )
    )

    assert probed == [[fresh_dead, live]], "the memoized board is not probed"
    assert len(results) == 4, "every candidate still gets a result row"
    skipped = [result for result in results if "known dead board" in result[3]]
    assert [result[0] for result in skipped] == [gone, gone_again], (
        "every row of a memoized board is skipped, not just the row that taught the memo"
    )
    assert all(result[1] is False for result in skipped)
    assert all("api.lever.co/gone" in result[3] for result in skipped)

    written = json.loads(path.read_text(encoding="utf-8"))["boards"]
    assert set(written) == {"api.lever.co/gone", "boards-api.greenhouse.io/gone"}
    assert written["api.lever.co/gone"]["hits"] == 1, "a skipped board was not probed again"
    assert written["boards-api.greenhouse.io/gone"]["hits"] == 1
