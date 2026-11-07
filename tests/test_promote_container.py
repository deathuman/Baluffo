from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from tools.release import promote_container


def test_build_commands_pulls_before_recreating(tmp_path: Path) -> None:
    commands = promote_container.build_commands(tmp_path / "deathuman-baluffo")

    assert commands[0][-1] == "pull"
    assert commands[1][-2:] == ["up", "-d"]
    assert all(command[:2] == ["docker", "compose"] for command in commands)


def test_health_matches_requires_version_and_commit() -> None:
    payload = {"appVersion": "0.3.040", "build": {"commit": "abc"}}

    assert promote_container.health_matches(payload, version="0.3.040", commit="abc")
    assert not promote_container.health_matches(payload, version="0.3.039", commit="abc")
    assert not promote_container.health_matches(payload, version="0.3.040", commit="def")
    assert promote_container.health_matches(payload, version="0.3.040", commit="")


def test_promote_polls_until_the_running_build_matches() -> None:
    answers = iter(
        [
            {"appVersion": "0.3.039", "build": {"commit": "old"}},
            {"appVersion": "0.3.040", "build": {"commit": "new"}},
        ]
    )
    clock = iter([0.0, 1.0, 2.0, 3.0, 4.0])
    ran: list[list[str]] = []

    def run(command: Sequence[str]) -> int:
        ran.append(list(command))
        return 0

    exit_code = promote_container.promote(
        compose_dir=Path("deathuman-baluffo"),
        base_url="http://example.test",
        version="0.3.040",
        commit="new",
        timeout_s=30.0,
        dry_run=False,
        run=run,
        health=lambda: next(answers),
        sleep=lambda _seconds: None,
        now=lambda: next(clock),
    )

    assert exit_code == 0
    assert [command[-1] for command in ran] == ["pull", "-d"]


def test_promote_dry_run_runs_nothing(tmp_path: Path) -> None:
    ran: list[list[str]] = []

    def run(command: Sequence[str]) -> int:
        ran.append(list(command))
        return 0

    exit_code = promote_container.promote(
        compose_dir=tmp_path,
        base_url="http://example.test",
        version="0.3.040",
        commit="",
        timeout_s=5.0,
        dry_run=True,
        run=run,
    )

    assert exit_code == 0
    assert ran == []
