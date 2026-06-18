from __future__ import annotations

import json
from argparse import Namespace
from pathlib import Path

import pytest

from src.ship import update_manager_cli as cli


def test_update_manager_cli_expected_failure_returns_json_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        cli,
        "parse_args",
        lambda: Namespace(
            command="apply",
            signing_key="test-key",
            root="ship-root",
            bundle_zip="bundle.zip",
            manifest="manifest.json",
        ),
    )

    def _apply_update(
        _root: Path,
        _bundle_zip: Path,
        _manifest_path: Path,
        _signing_key: str,
    ) -> dict[str, object]:
        raise RuntimeError("update artifact rejected")

    monkeypatch.setattr(cli, "apply_update", _apply_update)

    assert cli.main() == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload == {"ok": False, "error": "update artifact rejected"}


def test_update_manager_cli_unexpected_failure_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cli,
        "parse_args",
        lambda: Namespace(
            command="apply",
            signing_key="test-key",
            root="ship-root",
            bundle_zip="bundle.zip",
            manifest="manifest.json",
        ),
    )

    def _apply_update(
        _root: Path,
        _bundle_zip: Path,
        _manifest_path: Path,
        _signing_key: str,
    ) -> dict[str, object]:
        raise AssertionError("unexpected cli bug")

    monkeypatch.setattr(cli, "apply_update", _apply_update)

    with pytest.raises(AssertionError, match="unexpected cli bug"):
        cli.main()
