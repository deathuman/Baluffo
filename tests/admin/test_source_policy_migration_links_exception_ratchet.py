from __future__ import annotations

import pytest

from src.bridge.source_policy_migration_links import _source_id


def test_source_id_falls_back_for_expected_source_identity_failures() -> None:
    class Api:
        @staticmethod
        def source_identity(row: dict) -> str:
            raise TypeError("malformed source row")

    assert _source_id(Api(), {}) == ""


def test_source_id_does_not_swallow_unexpected_source_identity_failure() -> None:
    class Api:
        @staticmethod
        def source_identity(row: dict) -> str:
            raise AssertionError("unexpected source identity bug")

    with pytest.raises(AssertionError, match="unexpected source identity bug"):
        _source_id(Api(), {})
