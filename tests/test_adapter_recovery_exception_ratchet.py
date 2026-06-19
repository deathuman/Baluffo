from __future__ import annotations

import pytest

from src.jobs.adapters.recovery import run_recoverable_adapter_attempt


def test_recoverable_adapter_attempt_records_expected_adapter_failure() -> None:
    errors: list[str] = []

    result = run_recoverable_adapter_attempt(
        lambda: (_ for _ in ()).throw(RuntimeError("temporary adapter failure")),
        lambda exc: errors.append(str(exc)),
    )

    assert result is None
    assert errors == ["temporary adapter failure"]


def test_recoverable_adapter_attempt_does_not_swallow_unexpected_bug() -> None:
    errors: list[str] = []

    with pytest.raises(AssertionError, match="unexpected adapter invariant"):
        run_recoverable_adapter_attempt(
            lambda: (_ for _ in ()).throw(AssertionError("unexpected adapter invariant")),
            lambda exc: errors.append(str(exc)),
        )

    assert errors == []
