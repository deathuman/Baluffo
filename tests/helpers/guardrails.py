"""Stubs for repo-guardrail wiring tests.

A wiring test exists to prove that one check's failure reaches
``run_compat_group()``. The group's other checks each scan the repository, so
running them re-measures code the test never asserts on -- eight extra seconds
per test. Stub the ones under test out of the way and keep the target.
"""

from __future__ import annotations

import pytest

from tools.repo_health import repo_guardrails

__all__ = ["stub_other_compat_checks"]


def stub_other_compat_checks(monkeypatch: pytest.MonkeyPatch, keep: str) -> None:
    """Make every compat check except ``keep`` report no messages.

    ``keep`` must be a check the group actually runs: a typo would stub them all
    and let the wiring assertion pass vacuously.
    """
    names = [name for name, _check in repo_guardrails.COMPAT_CHECKS]
    assert keep in names, f"{keep!r} is not a compat check; expected one of {names!r}"
    for name in names:
        if name != keep:
            monkeypatch.setattr(repo_guardrails, name, lambda: [])
