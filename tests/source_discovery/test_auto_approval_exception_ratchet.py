from __future__ import annotations

from unittest import mock

import pytest

from ._helpers import (
    discovery_config_without_generator_stages,
    discovery_orchestrator,
    override_discovery_runtime,
    patch_empty_generator_stages,
    workspace_tmpdir,
)


def _run_minimal_discovery_with_auto_approval_failure(error: BaseException) -> dict:
    with workspace_tmpdir("source-discovery-auto-approval-ratchet") as root:
        with override_discovery_runtime(root, studio_seeds=[], static_candidates=[]):
            with (
                patch_empty_generator_stages(probe=lambda *_args, **_kwargs: (False, 0, "")),
                mock.patch.object(
                    discovery_orchestrator,
                    "apply_discovery_auto_approval",
                    side_effect=error,
                ),
            ):
                return discovery_orchestrator.run_discovery(
                    timeout_s=1,
                    top_n=0,
                    preset="uncapped",
                    mode="dynamic",
                    include_web_search=False,
                    discovery_config=discovery_config_without_generator_stages(
                        autoApproveHealthyPendingOnComplete=True
                    ),
                    fetcher=lambda *_args, **_kwargs: "",
                )


def test_auto_approval_expected_failure_marks_runtime_failed() -> None:
    report = _run_minimal_discovery_with_auto_approval_failure(
        ValueError("bad auto-approval state")
    )

    auto_approval = (report.get("runtime") or {}).get("autoApproval") or {}
    assert auto_approval["enabled"] is True
    assert auto_approval["status"] == "failed"
    assert auto_approval["error"] == "bad auto-approval state"


def test_auto_approval_unexpected_failure_propagates() -> None:
    with pytest.raises(AssertionError, match="unexpected auto-approval bug"):
        _run_minimal_discovery_with_auto_approval_failure(
            AssertionError("unexpected auto-approval bug")
        )
