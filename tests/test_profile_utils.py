from __future__ import annotations

import pytest

from src.shared import profile_utils
from tests.helpers.temp_paths import workspace_tmpdir


def test_run_profiled_is_transparent_when_disabled(monkeypatch) -> None:
    with workspace_tmpdir("profile-disabled") as data_dir:
        monkeypatch.delenv("BALUFFO_PROFILE", raising=False)
        monkeypatch.setenv("BALUFFO_DATA_DIR", str(data_dir))

        result = profile_utils.run_profiled(lambda left, right: left + right, 2, 3)

        assert result == 5
        assert not (data_dir / "perf-profiles").exists()


def test_run_profiled_writes_profile_artifacts_when_enabled(monkeypatch) -> None:
    with workspace_tmpdir("profile-enabled") as data_dir:
        monkeypatch.setenv("BALUFFO_PROFILE", "1")
        monkeypatch.setenv("BALUFFO_DATA_DIR", str(data_dir))

        result = profile_utils.run_profiled(
            lambda value: sum(range(value)),
            10,
            profile_name="adapter_static_source::Example Jobs",
        )

        profile_dir = data_dir / "perf-profiles"
        assert result == 45
        assert (profile_dir / "adapter_static_source_Example_Jobs.prof").exists()
        assert (profile_dir / "adapter_static_source_Example_Jobs.prof.txt").exists()


def test_run_profiled_dumps_profile_and_reraises_exception(monkeypatch) -> None:
    with workspace_tmpdir("profile-exception") as data_dir:
        monkeypatch.setenv("BALUFFO_PROFILE", "yes")
        monkeypatch.setenv("BALUFFO_DATA_DIR", str(data_dir))

        def fail() -> None:
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            profile_utils.run_profiled(fail, profile_name="discovery/full run")

        profile_dir = data_dir / "perf-profiles"
        assert (profile_dir / "discovery_full_run.prof").exists()
        assert (profile_dir / "discovery_full_run.prof.txt").exists()


def test_sanitize_profile_name_falls_back_to_default() -> None:
    assert profile_utils.sanitize_profile_name("???") == "default"
