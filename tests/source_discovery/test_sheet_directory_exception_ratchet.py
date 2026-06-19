from __future__ import annotations

import pytest

from src.source_discovery import sheet_directory


def test_sheet_directory_url_validation_does_not_swallow_unexpected_runtime_bug(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        sheet_directory,
        "urlparse",
        lambda _url: (_ for _ in ()).throw(RuntimeError("unexpected urlparse bug")),
    )
    failures: list[dict[str, object]] = []

    with pytest.raises(RuntimeError, match="unexpected urlparse bug"):
        sheet_directory._append_sheet_entry_candidate(
            {
                "studio": "Buggy Sheet Studio",
                "careersUrl": "https://buggy.example/jobs",
                "openingsFlag": "yes",
            },
            provider_candidates=[],
            static_candidates=[],
            failures=failures,
        )

    assert failures == []
