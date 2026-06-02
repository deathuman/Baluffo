from __future__ import annotations

from pathlib import Path

import src.source_discovery.web_search_candidates as web_candidates
from src.source_discovery import audit_config
from src.url_hosts import url_host_matches_domain

from ._helpers import sd


def _sheet_csv() -> str:
    return """x,x,x,x
x,Studio,Hiring Location,Roles open,Link
x,Provider Studio,Remote,yes,https://boards.greenhouse.io/providerstudio
x,Static Studio,Remote,speculative,https://static.example.com/careers
"""


def _sheet_url(sheet_id: str = "sheet_test", gid: str = "1") -> str:
    return sd.game_studios_sheet_candidate_urls(sheet_id, gid)[0]


def _fetch_from(payloads: dict[str, str]):
    def fake_fetch(url: str, _: int) -> str:
        if url not in payloads:
            raise RuntimeError(f"unexpected URL: {url}")
        return payloads[url]

    return fake_fetch


def _sheet_audit_config(audit_path: str) -> dict[str, object]:
    return {
        "sheetDirectory": {
            "activeAuditPath": audit_path,
            "activeAuditTtlMinutes": 60,
            "activeAuditRecoveryEnabled": False,
        }
    }


def _web_audit_config(audit_path: str) -> dict[str, object]:
    return {
        "webSearch": {
            "activeAuditPath": audit_path,
            "activeAuditTtlMinutes": 60,
        }
    }


def _seeds() -> list[dict[str, object]]:
    return [
        {
            "studio": "Seed Studio",
            "careersUrl": "https://seed.example/careers",
            "nlPriority": True,
        },
        {
            "studio": "Search Studio",
            "nlPriority": False,
        },
    ]


def _web_fetcher(url: str, _timeout_s: int) -> str:
    if url == "https://seed.example/careers":
        return '<a href="https://boards.greenhouse.io/seedstudio/jobs/1">Role</a>'
    if url_host_matches_domain(url, "duckduckgo.com"):
        return '<a href="https://search.example/careers">Careers</a>'
    if url == "https://search.example/careers":
        return '<a href="https://boards.greenhouse.io/searchstudio/jobs/1">Role</a>'
    raise RuntimeError(f"unexpected URL: {url}")


def test_audit_config_container_mode_maps_relative_data_path_under_data_dir(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir = tmp_path / "runtime-data"
    monkeypatch.setenv("BALUFFO_RUNTIME_MODE", "container")
    monkeypatch.setenv("BALUFFO_DATA_DIR", str(data_dir))

    assert (
        audit_config.audit_artifact_path(
            {"example": {"activeAuditPath": "data/custom-audit.json"}},
            "example",
            default_filename="default-audit.json",
        )
        == data_dir / "custom-audit.json"
    )


def test_audit_config_container_mode_preserves_absolute_path(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir = tmp_path / "runtime-data"
    absolute_path = tmp_path / "absolute-audit.json"
    monkeypatch.setenv("BALUFFO_RUNTIME_MODE", "container")
    monkeypatch.setenv("BALUFFO_DATA_DIR", str(data_dir))

    assert (
        audit_config.audit_artifact_path(
            {"example": {"activeAuditPath": str(absolute_path)}},
            "example",
            default_filename="default-audit.json",
        )
        == absolute_path
    )


def test_audit_config_container_mode_maps_default_path_under_data_dir(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir = tmp_path / "runtime-data"
    monkeypatch.setenv("BALUFFO_RUNTIME_MODE", "container")
    monkeypatch.setenv("BALUFFO_DATA_DIR", str(data_dir))

    assert (
        audit_config.audit_artifact_path(
            {},
            "example",
            default_filename="default-audit.json",
        )
        == data_dir / "default-audit.json"
    )


def test_sheet_directory_audit_container_relative_path_writes_under_data_dir(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir = tmp_path / "data-volume"
    audit_path = data_dir / "sheet-directory-discovery-audit.json"
    monkeypatch.setenv("BALUFFO_RUNTIME_MODE", "container")
    monkeypatch.setenv("BALUFFO_DATA_DIR", str(data_dir))

    artifact, cache_hit = sd.run_sheet_directory_audit(
        5,
        sheet_id="sheet_test",
        gid="1",
        config=_sheet_audit_config("data/sheet-directory-discovery-audit.json"),
        fetcher=_fetch_from({_sheet_url(): _sheet_csv()}),
    )

    assert cache_hit is False
    assert audit_path.exists()
    assert artifact["adapter"] == "sheet_directory"


def test_web_search_directory_audit_container_relative_path_writes_under_data_dir(
    tmp_path: Path,
    monkeypatch,
) -> None:
    data_dir = tmp_path / "data-volume"
    audit_path = data_dir / "web-search-discovery-audit.json"
    monkeypatch.setenv("BALUFFO_RUNTIME_MODE", "container")
    monkeypatch.setenv("BALUFFO_DATA_DIR", str(data_dir))

    artifact, cache_hit = web_candidates.run_web_search_directory_audit(
        5,
        studio_seeds=_seeds(),
        include_seed_careers=True,
        include_web_search=True,
        config=_web_audit_config("data/web-search-discovery-audit.json"),
        fetcher=_web_fetcher,
        max_queries=1,
    )

    assert cache_hit is False
    assert audit_path.exists()
    assert artifact["adapter"] == "web_search"
