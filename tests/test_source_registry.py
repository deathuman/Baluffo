from pathlib import Path

from src import source_registry as sr
from tests.helpers.temp_paths import workspace_tmpdir


def test_source_identity_uses_explicit_id_when_present() -> None:
    row = {"id": "lever:account:sandboxvr", "adapter": "lever", "account": "sandboxvr"}
    token = sr.source_identity(row)
    assert token == "lever:account:sandboxvr"


def test_source_identity_prefers_adapter_keyed_fields() -> None:
    row = {"adapter": "lever", "account": "sandboxvr"}
    token = sr.source_identity(row)
    assert "lever:account:sandboxvr" in token


def test_unique_sources_deduplicates_by_identity() -> None:
    rows = [
        {"adapter": "workable", "account": "hutch"},
        {"adapter": "workable", "account": "hutch"},
        {"adapter": "workable", "account": "wargaming"},
    ]
    deduped = sr.unique_sources(rows)
    assert len(deduped) == 2


def test_save_json_atomic_and_load_array() -> None:
    with workspace_tmpdir("source-registry") as tmp:
        path = Path(tmp) / "registry.json"
        payload = [{"adapter": "smartrecruiters", "company_id": "Gameloft"}]
        sr.save_json_atomic(path, payload)
        loaded = sr.load_json_array(path, [])
        assert len(loaded) == 1
        assert loaded[0]["company_id"] == "Gameloft"


def test_normalize_source_url_trims_query_trailing_slash_and_case() -> None:
    normalized = sr.normalize_source_url("HTTPS://Jobs.Ashbyhq.com/Acme/jobs/?foo=1#frag")
    assert normalized == "https://jobs.ashbyhq.com/Acme/jobs"


def test_source_url_fingerprint_prefers_endpoint_fields() -> None:
    row = {
        "adapter": "workable",
        "account": "acme",
        "api_url": "https://apply.workable.com/api/v1/widget/accounts/acme/?details=true",
    }
    assert (
        sr.source_url_fingerprint(row)
        == "https://apply.workable.com/api/v1/widget/accounts/acme"
    )


def test_source_url_fingerprint_uses_static_pages_when_no_endpoint_field() -> None:
    row = {
        "adapter": "static",
        "pages": ["https://milestone.it/careers/?utm_source=x"],
    }
    assert sr.source_url_fingerprint(row) == "https://milestone.it/careers"
