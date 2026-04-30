from __future__ import annotations

from src.source_registry import source_identity
from tests.helpers.bridge_api import build_admin_bridge_api


def test_trigger_source_check_stages_provider_candidate_without_changing_static(
    admin_bridge_entrypoint_root,
    monkeypatch,
) -> None:
    api = build_admin_bridge_api()
    added = api.add_manual_source("https://staticstudio.example/careers")
    source_id = str(added.get("sourceId") or "")
    assert source_id

    def fake_fetch(url: str, _timeout: int, *, adapter: str, fetcher=None):  # noqa: ANN001
        if url == "https://staticstudio.example/careers":
            return '<a href="https://boards.greenhouse.io/staticstudio">Jobs</a>'
        return "<html></html>"

    monkeypatch.setattr("src.admin_bridge.discovery.fetch_text_with_retry", fake_fetch)
    result = api.trigger_source_check(source_id)

    assert result["started"]
    assert result["ok"]
    staged_result = result.get("stagedProviderCandidate") or {}
    assert staged_result["adapter"] == "greenhouse"
    state = api.load_state()
    pending = state["pending"]
    static_row = next(row for row in pending if source_identity(row) == source_id)
    staged = next(row for row in pending if row.get("createdFromAdvisory"))
    assert static_row["adapter"] == "static"
    assert static_row.get("hiddenFromDefault") is not True
    assert str(static_row.get("pendingReason") or "") == "manual"
    assert staged["adapter"] == "greenhouse"
    assert staged["createdFromAdvisory"] is True
    assert staged["candidateState"] == "validated"
    assert staged["registryState"] == "pending"
    assert staged["pendingReason"] == "provider_migration_candidate"
    assert staged["migrationSourceIdentity"] == source_id
