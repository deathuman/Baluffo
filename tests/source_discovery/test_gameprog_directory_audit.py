from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import pytest

from src.source_discovery import gameprog

from ._helpers import sd, workspace_tmpdir


def _gameprog_payloads() -> dict[str, str]:
    return {
        "https://gameprog.it/teams.json": """[
            {"name": "First Studio", "url": "https://first.example.com/", "place": "Rome"},
            {"name": "Second Studio", "url": "https://second.example.com/", "place": "Milan"}
        ]""",
        "https://first.example.com/": """
            <!doctype html><html><body><a href="/careers">Careers</a></body></html>
        """,
        "https://second.example.com/": """
            <!doctype html><html><body><a href="https://boards.greenhouse.io/second">Jobs</a></body></html>
        """,
    }


def _fetch_from(payloads: dict[str, str]) -> Callable[[str, int], str]:
    def fake_fetch(url: str, _: int) -> str:
        if url not in payloads:
            raise RuntimeError(f"unexpected URL: {url}")
        return payloads[url]

    return fake_fetch


def test_gameprog_audit_missing_artifact_executes_and_writes_boundaries() -> None:
    with workspace_tmpdir("gameprog-audit-missing") as root:
        audit_path = root / "gameprog-audit.json"
        config = {
            "gameprog": {
                "enabled": True,
                "activeAuditPath": str(audit_path),
                "activeAuditTtlMinutes": 60,
                "teamsUrl": "https://gameprog.it/teams.json",
                "websiteOnlyFallback": True,
                "maxStudios": 1,
            }
        }

        artifact, cache_hit = gameprog.run_gameprog_directory_audit(
            5,
            config=config,
            fetcher=_fetch_from(_gameprog_payloads()),
        )

        assert cache_hit is False
        assert audit_path.exists()
        assert artifact["schemaVersion"] == gameprog.GAMEPROG_AUDIT_SCHEMA_VERSION
        assert artifact["progress"]["complete"] is True
        assert artifact["progress"]["cursor"] == 1
        assert artifact["progress"]["completedUrlIdentities"] == ["https://first.example.com/"]
        assert artifact["summary"]["parsedRows"] == 2
        assert artifact["summary"]["eligibleRows"] == 1
        assert artifact["summary"]["websiteFetchJobs"] == 1
        assert artifact["summary"]["staticCandidates"] == 1
        assert artifact["summary"]["failures"] == 0
        assert artifact["runtime"]["configSignature"] == gameprog._gameprog_cache_signature(
            gameprog._gameprog_config_section(config)
        )
        assert artifact["timings"]["batches"]
        assert artifact["timings"]["totalsMs"]["teamsFetchMs"] >= 0
        assert artifact["timings"]["totalsMs"]["websiteFetchMs"] >= 0

        saved = json.loads(audit_path.read_text(encoding="utf-8"))
        assert saved["summary"]["artifactSizeBytes"] > 0


def test_gameprog_public_discovery_uses_audit_path_by_default() -> None:
    with workspace_tmpdir("gameprog-audit-default-enabled") as root:
        audit_path = root / "gameprog-audit.json"
        config = {
            "gameprog": {
                "enabled": True,
                "activeAuditPath": str(audit_path),
                "activeAuditTtlMinutes": 60,
                "teamsUrl": "https://gameprog.it/teams.json",
                "websiteOnlyFallback": True,
                "maxStudios": 1,
            }
        }

        provider_rows, static_rows, failures = sd.discover_gameprog_candidates(
            5,
            config=config,
            fetcher=_fetch_from(_gameprog_payloads()),
        )

        assert provider_rows == []
        assert len(static_rows) == 1
        assert failures == []
        assert audit_path.exists()


def test_gameprog_teams_fetch_failure_stays_in_failure_channel() -> None:
    config = {
        "gameprog": {
            "enabled": True,
            "activeAuditTtlMinutes": 0,
            "activeAuditPath": str(Path(".tmp") / "gameprog-teams-fetch-failure-audit.json"),
            "teamsUrl": "https://gameprog.it/teams.json",
            "websiteOnlyFallback": True,
            "maxStudios": 1,
        }
    }

    provider_rows, static_rows, failures = sd.discover_gameprog_candidates(
        5,
        config=config,
        fetcher=lambda *_args: (_ for _ in ()).throw(
            RuntimeError("fetch failed: teams unavailable")
        ),
    )

    assert provider_rows == []
    assert static_rows == []
    assert len(failures) == 1
    assert failures[0]["adapter"] == "gameprog"
    assert failures[0]["stage"] == "teams_json_fetch"
    assert failures[0]["error"] == "fetch failed: teams unavailable"


def test_gameprog_teams_fetch_propagates_unexpected_runtime_failure() -> None:
    config = {
        "gameprog": {
            "enabled": True,
            "activeAuditTtlMinutes": 0,
            "activeAuditPath": str(Path(".tmp") / "gameprog-teams-propagate-audit.json"),
            "teamsUrl": "https://gameprog.it/teams.json",
            "websiteOnlyFallback": True,
            "maxStudios": 1,
        }
    }

    with pytest.raises(RuntimeError, match="unexpected URL"):
        sd.discover_gameprog_candidates(
            5,
            config=config,
            fetcher=lambda *_args: (_ for _ in ()).throw(
                RuntimeError("unexpected URL: https://gameprog.it/teams.json")
            ),
        )


def test_gameprog_audit_reuses_fresh_completed_artifact_without_network_work() -> None:
    with workspace_tmpdir("gameprog-audit-reuse") as root:
        audit_path = root / "gameprog-audit.json"
        config = {
            "gameprog": {
                "enabled": True,
                "activeAuditPath": str(audit_path),
                "activeAuditTtlMinutes": 60,
                "teamsUrl": "https://gameprog.it/teams.json",
                "websiteOnlyFallback": True,
                "maxStudios": 1,
            }
        }
        first_artifact, first_cache_hit = gameprog.run_gameprog_directory_audit(
            5,
            config=config,
            fetcher=_fetch_from(_gameprog_payloads()),
        )

        second_artifact, second_cache_hit = gameprog.run_gameprog_directory_audit(
            5,
            config=config,
            fetcher=lambda *_args: (_ for _ in ()).throw(
                AssertionError("fresh audit artifact should bypass network work")
            ),
        )

        assert first_cache_hit is False
        assert second_cache_hit is True
        assert second_artifact == first_artifact

        provider_rows, static_rows, failures = sd.discover_gameprog_candidates(
            5,
            config=config,
            fetcher=lambda *_args: (_ for _ in ()).throw(
                AssertionError("fresh audit artifact should bypass discovery fetches")
            ),
        )

        assert provider_rows == first_artifact["providerCandidates"]
        assert static_rows == first_artifact["staticCandidates"]
        assert failures == first_artifact["failures"]


def test_gameprog_audit_output_matches_same_inputs_across_artifacts() -> None:
    with workspace_tmpdir("gameprog-audit-equivalence") as root:
        first_audit_path = root / "gameprog-first-audit.json"
        audit_path = root / "gameprog-audit.json"
        base_config = {
            "gameprog": {
                "enabled": True,
                "activeAuditPath": str(first_audit_path),
                "activeAuditTtlMinutes": 0,
                "teamsUrl": "https://gameprog.it/teams.json",
                "websiteOnlyFallback": True,
                "maxStudios": 2,
            }
        }
        audit_config = {
            "gameprog": {
                **base_config["gameprog"],
                "activeAuditPath": str(audit_path),
                "activeAuditTtlMinutes": 60,
            }
        }

        first_rows = sd.discover_gameprog_candidates(
            5,
            config=base_config,
            fetcher=_fetch_from(_gameprog_payloads()),
        )
        audit_rows = sd.discover_gameprog_candidates(
            5,
            config=audit_config,
            fetcher=_fetch_from(_gameprog_payloads()),
        )

        assert audit_rows == first_rows


def test_gameprog_audit_records_website_fetch_failures_in_failure_channel() -> None:
    with workspace_tmpdir("gameprog-audit-failures") as root:
        audit_path = root / "gameprog-audit.json"
        config = {
            "gameprog": {
                "enabled": True,
                "activeAuditPath": str(audit_path),
                "activeAuditTtlMinutes": 60,
                "teamsUrl": "https://gameprog.it/teams.json",
                "websiteOnlyFallback": False,
                "maxStudios": 1,
            }
        }
        payloads = {
            "https://gameprog.it/teams.json": """[
                {"name": "Broken Studio", "url": "https://broken.example.com/", "place": "Rome"}
            ]""",
        }

        provider_rows, static_rows, failures = sd.discover_gameprog_candidates(
            5,
            config=config,
            fetcher=_fetch_from(payloads),
        )

        assert provider_rows == []
        assert static_rows == []
        assert len(failures) == 1
        assert failures[0]["adapter"] == "gameprog"
        assert failures[0]["stage"] == "website_fetch"

        artifact = json.loads(audit_path.read_text(encoding="utf-8"))
        assert artifact["summary"]["websiteFetchFailures"] == 1
        assert artifact["summary"]["failures"] == 1
        assert artifact["failureCounts"] == {"website_fetch": 1}
        assert artifact["failures"] == failures


def test_gameprog_audit_recovers_static_candidate_before_weak_fallback() -> None:
    with workspace_tmpdir("gameprog-audit-recovery-static") as root:
        audit_path = root / "gameprog-audit.json"
        config = {
            "gameprog": {
                "enabled": True,
                "activeAuditRecoveryEnabled": True,
                "activeAuditPath": str(audit_path),
                "activeAuditTtlMinutes": 60,
                "teamsUrl": "https://gameprog.it/teams.json",
                "websiteOnlyFallback": True,
                "maxStudios": 1,
            }
        }
        payloads = {
            "https://gameprog.it/teams.json": """[
                {"name": "Recoverable Studio", "url": "https://recover.example.com/", "place": "Rome"}
            ]""",
            "https://recover.example.com/": "<html><body>Recoverable Studio</body></html>",
            "https://recover.example.com/careers": """
                <a href="/jobs/engineer">Engineer</a><a href="/jobs/designer">Designer</a>
            """,
            "https://recover.example.com/jobs": "<html><body></body></html>",
        }

        _provider_rows, static_rows, failures = sd.discover_gameprog_candidates(
            5,
            config=config,
            fetcher=_fetch_from(payloads),
        )

        assert failures == []
        assert len(static_rows) == 1
        assert static_rows[0]["listing_url"] == "https://recover.example.com/careers"
        assert "gameprog_no_current_openings" not in static_rows[0]["evidenceTypes"]
        artifact = json.loads(audit_path.read_text(encoding="utf-8"))
        assert artifact["summary"]["recoveryFetchAttempts"] == 2
        assert artifact["summary"]["recoveredStaticCandidates"] == 1


def test_gameprog_audit_recovery_miss_keeps_existing_weak_fallback() -> None:
    with workspace_tmpdir("gameprog-audit-recovery-miss") as root:
        audit_path = root / "gameprog-audit.json"
        config = {
            "gameprog": {
                "enabled": True,
                "activeAuditRecoveryEnabled": True,
                "activeAuditPath": str(audit_path),
                "activeAuditTtlMinutes": 60,
                "teamsUrl": "https://gameprog.it/teams.json",
                "websiteOnlyFallback": True,
                "maxStudios": 1,
            }
        }
        payloads = {
            "https://gameprog.it/teams.json": """[
                {"name": "Fallback Studio", "url": "https://fallback.example.com/", "place": "Rome"}
            ]""",
            "https://fallback.example.com/": "<html><body>Fallback Studio</body></html>",
            "https://fallback.example.com/careers": "<html><body>No roles</body></html>",
            "https://fallback.example.com/jobs": "<html><body>No roles</body></html>",
            "https://fallback.example.com/join-us": "<html><body>No roles</body></html>",
            "https://fallback.example.com/work-with-us": "<html><body>No roles</body></html>",
            "https://fallback.example.com/company/careers": "<html><body>No roles</body></html>",
            "https://fallback.example.com/about/careers": "<html><body>No roles</body></html>",
        }

        _provider_rows, static_rows, failures = sd.discover_gameprog_candidates(
            5,
            config=config,
            fetcher=_fetch_from(payloads),
        )

        assert failures == []
        assert len(static_rows) == 1
        assert "gameprog_no_current_openings" in static_rows[0]["evidenceTypes"]


def test_gameprog_audit_signature_rebuilds_when_recovery_toggle_changes() -> None:
    with workspace_tmpdir("gameprog-audit-recovery-signature") as root:
        audit_path = root / "gameprog-audit.json"
        base_config = {
            "gameprog": {
                "enabled": True,
                "activeAuditPath": str(audit_path),
                "activeAuditTtlMinutes": 60,
                "teamsUrl": "https://gameprog.it/teams.json",
                "websiteOnlyFallback": True,
                "maxStudios": 1,
            }
        }
        payloads = {
            "https://gameprog.it/teams.json": """[
                {"name": "Recoverable Studio", "url": "https://recover.example.com/", "place": "Rome"}
            ]""",
            "https://recover.example.com/": "<html><body>Recoverable Studio</body></html>",
            "https://recover.example.com/careers": """
                <a href="/jobs/engineer">Engineer</a><a href="/jobs/designer">Designer</a>
            """,
            "https://recover.example.com/jobs": "<html><body></body></html>",
        }
        disabled_config = json.loads(json.dumps(base_config))
        disabled_config["gameprog"]["activeAuditRecoveryEnabled"] = False
        enabled_config = json.loads(json.dumps(base_config))
        enabled_config["gameprog"]["activeAuditRecoveryEnabled"] = True

        first_artifact, first_cache_hit = gameprog.run_gameprog_directory_audit(
            5,
            config=disabled_config,
            fetcher=_fetch_from(payloads),
        )
        second_artifact, second_cache_hit = gameprog.run_gameprog_directory_audit(
            5,
            config=enabled_config,
            fetcher=_fetch_from(payloads),
        )

        assert first_cache_hit is False
        assert second_cache_hit is False
        assert first_artifact["summary"]["recoveryFetchAttempts"] == 0
        assert second_artifact["summary"]["recoveryFetchAttempts"] == 2


def test_gameprog_audit_reuses_modern_audit_artifact() -> None:
    with workspace_tmpdir("gameprog-audit-reuse-modern") as root:
        audit_path = root / "gameprog-audit.json"
        config = {
            "gameprog": {
                "enabled": True,
                "activeAuditPath": str(audit_path),
                "teamsUrl": "https://gameprog.it/teams.json",
                "websiteOnlyFallback": True,
                "maxStudios": 1,
            }
        }
        payloads = _gameprog_payloads()
        calls: list[str] = []

        def fake_fetch(url: str, timeout_s: int) -> str:
            calls.append(url)
            return _fetch_from(payloads)(url, timeout_s)

        first_rows = sd.discover_gameprog_candidates(5, config=config, fetcher=fake_fetch)
        second_rows = sd.discover_gameprog_candidates(
            5,
            config=config,
            fetcher=lambda *_args: (_ for _ in ()).throw(
                AssertionError("fresh audit artifact should bypass fetches")
            ),
        )

        assert first_rows == second_rows
        assert calls
        assert audit_path.exists()
