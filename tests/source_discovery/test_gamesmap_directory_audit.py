from __future__ import annotations

import json

from src.source_discovery import directory_audit
from src.source_discovery.gamesmap_candidates import run_gamesmap_directory_audit

from ._helpers import _gamesmap_next_payload_html, sd, workspace_tmpdir


def _gamesmap_companies() -> list[dict[str, object]]:
    return [
        {
            "id": "1",
            "name": "Static Studio",
            "slug": "static-studio",
            "categories": [{"name": "Developer"}],
            "address": {"city": "Berlin", "state": "Berlin", "country": "DE"},
            "websites": ["https://static.example.com"],
        },
        {
            "id": "2",
            "name": "Provider Studio",
            "slug": "provider-studio",
            "categories": [{"name": "Developer"}],
            "address": {"city": "Hamburg", "state": "Hamburg", "country": "DE"},
            "websites": ["https://provider.example.com"],
        },
    ]


def _gamesmap_payloads() -> dict[str, str]:
    return {
        "https://www.gamesmap.de/en": _gamesmap_next_payload_html(_gamesmap_companies()),
        "https://static.example.com": """
            <!doctype html><html><body><a href="/careers">Careers</a></body></html>
        """,
        "https://provider.example.com": """
            <!doctype html><html><body><a href="https://boards.greenhouse.io/provider">Jobs</a></body></html>
        """,
    }


def _fetch_from(payloads: dict[str, str]):
    def fake_fetch(url: str, _: int) -> str:
        if url not in payloads:
            raise RuntimeError(f"unexpected URL: {url}")
        return payloads[url]

    return fake_fetch


def _gamesmap_config(audit_path: str | None = None) -> dict[str, object]:
    cfg: dict[str, object] = {
        "enabled": True,
        "baseUrl": "https://www.gamesmap.de",
        "indexUrls": ["https://www.gamesmap.de/en"],
        "websiteOnlyFallback": True,
        "maxDetailPages": 1,
        "allowedCategoryTokens": ["developer"],
        "blockedCategoryTokens": [],
    }
    if audit_path is not None:
        cfg.update(
            {
                "activeAuditEnabled": True,
                "activeAuditPath": audit_path,
                "activeAuditTtlMinutes": 60,
            }
        )
    return {"gamesmap": cfg}


def test_gamesmap_audit_missing_artifact_executes_and_writes_boundaries() -> None:
    with workspace_tmpdir("gamesmap-audit-missing") as root:
        audit_path = root / "gamesmap-audit.json"
        config = _gamesmap_config(str(audit_path))

        provider_rows, static_rows, failures = sd.discover_gamesmap_candidates(
            5,
            config=config,
            fetcher=_fetch_from(_gamesmap_payloads()),
        )

        assert provider_rows == []
        assert len(static_rows) == 1
        assert failures == []
        artifact = json.loads(audit_path.read_text(encoding="utf-8"))
        assert artifact["schemaVersion"] == 1
        assert artifact["adapter"] == "gamesmap"
        assert artifact["progress"]["complete"] is True
        assert artifact["progress"]["cursor"] == 1
        assert artifact["progress"]["completedUrlIdentities"] == ["https://static.example.com"]
        assert artifact["summary"]["parsedRows"] == 1
        assert artifact["summary"]["rowsWithWebsite"] == 1
        assert artifact["summary"]["eligibleRows"] == 1
        assert artifact["summary"]["websiteFetchJobs"] == 1
        assert artifact["summary"]["staticCandidates"] == 1
        assert artifact["summary"]["artifactSizeBytes"] > 0
        assert artifact["timings"]["totalsMs"]["indexFetchParseMs"] >= 0
        assert artifact["timings"]["totalsMs"]["websiteFetchMs"] >= 0


def test_gamesmap_audit_enabled_defaults_to_true_when_config_field_missing() -> None:
    with workspace_tmpdir("gamesmap-audit-default-enabled") as root:
        audit_path = root / "gamesmap-audit.json"
        config = _gamesmap_config()
        config["gamesmap"]["activeAuditPath"] = str(audit_path)
        config["gamesmap"]["activeAuditTtlMinutes"] = 60

        provider_rows, static_rows, failures = sd.discover_gamesmap_candidates(
            5,
            config=config,
            fetcher=_fetch_from(_gamesmap_payloads()),
        )

        assert provider_rows == []
        assert len(static_rows) == 1
        assert failures == []
        assert audit_path.exists()


def test_gamesmap_audit_reuses_fresh_artifact_without_network_work() -> None:
    with workspace_tmpdir("gamesmap-audit-reuse") as root:
        audit_path = root / "gamesmap-audit.json"
        config = _gamesmap_config(str(audit_path))
        first_rows = sd.discover_gamesmap_candidates(
            5,
            config=config,
            fetcher=_fetch_from(_gamesmap_payloads()),
        )

        second_rows = sd.discover_gamesmap_candidates(
            5,
            config=config,
            fetcher=lambda *_args: (_ for _ in ()).throw(
                AssertionError("fresh audit artifact should bypass network work")
            ),
        )

        assert second_rows == first_rows


def test_gamesmap_public_discovery_returns_audit_rows_for_same_inputs() -> None:
    with workspace_tmpdir("gamesmap-audit-equivalence") as root:
        public_audit_path = root / "gamesmap-public-audit.json"
        audit_path = root / "gamesmap-direct-audit.json"
        public_config = _gamesmap_config(str(public_audit_path))
        audit_config = _gamesmap_config(str(audit_path))

        public_rows = sd.discover_gamesmap_candidates(
            5,
            config=public_config,
            fetcher=_fetch_from(_gamesmap_payloads()),
        )
        artifact, _cache_hit = run_gamesmap_directory_audit(
            5,
            config=audit_config,
            fetcher=_fetch_from(_gamesmap_payloads()),
        )
        audit_rows = directory_audit.directory_audit_rows(artifact)

        assert audit_rows == public_rows


def test_gamesmap_audit_records_index_and_homepage_failures() -> None:
    with workspace_tmpdir("gamesmap-audit-index-failure") as root:
        audit_path = root / "gamesmap-audit.json"
        config = _gamesmap_config(str(audit_path))

        provider_rows, static_rows, failures = sd.discover_gamesmap_candidates(
            5,
            config=config,
            fetcher=lambda *_args: (_ for _ in ()).throw(RuntimeError("index down")),
        )

        assert provider_rows == []
        assert static_rows == []
        assert len(failures) == 1
        assert failures[0]["stage"] == "directory_index_fetch"
        artifact = json.loads(audit_path.read_text(encoding="utf-8"))
        assert artifact["failureCounts"] == {"directory_index_fetch": 1}

    with workspace_tmpdir("gamesmap-audit-homepage-failure") as root:
        audit_path = root / "gamesmap-audit.json"
        config = _gamesmap_config(str(audit_path))
        payloads = {
            "https://www.gamesmap.de/en": _gamesmap_next_payload_html(_gamesmap_companies()),
        }

        provider_rows, static_rows, failures = sd.discover_gamesmap_candidates(
            5,
            config=config,
            fetcher=_fetch_from(payloads),
        )

        assert provider_rows == []
        assert static_rows == []
        assert len(failures) == 1
        assert failures[0]["stage"] == "website_fetch"
        artifact = json.loads(audit_path.read_text(encoding="utf-8"))
        assert artifact["summary"]["websiteFetchFailures"] == 1
        assert artifact["failureCounts"] == {"website_fetch": 1}


def test_gamesmap_audit_recovers_static_candidate_with_provenance() -> None:
    with workspace_tmpdir("gamesmap-audit-recovery-static") as root:
        audit_path = root / "gamesmap-audit.json"
        config = _gamesmap_config(str(audit_path))
        config["gamesmap"]["activeAuditRecoveryEnabled"] = True
        companies = [
            {
                "id": "1",
                "name": "Recoverable Studio",
                "slug": "recoverable-studio",
                "categories": [{"name": "Developer"}],
                "address": {"city": "Berlin", "state": "Berlin", "country": "DE"},
                "websites": ["https://recover.example.com"],
            }
        ]
        payloads = {
            "https://www.gamesmap.de/en": _gamesmap_next_payload_html(companies),
            "https://recover.example.com": "<html><body>Recoverable Studio</body></html>",
            "https://recover.example.com/careers": """
                <a href="/jobs/engineer">Engineer</a><a href="/jobs/designer">Designer</a>
            """,
            "https://recover.example.com/jobs": "<html><body></body></html>",
        }

        provider_rows, static_rows, failures = sd.discover_gamesmap_candidates(
            5,
            config=config,
            fetcher=_fetch_from(payloads),
        )

        assert provider_rows == []
        assert failures == []
        assert len(static_rows) == 1
        assert static_rows[0]["listing_url"] == "https://recover.example.com/careers"
        assert static_rows[0]["sourceDirectory"] == "gamesmap"
        assert static_rows[0]["sourceDirectoryEntryUrl"].endswith("/recoverable-studio")
        artifact = json.loads(audit_path.read_text(encoding="utf-8"))
        assert artifact["summary"]["recoveryFetchAttempts"] == 2
        assert artifact["summary"]["recoveredStaticCandidates"] == 1


def test_gamesmap_audit_disabled_flag_uses_audit_cache_and_skips_legacy_cache() -> None:
    with workspace_tmpdir("gamesmap-audit-disabled") as root:
        audit_path = root / "gamesmap-audit.json"
        cache_path = root / "gamesmap-cache.json"
        config = _gamesmap_config()
        config["gamesmap"]["activeAuditEnabled"] = False
        config["gamesmap"]["activeAuditPath"] = str(audit_path)
        config["gamesmap"]["cachePath"] = str(cache_path)
        config["gamesmap"]["cacheTtlMinutes"] = 60
        calls: list[str] = []

        def fake_fetch(url: str, timeout_s: int) -> str:
            calls.append(url)
            return _fetch_from(_gamesmap_payloads())(url, timeout_s)

        first_rows = sd.discover_gamesmap_candidates(5, config=config, fetcher=fake_fetch)
        second_rows = sd.discover_gamesmap_candidates(
            5,
            config=config,
            fetcher=lambda *_args: (_ for _ in ()).throw(
                AssertionError("fresh audit artifact should bypass fetches")
            ),
        )

        assert second_rows == first_rows
        assert calls
        assert audit_path.exists()
        assert not cache_path.exists()
