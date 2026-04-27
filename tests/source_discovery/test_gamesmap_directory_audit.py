from __future__ import annotations

import json

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


def test_gamesmap_audit_output_matches_legacy_scan_for_same_inputs() -> None:
    with workspace_tmpdir("gamesmap-audit-equivalence") as root:
        audit_path = root / "gamesmap-audit.json"
        legacy_config = _gamesmap_config()
        legacy_config["gamesmap"]["activeAuditEnabled"] = False
        audit_config = _gamesmap_config(str(audit_path))

        legacy_rows = sd.discover_gamesmap_candidates(
            5,
            config=legacy_config,
            fetcher=_fetch_from(_gamesmap_payloads()),
        )
        audit_rows = sd.discover_gamesmap_candidates(
            5,
            config=audit_config,
            fetcher=_fetch_from(_gamesmap_payloads()),
        )

        assert audit_rows == legacy_rows


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


def test_gamesmap_audit_disabled_preserves_legacy_cache_behavior_and_writes_no_artifact() -> None:
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
                AssertionError("legacy cache should bypass fetches")
            ),
        )

        assert second_rows == first_rows
        assert calls
        assert cache_path.exists()
        assert not audit_path.exists()
