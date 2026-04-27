from __future__ import annotations

import src.source_discovery.web_search_candidates as web_candidates

from ._helpers import sd, workspace_tmpdir


def _sheet_url(sheet_id: str = "sheet_test", gid: str = "1") -> str:
    return sd.game_studios_sheet_candidate_urls(sheet_id, gid)[0]


def _fetch_from(payloads: dict[str, str]):
    def fake_fetch(url: str, _: int) -> str:
        if url not in payloads:
            raise RuntimeError(f"unexpected URL: {url}")
        return payloads[url]

    return fake_fetch


def test_sheet_directory_recovery_url_limit_changes_attempts_and_signature() -> None:
    with workspace_tmpdir("sheet-directory-audit-recovery-url-limit") as root:
        audit_path = root / "sheet-audit.json"
        csv_text = """Studio,Roles open,Link
Limit Sheet Studio,yes,https://limit.example.com/
"""
        payloads = {
            _sheet_url(): csv_text,
            "https://limit.example.com/careers": "<html><body>No roles</body></html>",
            "https://limit.example.com/jobs": "<html><body>No roles</body></html>",
            "https://limit.example.com/join-us": "<html><body>No roles</body></html>",
            "https://limit.example.com/work-with-us": "<html><body>No roles</body></html>",
        }

        first_artifact, first_cache_hit = sd.run_sheet_directory_audit(
            5,
            sheet_id="sheet_test",
            gid="1",
            config={
                "sheetDirectory": {
                    "activeAuditEnabled": True,
                    "activeAuditRecoveryEnabled": True,
                    "activeAuditRecoveryUrlLimit": 1,
                    "activeAuditPath": str(audit_path),
                    "activeAuditTtlMinutes": 60,
                }
            },
            fetcher=_fetch_from(payloads),
        )
        second_artifact, second_cache_hit = sd.run_sheet_directory_audit(
            5,
            sheet_id="sheet_test",
            gid="1",
            config={
                "sheetDirectory": {
                    "activeAuditEnabled": True,
                    "activeAuditRecoveryEnabled": True,
                    "activeAuditRecoveryUrlLimit": 2,
                    "activeAuditPath": str(audit_path),
                    "activeAuditTtlMinutes": 60,
                }
            },
            fetcher=_fetch_from(payloads),
        )

        assert first_cache_hit is False
        assert second_cache_hit is False
        assert first_artifact["summary"]["recoveryFetchAttempts"] == 2
        assert first_artifact["summary"]["recoveredStaticCandidates"] == 0
        assert second_artifact["summary"]["recoveryFetchAttempts"] == 4
        assert second_artifact["summary"]["recoveredStaticCandidates"] == 0


def test_web_search_recovery_url_limit_changes_attempts_and_signature() -> None:
    with workspace_tmpdir("web-search-audit-http-recovery-url-limit") as root:
        audit_path = root / "web-audit.json"
        payloads = {
            "https://limit-web.example/": "<html><body>No roles</body></html>",
            "https://limit-web.example/careers": "<html><body>No roles</body></html>",
            "https://limit-web.example/jobs": "<html><body>No roles</body></html>",
            "https://limit-web.example/join-us": "<html><body>No roles</body></html>",
            "https://limit-web.example/work-with-us": "<html><body>No roles</body></html>",
        }
        seeds = [{"studio": "Limit Web Studio", "careersUrl": "https://limit-web.example/"}]

        first_artifact, first_cache_hit = web_candidates.run_web_search_directory_audit(
            5,
            studio_seeds=seeds,
            include_seed_careers=True,
            include_web_search=False,
            config={
                "webSearch": {
                    "activeAuditEnabled": True,
                    "activeAuditRecoveryEnabled": True,
                    "activeAuditRecoveryUrlLimit": 1,
                    "activeAuditPath": str(audit_path),
                    "activeAuditTtlMinutes": 60,
                }
            },
            fetcher=_fetch_from(payloads),
        )
        second_artifact, second_cache_hit = web_candidates.run_web_search_directory_audit(
            5,
            studio_seeds=seeds,
            include_seed_careers=True,
            include_web_search=False,
            config={
                "webSearch": {
                    "activeAuditEnabled": True,
                    "activeAuditRecoveryEnabled": True,
                    "activeAuditRecoveryUrlLimit": 2,
                    "activeAuditPath": str(audit_path),
                    "activeAuditTtlMinutes": 60,
                }
            },
            fetcher=_fetch_from(payloads),
        )

        assert first_cache_hit is False
        assert second_cache_hit is False
        assert first_artifact["summary"]["recoveryFetchAttempts"] == 2
        assert first_artifact["summary"]["recoveredStaticCandidates"] == 0
        assert second_artifact["summary"]["recoveryFetchAttempts"] == 4
        assert second_artifact["summary"]["recoveredStaticCandidates"] == 0
