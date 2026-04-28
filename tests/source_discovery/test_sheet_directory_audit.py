from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from src.source_discovery import directory_audit

from ._helpers import sd, workspace_tmpdir


def _sheet_csv() -> str:
    return """x,x,x,x
x,Studio,Hiring Location,Roles open,Link
x,Provider Studio,Remote,yes,https://boards.greenhouse.io/providerstudio
x,Static Studio,Remote,speculative,https://static.example.com/careers
"""


def _sheet_url(sheet_id: str = "sheet_test", gid: str = "1") -> str:
    return sd.game_studios_sheet_candidate_urls(sheet_id, gid)[0]


def _sheet_urls(sheet_id: str = "sheet_test", gid: str = "1") -> list[str]:
    return sd.game_studios_sheet_candidate_urls(sheet_id, gid)


def _fetch_from(payloads: dict[str, str]):
    def fake_fetch(url: str, _: int) -> str:
        if url not in payloads:
            raise RuntimeError(f"unexpected URL: {url}")
        return payloads[url]

    return fake_fetch


def _audit_config(
    audit_path: str,
    *,
    recovery_enabled: bool | None = None,
) -> dict[str, object]:
    sheet_config: dict[str, object] = {
        "activeAuditEnabled": True,
        "activeAuditPath": audit_path,
        "activeAuditTtlMinutes": 60,
    }
    if recovery_enabled is not None:
        sheet_config["activeAuditRecoveryEnabled"] = bool(recovery_enabled)
    return {
        "sheetDirectory": {
            **sheet_config,
        }
    }


def test_sheet_directory_audit_missing_artifact_executes_and_writes_boundaries() -> None:
    with workspace_tmpdir("sheet-directory-audit-missing") as root:
        audit_path = root / "sheet-audit.json"

        artifact, cache_hit = sd.run_sheet_directory_audit(
            5,
            sheet_id="sheet_test",
            gid="1",
            config=_audit_config(str(audit_path), recovery_enabled=False),
            fetcher=_fetch_from({_sheet_url(): _sheet_csv()}),
        )

        assert cache_hit is False
        assert audit_path.exists()
        assert artifact["adapter"] == "sheet_directory"
        assert artifact["schemaVersion"] == 1
        assert artifact["progress"]["complete"] is True
        assert artifact["progress"]["cursor"] == 2
        assert artifact["summary"]["csvUrlAttempts"] == 1
        assert artifact["summary"]["selectedCsvUrl"] == _sheet_url()
        assert artifact["summary"]["rawRows"] == 2
        assert artifact["summary"]["eligibleRows"] == 2
        assert artifact["summary"]["providerCandidates"] == 1
        assert artifact["summary"]["staticCandidates"] == 1
        assert artifact["summary"]["failures"] == 0
        assert "recoveryFetchAttempts" not in artifact["summary"]
        assert artifact["timings"]["totalsMs"]["csvFetchMs"] >= 0
        assert artifact["timings"]["totalsMs"]["parseMs"] >= 0
        assert artifact["timings"]["totalsMs"]["candidateAnalysisMs"] >= 0


def test_sheet_directory_audit_default_recovery_runs_and_reuses_artifact() -> None:
    with workspace_tmpdir("sheet-directory-audit-default-recovery") as root:
        audit_path = root / "sheet-audit.json"
        csv_text = """Studio,Roles open,Link
Default Recovery Studio,yes,https://default-recover-sheet.example.com/
"""
        config = _audit_config(str(audit_path))

        first_artifact, first_cache_hit = sd.run_sheet_directory_audit(
            5,
            sheet_id="sheet_test",
            gid="1",
            config=config,
            fetcher=_fetch_from(
                {
                    _sheet_url(): csv_text,
                    "https://default-recover-sheet.example.com/careers": (
                        '<a href="/jobs/designer">Designer</a>'
                    ),
                    "https://default-recover-sheet.example.com/jobs": (
                        "<html><body>No roles</body></html>"
                    ),
                }
            ),
        )
        second_artifact, second_cache_hit = sd.run_sheet_directory_audit(
            5,
            sheet_id="sheet_test",
            gid="1",
            config=config,
            fetcher=lambda *_args: (_ for _ in ()).throw(
                AssertionError("fresh default recovery sheet artifact should bypass fetch")
            ),
        )

        assert first_cache_hit is False
        assert second_cache_hit is True
        assert second_artifact == first_artifact
        assert first_artifact["summary"]["recoveryFetchAttempts"] == 2
        assert first_artifact["summary"]["recoveredStaticCandidates"] == 1
        assert first_artifact["staticCandidates"][0]["listing_url"] == (
            "https://default-recover-sheet.example.com/careers"
        )


def test_sheet_directory_audit_reuses_fresh_completed_artifact_without_fetch() -> None:
    with workspace_tmpdir("sheet-directory-audit-reuse") as root:
        audit_path = root / "sheet-audit.json"
        config = _audit_config(str(audit_path), recovery_enabled=False)

        first_artifact, first_cache_hit = sd.run_sheet_directory_audit(
            5,
            sheet_id="sheet_test",
            gid="1",
            config=config,
            fetcher=_fetch_from({_sheet_url(): _sheet_csv()}),
        )
        second_artifact, second_cache_hit = sd.run_sheet_directory_audit(
            5,
            sheet_id="sheet_test",
            gid="1",
            config=config,
            fetcher=lambda *_args: (_ for _ in ()).throw(
                AssertionError("fresh sheet audit artifact should bypass fetch")
            ),
        )

        assert first_cache_hit is False
        assert second_cache_hit is True
        assert second_artifact == first_artifact


def test_sheet_directory_csv_fetch_falls_back_to_next_candidate_url() -> None:
    with workspace_tmpdir("sheet-directory-audit-url-fallback") as root:
        audit_path = root / "sheet-audit.json"
        urls = _sheet_urls()

        artifact, cache_hit = sd.run_sheet_directory_audit(
            5,
            sheet_id="sheet_test",
            gid="1",
            config=_audit_config(str(audit_path), recovery_enabled=False),
            fetcher=_fetch_from({urls[1]: _sheet_csv()}),
        )

        assert cache_hit is False
        assert artifact["summary"]["csvUrlAttempts"] == 2
        assert artifact["summary"]["selectedCsvUrl"] == urls[1]
        assert artifact["summary"]["providerCandidates"] == 1
        assert artifact["summary"]["staticCandidates"] == 1


def test_sheet_directory_audit_reruns_stale_wrong_schema_incomplete_or_signature_mismatch() -> None:
    cases = [
        {"schemaVersion": 0},
        {"schemaVersion": 1, "progress": {"complete": False}},
        {"schemaVersion": 1, "runtime": {"configSignature": {"sheetId": "other"}}},
        {
            "schemaVersion": 1,
            "updatedAt": (datetime.now(UTC) - timedelta(minutes=90)).isoformat(),
        },
    ]

    for index, existing in enumerate(cases):
        with workspace_tmpdir(f"sheet-directory-audit-rerun-{index}") as root:
            audit_path = root / "sheet-audit.json"
            payload = {
                "schemaVersion": 1,
                "updatedAt": datetime.now(UTC).isoformat(),
                "progress": {"complete": True},
                "runtime": {
                    "configSignature": {
                        "parserVersion": 1,
                        "sheetId": "sheet_test",
                        "gid": "1",
                        "maxRows": "",
                    }
                },
                **existing,
            }
            audit_path.write_text(json.dumps(payload), encoding="utf-8")

            artifact, cache_hit = sd.run_sheet_directory_audit(
                5,
                sheet_id="sheet_test",
                gid="1",
                config=_audit_config(str(audit_path), recovery_enabled=False),
                fetcher=_fetch_from({_sheet_url(): _sheet_csv()}),
            )

            assert cache_hit is False
            assert artifact["summary"]["providerCandidates"] == 1


def test_sheet_directory_public_discovery_returns_audit_rows_for_same_inputs() -> None:
    with workspace_tmpdir("sheet-directory-audit-equivalence") as root:
        public_audit_path = root / "sheet-public-audit.json"
        audit_path = root / "sheet-direct-audit.json"
        payloads = {_sheet_url(): _sheet_csv()}

        public_rows = sd.discover_game_studio_sheet_candidates(
            5,
            sheet_id="sheet_test",
            gid="1",
            config=_audit_config(str(public_audit_path), recovery_enabled=False),
            fetcher=_fetch_from(payloads),
        )
        artifact, _cache_hit = sd.run_sheet_directory_audit(
            5,
            sheet_id="sheet_test",
            gid="1",
            config=_audit_config(str(audit_path), recovery_enabled=False),
            fetcher=_fetch_from(payloads),
        )
        audit_rows = directory_audit.directory_audit_rows(artifact)

        assert audit_rows == public_rows


def test_sheet_directory_audit_records_fetch_parse_and_invalid_url_failures() -> None:
    with workspace_tmpdir("sheet-directory-audit-fetch-failure") as root:
        audit_path = root / "sheet-audit.json"

        artifact, _cache_hit = sd.run_sheet_directory_audit(
            5,
            sheet_id="sheet_test",
            gid="1",
            config=_audit_config(str(audit_path)),
            fetcher=lambda *_args: (_ for _ in ()).throw(RuntimeError("sheet down")),
        )

        assert artifact["summary"]["failures"] == 1
        assert artifact["summary"]["csvUrlAttempts"] == 3
        assert artifact["failureCounts"] == {"directory_index_fetch": 1}

    with workspace_tmpdir("sheet-directory-audit-parse-failure") as root:
        audit_path = root / "sheet-audit.json"

        artifact, _cache_hit = sd.run_sheet_directory_audit(
            5,
            sheet_id="sheet_test",
            gid="1",
            config=_audit_config(str(audit_path)),
            fetcher=_fetch_from({_sheet_url(): "Column A,Column B\nx,y\n"}),
        )

        assert artifact["summary"]["parseFailures"] == 1
        assert artifact["failureCounts"] == {"directory_parse": 1}

    with workspace_tmpdir("sheet-directory-audit-invalid-url") as root:
        audit_path = root / "sheet-audit.json"
        csv_text = """Studio,Roles open,Link
Bad Url Studio,yes,http://[broken
"""

        artifact, _cache_hit = sd.run_sheet_directory_audit(
            5,
            sheet_id="sheet_test",
            gid="1",
            config=_audit_config(str(audit_path)),
            fetcher=_fetch_from({_sheet_url(): csv_text}),
        )

        assert artifact["summary"]["invalidUrls"] == 1
        assert artifact["summary"]["failures"] == 1
        assert artifact["failureCounts"] == {"directory_detail_parse": 1}


def test_sheet_directory_audit_opt_in_recovery_replaces_static_fallback() -> None:
    with workspace_tmpdir("sheet-directory-audit-recovery-static") as root:
        audit_path = root / "sheet-audit.json"
        csv_text = """Studio,Roles open,Link
Recoverable Sheet Studio,yes,https://recover-sheet.example.com/
"""

        artifact, cache_hit = sd.run_sheet_directory_audit(
            5,
            sheet_id="sheet_test",
            gid="1",
            config=_audit_config(str(audit_path), recovery_enabled=True),
            fetcher=_fetch_from(
                {
                    _sheet_url(): csv_text,
                    "https://recover-sheet.example.com/careers": """
                        <a href="/jobs/engineer">Engineer</a>
                        <a href="/jobs/designer">Designer</a>
                    """,
                    "https://recover-sheet.example.com/jobs": "<html><body>No roles</body></html>",
                }
            ),
        )

        assert cache_hit is False
        assert artifact["summary"]["recoveryFetchAttempts"] == 2
        assert artifact["summary"]["recoveredStaticCandidates"] == 1
        assert artifact["summary"]["staticCandidates"] == 1
        assert artifact["staticCandidates"][0]["listing_url"] == (
            "https://recover-sheet.example.com/careers"
        )
        assert artifact["staticCandidates"][0]["sourceDirectoryEntryUrl"] == (
            "https://recover-sheet.example.com/"
        )


def test_sheet_directory_audit_opt_in_recovery_miss_keeps_static_fallback() -> None:
    with workspace_tmpdir("sheet-directory-audit-recovery-miss") as root:
        audit_path = root / "sheet-audit.json"
        csv_text = """Studio,Roles open,Link
Fallback Sheet Studio,speculative,https://fallback-sheet.example.com/
"""
        payloads = {
            _sheet_url(): csv_text,
            "https://fallback-sheet.example.com/careers": "<html><body>No roles</body></html>",
            "https://fallback-sheet.example.com/jobs": "<html><body>No roles</body></html>",
            "https://fallback-sheet.example.com/join-us": "<html><body>No roles</body></html>",
            "https://fallback-sheet.example.com/work-with-us": "<html><body>No roles</body></html>",
            "https://fallback-sheet.example.com/company/careers": (
                "<html><body>No roles</body></html>"
            ),
            "https://fallback-sheet.example.com/about/careers": (
                "<html><body>No roles</body></html>"
            ),
        }

        artifact, _cache_hit = sd.run_sheet_directory_audit(
            5,
            sheet_id="sheet_test",
            gid="1",
            config=_audit_config(str(audit_path), recovery_enabled=True),
            fetcher=_fetch_from(payloads),
        )

        assert artifact["summary"]["recoveryFetchAttempts"] == 6
        assert artifact["summary"]["recoveredStaticCandidates"] == 0
        assert artifact["summary"]["recoveryFailures"] == 0
        assert len(artifact["staticCandidates"]) == 1
        assert artifact["staticCandidates"][0]["listing_url"] == (
            "https://fallback-sheet.example.com/"
        )


def test_sheet_directory_audit_opt_in_recovery_failure_is_diagnostic_only() -> None:
    with workspace_tmpdir("sheet-directory-audit-recovery-failure") as root:
        audit_path = root / "sheet-audit.json"
        csv_text = """Studio,Roles open,Link
Down Sheet Studio,yes,https://down-sheet.example.com/
"""

        artifact, _cache_hit = sd.run_sheet_directory_audit(
            5,
            sheet_id="sheet_test",
            gid="1",
            config=_audit_config(str(audit_path), recovery_enabled=True),
            fetcher=_fetch_from({_sheet_url(): csv_text}),
        )

        assert artifact["summary"]["recoveryFailures"] > 0
        assert artifact["summary"]["failures"] == 0
        assert len(artifact["staticCandidates"]) == 1
        assert artifact["staticCandidates"][0]["listing_url"] == "https://down-sheet.example.com/"


def test_sheet_directory_audit_opt_in_recovery_keeps_invalid_url_failures() -> None:
    with workspace_tmpdir("sheet-directory-audit-recovery-invalid-url") as root:
        audit_path = root / "sheet-audit.json"
        csv_text = """Studio,Roles open,Link
Bad Url Studio,yes,http://[broken
"""

        artifact, _cache_hit = sd.run_sheet_directory_audit(
            5,
            sheet_id="sheet_test",
            gid="1",
            config=_audit_config(str(audit_path), recovery_enabled=True),
            fetcher=_fetch_from({_sheet_url(): csv_text}),
        )

        assert artifact["summary"]["invalidUrls"] == 1
        assert artifact["summary"]["failures"] == 1
        assert artifact["failureCounts"] == {"directory_detail_parse": 1}
        assert "recoveryFetchAttempts" not in artifact["summary"]


def test_sheet_directory_audit_signature_rebuilds_when_recovery_toggle_changes() -> None:
    with workspace_tmpdir("sheet-directory-audit-recovery-signature") as root:
        audit_path = root / "sheet-audit.json"
        csv_text = """Studio,Roles open,Link
Recoverable Sheet Studio,yes,https://recover-signature.example.com/
"""

        first_artifact, first_cache_hit = sd.run_sheet_directory_audit(
            5,
            sheet_id="sheet_test",
            gid="1",
            config=_audit_config(str(audit_path), recovery_enabled=False),
            fetcher=_fetch_from({_sheet_url(): csv_text}),
        )
        second_artifact, second_cache_hit = sd.run_sheet_directory_audit(
            5,
            sheet_id="sheet_test",
            gid="1",
            config=_audit_config(str(audit_path), recovery_enabled=True),
            fetcher=_fetch_from(
                {
                    _sheet_url(): csv_text,
                    "https://recover-signature.example.com/careers": (
                        '<a href="/jobs/engineer">Engineer</a>'
                    ),
                    "https://recover-signature.example.com/jobs": (
                        "<html><body>No roles</body></html>"
                    ),
                }
            ),
        )

        assert first_cache_hit is False
        assert second_cache_hit is False
        assert "recoveryFetchAttempts" not in first_artifact["summary"]
        assert second_artifact["summary"]["recoveryFetchAttempts"] == 2
