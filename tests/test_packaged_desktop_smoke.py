import base64
import json
import os
from datetime import datetime
from pathlib import Path
from unittest import mock
from urllib.request import Request, urlopen

import pytest

from src import packaged_desktop_smoke as smoke
from src import source_sync as source_sync
from src.ship.startup_profile import summarize_startup_metrics
from tests.helpers.temp_paths import workspace_tmpdir

pytestmark = [pytest.mark.packaging, pytest.mark.slow]


def _write_packaged_sync_bundle_config(
    portable_root: Path, *, key_derivation: str = "embedded"
) -> Path:
    app_dir = portable_root / "ship" / "app"
    version_dir = app_dir / "versions" / "0.1.32" / "packaging"
    version_dir.mkdir(parents=True, exist_ok=True)
    (app_dir / "current.txt").write_text("0.1.32\n", encoding="utf-8")
    private_key_pem = "-----BEGIN RSA PRIVATE KEY-----\nTEST\n-----END RSA PRIVATE KEY-----\n"
    salt_b64 = source_sync._base64url_encode(b"packaged-sync-rehearsal-salt")  # noqa: SLF001
    payload = {
        "schemaVersion": 1,
        "appId": "123456",
        "installationId": "999999",
        "repo": "owner/repo",
        "branch": "main",
        "path": "baluffo/source-sync.json",
        "allowedRepo": "owner/repo",
        "allowedBranch": "main",
        "allowedPathPrefix": "baluffo/source-sync.json",
    }
    if key_derivation == "embedded":
        payload.update(
            {
                "keyDerivation": "embedded",
                "embeddedKeyHint": "sync-smoke-hint",
                "embeddedKeyVersion": "v1",
                "keySalt": salt_b64,
                "privateKeyPemEnc": source_sync.encrypt_private_key_pem_with_passphrase(
                    private_key_pem,
                    salt_b64=salt_b64,
                    app_id="123456",
                    installation_id="999999",
                    passphrase=source_sync.build_embedded_passphrase(
                        hint="sync-smoke-hint", version="v1"
                    ),
                ),
            }
        )
    else:
        payload.update(
            {
                "keyDerivation": "machine",
                "keySalt": salt_b64,
                "privateKeyPemEnc": source_sync.encrypt_private_key_pem(
                    private_key_pem,
                    salt_b64=salt_b64,
                    app_id="123456",
                    installation_id="999999",
                ),
            }
        )
    config_path = version_dir / "github-app-sync-config.json"
    config_path.write_text(json.dumps(payload), encoding="utf-8")
    return config_path


def test_local_address_matches_listen_port() -> None:
    assert smoke._local_address_matches_listen_port("127.0.0.1:8080", 8080) is True
    assert smoke._local_address_matches_listen_port("127.0.0.1:8080", 8081) is False
    assert smoke._local_address_matches_listen_port("[::1]:9090", 9090) is True


def test_pids_listening_on_tcp_port_windows_parses_netstat() -> None:
    sample = (
        "\n"
        "Proto  Local Address          Foreign Address        State           PID\n"
        "TCP    127.0.0.1:50001        0.0.0.0:0              LISTENING       4242\n"
        "TCP    127.0.0.1:50002        0.0.0.0:0              LISTENING       4243\n"
        "TCP    192.168.1.1:50001      0.0.0.0:0              LISTENING       9999\n"
        "TCP    127.0.0.1:50003        10.0.0.1:443           ESTABLISHED     1111\n"
    )
    fake_completed = mock.Mock(stdout=sample, returncode=0)
    with mock.patch.object(smoke.os, "name", "nt"):
        with mock.patch.object(smoke.subprocess, "run", return_value=fake_completed) as run_mock:
            assert smoke.pids_listening_on_tcp_port_windows(50001) == {4242, 9999}
            assert smoke.pids_listening_on_tcp_port_windows(50002) == {4243}
            assert smoke.pids_listening_on_tcp_port_windows(50003) == set()
    assert run_mock.call_count == 3


def test_pids_listening_on_tcp_port_non_windows_returns_empty() -> None:
    with mock.patch.object(smoke.os, "name", "posix"):
        assert smoke.pids_listening_on_tcp_port_windows(9999) == set()


def test_read_startup_metrics_file_reads_jsonl_rows() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        metrics_path = Path(tmp) / "runtime-data" / "desktop-startup-metrics.jsonl"
        metrics_path.parent.mkdir(parents=True, exist_ok=True)
        metrics_path.write_text(
            "\n".join(
                [
                    json.dumps({"event": "desktop_launch_start", "fields": {"elapsedMs": 0}}),
                    json.dumps({"event": "desktop_window_shown", "fields": {"elapsedMs": 10}}),
                ]
            ),
            encoding="utf-8",
        )
        rows = smoke.read_startup_metrics_file(metrics_path.parent, limit=10)
        assert [row["event"] for row in rows] == ["desktop_launch_start", "desktop_window_shown"]


def test_inject_desktop_update_public_keys_writes_packaged_trust_files(tmp_path: Path) -> None:
    portable_root = tmp_path / "portable"
    version_dir = portable_root / "ship" / "app" / "versions" / "0.1.0" / "packaging"
    version_dir.mkdir(parents=True, exist_ok=True)
    (portable_root / "ship" / "app" / "current.txt").write_text("0.1.0\n", encoding="utf-8")

    smoke._inject_desktop_update_public_keys(
        portable_root,
        {"desktop-ed25519-rehearsal": "ZmFrZS1rZXk="},
    )

    expected = json.dumps({"desktop-ed25519-rehearsal": "ZmFrZS1rZXk="}, indent=2, sort_keys=True)
    assert (portable_root / "ship" / "app" / "desktop-update-public-keys.json").read_text(
        encoding="utf-8"
    ) == expected
    assert (
        portable_root
        / "ship"
        / "app"
        / "versions"
        / "0.1.0"
        / "packaging"
        / "desktop-update-public-keys.json"
    ).read_text(encoding="utf-8") == expected


def test_startup_profile_required_events_include_window_and_page_ready_markers() -> None:
    assert smoke.startup_profile_required_events("jobs") == (
        "desktop_launch_start",
        "desktop_site_ready",
        "desktop_window_created",
        "desktop_shell_window_shown",
        "jobs_module_boot_start",
        "jobs_first_render",
        "jobs_first_interactive",
    )
    assert smoke.startup_profile_required_events("admin")[-1] == "admin_ready"
    assert smoke.startup_profile_required_events("desktop-probe") == (
        "desktop_launch_start",
        "desktop_site_ready",
        "desktop_window_created",
        "desktop_shell_window_shown",
        "desktop_probe_html_parse_start",
        "desktop_probe_ready",
    )
    assert smoke.startup_profile_required_events("desktop-probe-head") == (
        "desktop_launch_start",
        "desktop_site_ready",
        "desktop_window_created",
        "desktop_shell_window_shown",
        "desktop_probe_head_html_parse_start",
        "desktop_probe_head_ready",
    )
    assert smoke.startup_profile_required_events("desktop-probe-css") == (
        "desktop_launch_start",
        "desktop_site_ready",
        "desktop_window_created",
        "desktop_shell_window_shown",
        "desktop_probe_css_html_parse_start",
        "desktop_probe_css_ready",
    )
    assert smoke.startup_profile_required_events("desktop-probe-inline") == (
        "desktop_launch_start",
        "desktop_site_ready",
        "desktop_window_created",
        "desktop_shell_window_shown",
        "desktop_probe_inline_html_parse_start",
        "desktop_probe_inline_ready",
    )


def test_startup_profile_summary_classifies_blank_probe_page_load_delay() -> None:
    rows = [
        {
            "ts": "2026-03-10T12:00:00+00:00",
            "event": "desktop_launch_start",
            "fields": {"elapsedMs": 0},
        },
        {
            "ts": "2026-03-10T12:00:01+00:00",
            "event": "desktop_site_ready",
            "fields": {"elapsedMs": 1000},
        },
        {
            "ts": "2026-03-10T12:00:01.100000+00:00",
            "event": "desktop_window_created",
            "fields": {"elapsedMs": 1100},
        },
        {
            "ts": "2026-03-10T12:00:01.300000+00:00",
            "event": "desktop_shell_window_shown",
            "fields": {"elapsedMs": 1300},
        },
        {
            "ts": "2026-03-10T12:00:08+00:00",
            "event": "desktop_probe_html_parse_start",
            "payload": {"elapsedMs": 8000},
        },
        {
            "ts": "2026-03-10T12:00:08.050000+00:00",
            "event": "desktop_probe_ready",
            "payload": {"elapsedMs": 8050},
        },
    ]
    summary = summarize_startup_metrics(rows, page="desktop-probe", profile_mode="cold")
    assert summary["classification"] == "desktop page load delayed"
    assert summary["firstUsableMs"] == 8050


def test_startup_profile_summary_supports_head_probe_page() -> None:
    rows = [
        {
            "ts": "2026-03-10T12:00:00+00:00",
            "event": "desktop_launch_start",
            "fields": {"elapsedMs": 0},
        },
        {
            "ts": "2026-03-10T12:00:01+00:00",
            "event": "desktop_site_ready",
            "fields": {"elapsedMs": 1000},
        },
        {
            "ts": "2026-03-10T12:00:01.100000+00:00",
            "event": "desktop_window_created",
            "fields": {"elapsedMs": 1100},
        },
        {
            "ts": "2026-03-10T12:00:01.300000+00:00",
            "event": "desktop_shell_window_shown",
            "fields": {"elapsedMs": 1300},
        },
        {
            "ts": "2026-03-10T12:00:02.500000+00:00",
            "event": "desktop_probe_head_html_parse_start",
            "payload": {"elapsedMs": 2500},
        },
        {
            "ts": "2026-03-10T12:00:02.550000+00:00",
            "event": "desktop_probe_head_ready",
            "payload": {"elapsedMs": 2550},
        },
    ]
    summary = summarize_startup_metrics(rows, page="desktop-probe-head", profile_mode="cold")
    assert summary["firstUsableEvent"] == "desktop_probe_head_ready"
    assert summary["firstUsableMs"] == 2550


def test_startup_profile_summary_supports_css_probe_page() -> None:
    rows = [
        {
            "ts": "2026-03-10T12:00:00+00:00",
            "event": "desktop_launch_start",
            "fields": {"elapsedMs": 0},
        },
        {
            "ts": "2026-03-10T12:00:01+00:00",
            "event": "desktop_site_ready",
            "fields": {"elapsedMs": 1000},
        },
        {
            "ts": "2026-03-10T12:00:01.100000+00:00",
            "event": "desktop_window_created",
            "fields": {"elapsedMs": 1100},
        },
        {
            "ts": "2026-03-10T12:00:01.300000+00:00",
            "event": "desktop_shell_window_shown",
            "fields": {"elapsedMs": 1300},
        },
        {
            "ts": "2026-03-10T12:00:03+00:00",
            "event": "desktop_probe_css_html_parse_start",
            "payload": {"elapsedMs": 3000},
        },
        {
            "ts": "2026-03-10T12:00:03.020000+00:00",
            "event": "desktop_probe_css_ready",
            "payload": {"elapsedMs": 3020},
        },
    ]
    summary = summarize_startup_metrics(rows, page="desktop-probe-css", profile_mode="cold")
    assert summary["firstUsableEvent"] == "desktop_probe_css_ready"
    assert summary["firstUsableMs"] == 3020


def test_startup_profile_summary_supports_inline_probe_page() -> None:
    rows = [
        {
            "ts": "2026-03-10T12:00:00+00:00",
            "event": "desktop_launch_start",
            "fields": {"elapsedMs": 0},
        },
        {
            "ts": "2026-03-10T12:00:01+00:00",
            "event": "desktop_site_ready",
            "fields": {"elapsedMs": 1000},
        },
        {
            "ts": "2026-03-10T12:00:01.100000+00:00",
            "event": "desktop_window_created",
            "fields": {"elapsedMs": 1100},
        },
        {
            "ts": "2026-03-10T12:00:01.300000+00:00",
            "event": "desktop_shell_window_shown",
            "fields": {"elapsedMs": 1300},
        },
        {
            "ts": "2026-03-10T12:00:02.100000+00:00",
            "event": "desktop_probe_inline_html_parse_start",
            "payload": {"elapsedMs": 2100},
        },
        {
            "ts": "2026-03-10T12:00:02.120000+00:00",
            "event": "desktop_probe_inline_ready",
            "payload": {"elapsedMs": 2120},
        },
    ]
    summary = summarize_startup_metrics(rows, page="desktop-probe-inline", profile_mode="cold")
    assert summary["firstUsableEvent"] == "desktop_probe_inline_ready"
    assert summary["firstUsableMs"] == 2120


def test_startup_profile_summary_classifies_local_auth_delay() -> None:
    rows = [
        {
            "ts": "2026-03-10T12:00:00+00:00",
            "event": "desktop_launch_start",
            "fields": {"elapsedMs": 0},
        },
        {
            "ts": "2026-03-10T12:00:01+00:00",
            "event": "desktop_site_ready",
            "fields": {"elapsedMs": 1000},
        },
        {
            "ts": "2026-03-10T12:00:01.200000+00:00",
            "event": "desktop_window_created",
            "fields": {"elapsedMs": 1200},
        },
        {
            "ts": "2026-03-10T12:00:01.400000+00:00",
            "event": "desktop_window_shown",
            "fields": {"elapsedMs": 1400},
        },
        {
            "ts": "2026-03-10T12:00:02+00:00",
            "event": "desktop_page_loaded",
            "fields": {"elapsedMs": 2000},
        },
        {
            "ts": "2026-03-10T12:00:02.100000+00:00",
            "event": "jobs_local_data_init_ready",
            "payload": {"elapsedMs": 2100},
        },
        {
            "ts": "2026-03-10T12:00:07.500000+00:00",
            "event": "jobs_auth_ready",
            "payload": {"elapsedMs": 7500},
        },
        {
            "ts": "2026-03-10T12:00:08+00:00",
            "event": "jobs_first_render",
            "payload": {"elapsedMs": 8000},
        },
        {
            "ts": "2026-03-10T12:00:08.200000+00:00",
            "event": "jobs_first_interactive",
            "payload": {"elapsedMs": 8200},
        },
    ]
    summary = summarize_startup_metrics(rows, page="jobs", profile_mode="cold")
    assert summary["classification"] == "local auth bootstrap delayed"
    assert summary["status"] == "failed"


def test_startup_profile_summary_prefers_timestamps_over_mixed_elapsed_ms_clocks() -> None:
    rows = [
        {
            "ts": "2026-03-10T12:00:00+00:00",
            "event": "desktop_launch_start",
            "fields": {"elapsedMs": 0},
        },
        {
            "ts": "2026-03-10T12:00:02+00:00",
            "event": "desktop_site_ready",
            "fields": {"elapsedMs": 2000},
        },
        {
            "ts": "2026-03-10T12:00:05+00:00",
            "event": "desktop_window_created",
            "fields": {"elapsedMs": 5000},
        },
        {
            "ts": "2026-03-10T12:00:05.200000+00:00",
            "event": "desktop_shell_window_shown",
            "fields": {"elapsedMs": 3000},
        },
        {
            "ts": "2026-03-10T12:00:05.500000+00:00",
            "event": "jobs_page_boot_start",
            "payload": {"elapsedMs": 100},
        },
        {
            "ts": "2026-03-10T12:00:05.600000+00:00",
            "event": "jobs_local_data_init_ready",
            "payload": {"elapsedMs": 120},
        },
        {
            "ts": "2026-03-10T12:00:05.700000+00:00",
            "event": "jobs_auth_ready",
            "payload": {"elapsedMs": 4},
        },
        {
            "ts": "2026-03-10T12:00:05.800000+00:00",
            "event": "jobs_first_render",
            "payload": {"elapsedMs": 1500},
        },
        {
            "ts": "2026-03-10T12:00:05.900000+00:00",
            "event": "jobs_first_interactive",
            "payload": {"elapsedMs": 1600},
        },
    ]

    summary = summarize_startup_metrics(rows, page="jobs", profile_mode="cold")
    stages = {stage["key"]: stage for stage in summary["stages"]}

    assert summary["classification"] == "browser launch / app-window creation delayed"
    assert summary["missingEvents"] == []
    assert stages["window_created_to_window_shown"]["durationMs"] == 200
    assert stages["page_loaded_to_local_data_ready"]["durationMs"] == 100
    assert stages["local_data_ready_to_auth_ready"]["durationMs"] == 100
    assert stages["auth_ready_to_first_render"]["durationMs"] == 100
    assert summary["firstUsableEvent"] == "jobs_first_interactive"
    assert summary["firstUsableMs"] == 5900


def test_startup_profile_summary_uses_inferred_shell_window_fallback_when_visibility_not_observed() -> (
    None
):
    rows = [
        {
            "ts": "2026-03-10T12:00:00+00:00",
            "event": "desktop_launch_start",
            "fields": {"elapsedMs": 0},
        },
        {
            "ts": "2026-03-10T12:00:01+00:00",
            "event": "desktop_site_ready",
            "fields": {"elapsedMs": 1000},
        },
        {
            "ts": "2026-03-10T12:00:01.100000+00:00",
            "event": "desktop_window_created",
            "fields": {"elapsedMs": 1100},
        },
        {
            "ts": "2026-03-10T12:00:01.700000+00:00",
            "event": "desktop_shell_window_shown_inferred",
            "fields": {"elapsedMs": 1700},
        },
        {
            "ts": "2026-03-10T12:00:01.900000+00:00",
            "event": "jobs_page_boot_start",
            "payload": {"elapsedMs": 1900},
        },
        {
            "ts": "2026-03-10T12:00:02.100000+00:00",
            "event": "jobs_local_data_init_ready",
            "payload": {"elapsedMs": 2100},
        },
        {
            "ts": "2026-03-10T12:00:02.200000+00:00",
            "event": "jobs_auth_ready",
            "payload": {"elapsedMs": 2200},
        },
        {
            "ts": "2026-03-10T12:00:02.400000+00:00",
            "event": "jobs_first_render",
            "payload": {"elapsedMs": 2400},
        },
        {
            "ts": "2026-03-10T12:00:02.500000+00:00",
            "event": "jobs_first_interactive",
            "payload": {"elapsedMs": 2500},
        },
    ]

    summary = summarize_startup_metrics(rows, page="jobs", profile_mode="warm")
    stages = {stage["key"]: stage for stage in summary["stages"]}

    assert summary["missingEvents"] == []
    assert (
        stages["window_created_to_window_shown"]["endEvent"]
        == "desktop_shell_window_shown_inferred"
    )
    assert stages["window_created_to_window_shown"]["durationMs"] == 600
    assert (
        stages["window_shown_to_page_loaded"]["startEvent"] == "desktop_shell_window_shown_inferred"
    )


def test_startup_profile_summary_does_not_report_inferred_reveal_after_page_boot() -> None:
    rows = [
        {
            "ts": "2026-03-10T12:00:00+00:00",
            "event": "desktop_launch_start",
            "fields": {"elapsedMs": 0},
        },
        {
            "ts": "2026-03-10T12:00:01+00:00",
            "event": "desktop_site_ready",
            "fields": {"elapsedMs": 1000},
        },
        {
            "ts": "2026-03-10T12:00:01.100000+00:00",
            "event": "desktop_window_created",
            "fields": {"elapsedMs": 1100},
        },
        {
            "ts": "2026-03-10T12:00:01.900000+00:00",
            "event": "desktop_shell_window_shown_inferred",
            "fields": {"elapsedMs": 1900},
        },
        {
            "ts": "2026-03-10T12:00:01.900000+00:00",
            "event": "jobs_page_boot_start",
            "payload": {"elapsedMs": 1900},
        },
        {
            "ts": "2026-03-10T12:00:02.000000+00:00",
            "event": "jobs_local_data_init_ready",
            "payload": {"elapsedMs": 2000},
        },
        {
            "ts": "2026-03-10T12:00:02.100000+00:00",
            "event": "jobs_auth_ready",
            "payload": {"elapsedMs": 2100},
        },
        {
            "ts": "2026-03-10T12:00:02.300000+00:00",
            "event": "jobs_first_render",
            "payload": {"elapsedMs": 2300},
        },
        {
            "ts": "2026-03-10T12:00:02.400000+00:00",
            "event": "jobs_first_interactive",
            "payload": {"elapsedMs": 2400},
        },
    ]

    summary = summarize_startup_metrics(rows, page="jobs", profile_mode="warm")
    stages = {stage["key"]: stage for stage in summary["stages"]}

    assert (
        stages["window_shown_to_page_loaded"]["startMs"]
        <= stages["window_shown_to_page_loaded"]["endMs"]
    )
    assert stages["window_shown_to_page_loaded"]["durationMs"] == 0


def test_startup_profile_summary_prefers_browser_created_timestamps_for_page_events() -> None:
    launch_ts_ms = int(datetime.fromisoformat("2026-03-10T12:00:00+00:00").timestamp() * 1000)
    rows = [
        {
            "ts": "2026-03-10T12:00:00+00:00",
            "event": "desktop_launch_start",
            "fields": {"elapsedMs": 0},
        },
        {
            "ts": "2026-03-10T12:00:01+00:00",
            "event": "desktop_site_ready",
            "fields": {"elapsedMs": 1000},
        },
        {
            "ts": "2026-03-10T12:00:01.100000+00:00",
            "event": "desktop_window_created",
            "fields": {"elapsedMs": 1100},
        },
        {
            "ts": "2026-03-10T12:00:01.300000+00:00",
            "event": "desktop_shell_window_shown",
            "fields": {"elapsedMs": 1300},
        },
        {
            "ts": "2026-03-10T12:00:04.400000+00:00",
            "event": "jobs_auth_ready",
            "payload": {"elapsedMs": 4100, "browserCreatedAtMs": launch_ts_ms + 2000},
            "browserTsMs": launch_ts_ms + 2000,
        },
        {
            "ts": "2026-03-10T12:00:06.800000+00:00",
            "event": "jobs_first_render",
            "payload": {"elapsedMs": 6500, "browserCreatedAtMs": launch_ts_ms + 2150},
            "browserTsMs": launch_ts_ms + 2150,
        },
        {
            "ts": "2026-03-10T12:00:06.900000+00:00",
            "event": "jobs_first_interactive",
            "payload": {"elapsedMs": 6600, "browserCreatedAtMs": launch_ts_ms + 2160},
            "browserTsMs": launch_ts_ms + 2160,
        },
    ]

    summary = summarize_startup_metrics(rows, page="jobs", profile_mode="warm")
    stages = {stage["key"]: stage for stage in summary["stages"]}

    assert stages["auth_ready_to_first_render"]["durationMs"] == 150
    assert stages["first_render_to_first_interactive"]["durationMs"] == 10
    assert summary["firstUsableMs"] == 2160


def test_startup_profile_summary_classifies_bridge_site_bootstrap_delay() -> None:
    rows = [
        {
            "ts": "2026-03-10T12:00:00+00:00",
            "event": "desktop_launch_start",
            "fields": {"elapsedMs": 0},
        },
        {
            "ts": "2026-03-10T12:00:03+00:00",
            "event": "desktop_site_ready",
            "fields": {"elapsedMs": 3000},
        },
        {
            "ts": "2026-03-10T12:00:03.500000+00:00",
            "event": "desktop_window_created",
            "fields": {"elapsedMs": 3500},
        },
        {
            "ts": "2026-03-10T12:00:03.700000+00:00",
            "event": "desktop_shell_window_shown",
            "fields": {"elapsedMs": 3700},
        },
        {
            "ts": "2026-03-10T12:00:03.900000+00:00",
            "event": "jobs_page_boot_start",
            "payload": {"elapsedMs": 3900},
        },
        {
            "ts": "2026-03-10T12:00:04+00:00",
            "event": "jobs_local_data_init_ready",
            "payload": {"elapsedMs": 4000},
        },
        {
            "ts": "2026-03-10T12:00:04.100000+00:00",
            "event": "jobs_auth_ready",
            "payload": {"elapsedMs": 4100},
        },
        {
            "ts": "2026-03-10T12:00:04.300000+00:00",
            "event": "jobs_first_render",
            "payload": {"elapsedMs": 4300},
        },
        {
            "ts": "2026-03-10T12:00:04.400000+00:00",
            "event": "jobs_first_interactive",
            "payload": {"elapsedMs": 4400},
        },
    ]

    summary = summarize_startup_metrics(rows, page="jobs", profile_mode="cold")

    assert summary["classification"] == "bridge/site bootstrap delayed"
    assert summary["status"] == "failed"


def test_ensure_portable_exe_raises_when_missing_and_build_still_missing() -> None:
    with (
        workspace_tmpdir("packaged-smoke") as tmp,
    ):
        exe_path = Path(tmp) / "dist" / "baluffo-portable" / "Baluffo.exe"
        with (
            mock.patch.object(smoke, "DEFAULT_EXE_PATH", exe_path),
            mock.patch.object(smoke, "run_portable_build") as build_mock,
        ):
            with pytest.raises(RuntimeError, match="Packaged desktop executable not found"):
                smoke.ensure_portable_exe(exe_path, rebuild=False)
            build_mock.assert_called_once()


def test_ensure_portable_exe_uses_rebuild_output_dir_when_requested() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        requested_exe = root / "dist" / "baluffo-portable" / "Baluffo.exe"
        rebuilt_dir = root / "artifacts" / "portable-build"
        rebuilt_exe = rebuilt_dir / "Baluffo.exe"
        rebuilt_dir.mkdir(parents=True, exist_ok=True)
        rebuilt_exe.write_text("exe", encoding="utf-8")
        with (
            mock.patch.object(smoke, "DEFAULT_EXE_PATH", requested_exe),
            mock.patch.object(smoke, "run_portable_build", return_value=rebuilt_exe) as build_mock,
        ):
            resolved = smoke.ensure_portable_exe(
                requested_exe, rebuild=True, rebuild_output_dir=rebuilt_dir
            )
        assert resolved == rebuilt_exe.resolve()
        build_mock.assert_called_once_with(rebuilt_dir)


def test_run_portable_build_cleans_pyinstaller_scratch_dirs_for_explicit_output_dir() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        output_dir = root / "artifacts" / "portable-build"
        output_dir.mkdir(parents=True, exist_ok=True)
        for name in smoke.PORTABLE_BUILD_SCRATCH_NAMES:
            candidate = output_dir.parent / name
            candidate.mkdir(parents=True, exist_ok=True)
            (candidate / "marker.txt").write_text("x", encoding="utf-8")
        with mock.patch.object(smoke.subprocess, "run") as run_mock:
            exe_path = smoke.run_portable_build(output_dir)
        assert exe_path == output_dir / "Baluffo.exe"
        run_mock.assert_called_once()
        for name in smoke.PORTABLE_BUILD_SCRATCH_NAMES:
            assert not (output_dir.parent / name).exists()


def test_prune_packaged_smoke_artifacts_keeps_recent_runs_and_current_dir() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp) / "packaged-desktop-smoke"
        root.mkdir(parents=True, exist_ok=True)
        current_dir = root / "20260416-120003"
        retained_dir = root / "20260416-120002"
        stale_dir_a = root / "20260416-120001"
        stale_dir_b = root / "20260416-120000"
        old_file = root / "jobs-pipeline-report.json"
        for path in (current_dir, retained_dir, stale_dir_a, stale_dir_b):
            path.mkdir(parents=True, exist_ok=True)
            (path / "marker.txt").write_text(path.name, encoding="utf-8")
        old_file.write_text("{}", encoding="utf-8")
        os.utime(retained_dir, (200.0, 200.0))
        os.utime(stale_dir_a, (100.0, 100.0))
        os.utime(stale_dir_b, (50.0, 50.0))
        os.utime(old_file, (10.0, 10.0))
        with mock.patch.object(smoke.time, "time", return_value=10_000.0):
            removed = smoke.prune_packaged_smoke_artifacts(
                root,
                keep_recent_runs=2,
                file_retention_s=60,
                current_artifacts_dir=current_dir,
            )
        assert current_dir.exists()
        assert retained_dir.exists()
        assert not stale_dir_a.exists()
        assert not stale_dir_b.exists()
        assert not old_file.exists()
        assert {path.name for path in removed} == {
            stale_dir_a.name,
            stale_dir_b.name,
            old_file.name,
        }


def test_generate_packaged_smoke_run_token_is_collision_safe_with_entropy() -> None:
    now = smoke.datetime(2026, 4, 16, 12, 0, 0, 123456, tzinfo=smoke.UTC)
    first = smoke.generate_packaged_smoke_run_token(now=now, entropy_ns=101)
    second = smoke.generate_packaged_smoke_run_token(now=now, entropy_ns=202)

    assert first != second
    assert first.startswith("20260416-120000-123456-")
    assert second.startswith("20260416-120000-123456-")


def test_select_startup_probe_browser_prefers_chrome_then_brave_then_edge() -> None:
    candidates = [
        {"name": "chrome", "path": "C:/Chrome/chrome.exe"},
        {"name": "brave", "path": "C:/Brave/brave.exe"},
        {"name": "msedge", "path": "C:/Edge/msedge.exe"},
    ]
    with (
        mock.patch.object(
            smoke.desktop_app_mod,
            "resolve_chromium_browser_candidates",
            return_value=candidates,
        ),
        mock.patch.object(smoke.desktop_app_mod, "chromium_app_mode_supported", return_value=True),
    ):
        selected = smoke.select_startup_probe_browser({})

    assert selected == {
        "browserName": "chrome",
        "browserPath": "C:/Chrome/chrome.exe",
    }


def test_select_startup_probe_browser_uses_edge_only_when_other_candidates_unavailable() -> None:
    candidates = [
        {"name": "chrome", "path": "C:/Chrome/chrome.exe"},
        {"name": "brave", "path": "C:/Brave/brave.exe"},
        {"name": "msedge", "path": "C:/Edge/msedge.exe"},
    ]

    def fake_supported(candidate, env=None):  # noqa: ANN001, ANN202
        return str(candidate.get("name")) == "msedge"

    with (
        mock.patch.object(
            smoke.desktop_app_mod,
            "resolve_chromium_browser_candidates",
            return_value=candidates,
        ),
        mock.patch.object(
            smoke.desktop_app_mod,
            "chromium_app_mode_supported",
            side_effect=fake_supported,
        ),
    ):
        selected = smoke.select_startup_probe_browser({"BALUFFO_DESKTOP_ALLOW_EDGE_APP_MODE": "1"})

    assert selected == {
        "browserName": "msedge",
        "browserPath": "C:/Edge/msedge.exe",
    }


def test_select_startup_probe_browser_fails_when_no_supported_candidate_exists() -> None:
    with (
        mock.patch.object(
            smoke.desktop_app_mod,
            "resolve_chromium_browser_candidates",
            return_value=[{"name": "msedge", "path": "C:/Edge/msedge.exe"}],
        ),
        mock.patch.object(smoke.desktop_app_mod, "chromium_app_mode_supported", return_value=False),
    ):
        with pytest.raises(
            RuntimeError, match="No supported managed Chromium probe browser available"
        ):
            smoke.select_startup_probe_browser({})


def test_ensure_portable_exe_rebuilds_default_dist_when_exe_older_than_sources() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        fake_default = Path(tmp) / "dist" / "baluffo-portable" / "Baluffo.exe"
        fake_default.parent.mkdir(parents=True, exist_ok=True)
        fake_default.write_text("old", encoding="utf-8")
        old = 1_000_000.0
        os.utime(fake_default, (old, old))
        with (
            mock.patch.object(smoke, "DEFAULT_EXE_PATH", fake_default),
            mock.patch.object(smoke, "run_portable_build", return_value=fake_default) as build_mock,
        ):
            smoke.ensure_portable_exe(fake_default, rebuild=False)
        build_mock.assert_called_once_with(None)


def test_default_portable_exe_becomes_stale_when_frontend_asset_is_newer() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        fake_default = root / "dist" / "baluffo-portable" / "Baluffo.exe"
        frontend_asset = root / "frontend" / "jobs" / "app" / "feed.js"
        fake_default.parent.mkdir(parents=True, exist_ok=True)
        frontend_asset.parent.mkdir(parents=True, exist_ok=True)
        fake_default.write_text("old", encoding="utf-8")
        frontend_asset.write_text("newer", encoding="utf-8")
        old = 1_000_000.0
        new = old + 100
        os.utime(fake_default, (old, old))
        os.utime(frontend_asset, (new, new))

        with (
            mock.patch.object(smoke, "ROOT", root),
            mock.patch.object(smoke, "DEFAULT_EXE_PATH", fake_default),
            mock.patch.object(smoke, "_PORTABLE_EXE_FRESHNESS_MARKERS", ()),
            mock.patch.object(smoke, "_PORTABLE_EXE_FRESHNESS_DIRS", (root / "frontend",)),
        ):
            assert smoke._default_portable_exe_stale(fake_default) is True


def test_ensure_portable_exe_honors_explicit_path_without_rebuilding() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        explicit_exe = Path(tmp) / "_out" / "latest" / "build" / "portable" / "Baluffo.exe"
        explicit_exe.parent.mkdir(parents=True, exist_ok=True)
        explicit_exe.write_text("exe", encoding="utf-8")
        with mock.patch.object(smoke, "run_portable_build") as build_mock:
            resolved = smoke.ensure_portable_exe(explicit_exe, rebuild=True)
        assert resolved == explicit_exe.resolve()
        build_mock.assert_not_called()


def test_ensure_portable_exe_rejects_missing_explicit_path_instead_of_building_default() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        explicit_exe = Path(tmp) / "_out" / "latest" / "build" / "portable" / "Baluffo.exe"
        with mock.patch.object(smoke, "run_portable_build") as build_mock:
            with pytest.raises(RuntimeError, match="Packaged desktop executable not found"):
                smoke.ensure_portable_exe(explicit_exe, rebuild=True)
        build_mock.assert_not_called()


def test_parse_packaged_node_smoke_report_reads_scenarios() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        report_path = Path(tmp) / "smoke-report.json"
        report_path.write_text(
            json.dumps(
                {
                    "ok": False,
                    "scenarios": [
                        {
                            "name": "Jobs startup",
                            "status": "passed",
                            "durationMs": 1200,
                            "error": "",
                        },
                        {
                            "name": "Admin action",
                            "status": "failed",
                            "durationMs": 500,
                            "error": "unlock failed",
                        },
                    ],
                }
            ),
            encoding="utf-8",
        )
        rows = smoke.parse_packaged_node_smoke_report(report_path)
        assert len(rows) == 2
        assert rows[0]["name"] == "Jobs startup"
        assert rows[1]["error"] == "unlock failed"


def test_collect_packaged_smoke_env_diagnostics_reports_paths_and_elevation() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        exe_path = root / "dist" / "baluffo-portable" / "Baluffo.exe"
        exe_path.parent.mkdir(parents=True, exist_ok=True)
        exe_path.write_text("exe", encoding="utf-8")
        env = {"TMP": str(root / "tmp"), "TEMP": str(root / "temp")}
        with (
            mock.patch.object(smoke, "DEFAULT_EXE_PATH", exe_path),
            mock.patch.object(smoke, "is_windows_process_elevated", return_value=True),
        ):
            diagnostics = smoke.collect_packaged_smoke_env_diagnostics(
                artifacts_dir=root / "artifacts",
                requested_exe_path=exe_path,
                exe_path=exe_path,
                node_smoke_script=smoke.DEFAULT_NODE_SMOKE_SCRIPT,
                node_command=["C:/Program Files/nodejs/node.exe"],
                env=env,
            )
        assert diagnostics["requestedExePath"] == str(exe_path.resolve())
        assert diagnostics["defaultExePath"] == str(exe_path.resolve())
        assert diagnostics["exePathMode"] == "default-dist"
        assert diagnostics["exePathSource"] == "default-dist"
        assert diagnostics["explicitExePathFreshness"] == "n/a"
        assert diagnostics["rebuiltPortableExe"] is False
        assert diagnostics["artifactsDirWritable"]
        assert diagnostics["exeParentWritable"]
        assert diagnostics["nodePath"] == "C:/Program Files/nodejs/node.exe"
        assert diagnostics["tmp"] == str(root / "tmp")
        assert diagnostics["temp"] == str(root / "temp")
        assert diagnostics["isElevated"]


def test_collect_packaged_smoke_env_diagnostics_reports_explicit_path_freshness() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        explicit_exe = root / "_out" / "latest" / "build" / "portable" / "Baluffo.exe"
        explicit_exe.parent.mkdir(parents=True, exist_ok=True)
        explicit_exe.write_text("exe", encoding="utf-8")
        with (
            mock.patch.object(smoke, "_portable_exe_marker_staleness", return_value="stale"),
            mock.patch.object(smoke, "is_windows_process_elevated", return_value=False),
        ):
            diagnostics = smoke.collect_packaged_smoke_env_diagnostics(
                artifacts_dir=root / "artifacts",
                requested_exe_path=explicit_exe,
                exe_path=explicit_exe,
                node_smoke_script=smoke.DEFAULT_NODE_SMOKE_SCRIPT,
                node_command=["node"],
                env={},
            )
        assert diagnostics["exePathMode"] == "explicit-path"
        assert diagnostics["exePathSource"] == "explicit-path"
        assert diagnostics["explicitExePathFreshness"] == "stale"
        assert diagnostics["rebuiltPortableExe"] is False


def test_collect_packaged_smoke_env_diagnostics_reports_rebuilt_default_dist() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        exe_path = root / "dist" / "baluffo-portable" / "Baluffo.exe"
        exe_path.parent.mkdir(parents=True, exist_ok=True)
        exe_path.write_text("exe", encoding="utf-8")
        with (
            mock.patch.object(smoke, "DEFAULT_EXE_PATH", exe_path),
            mock.patch.object(smoke, "is_windows_process_elevated", return_value=False),
        ):
            diagnostics = smoke.collect_packaged_smoke_env_diagnostics(
                artifacts_dir=root / "artifacts",
                requested_exe_path=exe_path,
                exe_path=exe_path,
                node_smoke_script=smoke.DEFAULT_NODE_SMOKE_SCRIPT,
                rebuilt_portable_dir=root / "artifacts" / "portable-build",
                node_command=["node"],
                env={},
            )
        assert diagnostics["exePathMode"] == "default-dist"
        assert diagnostics["exePathSource"] == "rebuilt-dist"
        assert diagnostics["explicitExePathFreshness"] == "n/a"
        assert diagnostics["rebuiltPortableExe"] is True


def test_packaged_pipeline_smoke_mode_is_enabled_only_for_jobs_pipeline_script() -> None:
    assert (
        smoke.packaged_pipeline_smoke_mode(smoke.JOBS_PIPELINE_NODE_SMOKE_SCRIPT) == "stub-success"
    )
    assert smoke.packaged_pipeline_smoke_mode(smoke.DEFAULT_NODE_SMOKE_SCRIPT) == ""
    assert smoke.packaged_runtime_env_overrides(smoke.JOBS_PIPELINE_NODE_SMOKE_SCRIPT) == {
        "BALUFFO_PACKAGED_SMOKE_PIPELINE_MODE": "stub-success"
    }
    assert smoke.packaged_runtime_env_overrides(smoke.DEFAULT_NODE_SMOKE_SCRIPT) == {}


def test_packaged_runtime_env_overrides_can_isolate_local_appdata_per_run() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        artifacts_dir = Path(tmp) / "artifacts"
        overrides = smoke.packaged_runtime_env_overrides(
            smoke.JOBS_PIPELINE_NODE_SMOKE_SCRIPT,
            artifacts_dir=artifacts_dir,
            session_scope="jobs-pipeline",
        )

        assert overrides["BALUFFO_PACKAGED_SMOKE_PIPELINE_MODE"] == "stub-success"
        assert Path(overrides["LOCALAPPDATA"]).resolve() == (
            smoke.packaged_desktop_local_appdata_root(
                artifacts_dir, session_scope="jobs-pipeline"
            ).resolve()
        )


def test_packaged_runtime_env_overrides_sets_startup_profile_mode_for_probes() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        overrides = smoke.packaged_runtime_env_overrides(
            artifacts_dir=Path(tmp) / "artifacts",
            startup_probe=True,
            profile_mode="warm",
        )

        assert overrides["BALUFFO_DESKTOP_ALLOW_EDGE_APP_MODE"] == "1"
        assert overrides[smoke.desktop_app_mod.STARTUP_PROFILE_MODE_ENV] == "warm"


def test_classify_subprocess_error_marks_spawn_eperm() -> None:
    error = PermissionError("spawn EPERM")
    assert smoke.classify_subprocess_error(error) == "node_process_spawn_blocked"
    assert (
        smoke.classify_subprocess_error("Error: spawn EPERM") == "playwright_worker_spawn_blocked"
    )
    assert (
        smoke.classify_subprocess_error("browserType.launch: spawn EPERM")
        == "node_process_spawn_blocked"
    )


@pytest.mark.slow
def test_run_packaged_smoke_writes_failure_report_on_runtime_timeout() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        process = mock.Mock()
        process.pid = 4242
        process.poll.return_value = None
        stdout_handle = mock.Mock()
        stderr_handle = mock.Mock()
        args = smoke.parse_args(
            [
                "--exe-path",
                str(exe_path),
                "--report-path",
                str(report_path),
                "--artifacts-dir",
                str(artifacts_dir),
                "--startup-probe",
                "--rebuild",
            ]
        )
        with (
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(
                smoke, "launch_packaged_exe", return_value=(process, stdout_handle, stderr_handle)
            ),
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime",
                side_effect=TimeoutError("timed out waiting for bridge"),
            ),
            mock.patch.object(smoke, "terminate_process_tree") as terminate_mock,
            mock.patch.object(smoke, "cleanup_orphaned_desktop_ports_nt"),
            mock.patch.object(smoke, "fetch_startup_metrics", return_value=[]),
            mock.patch.object(smoke, "read_startup_metrics_file", return_value=[]),
            mock.patch.object(
                smoke,
                "collect_packaged_smoke_env_diagnostics",
                return_value={"tmp": "C:/tmp", "temp": "C:/tmp", "isElevated": False},
            ),
        ):
            payload = smoke.run_packaged_smoke(args)
        assert not payload["ok"]
        assert payload["failure"]["step"] == "runner"
        assert "timed out waiting for bridge" in payload["failure"]["message"]
        assert payload["environment"]["tmp"] == "C:/tmp"
        assert report_path.exists()
        saved = json.loads(report_path.read_text(encoding="utf-8"))
        assert not saved["ok"]
        assert Path(saved["artifacts"]["reportPath"]).exists()
        assert terminate_mock.call_count >= 1
        assert terminate_mock.call_args_list[-1] == mock.call(process)
        assert stdout_handle.close.call_count >= 1
        assert stderr_handle.close.call_count >= 1


def test_wait_for_packaged_runtime_rejects_default_browser_launch_for_startup_probe() -> None:
    process = mock.Mock()
    process.poll.return_value = None
    with (
        mock.patch.object(smoke, "fetch_json", side_effect=[{"ok": True}, {"ok": True}]),
        mock.patch.object(
            smoke,
            "fetch_startup_metrics",
            return_value=[
                {
                    "event": "desktop_browser_launch_selected",
                    "fields": {"mode": "default-browser"},
                }
            ],
        ),
        mock.patch.object(smoke.time, "monotonic", side_effect=[0.0, 0.0]),
    ):
        with pytest.raises(RuntimeError, match="managed Chromium app window"):
            smoke.wait_for_packaged_runtime(
                process,
                site_base_url="http://127.0.0.1:8080",
                bridge_base_url="http://127.0.0.1:8877",
                timeout_s=5.0,
                require_managed_window=True,
                require_page_ready=False,
            )


def test_wait_for_runtime_events_retries_transient_bridge_reset() -> None:
    rows = [
        {"event": "jobs_first_render", "payload": {"elapsedMs": 1200}},
        {"event": "jobs_first_interactive", "payload": {"elapsedMs": 1400}},
    ]
    with (
        mock.patch.object(
            smoke,
            "fetch_startup_metrics",
            side_effect=[OSError("[WinError 10054] reset"), rows],
        ),
        mock.patch.object(smoke.time, "monotonic", side_effect=[0.0, 0.0, 1.0]),
        mock.patch.object(smoke.time, "sleep"),
    ):
        result = smoke.wait_for_runtime_events(
            "http://127.0.0.1:8877",
            ("jobs_first_render", "jobs_first_interactive"),
            timeout_s=5.0,
        )
    assert result == rows


def test_wait_for_runtime_events_accepts_inferred_shell_window_event_alias() -> None:
    rows = [{"event": "desktop_shell_window_shown_inferred", "fields": {"elapsedMs": 900}}]
    with (
        mock.patch.object(smoke, "fetch_startup_metrics", return_value=rows),
        mock.patch.object(smoke.time, "monotonic", side_effect=[0.0, 0.0]),
    ):
        result = smoke.wait_for_runtime_events(
            "http://127.0.0.1:8877",
            ("desktop_shell_window_shown",),
            timeout_s=5.0,
        )

    assert result == rows


def test_run_packaged_smoke_profile_only_waits_for_jobs_startup_events() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        process = mock.Mock()
        process.pid = 999
        process.poll.return_value = None
        stdout_handle = mock.Mock()
        stderr_handle = mock.Mock()
        args = smoke.parse_args(
            [
                "--exe-path",
                str(exe_path),
                "--report-path",
                str(report_path),
                "--artifacts-dir",
                str(artifacts_dir),
                "--startup-probe",
                "--profile-only",
            ]
        )
        captured_env: dict[str, str] = {}
        startup_metrics = [
            {"event": "desktop_launch_start", "fields": {"elapsedMs": 0}},
            {"event": "desktop_site_ready", "fields": {"elapsedMs": 400}},
            {"event": "desktop_window_created", "fields": {"elapsedMs": 700}},
            {"event": "desktop_shell_window_shown", "fields": {"elapsedMs": 900}},
            {"event": "jobs_module_boot_start", "payload": {"elapsedMs": 1100}},
            {"event": "jobs_local_data_init_ready", "payload": {"elapsedMs": 1300}},
            {"event": "jobs_auth_ready", "payload": {"elapsedMs": 1500}},
            {"event": "jobs_first_render", "payload": {"elapsedMs": 1800}},
            {"event": "jobs_first_interactive", "payload": {"elapsedMs": 2100}},
        ]

        def fake_launch_packaged_exe(*args, **kwargs):  # noqa: ANN002, ANN003
            captured_env.update(kwargs.get("env") or {})
            return process, stdout_handle, stderr_handle

        with (
            mock.patch.object(
                smoke,
                "select_startup_probe_browser",
                return_value={
                    "browserName": "chrome",
                    "browserPath": "C:/Chrome/chrome.exe",
                },
            ),
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(smoke, "launch_packaged_exe", side_effect=fake_launch_packaged_exe),
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime",
                return_value={
                    "health": {"ok": True},
                    "session": {"ok": True},
                    "startupMetrics": [{"event": "desktop_shell_window_shown"}],
                },
            ) as runtime_mock,
            mock.patch.object(
                smoke,
                "wait_for_runtime_events",
                return_value=startup_metrics,
            ) as runtime_events_mock,
            mock.patch.object(smoke, "capture_runtime_snapshot", return_value={}),
            mock.patch.object(
                smoke,
                "collect_packaged_smoke_env_diagnostics",
                return_value={"tmp": "C:/tmp", "temp": "C:/tmp", "isElevated": False},
            ),
            mock.patch.object(
                smoke,
                "summarize_startup_metrics",
                return_value={
                    "status": "passed",
                    "classification": "ok",
                    "firstUsableEvent": "jobs_first_interactive",
                    "firstUsableMs": 2100,
                    "stages": [],
                },
            ),
            mock.patch.object(smoke, "write_startup_summary"),
            mock.patch.object(smoke, "terminate_process_tree"),
        ):
            payload = smoke.run_packaged_smoke(args)

        assert payload["ok"] is True
        assert payload["startupMetrics"] == startup_metrics
        assert captured_env["BALUFFO_DESKTOP_ALLOW_EDGE_APP_MODE"] == "1"
        assert captured_env[smoke.desktop_app_mod.STARTUP_PROFILE_MODE_ENV] == "cold"
        assert (
            captured_env[smoke.desktop_app_mod.PREFERRED_BROWSER_PATH_ENV] == "C:/Chrome/chrome.exe"
        )
        assert payload["probeBrowser"]["preferredBrowserName"] == "chrome"
        assert payload["probeBrowser"]["preferredBrowserPath"] == "C:/Chrome/chrome.exe"
        runtime_mock.assert_called_once()
        assert runtime_mock.call_args.kwargs["require_managed_window"] is True
        assert runtime_mock.call_args.kwargs["require_page_ready"] is False
        runtime_events_mock.assert_called_once_with(
            payload["bridgeBaseUrl"],
            smoke.startup_profile_required_events("jobs"),
            timeout_s=mock.ANY,
        )
        assert "smokeReport" not in payload["artifacts"]


def test_run_warmup_launch_uses_warm_startup_profile_mode() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        captured_env: dict[str, str] = {}
        process = mock.Mock()
        process.pid = 777
        process.poll.return_value = None
        stdout_handle = mock.Mock()
        stderr_handle = mock.Mock()

        def fake_launch_packaged_exe(*args, **kwargs):  # noqa: ANN002, ANN003
            captured_env.update(kwargs.get("env") or {})
            return process, stdout_handle, stderr_handle

        with (
            mock.patch.object(smoke, "choose_free_port", side_effect=[51001, 51002]),
            mock.patch.object(smoke, "launch_packaged_exe", side_effect=fake_launch_packaged_exe),
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime",
                return_value={"health": {"ok": True}, "session": {"ok": True}},
            ),
            mock.patch.object(smoke.time, "sleep"),
            mock.patch.object(smoke, "terminate_process_tree"),
            mock.patch.object(smoke, "cleanup_orphaned_desktop_ports_nt"),
        ):
            smoke.run_warmup_launch(
                exe_path,
                artifacts_root=root / "artifacts",
                open_path="jobs.html",
                runtime_timeout_s=5.0,
                startup_probe=True,
            )

        assert captured_env["BALUFFO_DESKTOP_ALLOW_EDGE_APP_MODE"] == "1"
        assert captured_env[smoke.desktop_app_mod.STARTUP_PROFILE_MODE_ENV] == "warm"


def test_run_packaged_smoke_writes_success_report_and_artifacts() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        process = mock.Mock()
        process.pid = 999
        process.poll.return_value = None
        stdout_handle = mock.Mock()
        stderr_handle = mock.Mock()
        args = smoke.parse_args(
            [
                "--exe-path",
                str(exe_path),
                "--report-path",
                str(report_path),
                "--artifacts-dir",
                str(artifacts_dir),
                "--startup-probe",
            ]
        )
        startup_metrics = [{"event": "desktop_site_ready"}]
        scenarios = [
            {"name": "Startup", "status": "passed", "durationMs": 200, "error": ""},
            {"name": "Auth continuity", "status": "passed", "durationMs": 300, "error": ""},
        ]
        with (
            mock.patch.object(
                smoke,
                "select_startup_probe_browser",
                return_value={
                    "browserName": "chrome",
                    "browserPath": "C:/Chrome/chrome.exe",
                },
            ),
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(
                smoke, "launch_packaged_exe", return_value=(process, stdout_handle, stderr_handle)
            ),
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime",
                return_value={
                    "health": {"ok": True},
                    "session": {"ok": True, "user": None},
                    "startupMetrics": startup_metrics,
                },
            ),
            mock.patch.object(
                smoke,
                "wait_for_runtime_events",
                return_value=startup_metrics,
            ),
            mock.patch.object(
                smoke,
                "capture_runtime_snapshot",
                return_value={
                    "opsHealthSnapshot": str(artifacts_dir / "ops-health.json"),
                    "sessionSnapshot": str(artifacts_dir / "session.json"),
                    "startupMetricsSnapshot": str(artifacts_dir / "startup.json"),
                },
            ),
            mock.patch.object(
                smoke,
                "run_packaged_node_smoke",
                return_value={
                    "exitCode": 0,
                    "reportPath": str(artifacts_dir / "smoke-report.json"),
                    "outputDir": str(artifacts_dir / "smoke-output"),
                    "scenarios": scenarios,
                    "failureCategory": "",
                    "runnerError": "",
                    "environment": {
                        "tmp": str(artifacts_dir / "tmp"),
                        "temp": str(artifacts_dir / "tmp"),
                        "isElevated": False,
                    },
                },
            ),
            mock.patch.object(
                smoke,
                "summarize_startup_metrics",
                return_value={
                    "status": "passed",
                    "classification": "ok",
                    "firstUsableMs": 9000,
                    "stages": [],
                },
            ),
            mock.patch.object(
                smoke,
                "write_startup_summary",
            ),
            mock.patch.object(smoke, "terminate_process_tree") as terminate_mock,
        ):
            payload = smoke.run_packaged_smoke(args)
        assert payload["ok"]
        assert payload["scenarios"][0]["name"] == "Startup Profile"
        assert payload["scenarios"][1:] == scenarios
        assert payload["startupMetrics"] == startup_metrics
        assert payload["environment"]["tmp"] == str(artifacts_dir / "tmp")
        assert report_path.exists()
        saved = json.loads(report_path.read_text(encoding="utf-8"))
        assert saved["ok"]
        assert saved["artifacts"]["smokeReport"] == str(artifacts_dir / "smoke-report.json")
        assert saved["artifacts"]["smokeRunnerStdout"] == str(
            artifacts_dir / "smoke-runner-stdout.log"
        )
        assert saved["artifacts"]["playwrightReport"] == str(artifacts_dir / "smoke-report.json")
        assert saved["artifacts"]["playwrightStdout"] == str(
            artifacts_dir / "smoke-runner-stdout.log"
        )
        terminate_mock.assert_called_once_with(process)
        stdout_handle.close.assert_called_once()
        stderr_handle.close.assert_called_once()


def test_run_packaged_smoke_fails_startup_probe_when_no_managed_browser_is_available() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        args = smoke.parse_args(
            [
                "--exe-path",
                str(exe_path),
                "--report-path",
                str(report_path),
                "--artifacts-dir",
                str(artifacts_dir),
                "--startup-probe",
                "--profile-only",
            ]
        )
        with (
            mock.patch.object(
                smoke,
                "select_startup_probe_browser",
                side_effect=RuntimeError(
                    "No supported managed Chromium probe browser available. "
                    "Install Chrome, Brave, or an Edge build that can launch in app mode."
                ),
            ),
            mock.patch.object(smoke, "terminate_process_tree") as terminate_mock,
        ):
            payload = smoke.run_packaged_smoke(args)

        assert payload["ok"] is False
        assert payload["failure"]["category"] == "probe_browser_unavailable"
        assert (
            "No supported managed Chromium probe browser available" in payload["failure"]["message"]
        )
        assert payload["probeBrowser"]["preferredBrowserName"] == ""
        terminate_mock.assert_called_once_with(None)


def test_run_packaged_smoke_classifies_default_browser_launch_as_non_authoritative() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        process = mock.Mock()
        process.pid = 999
        process.poll.return_value = None
        stdout_handle = mock.Mock()
        stderr_handle = mock.Mock()
        args = smoke.parse_args(
            [
                "--exe-path",
                str(exe_path),
                "--report-path",
                str(report_path),
                "--artifacts-dir",
                str(artifacts_dir),
                "--startup-probe",
                "--profile-only",
            ]
        )
        partial_metrics = [
            {
                "event": "desktop_browser_launch_selected",
                "fields": {
                    "browser": "msedge",
                    "browserPath": "C:/Edge/msedge.exe",
                    "mode": "default-browser",
                },
            }
        ]
        with (
            mock.patch.object(
                smoke,
                "select_startup_probe_browser",
                return_value={
                    "browserName": "msedge",
                    "browserPath": "C:/Edge/msedge.exe",
                },
            ),
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(
                smoke, "launch_packaged_exe", return_value=(process, stdout_handle, stderr_handle)
            ),
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime",
                side_effect=RuntimeError(
                    "Startup probe requires a managed Chromium app window; "
                    "desktop launch mode was 'default-browser'."
                ),
            ),
            mock.patch.object(smoke, "fetch_startup_metrics", return_value=partial_metrics),
            mock.patch.object(smoke, "read_startup_metrics_file", return_value=[]),
            mock.patch.object(
                smoke,
                "summarize_startup_metrics",
                return_value={
                    "status": "failed",
                    "classification": "metrics incomplete",
                    "missingEvents": ["jobs_first_render", "jobs_first_interactive"],
                    "stages": [],
                },
            ),
            mock.patch.object(smoke, "write_startup_summary"),
            mock.patch.object(smoke, "terminate_process_tree"),
            mock.patch.object(smoke, "cleanup_orphaned_desktop_ports_nt"),
        ):
            payload = smoke.run_packaged_smoke(args)

        assert payload["ok"] is False
        assert payload["failure"]["category"] == "non_authoritative_browser_launch"
        assert payload["startupProfile"]["classification"] == "non-authoritative browser launch"
        assert payload["probeBrowser"]["launchMode"] == "default-browser"
        assert payload["probeBrowser"]["selectedBrowserName"] == "msedge"


def test_run_packaged_smoke_classifies_chromium_app_crash_before_jobs_markers() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        process = mock.Mock()
        process.pid = 999
        process.poll.return_value = None
        stdout_handle = mock.Mock()
        stderr_handle = mock.Mock()
        args = smoke.parse_args(
            [
                "--exe-path",
                str(exe_path),
                "--report-path",
                str(report_path),
                "--artifacts-dir",
                str(artifacts_dir),
                "--startup-probe",
                "--profile-only",
            ]
        )
        partial_metrics = [
            {"event": "desktop_launch_start", "fields": {"elapsedMs": 0}},
            {"event": "desktop_site_ready", "fields": {"elapsedMs": 300}},
            {"event": "desktop_window_created", "fields": {"elapsedMs": 550}},
            {"event": "desktop_shell_window_shown", "fields": {"elapsedMs": 800}},
            {
                "event": "desktop_browser_launch_selected",
                "fields": {
                    "browser": "chrome",
                    "browserPath": "C:/Chrome/chrome.exe",
                    "mode": "chromium-app",
                },
            },
            {"event": "jobs_module_boot_start", "payload": {"elapsedMs": 900}},
            {"event": "desktop_window_closed", "fields": {"reason": "bridge_exit"}},
        ]
        with (
            mock.patch.object(
                smoke,
                "select_startup_probe_browser",
                return_value={
                    "browserName": "chrome",
                    "browserPath": "C:/Chrome/chrome.exe",
                },
            ),
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(
                smoke, "launch_packaged_exe", return_value=(process, stdout_handle, stderr_handle)
            ),
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime",
                side_effect=OSError("[WinError 10054] An existing connection was forcibly closed"),
            ),
            mock.patch.object(smoke, "fetch_startup_metrics", return_value=partial_metrics),
            mock.patch.object(smoke, "read_startup_metrics_file", return_value=[]),
            mock.patch.object(
                smoke,
                "summarize_startup_metrics",
                return_value={
                    "status": "failed",
                    "classification": "metrics incomplete",
                    "missingEvents": ["jobs_first_render", "jobs_first_interactive"],
                    "stages": [],
                },
            ),
            mock.patch.object(smoke, "write_startup_summary"),
            mock.patch.object(smoke, "terminate_process_tree"),
            mock.patch.object(smoke, "cleanup_orphaned_desktop_ports_nt"),
        ):
            payload = smoke.run_packaged_smoke(args)

        assert payload["ok"] is False
        assert payload["failure"]["category"] == "browser_runtime_startup_failed"
        assert payload["startupProfile"]["classification"] == "browser runtime startup failed"
        assert payload["probeBrowser"]["launchMode"] == "chromium-app"
        assert payload["probeBrowser"]["windowClosedReason"] == "bridge_exit"


def test_run_packaged_smoke_uses_artifact_local_session_root_even_when_global_session_exists() -> (
    None
):
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        process = mock.Mock()
        process.pid = 999
        process.poll.return_value = None
        stdout_handle = mock.Mock()
        stderr_handle = mock.Mock()
        args = smoke.parse_args(
            [
                "--exe-path",
                str(exe_path),
                "--report-path",
                str(report_path),
                "--artifacts-dir",
                str(artifacts_dir),
                "--node-smoke-script",
                str(smoke.JOBS_PIPELINE_NODE_SMOKE_SCRIPT),
            ]
        )
        global_local_app_data = root / "global-localappdata"
        global_session_root = smoke.desktop_update_mod.resolve_desktop_session_root(
            {"LOCALAPPDATA": str(global_local_app_data)}
        )
        (global_session_root / smoke.DESKTOP_SESSION_STATE_FILE).write_text(
            json.dumps({"bridgePort": 8877, "dataDir": str(root / "stale-data")}),
            encoding="utf-8",
        )
        captured_env: dict[str, str] = {}

        def fake_launch_packaged_exe(*args, **kwargs):  # noqa: ANN002, ANN003
            captured_env.update(kwargs.get("env") or {})
            return process, stdout_handle, stderr_handle

        with (
            mock.patch.dict(os.environ, {"LOCALAPPDATA": str(global_local_app_data)}, clear=False),
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(smoke, "launch_packaged_exe", side_effect=fake_launch_packaged_exe),
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime",
                return_value={
                    "health": {"ok": True},
                    "session": {"ok": True},
                    "startupMetrics": [],
                },
            ),
            mock.patch.object(smoke, "capture_runtime_snapshot", return_value={}),
            mock.patch.object(
                smoke,
                "run_packaged_node_smoke",
                return_value={
                    "exitCode": 0,
                    "reportPath": str(artifacts_dir / "smoke-report.json"),
                    "outputDir": str(artifacts_dir / "smoke-output"),
                    "scenarios": [],
                    "failureCategory": "",
                    "runnerError": "",
                    "environment": {"tmp": "C:/tmp", "temp": "C:/tmp", "isElevated": False},
                },
            ),
            mock.patch.object(smoke, "terminate_process_tree"),
        ):
            payload = smoke.run_packaged_smoke(args)

        assert payload["ok"] is True
        assert Path(captured_env["LOCALAPPDATA"]).resolve() == (
            smoke.packaged_desktop_local_appdata_root(
                artifacts_dir, session_scope="runtime"
            ).resolve()
        )
        assert Path(captured_env["LOCALAPPDATA"]).resolve() != global_local_app_data.resolve()
        assert captured_env["BALUFFO_PACKAGED_SMOKE_PIPELINE_MODE"] == "stub-success"


def test_run_packaged_smoke_can_run_desktop_update_rehearsal_mode() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        args = smoke.parse_args(
            [
                "--exe-path",
                str(exe_path),
                "--report-path",
                str(report_path),
                "--artifacts-dir",
                str(artifacts_dir),
                "--desktop-update-rehearsal",
            ]
        )
        with (
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(
                smoke,
                "collect_packaged_smoke_env_diagnostics",
                return_value={"tmp": "C:/tmp", "temp": "C:/tmp", "isElevated": False},
            ),
            mock.patch.object(
                smoke,
                "run_desktop_update_rehearsal",
                return_value={
                    "name": "Packaged desktop updater rehearsal",
                    "slug": "desktop-update-rehearsal",
                    "status": "passed",
                    "durationMs": 1500,
                    "error": "",
                    "details": {
                        "helperStdoutLog": str(artifacts_dir / "helper.stdout.log"),
                        "helperStderrLog": str(artifacts_dir / "helper.stderr.log"),
                        "helperDiagnosticsLog": str(artifacts_dir / "helper.diagnostics.jsonl"),
                    },
                },
            ) as rehearsal_mock,
        ):
            payload = smoke.run_packaged_smoke(args)
        assert payload["ok"] is True
        assert payload["scenarios"][0]["slug"] == "desktop-update-rehearsal"
        assert payload["artifacts"]["helperStdout"] == str(artifacts_dir / "helper.stdout.log")
        assert payload["artifacts"]["helperStderr"] == str(artifacts_dir / "helper.stderr.log")
        assert payload["artifacts"]["helperDiagnostics"] == str(
            artifacts_dir / "helper.diagnostics.jsonl"
        )
        rehearsal_mock.assert_called_once()
        saved = json.loads(report_path.read_text(encoding="utf-8"))
        assert saved["ok"] is True


def test_packaged_sync_rehearsal_server_serves_fake_github_app_flow() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        portable_root = Path(tmp) / "portable"
        config_path = _write_packaged_sync_bundle_config(portable_root)
        loaded_path, _raw_payload, packaged_config = (
            smoke._load_portable_packaged_sync_rehearsal_config(  # noqa: SLF001
                portable_root
            )
        )
        assert loaded_path == config_path
        base_url, _stats, server, thread = smoke._start_packaged_sync_rehearsal_server(  # noqa: SLF001
            packaged_config=packaged_config,
            snapshot_payload={
                "schemaVersion": source_sync.SYNC_SCHEMA_VERSION,
                "generatedAt": "2026-04-19T12:00:00+00:00",
                "source": {"name": "packaged_sync_rehearsal"},
                "active": [],
                "pending": [],
                "rejected": [],
            },
        )
        try:
            token_request = Request(
                f"{base_url}/app/installations/999999/access_tokens",
                data=b"{}",
                headers={"Authorization": "Bearer rehearsal-jwt"},
                method="POST",
            )
            with urlopen(token_request, timeout=5) as response:  # noqa: S310
                token_payload = json.loads(response.read().decode("utf-8"))
            assert token_payload["token"] == "packaged-sync-rehearsal-token"

            content_request = Request(
                f"{base_url}/repos/owner/repo/contents/baluffo/source-sync.json?ref=main",
                headers={"Authorization": "Bearer packaged-sync-rehearsal-token"},
            )
            with urlopen(content_request, timeout=5) as response:  # noqa: S310
                content_payload = json.loads(response.read().decode("utf-8"))
            decoded = json.loads(base64.b64decode(content_payload["content"]).decode("utf-8"))
            assert content_payload["sha"] == "packaged-sync-rehearsal-sha"
            assert decoded["source"]["name"] == "packaged_sync_rehearsal"
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2.0)


def test_load_portable_packaged_sync_rehearsal_config_rejects_machine_key_derivation() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        portable_root = Path(tmp) / "portable"
        _write_packaged_sync_bundle_config(portable_root, key_derivation="machine")
        with pytest.raises(RuntimeError, match="keyDerivation=machine"):
            smoke._load_portable_packaged_sync_rehearsal_config(portable_root)  # noqa: SLF001


def test_run_packaged_smoke_can_run_sync_rehearsal_mode() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        args = smoke.parse_args(
            [
                "--exe-path",
                str(exe_path),
                "--report-path",
                str(report_path),
                "--artifacts-dir",
                str(artifacts_dir),
                "--sync-rehearsal",
            ]
        )
        with (
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(
                smoke,
                "collect_packaged_smoke_env_diagnostics",
                return_value={"tmp": "C:/tmp", "temp": "C:/tmp", "isElevated": False},
            ),
            mock.patch.object(
                smoke,
                "run_packaged_sync_rehearsal",
                return_value={
                    "name": "Packaged sync rehearsal",
                    "slug": "packaged-sync-rehearsal",
                    "status": "passed",
                    "durationMs": 1200,
                    "error": "",
                    "details": {
                        "runtimeStdout": str(artifacts_dir / "sync.stdout.log"),
                        "runtimeStderr": str(artifacts_dir / "sync.stderr.log"),
                    },
                },
            ) as rehearsal_mock,
        ):
            payload = smoke.run_packaged_smoke(args)
        assert payload["ok"] is True
        assert payload["scenarios"][0]["slug"] == "packaged-sync-rehearsal"
        assert payload["artifacts"]["syncRehearsalStdout"] == str(artifacts_dir / "sync.stdout.log")
        assert payload["artifacts"]["syncRehearsalStderr"] == str(artifacts_dir / "sync.stderr.log")
        rehearsal_mock.assert_called_once()
        saved = json.loads(report_path.read_text(encoding="utf-8"))
        assert saved["ok"] is True


def test_wait_for_relaunched_runtime_prefers_explicit_session_env_over_global_state() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        expected_data_dir = root / "portable" / "ship" / "data"
        expected_data_dir.mkdir(parents=True, exist_ok=True)
        global_env = {"LOCALAPPDATA": str(root / "global-localappdata")}
        global_session_root = smoke.desktop_update_mod.resolve_desktop_session_root(global_env)
        (global_session_root / smoke.DESKTOP_SESSION_STATE_FILE).write_text(
            json.dumps({"bridgePort": 8877, "dataDir": str(root / "wrong-data")}),
            encoding="utf-8",
        )
        run_env = {"LOCALAPPDATA": str(root / "run-localappdata")}
        run_session_root = smoke.desktop_update_mod.resolve_desktop_session_root(run_env)
        (run_session_root / smoke.DESKTOP_SESSION_STATE_FILE).write_text(
            json.dumps({"bridgePort": 4567, "dataDir": str(expected_data_dir)}),
            encoding="utf-8",
        )

        with (
            mock.patch.dict(os.environ, global_env, clear=False),
            mock.patch.object(
                smoke,
                "fetch_json",
                return_value={
                    "desktopMode": True,
                    "startupReady": True,
                    "appVersion": "0.1.22",
                },
            ) as fetch_mock,
        ):
            relaunched = smoke._wait_for_relaunched_runtime(
                expected_data_dir=expected_data_dir,
                expected_version="0.1.22",
                timeout_s=0.1,
                env=run_env,
            )

        assert relaunched["session"]["bridgePort"] == 4567
        fetch_mock.assert_called_once_with("http://127.0.0.1:4567/ops/health", timeout_s=5.0)


def test_run_desktop_update_rehearsal_clears_session_state_only_after_runtime_exit() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        portable_root = root / "portable"
        (portable_root / "ship" / "data").mkdir(parents=True, exist_ok=True)
        exe_path = portable_root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        (portable_root / "BaluffoUpdater.exe").write_text("helper", encoding="utf-8")
        process = mock.Mock()
        stdout_handle = mock.Mock()
        stderr_handle = mock.Mock()
        captured_env: dict[str, str] = {}
        session_state_path: Path | None = None

        def fake_archive_portable_dir(_portable_root: Path, output_path: Path) -> Path:
            output_path.write_bytes(b"portable-update")
            return output_path

        def fake_launch_packaged_exe(*args, **kwargs):  # noqa: ANN002, ANN003
            captured_env.update(kwargs.get("env") or {})
            return process, stdout_handle, stderr_handle

        def fake_wait_for_packaged_runtime(*args, **kwargs):  # noqa: ANN002, ANN003
            nonlocal session_state_path
            session_root = smoke.desktop_update_mod.resolve_desktop_session_root(captured_env)
            session_root.mkdir(parents=True, exist_ok=True)
            session_state_path = session_root / smoke.DESKTOP_SESSION_STATE_FILE
            session_state_path.write_text(
                json.dumps({"launcherPid": 6060, "launcherToken": "token"}),
                encoding="utf-8",
            )
            return {}

        def fake_wait_for_process_exit(*args, **kwargs):  # noqa: ANN002, ANN003
            assert session_state_path is not None
            assert session_state_path.exists()

        def fake_wait_for_relaunched_runtime(*args, **kwargs):  # noqa: ANN002, ANN003
            assert session_state_path is not None
            assert not session_state_path.exists()
            return {"session": {"launcherPid": 7001, "bridgePort": 7002, "sitePort": 7003}}

        with (
            mock.patch.object(smoke, "_inject_desktop_update_public_keys"),
            mock.patch.object(smoke, "_seed_rehearsal_local_data", return_value={}),
            mock.patch.object(
                smoke,
                "_archive_portable_dir",
                side_effect=fake_archive_portable_dir,
            ),
            mock.patch.object(
                smoke,
                "_start_desktop_update_release_server",
                return_value=("http://127.0.0.1:63092", mock.Mock(), mock.Mock()),
            ),
            mock.patch.object(
                smoke,
                "packaged_runtime_env_overrides",
                return_value={"LOCALAPPDATA": str(root / "desktop-localappdata")},
            ),
            mock.patch.object(smoke, "_preferred_desktop_browser_env", return_value={}),
            mock.patch.object(smoke, "clear_packaged_desktop_session_state"),
            mock.patch.object(smoke, "choose_free_port", side_effect=[63093, 63094]),
            mock.patch.object(
                smoke,
                "launch_packaged_exe",
                side_effect=fake_launch_packaged_exe,
            ),
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime",
                side_effect=fake_wait_for_packaged_runtime,
            ),
            mock.patch.object(
                smoke,
                "post_json",
                side_effect=[
                    (
                        200,
                        {"status": {"updateAvailable": True, "availability": "available"}},
                    ),
                    (
                        200,
                        {
                            "started": True,
                            "status": {"downloadState": "downloaded", "installState": "ready"},
                        },
                    ),
                    (200, {"started": True, "exitRequested": True}),
                ],
            ),
            mock.patch.object(
                smoke, "_wait_for_process_exit", side_effect=fake_wait_for_process_exit
            ),
            mock.patch.object(
                smoke,
                "_wait_for_relaunched_runtime",
                side_effect=fake_wait_for_relaunched_runtime,
            ),
            mock.patch.object(smoke, "_verify_rehearsal_local_data"),
            mock.patch.object(smoke, "_assert_desktop_update_helper_succeeded"),
            mock.patch.object(smoke, "terminate_process_tree"),
            mock.patch.object(smoke, "cleanup_orphaned_desktop_ports_nt"),
        ):
            result = smoke.run_desktop_update_rehearsal(
                exe_path=exe_path,
                artifacts_dir=root / "artifacts",
                runtime_timeout_s=5.0,
            )

        assert result["status"] == "passed"


def test_assert_desktop_update_helper_succeeded_rejects_failed_helper_stdout() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = smoke.desktop_update_mod.DesktopUpdatePaths.from_data_dir(data_dir)
        paths.helper_stdout_log_path.parent.mkdir(parents=True, exist_ok=True)
        paths.helper_stdout_log_path.write_text(
            json.dumps({"ok": False, "error": "boom"}),
            encoding="utf-8",
        )

        with pytest.raises(RuntimeError, match="Update helper reported failure"):
            smoke._assert_desktop_update_helper_succeeded(
                paths=paths,
                relaunch_bridge_port=0,
            )


def test_assert_desktop_update_helper_succeeded_ignores_malformed_diagnostics_lines() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        data_dir = Path(tmp) / "portable" / "ship" / "data"
        paths = smoke.desktop_update_mod.DesktopUpdatePaths.from_data_dir(data_dir)
        paths.helper_diagnostics_log_path.parent.mkdir(parents=True, exist_ok=True)
        paths.helper_diagnostics_log_path.write_text(
            "\n".join(
                [
                    '{"event": "helper_main_started"}',
                    "}}",
                    '{"event": "helper_main_succeeded"}',
                ]
            ),
            encoding="utf-8",
        )

        smoke._assert_desktop_update_helper_succeeded(
            paths=paths,
            relaunch_bridge_port=0,
        )


def test_run_packaged_smoke_classifies_spawn_failure_from_node_runner() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        process = mock.Mock()
        process.pid = 999
        process.poll.return_value = None
        stdout_handle = mock.Mock()
        stderr_handle = mock.Mock()
        args = smoke.parse_args(
            [
                "--exe-path",
                str(exe_path),
                "--report-path",
                str(report_path),
                "--artifacts-dir",
                str(artifacts_dir),
            ]
        )
        with (
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(
                smoke, "launch_packaged_exe", return_value=(process, stdout_handle, stderr_handle)
            ),
            mock.patch.object(
                smoke,
                "wait_for_packaged_runtime",
                return_value={
                    "health": {"ok": True},
                    "session": {"ok": True},
                    "startupMetrics": [],
                },
            ),
            mock.patch.object(
                smoke,
                "capture_runtime_snapshot",
                return_value={},
            ),
            mock.patch.object(
                smoke,
                "run_packaged_node_smoke",
                return_value={
                    "exitCode": 1,
                    "reportPath": str(artifacts_dir / "smoke-report.json"),
                    "outputDir": str(artifacts_dir / "smoke-output"),
                    "scenarios": [],
                    "failureCategory": "node_process_spawn_blocked",
                    "runnerError": "spawn EPERM",
                    "environment": {"tmp": "C:/tmp", "temp": "C:/tmp", "isElevated": True},
                },
            ),
            mock.patch.object(smoke, "terminate_process_tree"),
        ):
            payload = smoke.run_packaged_smoke(args)
        assert not payload["ok"]
        assert payload["failure"]["step"] == "playwright"
        assert payload["failure"]["category"] == "node_process_spawn_blocked"
        assert payload["failure"]["message"] == "spawn EPERM"
        assert payload["environment"]["isElevated"] is True


def test_run_packaged_smoke_fails_when_embedded_probe_fails() -> None:
    with workspace_tmpdir("packaged-smoke") as tmp:
        root = Path(tmp)
        report_path = root / "data" / "latest.json"
        artifacts_dir = root / "artifacts"
        exe_path = root / "Baluffo.exe"
        exe_path.write_text("exe", encoding="utf-8")
        args = smoke.parse_args(
            [
                "--exe-path",
                str(exe_path),
                "--report-path",
                str(report_path),
                "--artifacts-dir",
                str(artifacts_dir),
                "--embedded-probes",
            ]
        )
        failing_probe = {
            "name": "Embedded Jobs Ready",
            "status": "failed",
            "durationMs": 2500,
            "error": "Missing embedded runtime events: jobs_auth_ready",
            "startupProfile": {},
        }
        with (
            mock.patch.object(smoke, "ensure_portable_exe", return_value=exe_path),
            mock.patch.object(smoke, "run_embedded_runtime_probe", return_value=failing_probe),
            mock.patch.object(smoke, "terminate_process_tree") as terminate_mock,
        ):
            payload = smoke.run_packaged_smoke(args)
        assert not payload["ok"]
        assert payload["scenarios"] == [failing_probe, failing_probe, failing_probe]
        assert payload["failure"]["step"] == "runner"
        assert "Embedded Jobs Ready failed" in payload["failure"]["message"]
        terminate_mock.assert_called_once_with(None)


def test_classify_startup_probe_failure_uses_explicit_handoff_failure_category() -> None:
    rows = [
        {
            "event": "desktop_browser_launch_selected",
            "fields": {
                "browser": "chrome",
                "browserPath": "C:/Chrome/chrome.exe",
                "mode": "chromium-app",
            },
        },
        {"event": "desktop_browser_watchdog_handoff_failed", "fields": {}},
    ]

    classification, category = smoke.classify_startup_probe_failure(
        rows,
        error_message="startup markers never arrived",
        summary={"missingEvents": ["jobs_first_render", "jobs_first_interactive"]},
    )

    assert classification == "browser handoff/runtime startup failed"
    assert category == "browser_handoff_runtime_startup_failed"


def test_classify_startup_probe_failure_treats_confirmed_handoff_then_bridge_loss_as_runtime_failure() -> (
    None
):
    rows = [
        {
            "event": "desktop_browser_launch_selected",
            "fields": {
                "browser": "chrome",
                "browserPath": "C:/Chrome/chrome.exe",
                "mode": "chromium-app",
            },
        },
        {
            "event": "desktop_browser_watchdog_handoff_confirmed",
            "fields": {"evidence": "startup_metric"},
        },
        {"event": "desktop_window_closed", "fields": {"reason": "bridge_exit"}},
    ]

    classification, category = smoke.classify_startup_probe_failure(
        rows,
        error_message="[WinError 10054] An existing connection was forcibly closed",
        summary={"missingEvents": ["jobs_first_render", "jobs_first_interactive"]},
    )

    assert classification == "browser runtime startup failed"
    assert category == "browser_runtime_startup_failed"
