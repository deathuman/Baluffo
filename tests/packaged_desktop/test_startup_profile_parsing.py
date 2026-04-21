import json
from datetime import datetime
from pathlib import Path
from unittest import mock

import pytest

from src import packaged_desktop_smoke as smoke
from src.ship.startup_profile import summarize_startup_metrics
from tests.helpers.temp_paths import workspace_tmpdir

pytestmark = [pytest.mark.packaging, pytest.mark.slow]


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
