# ruff: noqa: F401
import json
import subprocess
import threading
from unittest import mock

from tests.helpers.concurrency import BlockingActiveCounter

from ._helpers import (
    Path,
    jf,
    jfr,
    jobs_common_registry,
    jobs_registry,
    static_scrapy,
    workspace_tmpdir,
)


def test_run_scrapy_static_source_handles_malformed_json() -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Scrapy Test Studio",
            "studio": "Scrapy Test Studio",
            "adapter": "scrapy_static",
            "pages": ["https://example.com/jobs"],
            "enabledByDefault": True,
        }
    ]
    fake_result = mock.Mock()
    fake_result.stdout = b"not json"
    fake_result.stderr = b"runner stderr"
    fake_result.returncode = 1
    try:
        with (
            mock.patch("subprocess.run", return_value=fake_result),
            mock.patch.object(
                static_scrapy, "registry_entries", return_value=list(jf.STUDIO_SOURCE_REGISTRY)
            ),
            mock.patch.object(static_scrapy, "set_source_diagnostics") as diag,
        ):
            rows = jf.run_scrapy_static_source(
                fetch_text=lambda _url, _timeout: "",
                timeout_s=5,
                retries=1,
                backoff_s=1.0,
            )
            assert rows == []
            diag.assert_called_once()
            args, kwargs = diag.call_args
            assert args[0] == "scrapy_static_sources"
            assert kwargs.get("adapter") == "scrapy_static"
            details = kwargs.get("details") or []
            assert details
            assert str(details[0].get("classification") or "") == "parse_error"
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev


def test_run_scrapy_static_source_timeout_is_not_requeued() -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Tequilaworks (Manual Website)",
            "studio": "Tequilaworks",
            "adapter": "scrapy_static",
            "pages": ["https://tequilaworks.com/en/careers"],
            "enabledByDefault": True,
        }
    ]
    try:
        with mock.patch(
            "subprocess.run", side_effect=subprocess.TimeoutExpired(cmd="runner", timeout=20)
        ):
            jf.SOURCE_DIAGNOSTICS.clear()
            rows = jf.run_scrapy_static_source(
                fetch_text=lambda _url, _timeout: "",
                timeout_s=5,
                retries=0,
                backoff_s=0,
            )
            assert rows == []
            detail = (
                (jf.SOURCE_DIAGNOSTICS.get("scrapy_static_sources") or {}).get("details") or [{}]
            )[0]
            assert str(detail.get("classification") or "") == "browser_timeout"
            assert not bool(detail.get("browserFallbackRecommended"))
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev


def test_run_scrapy_static_source_uses_child_script_command_when_frozen() -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Scrapy Test Studio",
            "studio": "Scrapy Test Studio",
            "adapter": "scrapy_static",
            "pages": ["https://example.com/jobs"],
            "enabledByDefault": True,
        }
    ]
    fake_result = mock.Mock()
    fake_result.stdout = json.dumps(
        {
            "ok": True,
            "jobs": [],
            "details": [{"status": "ok", "keptCount": 0, "fetchedCount": 0}],
            "partialErrors": [],
        }
    ).encode("utf-8")
    fake_result.stderr = b""
    fake_result.returncode = 0
    try:
        with (
            mock.patch("subprocess.run", return_value=fake_result) as run_mock,
            mock.patch.object(
                static_scrapy, "registry_entries", return_value=list(jf.STUDIO_SOURCE_REGISTRY)
            ),
            mock.patch.object(static_scrapy.sys, "frozen", True, create=True),
            mock.patch.object(
                static_scrapy.sys,
                "executable",
                "C:/tmp/Baluffo.exe",
            ),
        ):
            rows = jf.run_scrapy_static_source(
                fetch_text=lambda _url, _timeout: "",
                timeout_s=5,
                retries=0,
                backoff_s=0.0,
            )

        assert rows == []
        command = run_mock.call_args.args[0]
        src_root = static_scrapy.Path(static_scrapy.__file__).resolve().parents[2]
        runtime_root = src_root.parent
        assert command[:6] == [
            "C:/tmp/Baluffo.exe",
            "__child_script__",
            "--root",
            str(runtime_root),
            "--script",
            "scrapers/runner.py",
        ]
        assert command[6] == "--"
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev


def test_run_scrapy_static_source_uses_python_runner_command_when_not_frozen() -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    jf.STUDIO_SOURCE_REGISTRY = [
        {
            "name": "Scrapy Test Studio",
            "studio": "Scrapy Test Studio",
            "adapter": "scrapy_static",
            "pages": ["https://example.com/jobs"],
            "enabledByDefault": True,
        }
    ]
    fake_result = mock.Mock()
    fake_result.stdout = json.dumps(
        {
            "ok": True,
            "jobs": [],
            "details": [{"status": "ok", "keptCount": 0, "fetchedCount": 0}],
            "partialErrors": [],
        }
    ).encode("utf-8")
    fake_result.stderr = b""
    fake_result.returncode = 0
    try:
        with (
            mock.patch("subprocess.run", return_value=fake_result) as run_mock,
            mock.patch.object(
                static_scrapy, "registry_entries", return_value=list(jf.STUDIO_SOURCE_REGISTRY)
            ),
            mock.patch.object(static_scrapy.sys, "frozen", False, create=True),
            mock.patch.object(
                static_scrapy.sys,
                "executable",
                "C:/Python313/python.exe",
            ),
        ):
            rows = jf.run_scrapy_static_source(
                fetch_text=lambda _url, _timeout: "",
                timeout_s=5,
                retries=0,
                backoff_s=0.0,
            )

        assert rows == []
        command = run_mock.call_args.args[0]
        expected_runner = (
            static_scrapy.Path(static_scrapy.__file__).resolve().parents[2]
            / "scrapers"
            / "runner.py"
        )
        assert command == ["C:/Python313/python.exe", str(expected_runner)]
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev


def test_run_scrapy_static_source_processes_queue_with_bounded_parallelism() -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    sources = [
        {
            "name": "Scrapy Studio A",
            "studio": "Scrapy Studio A",
            "adapter": "scrapy_static",
            "pages": ["https://example.com/a"],
            "enabledByDefault": True,
        },
        {
            "name": "Tequilaworks (Manual Website)",
            "studio": "Tequilaworks",
            "adapter": "scrapy_static",
            "pages": ["https://example.com/timeout"],
            "enabledByDefault": True,
        },
        {
            "name": "Scrapy Studio B",
            "studio": "Scrapy Studio B",
            "adapter": "scrapy_static",
            "pages": ["https://example.com/b"],
            "enabledByDefault": True,
        },
    ]
    jf.STUDIO_SOURCE_REGISTRY = sources
    runners = BlockingActiveCounter(auto_release_at=2)
    progress_events: list[dict[str, object]] = []
    try:
        jf.SOURCE_DIAGNOSTICS.clear()

        def fake_run(_command, **kwargs):  # noqa: ANN001, ANN202
            payload = json.loads(kwargs["input"].decode("utf-8"))
            source_name = str(((payload.get("source") or {}).get("name")) or "")
            runners.enter()
            try:
                runners.wait_released()
                if source_name == "Tequilaworks (Manual Website)":
                    raise subprocess.TimeoutExpired(cmd="runner", timeout=20)
                result = mock.Mock()
                result.stdout = json.dumps(
                    {
                        "ok": True,
                        "jobs": [],
                        "details": [{"status": "ok", "keptCount": 0, "fetchedCount": 0}],
                        "partialErrors": [],
                    }
                ).encode("utf-8")
                result.stderr = b""
                result.returncode = 0
                return result
            finally:
                runners.exit()

        with (
            mock.patch("subprocess.run", side_effect=fake_run),
            mock.patch.object(static_scrapy, "registry_entries", return_value=list(sources)),
            mock.patch.object(
                static_scrapy,
                "set_source_diagnostics",
                wraps=static_scrapy.set_source_diagnostics,
            ) as diag_mock,
        ):
            rows = jf.run_scrapy_static_source(
                fetch_text=lambda _url, _timeout: "",
                timeout_s=5,
                retries=0,
                backoff_s=0.0,
                max_workers=2,
                progress_callback=lambda **kwargs: progress_events.append(dict(kwargs)),
            )

        assert rows == []
        assert runners.peak == 2
        assert diag_mock.call_count == len(sources)
        details = (jf.SOURCE_DIAGNOSTICS.get("scrapy_static_sources") or {}).get("details") or []
        assert len(details) == len(sources)
        assert any(
            str(detail.get("classification") or "") == "browser_timeout" for detail in details
        )
        assert any(
            int((event.get("counts") or {}).get("runningSources") or 0) >= 2
            for event in progress_events
        )
        assert any(
            int((event.get("counts") or {}).get("completedSources") or 0) == len(sources)
            for event in progress_events
        )
    finally:
        jf.STUDIO_SOURCE_REGISTRY = prev


def test_run_scrapy_static_source_emits_heartbeat_while_waiting_for_queue_child() -> None:
    prev = list(jf.STUDIO_SOURCE_REGISTRY)
    source = {
        "name": "Scrapy Heartbeat Studio",
        "studio": "Scrapy Heartbeat Studio",
        "adapter": "scrapy_static",
        "pages": ["https://example.com/heartbeat"],
        "enabledByDefault": True,
    }
    jf.STUDIO_SOURCE_REGISTRY = [source]
    release_runner = threading.Event()
    runner_started = threading.Event()
    heartbeat_seen = threading.Event()
    heartbeat_calls: list[str] = []
    errors: list[BaseException] = []
    try:
        result = mock.Mock()
        result.stdout = json.dumps(
            {
                "ok": True,
                "jobs": [],
                "details": [{"status": "ok", "keptCount": 0, "fetchedCount": 0}],
                "partialErrors": [],
            }
        ).encode("utf-8")
        result.stderr = b""
        result.returncode = 0

        def fake_run(_command, **_kwargs):  # noqa: ANN001, ANN202
            runner_started.set()
            release_runner.wait(timeout=2.0)
            return result

        def heartbeat_callback() -> None:
            heartbeat_calls.append("tick")
            heartbeat_seen.set()

        def target() -> None:
            try:
                jf.run_scrapy_static_source(
                    fetch_text=lambda _url, _timeout: "",
                    timeout_s=5,
                    retries=0,
                    backoff_s=0.0,
                    max_workers=1,
                    heartbeat_callback=heartbeat_callback,
                )
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)

        with (
            mock.patch("subprocess.run", side_effect=fake_run),
            mock.patch.object(static_scrapy, "registry_entries", return_value=[source]),
        ):
            thread = threading.Thread(target=target)
            thread.start()
            assert runner_started.wait(timeout=1.0)
            assert heartbeat_seen.wait(timeout=2.0)
            release_runner.set()
            thread.join(timeout=2.0)

        assert not thread.is_alive()
        assert not errors
        assert heartbeat_calls
    finally:
        release_runner.set()
        jf.STUDIO_SOURCE_REGISTRY = prev


def test_child_timeout_window_uses_shared_timeout_and_page_count() -> None:
    assert (
        static_scrapy._child_timeout_window_s(
            source_name="Tequilaworks (Manual Website)",
            timeout_s=15,
            pages=["https://example.com/jobs"],
        )
        == 40
    )
    assert (
        static_scrapy._child_timeout_window_s(
            source_name="Scrapy Studio",
            timeout_s=15,
            pages=["https://example.com/jobs", "https://example.com/jobs-2"],
        )
        == 120
    )


def test_scrapy_static_registration_in_default_loaders() -> None:
    assert "scrapy_static_sources" in jfr.DEFAULT_SOURCE_LOADER_NAMES
    assert "google_sheets_1er2oaxo" in jfr.DEFAULT_SOURCE_LOADER_NAMES
    assert "google_sheets_1mvqhxat" in jfr.DEFAULT_SOURCE_LOADER_NAMES
    assert jfr.SOURCE_REPORT_META["scrapy_static_sources"]["adapter"] == "scrapy_static"
    assert jfr.SOURCE_REPORT_META["google_sheets_1er2oaxo"]["adapter"] == "csv"
    assert jfr.SOURCE_REPORT_META["google_sheets_1mvqhxat"]["adapter"] == "csv"
    names = [name for name, _ in jf.default_source_loaders()]
    assert "scrapy_static_sources" in names
    assert "google_sheets_1er2oaxo" in names
    assert "google_sheets_1mvqhxat" in names


def test_scrapy_static_registry_from_browser_queue_collapses_by_source_id() -> None:
    """When the browser queue has multiple rows for the same sourceId, registry has one row per source with best URL."""
    with workspace_tmpdir("jobs-fetcher-registry-collapse") as tmp:
        queue_path = Path(tmp) / "jobs-browser-fallback-queue.json"
        queue_path.write_text(
            json.dumps(
                [
                    {
                        "adapter": "scrapy_static",
                        "sourceId": "static:supercell",
                        "name": "Supercell",
                        "studio": "Supercell",
                        "page": "https://supercell.com/en/careers/joining-supercell/",
                    },
                    {
                        "adapter": "scrapy_static",
                        "sourceId": "static:supercell",
                        "name": "Supercell",
                        "studio": "Supercell",
                        "page": "https://supercell.com/en/careers/",
                    },
                ],
                indent=2,
            ),
            encoding="utf-8",
        )
        with mock.patch.object(jobs_common_registry, "SCRAPY_BROWSER_QUEUE_PATH", queue_path):
            rows = jobs_registry.registry_entries("scrapy_static", enabled_only=True)
        assert len(rows) == 1
        assert rows[0].get("pages") == ["https://supercell.com/en/careers/"]
        assert rows[0].get("id") == "static:supercell"
