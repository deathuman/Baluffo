import argparse
import unittest
from pathlib import Path
from unittest import mock
import types
from types import SimpleNamespace

from scripts.ship import desktop_app


class DesktopAppTests(unittest.TestCase):
    def test_create_runtime_config_defaults_to_fixed_desktop_ports(self) -> None:
        root = Path("C:/tmp/baluffo-ship")
        args = argparse.Namespace(
            root=str(root),
            site_port=0,
            bridge_port=0,
            bridge_host="127.0.0.1",
            data_dir="",
            open_path="admin.html",
            title="",
            port=0,
            bind_host="127.0.0.1",
            child_mode="",
            desktop_runtime=False,
            startup_probe=False,
        )
        with mock.patch.object(desktop_app, "resolve_ship_root", return_value=root):
            config = desktop_app.create_runtime_config(args)

        self.assertEqual(config.ship_root, root)
        self.assertEqual(config.site_port, desktop_app.DEFAULT_SITE_PORT)
        self.assertEqual(config.bridge_port, desktop_app.DEFAULT_BRIDGE_PORT)
        self.assertEqual(config.data_dir, root / "data")
        self.assertEqual(config.open_path, "admin.html")
        self.assertEqual(config.title, desktop_app.WINDOW_TITLE)

    def test_create_runtime_config_defaults_to_jobs_entry(self) -> None:
        root = Path("C:/tmp/baluffo-ship")
        args = argparse.Namespace(
            root=str(root),
            site_port=0,
            bridge_port=0,
            bridge_host="127.0.0.1",
            data_dir="",
            open_path="",
            title="",
            port=0,
            bind_host="127.0.0.1",
            child_mode="",
            desktop_runtime=False,
            startup_probe=False,
        )
        with mock.patch.object(desktop_app, "resolve_ship_root", return_value=root):
            config = desktop_app.create_runtime_config(args)

        self.assertEqual(config.open_path, "jobs.html")

    def test_build_open_url_marks_desktop_mode(self) -> None:
        config = desktop_app.DesktopRuntimeConfig(
            ship_root=Path("C:/tmp/baluffo-ship"),
            site_port=8080,
            bridge_port=8877,
            bridge_host="127.0.0.1",
            data_dir=Path("C:/tmp/baluffo-ship/data"),
            open_path="jobs.html",
            title="Baluffo",
            startup_probe=False,
        )

        self.assertEqual(
            desktop_app.build_open_url(config),
            "http://127.0.0.1:8080/jobs.html?desktop=1&bridgePort=8877&bridgeHost=127.0.0.1",
        )

    def test_build_open_url_marks_startup_probe_when_enabled(self) -> None:
        config = desktop_app.DesktopRuntimeConfig(
            ship_root=Path("C:/tmp/baluffo-ship"),
            site_port=8080,
            bridge_port=8877,
            bridge_host="127.0.0.1",
            data_dir=Path("C:/tmp/baluffo-ship/data"),
            open_path="jobs.html",
            title="Baluffo",
            startup_probe=True,
        )

        self.assertEqual(
            desktop_app.build_open_url(config),
            "http://127.0.0.1:8080/jobs.html?desktop=1&bridgePort=8877&bridgeHost=127.0.0.1&startupProbe=1",
        )

    def test_build_loading_html_contains_shell_markup(self) -> None:
        html = desktop_app.build_loading_html("Baluffo")

        self.assertIn("Starting Baluffo", html)
        self.assertIn('class="shell"', html)
        self.assertIn("Loading interface...", html)

    def test_build_webview_storage_path_uses_local_user_data_root(self) -> None:
        data_dir = Path("C:/tmp/baluffo-ship/data")

        self.assertEqual(
            desktop_app.build_webview_storage_path(data_dir),
            data_dir / "local-user-data" / "webview",
        )

    def test_resolve_webview2_browser_arguments_supports_disable_gpu_and_dedupes(self) -> None:
        env = {
            "BALUFFO_WEBVIEW2_ADDITIONAL_BROWSER_ARGUMENTS": "--foo=1 --disable-gpu",
            "WEBVIEW2_ADDITIONAL_BROWSER_ARGUMENTS": "--bar=2",
            "BALUFFO_WEBVIEW2_DISABLE_GPU": "1",
        }

        self.assertEqual(
            desktop_app.resolve_webview2_browser_arguments(env),
            "--foo=1 --disable-gpu --bar=2",
        )

    def test_configure_webview_runtime_applies_runtime_path_and_browser_args(self) -> None:
        fake_webview = SimpleNamespace(settings={"WEBVIEW2_RUNTIME_PATH": None})
        env = {
            "BALUFFO_WEBVIEW2_RUNTIME_PATH": "C:/fixed-runtime",
            "BALUFFO_WEBVIEW2_ADDITIONAL_BROWSER_ARGUMENTS": "--disable-gpu",
        }

        with mock.patch.dict(desktop_app.os.environ, {}, clear=True):
            result = desktop_app.configure_webview_runtime(fake_webview, env=env)
            self.assertEqual(desktop_app.os.environ["WEBVIEW2_ADDITIONAL_BROWSER_ARGUMENTS"], "--disable-gpu")

        self.assertEqual(fake_webview.settings["WEBVIEW2_RUNTIME_PATH"], "C:/fixed-runtime")
        self.assertEqual(result["runtimePath"], "C:/fixed-runtime")
        self.assertEqual(result["browserArguments"], "--disable-gpu")

    def test_begin_window_app_bootstrap_traces_shell_and_stashes_url(self) -> None:
        window = SimpleNamespace()
        with mock.patch.object(desktop_app, "_append_startup_trace") as trace_mock:
            desktop_app._begin_window_app_bootstrap(Path("C:/tmp/data"), 10.0, window, "http://127.0.0.1:8080/jobs.html")

        event_names = [call.args[1] for call in trace_mock.call_args_list]
        self.assertEqual(event_names, ["desktop_shell_window_shown"])
        self.assertEqual(window._baluffo_pending_app_url, "http://127.0.0.1:8080/jobs.html")

    def test_schedule_window_app_navigation_traces_and_loads_url(self) -> None:
        window = mock.Mock()
        with mock.patch.object(desktop_app, "_append_startup_trace") as trace_mock, mock.patch.object(
            desktop_app.threading, "Thread"
        ) as thread_ctor:
            holder = {}

            def capture_thread(*, target, name, daemon):
                holder["target"] = target
                holder["name"] = name
                holder["daemon"] = daemon
                return mock.Mock(start=mock.Mock(side_effect=target))

            thread_ctor.side_effect = capture_thread
            desktop_app._schedule_window_app_navigation(Path("C:/tmp/data"), 10.0, window, "http://127.0.0.1:8080/jobs.html", delay_ms=0)

        event_names = [call.args[1] for call in trace_mock.call_args_list]
        self.assertEqual(event_names, ["desktop_shell_navigation_scheduled", "desktop_shell_navigation_start", "desktop_window_load_url"])
        self.assertEqual(holder["name"], "baluffo-shell-handoff")
        self.assertTrue(holder["daemon"])
        window.load_url.assert_called_once_with("http://127.0.0.1:8080/jobs.html")

    def test_track_window_loaded_events_distinguishes_shell_and_app_load(self) -> None:
        loaded_event = []
        before_load_event = []
        shown_event = []

        class EventHook:
            def __init__(self, bucket):
                self.bucket = bucket

            def __iadd__(self, handler):
                self.bucket.append(handler)
                return self

        window = SimpleNamespace(
            events=SimpleNamespace(
                loaded=EventHook(loaded_event),
                before_load=EventHook(before_load_event),
                shown=EventHook(shown_event),
            ),
            show=mock.Mock(),
            _baluffo_pending_app_url="http://127.0.0.1:8080/jobs.html",
        )
        with mock.patch.object(desktop_app, "_append_startup_trace") as trace_mock, mock.patch.object(
            desktop_app, "_schedule_window_app_navigation"
        ) as schedule_mock:
            desktop_app._track_window_loaded_events(window, Path("C:/tmp/data"), 10.0)
            self.assertEqual(len(loaded_event), 1)
            self.assertEqual(len(before_load_event), 1)
            self.assertEqual(len(shown_event), 1)
            shown_event[0]()
            before_load_event[0]()
            loaded_event[0]()
            before_load_event[0]()
            loaded_event[0]()

        event_names = [call.args[1] for call in trace_mock.call_args_list]
        self.assertIn("desktop_shell_window_shown", event_names)
        self.assertIn("desktop_window_event_shown", event_names)
        self.assertIn("desktop_shell_before_load", event_names)
        self.assertIn("desktop_page_before_load", event_names)
        self.assertIn("desktop_shell_loaded", event_names)
        self.assertIn("desktop_page_loaded", event_names)
        self.assertIn("desktop_window_shown", event_names)
        schedule_mock.assert_called_once_with(Path("C:/tmp/data"), 10.0, window, "http://127.0.0.1:8080/jobs.html")
        window.show.assert_called_once()

    def test_ensure_runtime_ports_raises_when_site_port_busy(self) -> None:
        config = desktop_app.DesktopRuntimeConfig(
            ship_root=Path("C:/tmp/baluffo-ship"),
            site_port=8080,
            bridge_port=8877,
            bridge_host="127.0.0.1",
            data_dir=Path("C:/tmp/baluffo-ship/data"),
            open_path="jobs.html",
            title="Baluffo",
            startup_probe=False,
        )

        with mock.patch.object(desktop_app, "_port_is_available", side_effect=[False, True]):
            with self.assertRaisesRegex(RuntimeError, "site port 8080 is already in use"):
                desktop_app.ensure_runtime_ports(config)

    def test_ensure_desktop_prerequisites_raises_when_webview_missing(self) -> None:
        original_import = __import__

        def fake_import(name, *args, **kwargs):
            if name == "webview":
                raise ImportError("missing")
            return original_import(name, *args, **kwargs)

        with mock.patch("builtins.__import__", side_effect=fake_import):
            with self.assertRaisesRegex(RuntimeError, "desktop components are missing"):
                desktop_app.ensure_desktop_prerequisites()

    def test_ensure_desktop_prerequisites_requires_webview2_on_windows(self) -> None:
        fake_webview = types.SimpleNamespace()
        with mock.patch("builtins.__import__", return_value=fake_webview), mock.patch.object(
            desktop_app, "os"
        ) as os_mock, mock.patch.object(desktop_app, "is_webview2_available", return_value=False):
            os_mock.name = "nt"
            os_mock.environ = {}
            with self.assertRaisesRegex(RuntimeError, "WebView2 Runtime"):
                desktop_app.ensure_desktop_prerequisites()

    def test_main_opens_installer_prompt_for_missing_webview2(self) -> None:
        with mock.patch.object(desktop_app, "create_runtime_config", return_value=object()), mock.patch.object(
            desktop_app,
            "ensure_desktop_prerequisites",
            side_effect=RuntimeError("Microsoft Edge WebView2 Runtime is required"),
        ), mock.patch.object(desktop_app, "show_native_message", return_value=True) as show_mock, mock.patch.object(
            desktop_app.webbrowser, "open"
        ) as open_mock:
            exit_code = desktop_app.main([])

        self.assertEqual(exit_code, 1)
        show_mock.assert_called_once()
        open_mock.assert_called_once_with(desktop_app.WEBVIEW2_INSTALL_URL)

    def test_main_child_script_mode_runs_script_with_forwarded_args(self) -> None:
        with mock.patch.object(desktop_app, "runpy") as runpy_mock, mock.patch.object(
            desktop_app.Path, "exists", return_value=True
        ):
            exit_code = desktop_app.main(
                [
                    "__child_script__",
                    "--root",
                    str(Path.cwd()),
                    "--script",
                    "source_discovery.py",
                    "--",
                    "--mode",
                    "dynamic",
                ]
            )
        self.assertEqual(exit_code, 0)
        runpy_mock.run_path.assert_called_once()

    def test_launch_desktop_app_uses_shell_handoff_with_runtime_overrides(self) -> None:
        data_dir = Path("C:/tmp/baluffo-ship/data")
        config = desktop_app.DesktopRuntimeConfig(
            ship_root=Path("C:/tmp/baluffo-ship"),
            site_port=8080,
            bridge_port=8877,
            bridge_host="127.0.0.1",
            data_dir=data_dir,
            open_path="jobs.html",
            title="Baluffo",
            startup_probe=False,
        )
        fake_window = mock.Mock()
        fake_webview = SimpleNamespace(
            create_window=mock.Mock(return_value=fake_window),
            start=mock.Mock(),
            settings={"WEBVIEW2_RUNTIME_PATH": None},
        )

        with mock.patch.object(desktop_app, "ensure_runtime_ports"), mock.patch.object(
            desktop_app, "start_child_process", side_effect=[SimpleNamespace(pid=101), SimpleNamespace(pid=202)]
        ), mock.patch.object(desktop_app, "wait_for_url"), mock.patch.dict(
            "sys.modules", {"webview": fake_webview}
        ), mock.patch.object(
            desktop_app, "_install_winforms_shutdown_exception_guard"
        ), mock.patch.object(desktop_app, "_track_window_loaded_events"), mock.patch.object(
            desktop_app, "terminate_process"
        ), mock.patch.object(desktop_app, "_append_startup_trace"), mock.patch.object(
            desktop_app.Path, "mkdir"
        ), mock.patch.dict(desktop_app.os.environ, {}, clear=True):
            desktop_app.os.environ["BALUFFO_WEBVIEW2_DISABLE_GPU"] = "1"
            desktop_app.os.environ["BALUFFO_WEBVIEW2_RUNTIME_PATH"] = "C:/fixed-runtime"
            desktop_app.launch_desktop_app(config)
            self.assertEqual(desktop_app.os.environ["WEBVIEW2_ADDITIONAL_BROWSER_ARGUMENTS"], "--disable-gpu")

        fake_webview.start.assert_called_once()
        create_args, create_kwargs = fake_webview.create_window.call_args
        self.assertEqual(create_args[0], "Baluffo")
        self.assertIn("Loading interface...", create_kwargs["html"])
        start_args, start_kwargs = fake_webview.start.call_args
        self.assertIs(start_args[0], desktop_app._begin_window_app_bootstrap)
        self.assertEqual(
            start_kwargs["args"],
            (
                data_dir,
                mock.ANY,
                fake_window,
                "http://127.0.0.1:8080/jobs.html?desktop=1&bridgePort=8877&bridgeHost=127.0.0.1",
            ),
        )
        self.assertEqual(fake_webview.settings["WEBVIEW2_RUNTIME_PATH"], "C:/fixed-runtime")


if __name__ == "__main__":
    unittest.main()
