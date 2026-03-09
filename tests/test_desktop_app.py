import argparse
import unittest
from pathlib import Path
from unittest import mock
import types

from scripts.ship import desktop_app


class DesktopAppTests(unittest.TestCase):
    def test_create_runtime_config_defaults_bridge_to_admin_port(self) -> None:
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
        )
        with mock.patch.object(desktop_app, "resolve_ship_root", return_value=root), mock.patch.object(
            desktop_app, "find_free_port", return_value=8099
        ):
            config = desktop_app.create_runtime_config(args)

        self.assertEqual(config.ship_root, root)
        self.assertEqual(config.site_port, 8099)
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
        )
        with mock.patch.object(desktop_app, "resolve_ship_root", return_value=root), mock.patch.object(
            desktop_app, "find_free_port", return_value=8099
        ):
            config = desktop_app.create_runtime_config(args)

        self.assertEqual(config.open_path, "jobs.html")

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


if __name__ == "__main__":
    unittest.main()
