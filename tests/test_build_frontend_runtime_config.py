import unittest
from pathlib import Path
from unittest import mock

from scripts import build_frontend_runtime_config as frontend_runtime_config


class BuildFrontendRuntimeConfigTests(unittest.TestCase):
    def test_build_payload_contains_only_frontend_safe_fields(self) -> None:
        with mock.patch.object(
            frontend_runtime_config,
            "get_bridge_defaults",
            return_value={"host": "192.168.1.10", "port": 9000},
        ), mock.patch.object(
            frontend_runtime_config,
            "get_security_defaults",
            return_value={"admin_pin_default": "2468", "github_app_enabled_default": False},
        ):
            payload = frontend_runtime_config.build_frontend_runtime_config_payload()
        self.assertEqual(payload["bridge"], {"host": "192.168.1.10", "port": 9000})
        self.assertEqual(
            payload["security"],
            {"admin_pin_default": "2468", "github_app_enabled_default": False},
        )
        self.assertEqual(set(payload.keys()), {"bridge", "security"})

    def test_render_js_exports_frozen_payload(self) -> None:
        text = frontend_runtime_config.render_frontend_runtime_config_js(
            {
                "bridge": {"host": "127.0.0.1", "port": 8877},
                "security": {"admin_pin_default": "1234", "github_app_enabled_default": True},
            }
        )
        self.assertIn("BALUFFO_FRONTEND_RUNTIME_CONFIG", text)
        self.assertIn('"admin_pin_default": "1234"', text)
        self.assertIn("Object.freeze", text)

    def test_checked_in_generated_file_matches_current_render(self) -> None:
        expected = frontend_runtime_config.render_frontend_runtime_config_js(
            frontend_runtime_config.build_frontend_runtime_config_payload()
        )
        actual = (frontend_runtime_config.ROOT / "frontend-runtime-config.js").read_text(encoding="utf-8")
        self.assertMultiLineEqual(
            actual,
            expected,
            msg="frontend-runtime-config.js is stale; run `npm run build:frontend-runtime-config`.",
        )


if __name__ == "__main__":
    unittest.main()
