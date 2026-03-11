import json
import unittest
from pathlib import Path
from unittest import mock

from scripts import baluffo_config
from tests.temp_paths import workspace_tmpdir


class BaluffoConfigTests(unittest.TestCase):
    def test_load_config_reads_base_file(self) -> None:
        with workspace_tmpdir("baluffo-config") as tmp:
            root = Path(tmp)
            base_path = root / "baluffo.config.json"
            local_path = root / "baluffo.config.local.json"
            base_path.write_text(json.dumps({"bridge": {"port": 9911}}), encoding="utf-8")
            with mock.patch.object(baluffo_config, "ROOT", root), mock.patch.object(
                baluffo_config, "BASE_CONFIG_PATH", base_path
            ), mock.patch.object(baluffo_config, "LOCAL_CONFIG_PATH", local_path):
                payload = baluffo_config.load_config()
            self.assertEqual(int(payload["bridge"]["port"]), 9911)

    def test_load_config_merges_local_override(self) -> None:
        with workspace_tmpdir("baluffo-config") as tmp:
            root = Path(tmp)
            base_path = root / "baluffo.config.json"
            local_path = root / "baluffo.config.local.json"
            base_path.write_text(json.dumps({"bridge": {"host": "127.0.0.1", "port": 8877}}), encoding="utf-8")
            local_path.write_text(json.dumps({"bridge": {"port": 9911}}), encoding="utf-8")
            with mock.patch.object(baluffo_config, "ROOT", root), mock.patch.object(
                baluffo_config, "BASE_CONFIG_PATH", base_path
            ), mock.patch.object(baluffo_config, "LOCAL_CONFIG_PATH", local_path):
                payload = baluffo_config.load_config()
            self.assertEqual(str(payload["bridge"]["host"]), "127.0.0.1")
            self.assertEqual(int(payload["bridge"]["port"]), 9911)

    def test_get_bridge_defaults_falls_back_when_local_missing(self) -> None:
        with workspace_tmpdir("baluffo-config") as tmp:
            root = Path(tmp)
            base_path = root / "baluffo.config.json"
            local_path = root / "baluffo.config.local.json"
            base_path.write_text(json.dumps({"bridge": {"log_level": "debug"}}), encoding="utf-8")
            with mock.patch.object(baluffo_config, "ROOT", root), mock.patch.object(
                baluffo_config, "BASE_CONFIG_PATH", base_path
            ), mock.patch.object(baluffo_config, "LOCAL_CONFIG_PATH", local_path):
                defaults = baluffo_config.get_bridge_defaults()
            self.assertEqual(str(defaults["log_level"]), "debug")
            self.assertEqual(int(defaults["port"]), 8877)

    def test_get_desktop_defaults_coerces_ports_and_bools(self) -> None:
        with workspace_tmpdir("baluffo-config") as tmp:
            root = Path(tmp)
            base_path = root / "baluffo.config.json"
            local_path = root / "baluffo.config.local.json"
            base_path.write_text(
                json.dumps({"desktop": {"site_port": "9100", "bridge_port": "9200"}}),
                encoding="utf-8",
            )
            with mock.patch.object(baluffo_config, "ROOT", root), mock.patch.object(
                baluffo_config, "BASE_CONFIG_PATH", base_path
            ), mock.patch.object(baluffo_config, "LOCAL_CONFIG_PATH", local_path):
                defaults = baluffo_config.get_desktop_defaults()
            self.assertEqual(int(defaults["site_port"]), 9100)
            self.assertEqual(int(defaults["bridge_port"]), 9200)

    def test_get_storage_defaults_resolves_configured_paths(self) -> None:
        with workspace_tmpdir("baluffo-config") as tmp:
            root = Path(tmp)
            base_path = root / "baluffo.config.json"
            local_path = root / "baluffo.config.local.json"
            base_path.write_text(
                json.dumps(
                    {
                        "storage": {
                            "data_dir": "custom-data",
                            "source_discovery_config_path": "config/discovery.json",
                            "source_discovery_log_path": "logs/discovery.log",
                            "social_sources_config_path": "config/social.json",
                        }
                    }
                ),
                encoding="utf-8",
            )
            with mock.patch.object(baluffo_config, "ROOT", root), mock.patch.object(
                baluffo_config, "BASE_CONFIG_PATH", base_path
            ), mock.patch.object(baluffo_config, "LOCAL_CONFIG_PATH", local_path):
                defaults = baluffo_config.get_storage_defaults()
            self.assertEqual(defaults["data_dir"], root / "custom-data")
            self.assertEqual(defaults["source_discovery_config_path"], root / "config" / "discovery.json")
            self.assertEqual(defaults["source_discovery_log_path"], root / "logs" / "discovery.log")
            self.assertEqual(defaults["social_sources_config_path"], root / "config" / "social.json")


if __name__ == "__main__":
    unittest.main()
