import json
import os
from pathlib import Path
from unittest import mock

from src import baluffo_config
from tests.helpers.temp_paths import workspace_tmpdir


def test_load_config_reads_base_file() -> None:
    with workspace_tmpdir("baluffo-config") as tmp:
        root = Path(tmp)
        base_path = root / "baluffo.config.json"
        local_path = root / "baluffo.config.local.json"
        base_path.write_text(json.dumps({"bridge": {"port": 9911}}), encoding="utf-8")
        with (
            mock.patch.object(baluffo_config, "ROOT", root),
            mock.patch.object(baluffo_config, "BASE_CONFIG_PATH", base_path),
            mock.patch.object(baluffo_config, "LOCAL_CONFIG_PATH", local_path),
        ):
            payload = baluffo_config.load_config()
        assert int(payload["bridge"]["port"]) == 9911


def test_load_config_merges_local_override() -> None:
    with workspace_tmpdir("baluffo-config") as tmp:
        root = Path(tmp)
        base_path = root / "baluffo.config.json"
        local_path = root / "baluffo.config.local.json"
        base_path.write_text(
            json.dumps({"bridge": {"host": "127.0.0.1", "port": 8877}}), encoding="utf-8"
        )
        local_path.write_text(json.dumps({"bridge": {"port": 9911}}), encoding="utf-8")
        with (
            mock.patch.object(baluffo_config, "ROOT", root),
            mock.patch.object(baluffo_config, "BASE_CONFIG_PATH", base_path),
            mock.patch.object(baluffo_config, "LOCAL_CONFIG_PATH", local_path),
        ):
            payload = baluffo_config.load_config()
        assert str(payload["bridge"]["host"]) == "127.0.0.1"
        assert int(payload["bridge"]["port"]) == 9911


def test_get_bridge_defaults_falls_back_when_local_missing() -> None:
    with workspace_tmpdir("baluffo-config") as tmp:
        root = Path(tmp)
        base_path = root / "baluffo.config.json"
        local_path = root / "baluffo.config.local.json"
        base_path.write_text(json.dumps({"bridge": {"log_level": "debug"}}), encoding="utf-8")
        with (
            mock.patch.object(baluffo_config, "ROOT", root),
            mock.patch.object(baluffo_config, "BASE_CONFIG_PATH", base_path),
            mock.patch.object(baluffo_config, "LOCAL_CONFIG_PATH", local_path),
        ):
            defaults = baluffo_config.get_bridge_defaults()
        assert str(defaults["log_level"]) == "debug"
        assert int(defaults["port"]) == 8877


def test_get_desktop_defaults_coerces_ports_and_bools() -> None:
    with workspace_tmpdir("baluffo-config") as tmp:
        root = Path(tmp)
        base_path = root / "baluffo.config.json"
        local_path = root / "baluffo.config.local.json"
        base_path.write_text(
            json.dumps({"desktop": {"site_port": "9100", "bridge_port": "9200"}}),
            encoding="utf-8",
        )
        with (
            mock.patch.object(baluffo_config, "ROOT", root),
            mock.patch.object(baluffo_config, "BASE_CONFIG_PATH", base_path),
            mock.patch.object(baluffo_config, "LOCAL_CONFIG_PATH", local_path),
        ):
            defaults = baluffo_config.get_desktop_defaults()
        assert int(defaults["site_port"]) == 9100
        assert int(defaults["bridge_port"]) == 9200


def test_get_storage_defaults_resolves_configured_paths() -> None:
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
        with (
            mock.patch.object(baluffo_config, "ROOT", root),
            mock.patch.object(baluffo_config, "BASE_CONFIG_PATH", base_path),
            mock.patch.object(baluffo_config, "LOCAL_CONFIG_PATH", local_path),
        ):
            defaults = baluffo_config.get_storage_defaults()
        assert defaults["data_dir"] == root / "custom-data"
        assert defaults["source_discovery_config_path"] == root / "config" / "discovery.json"
        assert defaults["source_discovery_log_path"] == root / "logs" / "discovery.log"
        assert defaults["social_sources_config_path"] == root / "config" / "social.json"


def test_committed_config_declares_runtime_storage_paths(repo_root: Path) -> None:
    payload = json.loads((repo_root / "baluffo.config.json").read_text(encoding="utf-8"))
    storage = payload["storage"]

    assert storage["data_dir"] == "data"
    assert storage["source_discovery_config_path"] == "data/source-discovery-config.json"
    assert storage["source_discovery_log_path"] == "data/source-discovery.log"
    assert storage["social_sources_config_path"] == "data/social-sources-config.json"


def test_get_storage_defaults_honors_baluffo_data_dir_override_for_derived_paths() -> None:
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
        override_dir = root / "_out" / "isolated-discovery"
        with (
            mock.patch.object(baluffo_config, "ROOT", root),
            mock.patch.object(baluffo_config, "BASE_CONFIG_PATH", base_path),
            mock.patch.object(baluffo_config, "LOCAL_CONFIG_PATH", local_path),
            mock.patch.dict(os.environ, {"BALUFFO_DATA_DIR": str(override_dir)}, clear=False),
        ):
            defaults = baluffo_config.get_storage_defaults()
        assert defaults["data_dir"] == override_dir
        assert (
            defaults["source_discovery_config_path"]
            == override_dir / "source-discovery-config.json"
        )
        assert defaults["source_discovery_log_path"] == override_dir / "source-discovery.log"
        assert defaults["social_sources_config_path"] == override_dir / "social-sources-config.json"
