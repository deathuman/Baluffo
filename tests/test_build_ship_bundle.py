import json
import sys
from pathlib import Path
from unittest import mock

import pytest

from scripts import build_ship_bundle
from scripts.build_ship_bundle import build_bundle
from src import source_sync
from src.app_version import APP_VERSION
from src.shared.json_io import read_json
from tests.helpers.ship_bundle import copy_minimal_app_version
from tests.helpers.temp_paths import workspace_tmpdir

pytestmark = pytest.mark.packaging

_LAUNCHER_EXT = ".sh" if sys.platform != "win32" else ".ps1"


def _build_with_temp_packaged_config(
    tmp: str,
    *,
    env: dict[str, str] | None = None,
) -> Path:
    temp_root = Path(tmp)
    packaging_dir = temp_root / "packaging"
    packaging_dir.mkdir(parents=True, exist_ok=True)
    config_path = packaging_dir / "github-app-sync-config.json"
    template_path = (
        Path(__file__).resolve().parents[1] / "packaging" / "github-app-sync-config.template.json"
    )
    with (
        mock.patch("scripts.build_ship_bundle.PACKAGED_SYNC_CONFIG_PATH", config_path),
        mock.patch("scripts.build_ship_bundle.PACKAGED_SYNC_CONFIG_TEMPLATE_PATH", template_path),
        mock.patch.dict("os.environ", env or {}, clear=False),
    ):
        return build_bundle(temp_root / "dist" / "baluffo-ship", "1.2.3")


def test_bundle_contains_runtime_assets_and_seeded_runtime_data_only() -> None:
    with workspace_tmpdir("build-ship-bundle") as tmp:
        config_path = Path(tmp) / "packaging" / "github-app-sync-config.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(
            json.dumps(
                {
                    "schemaVersion": 1,
                    "appId": "123456",
                    "installationId": "999999",
                    "repo": "owner/repo",
                    "branch": "main",
                    "path": "baluffo/source-sync.json",
                    "privateKeyPem": "-----BEGIN RSA PRIVATE KEY-----\nTEST\n-----END RSA PRIVATE KEY-----",
                }
            ),
            encoding="utf-8",
        )
        output = _build_with_temp_packaged_config(tmp)
        version_root = output / "app" / "versions" / "1.2.3"
        assert (output / f"run-site{_LAUNCHER_EXT}").exists()
        assert (output / f"run-bridge{_LAUNCHER_EXT}").exists()
        assert (output / "RELEASE_GUIDE.md").exists()
        assert (output / "app" / "update-manifest.json").exists()
        assert (output / "DESKTOP_UPDATE_MANIFEST_SCHEMA.json").exists()
        assert (version_root / "admin.html").exists()
        assert (version_root / "frontend" / "admin" / "app.js").exists()
        assert (version_root / "src" / "admin_bridge.py").exists()
        assert (version_root / "src" / "app_version.py").exists()
        assert (version_root / "src" / "baluffo_version.py").exists()
        assert (version_root / "src" / "baluffo_config.py").exists()
        assert (version_root / "src" / "exceptions.py").exists()
        assert (version_root / "src" / "local_data_store.py").exists()
        assert (version_root / "src" / "local_data_store_attachments.py").exists()
        assert (version_root / "src" / "local_data_store_backup.py").exists()
        assert (version_root / "src" / "local_data_store_profiles.py").exists()
        assert (version_root / "src" / "local_data_store_saved_jobs.py").exists()
        assert (version_root / "src" / "local_data_store_shared.py").exists()
        assert (version_root / "src" / "local_data_store_tracking.py").exists()
        assert (version_root / "src" / "source_sync.py").exists()
        assert (version_root / "src" / "source_sync_config.py").exists()
        assert (version_root / "src" / "source_sync_crypto.py").exists()
        assert (version_root / "src" / "source_sync_runtime.py").exists()
        assert (version_root / "src" / "source_sync_snapshot.py").exists()
        assert (version_root / "src" / "ship" / "desktop_update.py").exists()
        assert (version_root / "src" / "ship" / "startup_telemetry.py").exists()
        assert (version_root / "src" / "jobs" / "common" / "contracts_fetch_report.py").exists()
        assert (version_root / "src" / "jobs" / "common" / "registry_defaults.py").exists()
        # Required by admin_bridge → jobs.common and bridge routes (packaged desktop must resolve src.shared, src.core).
        assert (version_root / "src" / "shared" / "regex.py").exists()
        assert (version_root / "src" / "shared" / "utils.py").exists()
        assert (version_root / "src" / "core" / "contracts.py").exists()
        assert (version_root / "src" / "core" / "schemas.py").exists()
        assert (version_root / "src" / "discovery_seed_catalog.json").exists()
        assert (version_root / "baluffo.config.json").exists()
        assert (version_root / "frontend-runtime-config.js").exists()
        assert (version_root / "frontend" / "shared" / "local-data" / "app-client.js").exists()
        assert (version_root / "frontend" / "shared" / "local-data" / "desktop-client.js").exists()
        assert (version_root / "frontend" / "shared" / "local-data" / "browser-client.js").exists()
        assert (version_root / "frontend" / "shared" / "config" / "admin-config.js").exists()
        assert (version_root / "frontend" / "jobs" / "state.js").exists()
        assert (version_root / "frontend" / "jobs" / "parsing-utils.js").exists()
        assert (version_root / "frontend" / "saved" / "zip-utils.js").exists()
        # fmt: off
        runtime_assets = ("desktop-probe-css.html", "desktop-probe.html", "desktop-probe-head.html", "desktop-probe-inline.html", "favicon.ico", "startup-probe.js", "packaging/README.md", "packaging/github-app-sync-config.template.json")
        # fmt: on
        assert all((version_root / rel_path).exists() for rel_path in runtime_assets)
        version_contract_dir = version_root / "data"
        expected_version_contract_files = {
            version_contract_dir / rel_path
            for rel_path in build_ship_bundle.APP_VERSION_CONTRACT_FILES
        }
        actual_version_contract_files = {
            path for path in version_contract_dir.rglob("*") if path.is_file()
        }
        assert actual_version_contract_files == expected_version_contract_files
        assert not (version_root / "data" / "jobs-fetch-report.json").exists()
        assert not (version_root / "package-lock.json").exists()
        assert not (version_root / "LOCAL_SETUP.md").exists()
        assert not (version_root / "src" / "run_py_tests.cmd").exists()
        assert not (version_root / "src" / "build_sync_app_config.py").exists()
        assert not (version_root / "baluffo.config.local.json").exists()
        assert (version_root / "packaging" / "github-app-sync-config.json").exists()
        jobs_html = (version_root / "jobs.html").read_text(encoding="utf-8")
        assert '<script src="frontend-runtime-config.js"></script>' in jobs_html
        assert "?v=" not in jobs_html
        bundled_release_guide = (output / "RELEASE_GUIDE.md").read_text(encoding="utf-8")
        assert "# Release Guide" in bundled_release_guide

        seeded_report = json.loads(
            (output / "data" / "jobs-fetch-report.json").read_text(encoding="utf-8")
        )
        assert seeded_report == {
            "schemaVersion": 1,
            "runId": "",
            "startedAt": "",
            "finishedAt": "",
            "runtime": {"lifecycle": {"owner": "fetch_report", "heartbeatAt": ""}},
            "summary": {"outputCount": 0, "failedSources": 0, "sourceCount": 0},
            "taskProgress": {
                "active": False,
                "phaseKey": "",
                "phaseLabel": "",
                "mode": "indeterminate",
                "ratio": 0.0,
                "counts": {},
            },
            "sources": [],
            "outputs": {"report": str(output / "data" / "jobs-fetch-report.json")},
        }
        assert isinstance(seeded_report["sources"], list)
        assert not (output / "data" / "jobs-unified.json.gz").exists()
        assert not (output / "data" / "jobs-unified.json").exists()
        assert not (output / "data" / "jobs-unified-light.json.gz").exists()
        assert not (output / "data" / "jobs-unified-light.json").exists()
        assert not (output / "data" / "jobs-unified.csv").exists()
        assert not (output / "data" / "jobs-unified-startup.json").exists()
        assert not list(output.glob("**/baluffo-runtime.db*"))
        assert (output / "data" / "jobs-source-state.json.gz").exists()
        assert not (output / "data" / "jobs-source-state.json").exists()
        assert read_json(output / "data" / "jobs-source-state.json", {}) == {
            "schemaVersion": 1,
            "updatedAt": "",
            "sources": {},
        }


def test_parse_args_defaults_to_shared_app_version() -> None:
    with mock.patch("sys.argv", ["build_ship_bundle.py"]):
        from scripts import build_ship_bundle

        args = build_ship_bundle.parse_args()
    assert args.bundle_version == APP_VERSION


def test_bundle_does_not_generate_startup_preview_from_local_jobs_rows() -> None:
    with workspace_tmpdir("build-ship-bundle") as tmp:
        rows = [{"title": f"Role {index}", "company": "Studio"} for index in range(300)]
        data_dir = Path(tmp) / "data"
        config_path = Path(tmp) / "packaging" / "github-app-sync-config.json"
        data_dir.mkdir(parents=True, exist_ok=True)
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(
            json.dumps(
                {
                    "schemaVersion": 1,
                    "appId": "123456",
                    "installationId": "999999",
                    "repo": "owner/repo",
                    "branch": "main",
                    "path": "baluffo/source-sync.json",
                    "privateKeyPem": "-----BEGIN RSA PRIVATE KEY-----\nTEST\n-----END RSA PRIVATE KEY-----",
                }
            ),
            encoding="utf-8",
        )
        (data_dir / "jobs-unified-light.json").write_text(
            json.dumps(rows, ensure_ascii=False),
            encoding="utf-8",
        )
        output = _build_with_temp_packaged_config(tmp)
        assert not (output / "data" / "jobs-unified-startup.json").exists()
        assert not (output / "data" / "jobs-unified-light.json").exists()
        assert not (output / "data" / "jobs-unified-light.json.gz").exists()


def test_bundle_generates_packaged_sync_config_from_build_env() -> None:
    with workspace_tmpdir("build-ship-bundle") as tmp:
        private_key_path = Path(tmp) / "packaging" / "github-app-private-key.pem"
        private_key_path.parent.mkdir(parents=True, exist_ok=True)
        private_key_path.write_text(
            "-----BEGIN RSA PRIVATE KEY-----\nTEST\n-----END RSA PRIVATE KEY-----\n",
            encoding="utf-8",
        )
        with (
            mock.patch(
                "scripts.build_ship_bundle._candidate_local_packaged_sync_config_paths",
                return_value=[],
            ),
            mock.patch("scripts.build_ship_bundle._validate_private_key_pem"),
            mock.patch(
                "scripts.build_ship_bundle._copy_app_version",
                side_effect=copy_minimal_app_version,
            ),
            mock.patch("scripts.build_ship_bundle.refresh_runtime_bootstrap"),
        ):
            output = _build_with_temp_packaged_config(
                tmp,
                env={
                    "BALUFFO_SYNC_BUILD_APP_ID": "123456",
                    "BALUFFO_SYNC_BUILD_INSTALLATION_ID": "999999",
                    "BALUFFO_SYNC_BUILD_REPO": "owner/repo",
                    "BALUFFO_SYNC_BUILD_PRIVATE_KEY_PATH": str(private_key_path),
                },
            )
        packaged_config = json.loads(
            (
                output / "app" / "versions" / "1.2.3" / "packaging" / "github-app-sync-config.json"
            ).read_text(encoding="utf-8")
        )
        assert packaged_config["appId"] == "123456"
        assert packaged_config["installationId"] == "999999"
        assert packaged_config["repo"] == "owner/repo"
        assert packaged_config["allowedRepo"] == "owner/repo"
        assert packaged_config["allowedBranch"] == "main"
        assert packaged_config["allowedPathPrefix"] == "baluffo/source-sync.json"
        assert packaged_config["keyDerivation"] == "embedded"
        assert "privateKeyPem" not in packaged_config
        assert packaged_config["privateKeyPemEnc"].startswith("v2.")


def test_bundle_embeds_desktop_update_public_keys_from_build_env() -> None:
    with workspace_tmpdir("build-ship-bundle") as tmp:
        config_path = Path(tmp) / "packaging" / "github-app-sync-config.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(
            json.dumps(
                {
                    "schemaVersion": 1,
                    "appId": "123456",
                    "installationId": "999999",
                    "repo": "owner/repo",
                    "branch": "main",
                    "path": "baluffo/source-sync.json",
                    "privateKeyPem": "-----BEGIN RSA PRIVATE KEY-----\nTEST\n-----END RSA PRIVATE KEY-----",
                }
            ),
            encoding="utf-8",
        )
        with (
            mock.patch(
                "scripts.build_ship_bundle._copy_app_version",
                side_effect=copy_minimal_app_version,
            ),
            mock.patch("scripts.build_ship_bundle.refresh_runtime_bootstrap"),
        ):
            output = _build_with_temp_packaged_config(
                tmp,
                env={
                    "BALUFFO_DESKTOP_UPDATE_PUBLIC_KEYS_JSON": json.dumps(
                        {"desktop-ed25519-2026-01": "cHVibGljLWtleS1iYXNlNjQ="}
                    )
                },
            )
        bundled_payload = json.loads(
            (output / "app" / "desktop-update-public-keys.json").read_text(encoding="utf-8")
        )
        assert bundled_payload == {"desktop-ed25519-2026-01": "cHVibGljLWtleS1iYXNlNjQ="}
        version_payload = json.loads(
            (
                output
                / "app"
                / "versions"
                / "1.2.3"
                / "packaging"
                / "desktop-update-public-keys.json"
            ).read_text(encoding="utf-8")
        )
        assert version_payload == bundled_payload


def test_bundle_writes_desktop_update_repo_config_from_build_env() -> None:
    with workspace_tmpdir("build-ship-bundle") as tmp:
        config_path = Path(tmp) / "packaging" / "github-app-sync-config.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(
            json.dumps(
                {
                    "schemaVersion": 1,
                    "appId": "123456",
                    "installationId": "999999",
                    "repo": "owner/repo",
                    "branch": "main",
                    "path": "baluffo/source-sync.json",
                    "privateKeyPem": "-----BEGIN RSA PRIVATE KEY-----\nTEST\n-----END RSA PRIVATE KEY-----",
                }
            ),
            encoding="utf-8",
        )
        with (
            mock.patch(
                "scripts.build_ship_bundle._copy_app_version",
                side_effect=copy_minimal_app_version,
            ),
            mock.patch("scripts.build_ship_bundle.refresh_runtime_bootstrap"),
        ):
            output = _build_with_temp_packaged_config(
                tmp,
                env={"BALUFFO_DESKTOP_UPDATE_REPO": "owner/app-release"},
            )
        bundled_payload = json.loads(
            (
                output / "app" / "versions" / "1.2.3" / "packaging" / "desktop-update-config.json"
            ).read_text(encoding="utf-8")
        )
        assert bundled_payload == {"repo": "owner/app-release"}


def test_bundle_rejects_invalid_private_key_from_build_env() -> None:
    with workspace_tmpdir("build-ship-bundle") as tmp:
        private_key_path = Path(tmp) / "packaging" / "github-app-private-key.pem"
        private_key_path.parent.mkdir(parents=True, exist_ok=True)
        private_key_path.write_text(
            "-----BEGIN RSA PRIVATE KEY-----\nTEST\n-----END RSA PRIVATE KEY-----\n",
            encoding="utf-8",
        )
        with mock.patch(
            "scripts.build_ship_bundle._candidate_local_packaged_sync_config_paths", return_value=[]
        ):
            with pytest.raises(RuntimeError, match="Invalid packaged sync private key"):
                _build_with_temp_packaged_config(
                    tmp,
                    env={
                        "BALUFFO_SYNC_BUILD_APP_ID": "123456",
                        "BALUFFO_SYNC_BUILD_INSTALLATION_ID": "999999",
                        "BALUFFO_SYNC_BUILD_REPO": "owner/repo",
                        "BALUFFO_SYNC_BUILD_PRIVATE_KEY_PATH": str(private_key_path),
                    },
                )


def test_bundle_restores_packaged_sync_config_from_local_env_path() -> None:
    with workspace_tmpdir("build-ship-bundle") as tmp:
        source_config_path = Path(tmp) / "secrets" / "github-app-sync-config.json"
        source_config_path.parent.mkdir(parents=True, exist_ok=True)
        source_payload = {
            "schemaVersion": 1,
            "appId": "123456",
            "installationId": "999999",
            "repo": "owner/repo",
            "branch": "main",
            "path": "baluffo/source-sync.json",
            "allowedRepo": "owner/repo",
            "allowedBranch": "main",
            "allowedPathPrefix": "baluffo/source-sync.json",
            "keyDerivation": "embedded",
            "embeddedKeyHint": "hint",
            "embeddedKeyVersion": "v1",
            "keySalt": "salt",
            "privateKeyPemEnc": "ciphertext",
        }
        source_config_path.write_text(json.dumps(source_payload), encoding="utf-8")
        with (
            mock.patch(
                "scripts.build_ship_bundle._copy_app_version",
                side_effect=copy_minimal_app_version,
            ),
            mock.patch("scripts.build_ship_bundle.refresh_runtime_bootstrap"),
        ):
            output = _build_with_temp_packaged_config(
                tmp,
                env={"BALUFFO_SYNC_BUILD_CONFIG_PATH": str(source_config_path)},
            )
        bundled_config_path = (
            output / "app" / "versions" / "1.2.3" / "packaging" / "github-app-sync-config.json"
        )
        bundled_config = json.loads(bundled_config_path.read_text(encoding="utf-8"))
        assert bundled_config["appId"] == source_payload["appId"]
        assert bundled_config["installationId"] == source_payload["installationId"]


def test_bundle_uses_sync_app_config_env_path_before_local_paths() -> None:
    with workspace_tmpdir("build-ship-bundle") as tmp:
        source_config_path = Path(tmp) / "secrets" / "github-app-sync-config.json"
        source_config_path.parent.mkdir(parents=True, exist_ok=True)
        source_payload = {
            "schemaVersion": 1,
            "appId": "123456",
            "installationId": "999999",
            "repo": "env/repo",
            "branch": "main",
            "path": "baluffo/source-sync.json",
            "allowedRepo": "env/repo",
            "allowedBranch": "main",
            "allowedPathPrefix": "baluffo/source-sync.json",
            "keyDerivation": "embedded",
            "embeddedKeyHint": "hint",
            "embeddedKeyVersion": "v1",
            "keySalt": "salt",
            "privateKeyPemEnc": "ciphertext",
        }
        source_config_path.write_text(json.dumps(source_payload), encoding="utf-8")
        with (
            mock.patch(
                "scripts.build_ship_bundle._copy_app_version",
                side_effect=copy_minimal_app_version,
            ),
            mock.patch("scripts.build_ship_bundle.refresh_runtime_bootstrap"),
        ):
            output = _build_with_temp_packaged_config(
                tmp,
                env={source_sync.PACKAGED_SYNC_CONFIG_ENV: str(source_config_path)},
            )
        bundled_config = json.loads(
            (
                output / "app" / "versions" / "1.2.3" / "packaging" / "github-app-sync-config.json"
            ).read_text(encoding="utf-8")
        )
        assert bundled_config["repo"] == "env/repo"


def test_bundle_normalizes_machine_bound_packaged_sync_config_for_portable_build() -> None:
    with workspace_tmpdir("build-ship-bundle") as tmp:
        private_key_pem = "-----BEGIN RSA PRIVATE KEY-----\nTEST\n-----END RSA PRIVATE KEY-----\n"
        config_path = Path(tmp) / "packaging" / "github-app-sync-config.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        source_payload = {
            "schemaVersion": 1,
            "appId": "123456",
            "installationId": "999999",
            "repo": "owner/repo",
            "branch": "main",
            "path": "baluffo/source-sync.json",
            "allowedRepo": "owner/repo",
            "allowedBranch": "main",
            "allowedPathPrefix": "baluffo/source-sync.json",
            "keyDerivation": "machine",
            "keySalt": source_sync._base64url_encode(b"unit-test-salt-123"),  # noqa: SLF001
            "privateKeyPemEnc": source_sync.encrypt_private_key_pem(
                private_key_pem,
                salt_b64=source_sync._base64url_encode(b"unit-test-salt-123"),  # noqa: SLF001
                app_id="123456",
                installation_id="999999",
            ),
        }
        config_path.write_text(json.dumps(source_payload), encoding="utf-8")
        with (
            mock.patch(
                "scripts.build_ship_bundle._copy_app_version",
                side_effect=copy_minimal_app_version,
            ),
            mock.patch("scripts.build_ship_bundle.refresh_runtime_bootstrap"),
        ):
            output = _build_with_temp_packaged_config(tmp)
        bundled_config_path = (
            output / "app" / "versions" / "1.2.3" / "packaging" / "github-app-sync-config.json"
        )
        bundled_config = json.loads(bundled_config_path.read_text(encoding="utf-8"))
        assert bundled_config["keyDerivation"] == "embedded"
        assert bundled_config["privateKeyPemEnc"] != source_payload["privateKeyPemEnc"]
        assert bundled_config["privateKeyPemEnc"].startswith("v2.")
        loaded_config = source_sync.load_packaged_sync_config(
            env={source_sync.PACKAGED_SYNC_CONFIG_ENV: str(bundled_config_path)}
        )
        assert loaded_config is not None
        assert loaded_config.private_key_pem == private_key_pem


def test_versioned_packaged_sync_portability_invariant_rejects_machine_config() -> None:
    with workspace_tmpdir("build-ship-bundle") as tmp:
        config_path = Path(tmp) / "packaging" / "github-app-sync-config.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(
            json.dumps(
                {
                    "schemaVersion": 1,
                    "appId": "123456",
                    "installationId": "999999",
                    "repo": "owner/repo",
                    "branch": "main",
                    "path": "baluffo/source-sync.json",
                    "keyDerivation": "machine",
                    "keySalt": "salt",
                    "privateKeyPemEnc": "ciphertext",
                }
            ),
            encoding="utf-8",
        )
        with pytest.raises(RuntimeError, match="cannot use keyDerivation=machine"):
            build_ship_bundle._assert_versioned_packaged_sync_config_portable(config_path)  # noqa: SLF001


def test_versioned_packaged_sync_portability_invariant_accepts_embedded_config() -> None:
    with workspace_tmpdir("build-ship-bundle") as tmp:
        config_path = Path(tmp) / "packaging" / "github-app-sync-config.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(
            json.dumps(
                {
                    "schemaVersion": 1,
                    "appId": "123456",
                    "installationId": "999999",
                    "repo": "owner/repo",
                    "branch": "main",
                    "path": "baluffo/source-sync.json",
                    "keyDerivation": "embedded",
                    "embeddedKeyHint": "hint",
                    "embeddedKeyVersion": "v1",
                    "keySalt": "salt",
                    "privateKeyPemEnc": "ciphertext",
                }
            ),
            encoding="utf-8",
        )
        build_ship_bundle._assert_versioned_packaged_sync_config_portable(config_path)  # noqa: SLF001


def test_bundle_derives_desktop_update_repo_from_git_remote() -> None:
    with workspace_tmpdir("build-ship-bundle") as tmp:
        config_path = Path(tmp) / "packaging" / "github-app-sync-config.json"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(
            json.dumps(
                {
                    "schemaVersion": 1,
                    "appId": "123456",
                    "installationId": "999999",
                    "repo": "owner/repo",
                    "branch": "main",
                    "path": "baluffo/source-sync.json",
                    "privateKeyPem": "-----BEGIN RSA PRIVATE KEY-----\nTEST\n-----END RSA PRIVATE KEY-----",
                }
            ),
            encoding="utf-8",
        )
        with (
            mock.patch(
                "scripts.build_ship_bundle.subprocess.run",
                return_value=mock.Mock(
                    returncode=0, stdout="https://github.com/example/Baluffo.git\n"
                ),
            ),
            mock.patch(
                "scripts.build_ship_bundle._copy_app_version",
                side_effect=copy_minimal_app_version,
            ),
            mock.patch("scripts.build_ship_bundle.refresh_runtime_bootstrap"),
        ):
            output = _build_with_temp_packaged_config(tmp)
        bundled_payload = json.loads(
            (
                output / "app" / "versions" / "1.2.3" / "packaging" / "desktop-update-config.json"
            ).read_text(encoding="utf-8")
        )
        assert bundled_payload == {"repo": "example/Baluffo"}
