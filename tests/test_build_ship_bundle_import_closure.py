import json
from pathlib import Path

import pytest

from scripts import build_ship_bundle
from scripts.build_ship_bundle import build_bundle
from tests.helpers.temp_paths import workspace_tmpdir

pytestmark = pytest.mark.packaging


def test_validate_private_key_pem_accepts_valid_rsa_pem_with_escaped_newlines() -> None:
    private_key_pem = (
        "-----BEGIN RSA PRIVATE KEY-----\n"
        "MB0CAQACAgyhAgERAgIKwQIBPQIBNQIBNQIBMQIBJg==\n"
        "-----END RSA PRIVATE KEY-----"
    )
    build_ship_bundle._validate_private_key_pem(  # noqa: SLF001
        private_key_pem.replace("\n", "\\n"),
        source="unit-test",
    )


def _write_packaged_sync_config(tmp: str) -> None:
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


def _build_bundle(tmp: str) -> Path:
    _write_packaged_sync_config(tmp)
    return build_bundle(Path(tmp) / "dist" / "baluffo-ship", "1.2.3")


def test_bundle_version_python_imports_resolve_from_packaged_root() -> None:
    with workspace_tmpdir("build-ship-bundle-imports") as tmp:
        output = _build_bundle(tmp)
        version_root = output / "app" / "versions" / "1.2.3"

        build_ship_bundle.validate_app_version_python_imports(version_root)
        assert not list(tmp.glob("baluffo-runtime.db*"))


def test_bundle_version_python_import_validation_fails_for_missing_leaf_module() -> None:
    with workspace_tmpdir("build-ship-bundle-imports") as tmp:
        output = _build_bundle(tmp)
        version_root = output / "app" / "versions" / "1.2.3"
        (version_root / "src" / "source_registry_auto_approval.py").unlink()

        with pytest.raises(RuntimeError) as exc_info:
            build_ship_bundle.validate_app_version_python_imports(version_root)

        message = str(exc_info.value)
        assert "Ship bundle Python import validation failed" in message
        assert "source_registry_auto_approval" in message


def test_bundle_version_storage_validation_fails_for_missing_migration_resource() -> None:
    with workspace_tmpdir("build-ship-bundle-imports") as tmp:
        output = _build_bundle(tmp)
        version_root = output / "app" / "versions" / "1.2.3"
        (version_root / "src" / "storage" / "migrations" / "004_jobs_feed.sql").unlink()

        with pytest.raises(RuntimeError) as exc_info:
            build_ship_bundle.validate_app_version_python_imports(version_root)

        message = str(exc_info.value)
        assert "Ship bundle Python storage validation failed" in message
        assert "migration sequence" in message or "004_jobs_feed" in message


def test_bundle_contains_python_import_closure_modules() -> None:
    with workspace_tmpdir("build-ship-bundle-imports") as tmp:
        output = _build_bundle(tmp)
        version_root = output / "app" / "versions" / "1.2.3"
        required_modules = (
            "src/source_registry_auto_approval.py",
            "src/source_registry_canonicalize.py",
            "src/source_registry_identity.py",
            "src/source_registry_io.py",
            "src/source_registry_policy.py",
            "src/source_registry_state.py",
            "src/storage_metrics.py",
            "src/ship/jobs_first_run_state.py",
            "src/ship/runtime_launcher.py",
            "src/ship/update_manager.py",
            "src/ship/update_manager_apply.py",
            "src/ship/update_manager_bootstrap.py",
            "src/ship/update_manager_cli.py",
            "src/ship/update_manager_paths.py",
            "src/ship/update_manager_recovery.py",
            "src/ship/update_manager_state.py",
            "src/ship/update_manager_validation.py",
            "src/storage/baluffo_store.py",
            "src/storage/migrations/001_initial.sql",
            "src/storage/migrations/002_task_events.sql",
            "src/storage/migrations/003_fetch_source_runs.sql",
            "src/storage/migrations/004_jobs_feed.sql",
            "src/storage/migrations/005_task_sync_runtime.sql",
            "src/storage/migrations/006_source_run_runtime.sql",
            "src/storage/migrations/007_jobs_feed_runtime.sql",
            "src/storage/migrations/008_source_registry_runtime.sql",
            "src/storage/job_runtime.py",
            "src/storage/source_registry_runtime.py",
            "src/storage/source_runtime.py",
            "src/storage/evidence_archive.py",
        )

        assert all((version_root / rel_path).exists() for rel_path in required_modules)
