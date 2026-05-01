import json
from pathlib import Path

from src import source_registry as sr
from src.jobs.common.sources import load_registry_from_file
from tests.helpers.temp_paths import workspace_tmpdir


def _write_seed(root: Path, bucket: str, rows: list[dict]) -> Path:
    path = root / "defaults" / f"source-registry-{bucket}.seed.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows), encoding="utf-8")
    return path


def test_registry_loads_seed_when_runtime_file_is_missing() -> None:
    with workspace_tmpdir("source-registry") as tmp:
        root = Path(tmp)
        runtime_path = root / "source-registry-active.json"
        _write_seed(root, "active", [{"id": "seed-active", "adapter": "static"}])

        assert not runtime_path.exists()
        assert sr.load_json_array(runtime_path, [])[0]["id"] == "seed-active"


def test_runtime_registry_file_overrides_seed_file() -> None:
    with workspace_tmpdir("source-registry") as tmp:
        root = Path(tmp)
        runtime_path = root / "source-registry-active.json"
        _write_seed(root, "active", [{"id": "seed-active", "adapter": "static"}])
        runtime_path.write_text(
            json.dumps([{"id": "runtime-active", "adapter": "greenhouse"}]),
            encoding="utf-8",
        )

        assert sr.load_json_array(runtime_path, [])[0]["id"] == "runtime-active"


def test_registry_writes_target_runtime_file_without_mutating_seed() -> None:
    with workspace_tmpdir("source-registry") as tmp:
        root = Path(tmp)
        runtime_path = root / "source-registry-pending.json"
        seed_payload = [{"id": "seed-pending", "adapter": "static"}]
        seed_path = _write_seed(root, "pending", seed_payload)

        sr.save_json_atomic(runtime_path, [{"id": "runtime-pending", "adapter": "lever"}])

        assert json.loads(seed_path.read_text(encoding="utf-8")) == seed_payload
        assert sr.load_json_array(runtime_path, [])[0]["id"] == "runtime-pending"


def test_jobs_registry_loader_uses_seed_when_runtime_file_is_missing() -> None:
    with workspace_tmpdir("source-registry") as tmp:
        root = Path(tmp)
        runtime_path = root / "source-registry-active.json"
        _write_seed(root, "active", [{"id": "jobs-seed", "adapter": "static"}])

        rows = load_registry_from_file(runtime_path, [{"id": "fallback", "adapter": "static"}])

        assert rows == [{"id": "jobs-seed", "adapter": "static"}]
