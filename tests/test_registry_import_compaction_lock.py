"""Regression pin for the import-lock startup stall.

The bridge lazily imports route modules on a GET. That import reaches
``src.jobs.registry``, whose module body loads the source registry through
``BaluffoStore``. When that transient store scheduled a compaction, ``close()``
joined the rewrite while the importing thread was still executing the module, so
every request whose import chain touched it waited on that module's import lock
-- ``/ops/health`` included.

Both tests park a compaction thread inside ``vacuum``. The first proves the
module-level open/close does not wait for it. The second proves a health GET
whose import path reaches the module still answers while that import is running.
"""

from __future__ import annotations

import importlib
import itertools
import sys
import threading
import time
from pathlib import Path
from types import ModuleType

import pytest

from src.storage import BaluffoStore
from tests.helpers.temp_paths import workspace_tmpdir

_SEQUENCE = itertools.count(1)


def _names() -> tuple[str, str, str]:
    token = next(_SEQUENCE)
    return (
        f"compaction_registry_probe_{token}",
        f"compaction_health_probe_{token}",
        f"compaction_probe_state_{token}",
    )


def _start_blocked_compaction(
    monkeypatch: pytest.MonkeyPatch,
    data_dir: Path,
    entered: threading.Event,
    release: threading.Event,
) -> BaluffoStore:
    """Open a deferred store and park its compaction thread inside ``vacuum``."""

    def blocking_vacuum(self: BaluffoStore, *, reason: str = "") -> dict[str, object]:
        entered.set()
        release.wait(timeout=20.0)
        return {"status": "ok", "reason": reason}

    monkeypatch.setattr(BaluffoStore, "_compaction_needed", lambda self, sizes: True)
    monkeypatch.setattr(BaluffoStore, "vacuum", blocking_vacuum)
    store = BaluffoStore(data_dir, defer_maintenance=True)
    store.vacuum_idle_s = 0.0
    assert store.resume_maintenance_after_serving(reason="test-serve", grace_s=0.0) is True
    assert entered.wait(timeout=5.0), "compaction thread never reached vacuum"
    return store


def _force_open_write(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make a later store's open commit a write, as migrations can on real data.

    The pre-fix stall needs the open to reach the write path, which is what
    schedules the compaction the close then waits for.
    """
    original_run_migrations = BaluffoStore.run_migrations

    def run_migrations_with_open_write(self: BaluffoStore) -> list[str]:
        applied = original_run_migrations(self)
        self.write(lambda conn: None, operation="test-open-write")
        return applied

    monkeypatch.setattr(BaluffoStore, "run_migrations", run_migrations_with_open_write)


def _write_registry_probe(root: Path, module: str, state_module: str) -> None:
    (root / f"{module}.py").write_text(
        "from pathlib import Path\n"
        "\n"
        f"import {state_module} as state\n"
        "from src.jobs.common.sources import _load_registry_from_sqlite_authority\n"
        "\n"
        "state.registry_started = True\n"
        "_load_registry_from_sqlite_authority(Path(state.registry_path))\n"
        "state.registry_finished = True\n",
        encoding="utf-8",
    )


def _write_health_probe(root: Path, module: str, state_module: str, registry_module: str) -> None:
    """A GET that imports the module whose body is loading the registry."""
    (root / f"{module}.py").write_text(
        f"import {registry_module}\n"
        f"import {state_module} as state\n"
        "from src.bridge.routes.get_ops_status import handle_ops_status_routes\n"
        "from tests.helpers.bridge_api import FakeHandler\n"
        "\n"
        "handler = FakeHandler()\n"
        "state.handled = handle_ops_status_routes(\n"
        '    handler, api=state.api, path="/ops/health", query={}\n'
        ")\n"
        'state.status = handler.sent[-1]["status"]\n'
        'state.payload = handler.sent[-1]["payload"]\n'
        "state.health_finished = True\n",
        encoding="utf-8",
    )


def _register_state(state_module: str, registry_path: Path) -> ModuleType:
    state = ModuleType(state_module)
    state.registry_path = str(registry_path)  # type: ignore[attr-defined]
    state.registry_started = False  # type: ignore[attr-defined]
    state.registry_finished = False  # type: ignore[attr-defined]
    state.health_finished = False  # type: ignore[attr-defined]
    sys.modules[state_module] = state
    return state


def _clear_probes(root: Path, *module_names: str) -> None:
    for name in module_names:
        sys.modules.pop(name, None)
    if str(root) in sys.path:
        sys.path.remove(str(root))


def test_module_level_store_close_does_not_wait_for_running_compaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = threading.Event()
    release = threading.Event()
    registry_module, health_module, state_module = _names()
    with workspace_tmpdir("registry-import-compaction") as tmp:
        root = Path(tmp)
        store = _start_blocked_compaction(monkeypatch, root, entered, release)
        _force_open_write(monkeypatch)
        state = _register_state(state_module, root / "source-registry-active.json")
        _write_registry_probe(root, registry_module, state_module)
        sys.path.insert(0, str(root))
        worker = threading.Thread(
            target=importlib.import_module, args=(registry_module,), daemon=True
        )
        try:
            worker.start()
            # The bounded join is the assertion: a join timeout means the module
            # import never returned while the compaction was still running.
            worker.join(timeout=4.0)
            assert not worker.is_alive(), (
                "the module-level store close waited for the blocked compaction, "
                "which holds the import lock for the whole rewrite"
            )
            assert state.registry_finished is True
            assert entered.is_set(), "the compaction should still be running, not joined"
        finally:
            release.set()
            worker.join(timeout=10.0)
            _clear_probes(root, registry_module, health_module, state_module)
            store.close()


def test_health_get_answers_while_registry_import_holds_the_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered = threading.Event()
    release = threading.Event()
    registry_module, health_module, state_module = _names()
    with workspace_tmpdir("health-during-compaction") as tmp:
        root = Path(tmp)
        store = _start_blocked_compaction(monkeypatch, root, entered, release)
        _force_open_write(monkeypatch)
        state = _register_state(state_module, root / "source-registry-active.json")

        class _HealthApi:
            def compute_ops_health(self) -> dict[str, object]:
                health = store.health()
                return {
                    "ok": True,
                    "startupReady": True,
                    "storeHealthy": bool(health.get("healthy")),
                }

        state.api = _HealthApi()  # type: ignore[attr-defined]
        _write_registry_probe(root, registry_module, state_module)
        _write_health_probe(root, health_module, state_module, registry_module)
        sys.path.insert(0, str(root))
        registry_thread = threading.Thread(
            target=importlib.import_module, args=(registry_module,), daemon=True
        )
        health_thread = threading.Thread(
            target=importlib.import_module, args=(health_module,), daemon=True
        )
        try:
            registry_thread.start()
            deadline = time.monotonic() + 2.0
            while not state.registry_started and time.monotonic() < deadline:
                time.sleep(0.01)
            assert state.registry_started is True

            health_thread.start()
            health_thread.join(timeout=2.0)
            assert not health_thread.is_alive(), (
                "the health GET's import blocked behind the registry import while it "
                "waited for the compaction"
            )
            assert state.health_finished is True
            assert state.status == 200
            payload = state.payload
            assert payload["startupReady"] is True
            assert payload["storeHealthy"] is True
        finally:
            release.set()
            registry_thread.join(timeout=10.0)
            health_thread.join(timeout=10.0)
            _clear_probes(root, registry_module, health_module, state_module)
            store.close()
