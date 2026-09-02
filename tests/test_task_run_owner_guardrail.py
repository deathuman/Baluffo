from __future__ import annotations

import ast
import inspect
from pathlib import Path

from tools.repo_health import repo_guardrails
from tools.repo_health.task_run_owner_policy import (
    ROOT,
    START_RUN_CALLEE_NAMES,
    check_task_run_start_owner_fields,
    find_ownerless_start_run_calls,
)


def _scan(tmp_path: Path, source: str, filename: str = "src/bridge/sample.py") -> list[str]:
    module_path = tmp_path / filename
    module_path.parent.mkdir(parents=True, exist_ok=True)
    module_path.write_text(source, encoding="utf-8")
    return find_ownerless_start_run_calls(source, filename)


def test_start_run_without_owner_fields_fails(tmp_path: Path) -> None:
    failures = _scan(
        tmp_path,
        "def launch(run_id: str) -> None:\n"
        "    lifecycle.start_run(\n"
        "        run_id=run_id,\n"
        "        task_type='sync',\n"
        "        started_at='2026-09-02T00:00:00+00:00',\n"
        "    )\n",
    )

    assert len(failures) == 1
    assert "src/bridge/sample.py:2" in failures[0]
    assert "owner_kind/owner_pid" in failures[0]


def test_start_run_with_owner_kind_passes(tmp_path: Path) -> None:
    failures = _scan(
        tmp_path,
        "def launch(run_id: str, pid: int) -> None:\n"
        "    lifecycle.start_run(\n"
        "        run_id=run_id,\n"
        "        task_type='sync',\n"
        "        owner_kind='process',\n"
        "        owner_pid=pid,\n"
        "    )\n",
    )

    assert failures == []


def test_start_run_with_owner_pid_only_passes(tmp_path: Path) -> None:
    failures = _scan(
        tmp_path,
        "def launch(run_id: str, pid: int) -> None:\n"
        "    lifecycle.start_run(run_id=run_id, task_type='fetch', owner_pid=pid)\n",
    )

    assert failures == []


def test_start_lifecycle_run_without_owner_fields_fails(tmp_path: Path) -> None:
    failures = _scan(
        tmp_path,
        "def launch(run_id: str) -> None:\n"
        "    start_lifecycle_run(run_id=run_id, task_type='pipeline')\n",
    )

    assert len(failures) == 1
    assert "start_lifecycle_run" in failures[0]


def test_kwargs_forwarder_shim_is_allowed(tmp_path: Path) -> None:
    failures = _scan(
        tmp_path,
        "class Facade:\n"
        "    def start_run(self, **kwargs):\n"
        "        return self._get_service().start_run(**kwargs)\n",
    )

    assert failures == []


def test_terminal_and_heartbeat_writers_are_out_of_scope(tmp_path: Path) -> None:
    failures = _scan(
        tmp_path,
        "def finish(run_id: str) -> None:\n"
        "    lifecycle.finish_run(run_id, 'sync', finished_at='2026-09-02T01:00:00+00:00')\n"
        "def fail(run_id: str) -> None:\n"
        "    lifecycle.fail_run(run_id, 'sync', terminal_reason='failed')\n"
        "def cancel(run_id: str) -> None:\n"
        "    lifecycle.cancel_run(run_id, 'sync', terminal_reason='canceled')\n"
        "def orphan(run_id: str) -> None:\n"
        "    lifecycle.orphan_run(run_id, 'sync', terminal_reason='owner_inactive')\n"
        "def beat(run_id: str) -> None:\n"
        "    lifecycle.heartbeat_run(run_id, 'sync')\n",
    )

    assert failures == []


def test_other_namesakes_do_not_trigger(tmp_path: Path) -> None:
    failures = _scan(
        tmp_path,
        "def build(run_id: str) -> dict:\n"
        "    return dict(start_run=run_id)\n\n"
        "def unrelated(start_run=None) -> None:\n"
        "    return None\n",
    )

    assert failures == []


def test_real_repo_currently_has_no_ownerless_start_run_calls() -> None:
    failures = check_task_run_start_owner_fields()

    assert failures == []


def test_scanner_covers_the_real_start_run_call_surface() -> None:
    """The allowlist must stay narrow: the facade forwarder plus real owners."""
    surface: set[tuple[str, int]] = set()
    for path in (ROOT / "src").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                if node.func.attr in START_RUN_CALLEE_NAMES:
                    surface.add((path.relative_to(ROOT).as_posix(), node.lineno))

    assert surface, "scanner found no start_run call sites; pattern drifted"
    # The known forwarder shim must remain the only non-owner call in src/.
    assert ("src/bridge/admin_task_lifecycle.py", 153) in surface


def test_compat_group_includes_the_owner_fields_check() -> None:
    assert "compat" in repo_guardrails.GROUPS
    source = inspect.getsource(repo_guardrails.run_compat_group)
    assert "check_task_run_start_owner_fields" in source
