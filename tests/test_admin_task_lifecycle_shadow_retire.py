"""Regression guard for retiring the taskRuns/taskEvents shadow dual-write.

The shadow authority mode used to read JSON and compare it against SQLite on
every request. SQLite is now the sole authority for these surfaces (seeded in
BaluffoStore.DEFAULT_AUTHORITY_MODES), so shadow must behave as pure SQLite
with no JSON read or projection comparison.
"""

from __future__ import annotations

from types import SimpleNamespace

from src.bridge.admin_task_lifecycle import AdminTaskLifecycle


def _make_lifecycle(mode: str, diagnostics: list[dict[str, object]]):
    fake_store = SimpleNamespace(
        store=SimpleNamespace(get_authority_modes=lambda: {"taskRuns": mode})
    )
    return (
        AdminTaskLifecycle(
            lifecycle_path=lambda: __import__("pathlib").Path("x"),
            max_rows=lambda: 100,
            lock=__import__("threading").RLock(),
            load_json_object=lambda *_a, **_k: {},
            save_json_atomic=lambda *_a, **_k: None,
            now_iso=lambda: "2026-01-01T00:00:00Z",
            parse_iso=lambda v: v,
            task_runtime_store=lambda: fake_store,
            record_storage_diagnostic=lambda **fields: diagnostics.append(fields),
        ),
        fake_store,
    )


def _read(mode: str, diagnostics: list[dict[str, object]]):
    lifecycle, _ = _make_lifecycle(mode, diagnostics)
    json_rows = [{"runId": "json-1", "taskType": "fetch"}]
    sqlite_rows = [{"runId": "sqlite-1", "taskType": "fetch"}]
    return lifecycle._read_task_rows(
        surface="taskRuns",
        json_rows=lambda: json_rows,
        sqlite_reader=lambda _store: sqlite_rows,
    )


def test_shadow_mode_reads_pure_sqlite_without_comparison():
    diagnostics: list[dict[str, object]] = []
    result = _read("shadow", diagnostics)

    assert result == [{"runId": "sqlite-1", "taskType": "fetch"}]
    assert not any(d.get("code") == "taskRuns_read_projection_mismatch" for d in diagnostics)


def test_sqlite_mode_reads_sqlite():
    assert _read("sqlite", []) == [{"runId": "sqlite-1", "taskType": "fetch"}]


def test_json_mode_reads_json_fallback():
    assert _read("json", []) == [{"runId": "json-1", "taskType": "fetch"}]


def test_missing_runtime_store_falls_back_to_json():
    lifecycle = AdminTaskLifecycle(
        lifecycle_path=lambda: __import__("pathlib").Path("x"),
        max_rows=lambda: 100,
        lock=__import__("threading").RLock(),
        load_json_object=lambda *_a, **_k: {},
        save_json_atomic=lambda *_a, **_k: None,
        now_iso=lambda: "2026-01-01T00:00:00Z",
        parse_iso=lambda v: v,
        task_runtime_store=lambda: None,
    )
    result = lifecycle._read_task_rows(
        surface="taskRuns",
        json_rows=lambda: [{"runId": "json-1"}],
        sqlite_reader=lambda _store: [{"runId": "sqlite-1"}],
    )
    assert result == [{"runId": "json-1"}]
