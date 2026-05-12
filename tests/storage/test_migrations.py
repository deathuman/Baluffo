from __future__ import annotations

from src.storage import BaluffoStore
from tests.helpers.temp_paths import workspace_tmpdir


def test_migrations_are_idempotent_and_create_expected_tables() -> None:
    with workspace_tmpdir("baluffo-store-migrations") as data_dir:
        with BaluffoStore(data_dir) as store:
            first_applied = store.applied_migrations()
            second_applied_now = store.run_migrations()
            second_applied = store.applied_migrations()
            table_names = {
                str(row["name"])
                for row in store.execute_read(
                    "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
                )
            }

        assert first_applied == ["001", "002", "003", "004", "005"]
        assert second_applied_now == []
        assert second_applied == first_applied
        assert {
            "schema_migrations",
            "storage_authority_modes",
            "sources",
            "source_health",
            "source_runs",
            "task_runs",
            "task_events",
            "sync_runs",
            "jobs",
            "job_sources",
        }.issubset(table_names)
