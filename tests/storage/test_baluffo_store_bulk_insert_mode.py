from __future__ import annotations

import sqlite3

from src.storage import BaluffoStore
from tests.helpers.temp_paths import workspace_tmpdir


def test_bulk_insert_mode_disables_wal_autocheckpoint_and_restores_it() -> None:
    with workspace_tmpdir("baluffo-store-bulk-insert") as data_dir:
        with BaluffoStore(data_dir) as store:

            def auto_checkpoint(conn: sqlite3.Connection) -> int:
                return int(conn.execute("PRAGMA wal_autocheckpoint").fetchone()[0])

            before = store.write(auto_checkpoint)
            assert before > 0
            store.write(
                lambda conn: conn.execute(
                    "CREATE TABLE bulk_insert_probe (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)"
                )
            )

            with store.bulk_insert_mode():
                assert store.write(auto_checkpoint) == 0
                store.write(
                    lambda conn: conn.executemany(
                        "INSERT INTO bulk_insert_probe(id, payload) VALUES (?, ?)",
                        [(index, "x" * 256) for index in range(2_000)],
                    )
                )
                assert store.file_sizes()["walBytes"] > 0

            assert store.write(auto_checkpoint) == before
            assert store.file_sizes()["walBytes"] == 0
