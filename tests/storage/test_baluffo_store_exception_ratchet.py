from __future__ import annotations

import pytest

from src.storage import BaluffoStore


def test_write_rolls_back_and_reraises_callback_exception(tmp_path) -> None:
    with BaluffoStore(tmp_path / "data") as store:

        def fail_after_insert(conn) -> None:  # noqa: ANN001
            conn.execute(
                """
                INSERT INTO storage_authority_modes(surface, mode, updated_at)
                VALUES ('unit-test-surface', 'json', '2026-06-18T00:00:00Z')
                """
            )
            raise RuntimeError("callback bug")

        with pytest.raises(RuntimeError, match="callback bug"):
            store.write(fail_after_insert)

        rows = store.execute_read(
            """
            SELECT surface FROM storage_authority_modes
            WHERE surface = 'unit-test-surface'
            """
        )

        assert rows == []
        assert store.health()["healthy"] is False
        assert store.health()["lastWriteError"] == "callback bug"


def test_write_rolls_back_and_reraises_base_exception(tmp_path) -> None:
    with BaluffoStore(tmp_path / "data") as store:

        def interrupt_after_insert(conn) -> None:  # noqa: ANN001
            conn.execute(
                """
                INSERT INTO storage_authority_modes(surface, mode, updated_at)
                VALUES ('unit-test-interrupt', 'json', '2026-06-18T00:00:00Z')
                """
            )
            raise KeyboardInterrupt

        with pytest.raises(KeyboardInterrupt):
            store.write(interrupt_after_insert)

        rows = store.execute_read(
            """
            SELECT surface FROM storage_authority_modes
            WHERE surface = 'unit-test-interrupt'
            """
        )

        assert rows == []
        assert store.health()["healthy"] is False
