from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.admin._helpers import (
    cleanup_admin_bridge_entrypoint_root,
    configure_admin_bridge_entrypoint_root,
)


@pytest.fixture()
def admin_bridge_entrypoint_root(make_test_root, monkeypatch) -> Iterator[Path]:
    """Entry-point level admin_bridge fixture for module/singleton patch tests."""
    root = make_test_root("admin-bridge")
    yield configure_admin_bridge_entrypoint_root(monkeypatch, root)
    cleanup_admin_bridge_entrypoint_root(monkeypatch)
