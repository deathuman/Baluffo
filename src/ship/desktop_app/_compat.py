from __future__ import annotations

from types import ModuleType


def desktop_api() -> ModuleType:
    from src.ship import desktop_app as api

    return api
