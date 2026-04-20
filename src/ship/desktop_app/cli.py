from __future__ import annotations

from src.ship.desktop_app.launcher import main as _main


def main(argv: list[str] | None = None) -> int:
    return _main(argv)
