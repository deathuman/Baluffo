from __future__ import annotations

"""Path constants and layout model for the ship update manager."""

import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

UPDATER_VERSION = "1.0.0"
STATE_NAME = "update-state.json"
CURRENT_NAME = "current.txt"
LOG_NAME = "update-events.jsonl"
REQUIRED_VERSION_FILES = ("src/admin_bridge.py", "index.html", "jobs.html", "saved.html")
BOOTSTRAP_DIR_NAME = "runtime-bootstrap"
BOOTSTRAP_ROOT_HTML = ("index.html", "jobs.html", "saved.html")
BOOTSTRAP_VERSION_TAG = ".canonical-version"


@dataclass(frozen=True)
class ShipPaths:
    root: Path
    app: Path
    versions: Path
    staging: Path
    state: Path
    current: Path
    data: Path
    backups: Path
    migration_reports: Path
    logs: Path

    @staticmethod
    def from_root(root: Path) -> ShipPaths:
        app = root / "app"
        data = root / "data"
        return ShipPaths(
            root=root,
            app=app,
            versions=app / "versions",
            staging=app / "staging",
            state=app / STATE_NAME,
            current=app / CURRENT_NAME,
            data=data,
            backups=data / "backups",
            migration_reports=data / "migration-reports",
            logs=root / "logs",
        )
