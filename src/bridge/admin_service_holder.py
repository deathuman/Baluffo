from __future__ import annotations

import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class BridgeServices:
    sync_service: Any | None = None
    sync_service_data_dir: Path | None = None
    sync_config: Any | None = None
    sync_service_lock: Any = field(default_factory=threading.RLock)
    registry_service: Any | None = None
    registry_service_paths: tuple[Path, Path, Path] | None = None
    registry_service_lock: Any = field(default_factory=threading.RLock)
    discovery_service: Any | None = None
    discovery_service_paths: tuple[Path, Path, Path, Path] | None = None
    discovery_service_lock: Any = field(default_factory=threading.RLock)
    pipeline_service: Any | None = None
    pipeline_service_lock: Any = field(default_factory=threading.RLock)
    desktop_update_service: Any | None = None
    desktop_update_service_data_dir: Path | None = None
    desktop_update_service_lock: Any = field(default_factory=threading.RLock)

    def reset_sync_service(self) -> None:
        self.sync_service = None
        self.sync_service_data_dir = None
        self.sync_config = None

    def reset_desktop_update_service(self) -> None:
        self.desktop_update_service = None
        self.desktop_update_service_data_dir = None

    def reset_registry_service(self) -> None:
        self.registry_service = None
        self.registry_service_paths = None

    def reset_discovery_service(self) -> None:
        self.discovery_service = None
        self.discovery_service_paths = None

    def reset_pipeline_service(self) -> None:
        self.pipeline_service = None
