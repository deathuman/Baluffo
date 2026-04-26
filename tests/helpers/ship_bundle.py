import json
import shutil
from pathlib import Path

from scripts import build_ship_bundle


def copy_minimal_app_version(version_dir: Path) -> None:
    packaged_sync_config = build_ship_bundle._resolve_packaged_sync_config()
    desktop_update_repo = build_ship_bundle._resolve_desktop_update_repo()

    src_dir = version_dir / "src"
    ship_dir = src_dir / "ship"
    ship_dir.mkdir(parents=True, exist_ok=True)
    (version_dir / "packaging").mkdir(parents=True, exist_ok=True)
    for path, text in (
        (src_dir / "__init__.py", ""),
        (src_dir / "admin_bridge.py", "# test stub\n"),
        (ship_dir / "__init__.py", ""),
        (ship_dir / "runtime_launcher.py", "# test stub\n"),
    ):
        path.write_text(text, encoding="utf-8")
    if packaged_sync_config is not None:
        shutil.copy2(
            packaged_sync_config, version_dir / "packaging" / "github-app-sync-config.json"
        )
    if desktop_update_repo:
        (version_dir / "packaging" / build_ship_bundle.DESKTOP_UPDATE_CONFIG_FILE).write_text(
            json.dumps({"repo": desktop_update_repo}, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
