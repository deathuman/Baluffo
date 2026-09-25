from __future__ import annotations

from pathlib import Path

DISCOVERY_AUDIT_ARTIFACT_NAMES = (
    "gameprog-discovery-audit.json",
    "gamesmap-discovery-audit.json",
    "web-search-discovery-audit.json",
    "sheet-directory-discovery-audit.json",
    "gamedevmap-active-source-dry-run.json",
)


def _signature(path: Path) -> tuple[bool, int, int]:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return (False, 0, 0)
    return (True, stat.st_size, stat.st_mtime_ns)


def snapshot_discovery_audit_artifacts(repo_root: Path) -> dict[str, tuple[bool, int, int]]:
    data_dir = repo_root / "data"
    return {name: _signature(data_dir / name) for name in DISCOVERY_AUDIT_ARTIFACT_NAMES}


def changed_discovery_audit_artifacts(
    repo_root: Path,
    before: dict[str, tuple[bool, int, int]],
) -> list[str]:
    after = snapshot_discovery_audit_artifacts(repo_root)
    return [name for name in DISCOVERY_AUDIT_ARTIFACT_NAMES if before.get(name) != after.get(name)]


def format_discovery_audit_change(
    repo_root: Path,
    names: list[str],
) -> str:
    details: list[str] = []
    for name in names:
        path = repo_root / "data" / name
        details.append(f"- {path}: {snapshot_discovery_audit_artifacts(repo_root).get(name)}")
    return "\n".join(
        [
            "Tests changed discovery audit artifacts under the repository data directory:",
            *details,
            "Pin activeAuditPath to a repo-local test root; do not write real runtime audits during tests.",
        ]
    )


__all__: list[str] = [
    "DISCOVERY_AUDIT_ARTIFACT_NAMES",
    "changed_discovery_audit_artifacts",
    "format_discovery_audit_change",
    "snapshot_discovery_audit_artifacts",
]
