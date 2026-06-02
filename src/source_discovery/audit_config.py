from __future__ import annotations

import os
from pathlib import Path
from typing import Any


def config_section(
    config: dict[str, Any] | None,
    section_name: str | None = None,
    *,
    defaults: dict[str, Any] | None = None,
    flat_fallback: bool = True,
) -> dict[str, Any]:
    resolved = dict(defaults or {})
    source = config if isinstance(config, dict) else {}
    if section_name is None:
        resolved.update(source)
        return resolved
    section = source.get(section_name)
    if isinstance(section, dict):
        resolved.update(section)
    elif flat_fallback:
        resolved.update(source)
    return resolved


def positive_int(value: Any, default: int, *, minimum: int = 0) -> int:
    if value in {"", None}:
        value = default
    try:
        return max(minimum, int(value))
    except (TypeError, ValueError):
        return max(minimum, int(default))


def _container_runtime_enabled() -> bool:
    return str(os.environ.get("BALUFFO_RUNTIME_MODE") or "").strip().lower() == "container"


def _container_data_dir() -> Path | None:
    raw = str(os.environ.get("BALUFFO_DATA_DIR") or "").strip()
    if not raw:
        return None
    return Path(raw).expanduser()


def _strip_leading_data_segment(path: Path) -> Path:
    parts = path.parts
    if len(parts) > 1 and str(parts[0]).lower() == "data":
        return Path(*parts[1:])
    return path


def _container_audit_path(path: Path) -> Path:
    if path.is_absolute() or not _container_runtime_enabled():
        return path
    data_dir = _container_data_dir()
    if data_dir is None:
        return path
    return data_dir / _strip_leading_data_segment(path)


def audit_artifact_path(
    config: dict[str, Any] | None,
    section_name: str | None = None,
    *,
    default_filename: str,
    defaults: dict[str, Any] | None = None,
    flat_fallback: bool = True,
) -> Path:
    cfg = config_section(
        config,
        section_name,
        defaults=defaults,
        flat_fallback=flat_fallback,
    )
    raw = str(cfg.get("activeAuditPath") or "").strip()
    if raw:
        return _container_audit_path(Path(raw))
    default_path = Path("data") / default_filename
    container_path = _container_audit_path(default_path)
    if container_path != default_path:
        return container_path
    return Path(__file__).resolve().parents[2] / "data" / default_filename


def audit_ttl_minutes(
    config: dict[str, Any] | None,
    section_name: str | None = None,
    *,
    default: int = 360,
    fallback_ttl: int | None = None,
    defaults: dict[str, Any] | None = None,
    flat_fallback: bool = True,
) -> int:
    cfg = config_section(
        config,
        section_name,
        defaults=defaults,
        flat_fallback=flat_fallback,
    )
    fallback = fallback_ttl if fallback_ttl is not None else default
    raw = cfg.get("activeAuditTtlMinutes", None)
    if raw in {"", None}:
        return positive_int(fallback, default)
    return positive_int(raw, fallback)


def int_config_value(
    config: dict[str, Any] | None,
    key: str,
    *,
    default: int,
    section_name: str | None = None,
    defaults: dict[str, Any] | None = None,
    flat_fallback: bool = True,
    minimum: int = 0,
) -> int:
    cfg = config_section(
        config,
        section_name,
        defaults=defaults,
        flat_fallback=flat_fallback,
    )
    return positive_int(cfg.get(key, default), default, minimum=minimum)
