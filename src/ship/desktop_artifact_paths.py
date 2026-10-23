"""Resolve the artifact a desktop build installs updates from.

A Windows portable build is a directory tree replaced from a ZIP; a Linux
AppImage is a single file replaced in place. Everything that differs between
the two -- the updater helper's name, the install target, how to relaunch, and
which signed manifest describes the release -- comes from one descriptor here
so the service and the helper cannot disagree about it.

Side effects: reads the environment and filesystem metadata only.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path

from src.ship import desktop_update_constants as constants_mod
from src.ship import desktop_update_manifest as manifest_mod

ARTIFACT_KIND_PORTABLE_DIR = "portable_dir"
ARTIFACT_KIND_APPIMAGE = "appimage"

APPIMAGE_PATH_ENV = "APPIMAGE"
APPIMAGE_ARGV0_ENV = "ARGV0"
APPIMAGE_PATH_OVERRIDE_ENV = "BALUFFO_APPIMAGE_PATH"
APPIMAGE_EXTRACT_AND_RUN_ENV = "APPIMAGE_EXTRACT_AND_RUN"
APPIMAGE_EXTRACT_AND_RUN_ARG = "--appimage-extract-and-run"

_APPIMAGE_SUFFIXES = (".appimage",)
_EXTRACTED_APPIMAGE_MARKER = "appimage_extracted"
_TRUTHY = frozenset({"1", "true", "yes", "on"})


@dataclass(frozen=True)
class DesktopArtifact:
    """What this build updates, and how."""

    kind: str
    payload_root: Path
    helper_name: str
    manifest_asset: str
    replace_target: Path | None = None
    relaunch_args: tuple[str, ...] = field(default_factory=tuple)
    install_supported: bool = True

    @property
    def is_appimage(self) -> bool:
        return self.kind == ARTIFACT_KIND_APPIMAGE


def _env_map(env: Mapping[str, str] | None) -> Mapping[str, str]:
    return os.environ if env is None else env


def _payload_root() -> Path:
    if bool(getattr(sys, "frozen", False)):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[2]


def _appimage_path(raw: object) -> Path | None:
    text = str(raw or "").strip()
    if not text.lower().endswith(_APPIMAGE_SUFFIXES):
        return None
    path = Path(text).expanduser()
    try:
        if not path.is_file():
            return None
        return path.resolve()
    except OSError:
        return None


def resolve_appimage_path(
    *,
    env: Mapping[str, str] | None = None,
    argv: Sequence[str] | None = None,
) -> Path | None:
    """The AppImage file the user launched, or None when this is not one.

    The AppImage runtime exports `APPIMAGE` and `ARGV0`; the explicit override
    exists for rehearsal runs, where the payload is executed from an extraction
    directory and no runtime owns the launch.
    """
    env_map = _env_map(env)
    for name in (APPIMAGE_PATH_OVERRIDE_ENV, APPIMAGE_PATH_ENV, APPIMAGE_ARGV0_ENV):
        resolved = _appimage_path(env_map.get(name))
        if resolved is not None:
            return resolved
    tokens = list(argv if argv is not None else sys.argv)
    return _appimage_path(tokens[0]) if tokens else None


def running_from_extracted_appimage(*, env: Mapping[str, str] | None = None) -> bool:
    """Whether the payload runs from a temporary extraction, not a FUSE mount.

    `--appimage-extract-and-run` is consumed by the runtime, so the flag itself
    never reaches the app; the runtime's environment variable or the extraction
    directory in the running executable path both identify the mode.
    """
    env_map = _env_map(env)
    flag = str(env_map.get(APPIMAGE_EXTRACT_AND_RUN_ENV) or "").strip().lower()
    if flag in _TRUTHY:
        return True
    executable = str(sys.executable or "").replace("\\", "/").lower()
    return _EXTRACTED_APPIMAGE_MARKER in executable


def desktop_artifact(
    *,
    env: Mapping[str, str] | None = None,
    argv: Sequence[str] | None = None,
) -> DesktopArtifact:
    """Describe this build's update artifact.

    A source checkout ships no helper either way, so it keeps the portable
    directory contract on every platform: the update machinery has to stay
    usable and testable wherever the suite runs. Only a frozen build can say it
    has no install path, and only a frozen Linux build launched as an AppImage
    has one.
    """
    payload_root = _payload_root()
    if os.name == "nt" or not bool(getattr(sys, "frozen", False)):
        return DesktopArtifact(
            kind=ARTIFACT_KIND_PORTABLE_DIR,
            payload_root=payload_root,
            helper_name=constants_mod.DESKTOP_UPDATE_HELPER_NAME,
            manifest_asset=manifest_mod.DESKTOP_UPDATE_MANIFEST_ASSET,
        )
    target = resolve_appimage_path(env=env, argv=argv)
    if target is None:
        return DesktopArtifact(
            kind=ARTIFACT_KIND_PORTABLE_DIR,
            payload_root=payload_root,
            helper_name=constants_mod.DESKTOP_UPDATE_HELPER_NAME,
            manifest_asset=manifest_mod.DESKTOP_UPDATE_MANIFEST_ASSET_LINUX,
            install_supported=False,
        )
    return DesktopArtifact(
        kind=ARTIFACT_KIND_APPIMAGE,
        payload_root=payload_root,
        helper_name=constants_mod.DESKTOP_UPDATE_HELPER_NAME_LINUX,
        manifest_asset=manifest_mod.DESKTOP_UPDATE_MANIFEST_ASSET_LINUX,
        replace_target=target,
        relaunch_args=(
            (APPIMAGE_EXTRACT_AND_RUN_ARG,) if running_from_extracted_appimage(env=env) else ()
        ),
    )


__all__ = [
    "APPIMAGE_EXTRACT_AND_RUN_ARG",
    "APPIMAGE_EXTRACT_AND_RUN_ENV",
    "APPIMAGE_PATH_ENV",
    "APPIMAGE_PATH_OVERRIDE_ENV",
    "ARTIFACT_KIND_APPIMAGE",
    "ARTIFACT_KIND_PORTABLE_DIR",
    "DesktopArtifact",
    "desktop_artifact",
    "resolve_appimage_path",
    "running_from_extracted_appimage",
]
