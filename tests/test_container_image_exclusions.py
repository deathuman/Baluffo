"""Every tracked file the container image excludes must be explicitly classified.

The three `data/` audit reports were gated as shipped container code while
`.dockerignore` kept them out of the image, so editing one burned a version and
republished a tag whose content had not changed. This file owns that invariant;
the window/version logic stays in test_container_version_policy.py.
"""

from __future__ import annotations

import fnmatch
import pathlib
import subprocess

from tools.repo_health.container_version_policy import (
    IMAGE_EXCLUDED_BUT_SHIPPED,
    _is_shipped_path,
)

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _tracked_paths_under_data() -> list[str]:
    out = subprocess.run(
        ["git", "ls-files", "data/"], capture_output=True, text=True, check=True, cwd=str(ROOT)
    )
    return [line for line in out.stdout.splitlines() if line.strip()]


def _image_included_data_paths() -> set[str]:
    """Replay .dockerignore's `data/*` plus its `!data/...` re-inclusions."""
    included: set[str] = set()
    for raw in (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line == "data/*":
            included = set()
        elif line.startswith("!data/"):
            pattern = line[1:]
            included |= {
                tracked
                for tracked in _tracked_paths_under_data()
                if fnmatch.fnmatch(tracked, pattern)
            }
    return included


def test_every_image_excluded_data_file_is_classified() -> None:
    """No tracked file may sit outside both the shipped and non-shipped buckets.

    The three `data/` audit reports were gated as shipped while `.dockerignore`
    kept them out of the image, so editing one burned a version and republished a
    tag with no content change. The two runtime data files are genuinely shipped
    and are listed in IMAGE_EXCLUDED_BUT_SHIPPED. Anything else excluded from the
    image has to be non-shipped.
    """
    in_image = _image_included_data_paths()
    excluded = [p for p in _tracked_paths_under_data() if p not in in_image]
    assert excluded, "expected some tracked data/ files to be excluded from the image"

    unclassified = [
        path
        for path in excluded
        if _is_shipped_path(path) and path not in IMAGE_EXCLUDED_BUT_SHIPPED
    ]
    assert not unclassified, (
        "tracked files are excluded from the image but gated as shipped without being "
        f"declared in IMAGE_EXCLUDED_BUT_SHIPPED: {unclassified}"
    )


def test_image_excluded_but_shipped_entries_are_still_valid() -> None:
    """The allowlist cannot rot: each entry must stay tracked, excluded, and shipped."""
    in_image = _image_included_data_paths()
    tracked = set(_tracked_paths_under_data())
    for path in IMAGE_EXCLUDED_BUT_SHIPPED:
        assert path in tracked, f"{path} is listed but no longer tracked"
        assert path not in in_image, f"{path} is listed but is now in the image"
        assert _is_shipped_path(path), f"{path} is listed but is no longer gated shipped"
