#!/usr/bin/env python3
"""Ship-build step: bundle each desktop page into one esbuild asset.

The packaged site answers one HTTP request per module, and the jobs page's
module graph alone is ~115 files. `scripts/build_desktop_frontend.mjs` rewrites
the version folder's `admin.html`, `jobs.html` and `saved.html` to load a single
hashed asset each, the same way the container image bundles its pages. Node and
esbuild are build-time dependencies of this repo (the container build already
needs both), so a missing toolchain is a failed build rather than a silently
unbundled ship.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
BUNDLER_SCRIPT = ROOT / "scripts" / "build_desktop_frontend.mjs"
BUNDLED_PAGES = ("admin.html", "jobs.html", "saved.html")


def resolve_node_command() -> list[str]:
    node = shutil.which("node.exe") or shutil.which("node")
    return [node or "node"]


def bundle_desktop_pages(version_dir: Path) -> dict[str, Any]:
    """Rewrite the version folder's pages to load one bundled asset each."""
    version_root = Path(version_dir).expanduser().resolve()
    missing = [name for name in BUNDLED_PAGES if not (version_root / name).is_file()]
    if missing:
        raise RuntimeError(f"Desktop frontend bundle needs {', '.join(missing)} in {version_root}")
    result = subprocess.run(
        [*resolve_node_command(), str(BUNDLER_SCRIPT), "--root", str(version_root)],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        details = "\n".join(
            part for part in (result.stdout.strip(), result.stderr.strip()) if part
        ).strip()
        raise RuntimeError(
            "Desktop frontend bundle failed"
            f" (exit code {result.returncode}); run `npm ci` if esbuild is missing."
            f"\n{details}"
        )
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    try:
        payload = json.loads(lines[-1] if lines else "")
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Desktop frontend bundle printed no metadata: {result.stdout.strip()!r}"
        ) from exc
    pages = payload.get("pages")
    if not isinstance(pages, list) or not pages:
        raise RuntimeError(f"Desktop frontend bundle reported no pages: {payload!r}")
    validated: list[dict[str, Any]] = []
    for page in pages:
        if not isinstance(page, dict):
            raise RuntimeError(f"Desktop frontend bundle reported an invalid page: {page!r}")
        asset = str(page.get("asset") or "")
        if not asset or not (version_root / asset).is_file():
            raise RuntimeError(f"Desktop frontend bundle reported a missing asset: {asset!r}")
        # Splitting pages (Admin) also ship lazy chunks. They are not referenced
        # from the HTML, so a chunk that never reached the version tree would
        # only fail at runtime, after the shell was already interactive.
        chunks = page.get("chunks") or []
        if not isinstance(chunks, list):
            raise RuntimeError(f"Desktop frontend bundle reported invalid chunks: {chunks!r}")
        for chunk in chunks:
            chunk_asset = str((chunk or {}).get("asset") or "") if isinstance(chunk, dict) else ""
            if not chunk_asset or not (version_root / chunk_asset).is_file():
                raise RuntimeError(
                    f"Desktop frontend bundle reported a missing chunk: {chunk_asset!r}"
                )
        validated.append(page)
    return {"pages": validated}


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Bundle the desktop pages into one asset each.")
    parser.add_argument("version_dir", help="Ship version directory containing the pages.")
    args = parser.parse_args()
    payload = bundle_desktop_pages(Path(args.version_dir))
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
