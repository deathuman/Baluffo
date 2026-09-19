"""Shared helpers for the ``tools/repo_health`` AST inventory tools.

The bridge-API, desktop-update, updater and update-manager inventory tools all
parse Python sources, walk the repo's Python paths, print a JSON inventory, and
project their row dataclasses to JSON. Those helpers were byte-identical copies
before this module existed; they live here now so the tools keep only their own
scanning logic.

Import style: the tools are imported both ways in this repo — ``repo_guardrails``
adds this directory to ``sys.path`` and imports them flat, while tests import them
as ``tools.repo_health.<tool>``. Each tool therefore tries the qualified import
first and falls back to the flat one, so one module identity serves both.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Any


def parse_python(path: Path) -> ast.Module:
    """Parse one Python file, attributing syntax errors to its real path."""
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def iter_python_paths(repo_root: Path) -> list[Path]:
    """Every ``*.py`` under the scanned source roots, sorted and deduplicated."""
    roots = (
        repo_root / "src",
        repo_root / "tests",
        repo_root / "tools",
        repo_root / "scripts",
    )
    paths: list[Path] = []
    for root in roots:
        if root.exists():
            paths.extend(sorted(root.rglob("*.py")))
    return sorted(paths)


def relative_posix(path: Path, repo_root: Path) -> str:
    """Repo-relative posix path, so reports read the same on every platform."""
    return path.relative_to(repo_root).as_posix()


def print_inventory(inventory: tuple[Any, ...]) -> None:
    """Print a row inventory as the stable sorted-key JSON contract."""
    print(json.dumps([row.as_json() for row in inventory], indent=2, sort_keys=True))


class NameCategoriesReferencesRow:
    """JSON projection shared by rows keyed by name/categories/references.

    Mixin only: the concrete dataclasses keep their own fields, so their
    constructor, equality and hash behaviour is unchanged.
    """

    def as_json(self) -> dict[str, object]:
        return {
            "name": self.name,
            "categories": list(self.categories),
            "references": list(self.references),
        }


class PathModuleLineCategoriesRow:
    """JSON projection shared by rows keyed by path/module/line/categories."""

    def as_json(self) -> dict[str, object]:
        return {
            "path": self.path,
            "module": self.module,
            "line": self.line,
            "categories": list(self.categories),
        }
