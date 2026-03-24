#!/usr/bin/env python3
"""Migration contracts used by the ship update manager."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class MigrationResult:
    name: str
    ok: bool
    detail: str = ""


class BaseMigration:
    name = "base"

    def apply(self, data_path: Path) -> MigrationResult:
        return MigrationResult(self.name, True, "noop")

    def verify(self, data_path: Path) -> MigrationResult:
        return MigrationResult(self.name, True, "verified")

    def rollback(self, data_path: Path, backup_ref: Path) -> MigrationResult:
        return MigrationResult(self.name, True, f"rollback via {backup_ref}")


class NoopMigration(BaseMigration):
    name = "noop"


MIGRATIONS: dict[str, BaseMigration] = {
    "noop": NoopMigration(),
}


def resolve_migrations(names: Iterable[str]) -> list[BaseMigration]:
    resolved: list[BaseMigration] = []
    for raw_name in names:
        name = str(raw_name or "").strip()
        if not name:
            continue
        migration = MIGRATIONS.get(name)
        if not migration:
            raise ValueError(f"Unknown migration: {name}")
        resolved.append(migration)
    return resolved
