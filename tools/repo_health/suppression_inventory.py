from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

try:
    from tools.repo_health.repo_guardrails import ROOT, _suppression_codes
except ModuleNotFoundError:  # pragma: no cover - direct script execution path.
    from repo_guardrails import ROOT, _suppression_codes


@dataclass(frozen=True)
class SuppressionInventory:
    total: int
    by_code: Counter[str]
    by_file: Counter[str]
    by_code_file: Counter[tuple[str, str]]

    def as_json(self) -> dict[str, object]:
        return {
            "total": self.total,
            "byCode": dict(sorted(self.by_code.items())),
            "byFile": dict(sorted(self.by_file.items())),
            "byCodeFile": {
                f"{code} {path}": count for (code, path), count in sorted(self.by_code_file.items())
            },
        }


def collect_suppressions(root: Path = ROOT, *, scope: str = "src") -> SuppressionInventory:
    total = 0
    by_code: Counter[str] = Counter()
    by_file: Counter[str] = Counter()
    by_code_file: Counter[tuple[str, str]] = Counter()
    for path in sorted((root / scope).rglob("*.py")):
        rel_path = path.relative_to(root).as_posix()
        for line in path.read_text(encoding="utf-8").splitlines():
            codes = _suppression_codes(line)
            if not codes:
                continue
            total += 1
            by_file[rel_path] += 1
            for code in codes:
                by_code[code] += 1
                by_code_file[(code, rel_path)] += 1
    return SuppressionInventory(
        total=total,
        by_code=by_code,
        by_file=by_file,
        by_code_file=by_code_file,
    )


def _print_text(inventory: SuppressionInventory, *, top: int) -> None:
    print(f"total {inventory.total}")
    print("\nby code")
    for code, count in inventory.by_code.most_common():
        print(f"{code:16} {count}")
    print("\nby file")
    for path, count in inventory.by_file.most_common(top):
        print(f"{count:4} {path}")
    print("\nby code/file")
    for (code, path), count in inventory.by_code_file.most_common(top):
        print(f"{code:16} {count:4} {path}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Measure inline suppressions in source files.")
    parser.add_argument("--scope", default="src", help="Repo-relative directory to scan.")
    parser.add_argument("--top", type=int, default=30, help="Number of hotspot rows to print.")
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON.")
    args = parser.parse_args(argv)

    inventory = collect_suppressions(scope=str(args.scope))
    if args.json:
        print(json.dumps(inventory.as_json(), indent=2, sort_keys=True))
    else:
        _print_text(inventory, top=max(0, int(args.top)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
