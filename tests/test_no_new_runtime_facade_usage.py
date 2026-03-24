from __future__ import annotations

from pathlib import Path


def test_no_new_runtime_facade_usage_outside_runtime_module() -> None:
    """
    Guardrail: `_runtime.facade()` is legacy/compat-only and should not spread.
    """

    repo_root = Path(__file__).resolve().parents[1]
    jobs_root = repo_root / "src" / "jobs"
    allowed = {
        (jobs_root / "adapters" / "_runtime.py").resolve(),
    }

    offenders: list[str] = []
    for path in jobs_root.rglob("*.py"):
        resolved = path.resolve()
        if resolved in allowed:
            continue
        text = path.read_text(encoding="utf-8")
        if "_runtime.facade(" in text:
            offenders.append(str(path.relative_to(repo_root)))

    assert not offenders, (
        "Found new `_runtime.facade()` usage outside allowed module:\n- " + "\n- ".join(offenders)
    )
