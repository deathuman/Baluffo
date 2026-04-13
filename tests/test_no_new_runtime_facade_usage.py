from __future__ import annotations

from pathlib import Path


def test_no_new_runtime_facade_usage_outside_runtime_module() -> None:
    """
    Guardrail: `_runtime.facade()` is retired and `_runtime.py` should not return.
    """

    repo_root = Path(__file__).resolve().parents[1]
    jobs_root = repo_root / "src" / "jobs"
    runtime_module = jobs_root / "adapters" / "_runtime.py"
    assert not runtime_module.exists()

    offenders: list[str] = []
    for path in jobs_root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        if "_runtime.facade(" in text:
            offenders.append(str(path.relative_to(repo_root)))

    assert not offenders, "Found retired `_runtime.facade()` usage:\n- " + "\n- ".join(offenders)
