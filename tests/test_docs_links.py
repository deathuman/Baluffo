import re
from pathlib import Path

MARKDOWN_LINK_RE = re.compile(r"(?<!\!)\[[^\]]+\]\(([^)]+)\)")


def _iter_doc_paths(repo_root: Path) -> list[Path]:
    docs = sorted((repo_root / "docs").rglob("*.md"))
    tools = sorted((repo_root / "tools").rglob("*.md"))
    return [repo_root / "README.md", repo_root / "CONTRIBUTING.md", *docs, *tools]


def _local_markdown_target(raw: str) -> str | None:
    target = raw.strip()
    if target.startswith("<") and target.endswith(">"):
        target = target[1:-1]
    if not target or target.startswith("#"):
        return None
    if target.startswith(("http://", "https://", "mailto:")):
        return None
    return target.split("#", 1)[0]


def test_repo_docs_markdown_links_resolve(repo_root: Path) -> None:
    missing: list[str] = []

    for doc_path in _iter_doc_paths(repo_root):
        text = doc_path.read_text(encoding="utf-8")
        for raw_target in MARKDOWN_LINK_RE.findall(text):
            target = _local_markdown_target(raw_target)
            if target is None:
                continue
            resolved = (doc_path.parent / target).resolve()
            if not resolved.exists():
                missing.append(f"{doc_path.relative_to(repo_root)} -> {target}")

    assert not missing, "Broken local markdown links:\n" + "\n".join(sorted(missing))
