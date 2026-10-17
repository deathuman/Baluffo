"""Markdown link guardrails: local targets must exist, except gitignored evidence.

`_out/` holds perf runs, closeouts and probe bodies that are gitignored by design,
so a link into it can never resolve in a fresh clone. Every clean checkout failed
the docs group on those links, which hid the checks that could actually run.
"""

from __future__ import annotations

from pathlib import Path

from tools.repo_health import repo_guardrails


def _minimal_docs_root(tmp_path: Path) -> Path:
    """The doc scan always reads README/CONTRIBUTING, so a fake root needs them."""
    docs = tmp_path / "docs"
    docs.mkdir()
    (tmp_path / "README.md").write_text("", encoding="utf-8")
    (tmp_path / "CONTRIBUTING.md").write_text("", encoding="utf-8")
    return docs


def test_markdown_links_ignore_gitignored_out_evidence(tmp_path: Path, monkeypatch) -> None:
    """`_out/` links name evidence that lives outside the repository.

    Real case: docs/plans/discovery-performance-follow-up-plan.md links three
    `../../_out/perf-runs/...` artifacts, and every clone without `_out` failed
    the docs group on links that were valid where the evidence lived.
    """
    docs = _minimal_docs_root(tmp_path)
    (docs / "plan.md").write_text(
        "[closeout](../../_out/perf-runs/discovery/p6-closeout.md)\n", encoding="utf-8"
    )
    monkeypatch.setattr(repo_guardrails, "ROOT", tmp_path)

    assert repo_guardrails.check_markdown_links() == []


def test_markdown_links_still_report_a_missing_doc_target(tmp_path: Path, monkeypatch) -> None:
    """The evidence exemption must not turn the link check into a no-op."""
    docs = _minimal_docs_root(tmp_path)
    (docs / "plan.md").write_text("[gone](missing.md)\n", encoding="utf-8")
    monkeypatch.setattr(repo_guardrails, "ROOT", tmp_path)

    missing = repo_guardrails.check_markdown_links()
    assert len(missing) == 1
    assert "missing.md" in missing[0]
