#!/usr/bin/env python3
"""Verify per-definition byte fidelity of a module split against git HEAD.

When a large module is split into a thin coordinator plus sibling leaves, the
precondition is that every function/class/constant body moves over unchanged.
This tool fingerprints every top-level unit (def / class / constant assignment)
of the original module and checks that each one exists, exactly once, with the
same byte content, across the split leaves.

The original module is read from `git show HEAD:<path>` by default, so there is
no snapshot file to maintain. Pass `--snapshot FILE` when the original lives
only in the working tree (e.g. it was itself produced by an earlier, uncommitted
split) — the snapshot is a plain text copy of the pre-split file.

Typical split workflow:

    # 1. (optional) verify the working tree matches HEAD before splitting
    python tools/repo_health/bin/verify_split_fidelity.py src/bridge/foo.py

    # 2. after the split: coordinator + leaves vs the original
    python tools/repo_health/bin/verify_split_fidelity.py src/bridge/foo.py \
        --leaves src/bridge/foo.py src/bridge/foo_alpha.py src/bridge/foo_beta.py

Exit code is 0 when every original unit is present and byte-identical, and 1
otherwise. Bodies are compared with LF line endings and trailing blank lines
removed, so CRLF working trees and cosmetic end-of-file differences do not
produce false positives; everything else must match byte-for-byte.

AI boundary owns: split fidelity verification only; it does not check seam
observability, monkeypatch compatibility, or re-export surfaces — those remain
the responsibility of the split's own verification steps.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import re
import subprocess
from pathlib import Path
from typing import Any

# Top-level def/class, top-level constant assignment (plain or annotated), or
# decorator boundary. `^` with re.MULTILINE anchors to column 0, so nested
# defs are not matched. Decorator lines are boundaries owned by the following
# def/class, so a decorated unit's fingerprint includes its decorators and the
# preceding unit's slice stops cleanly at the `@`.
UNIT_RE = re.compile(
    r"^(?:def|class)\s+([A-Za-z_]\w*)"
    r"|^([A-Z_][A-Z0-9_]*)(?::[^=\n]+)?\s*="
    r"|^(@\S+)",
    re.MULTILINE,
)


def _normalize(text: str) -> str:
    return text.replace("\r\n", "\n").replace("\r", "\n")


def units(text: str) -> list[tuple[str, str, str]]:
    """Return (name, kind, body) for every top-level unit in `text`."""
    text = _normalize(text)
    matches = list(UNIT_RE.finditer(text))
    result: list[tuple[str, str, str]] = []
    for idx, m in enumerate(matches):
        def_name = m.group(1)
        const_name = m.group(2)
        if m.group(3) is not None:
            continue  # decorator boundary — owned by the next def/class
        name = def_name or const_name
        kind = "def/class" if def_name else "const"
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        start = m.start()
        prev = idx - 1
        while prev >= 0 and matches[prev].group(3) is not None:
            start = matches[prev].start()
            prev -= 1
        body = text[start:end].rstrip("\n")
        result.append((name, kind, body))
    return result


def _git_show_head(rel_path: str) -> str:
    completed = subprocess.run(
        ["git", "show", f"HEAD:{rel_path}"],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if completed.returncode != 0:
        raise SystemExit(
            f"error: `git show HEAD:{rel_path}` failed: {completed.stderr.strip()}\n"
            "The original must exist in git HEAD, or pass --snapshot FILE with a "
            "plain-text copy of the pre-split module."
        )
    return completed.stdout


def _read_ref(rel_path: str, snapshot: str | None) -> str:
    if snapshot is not None:
        return Path(snapshot).read_text(encoding="utf-8")
    return _git_show_head(rel_path)


def fingerprint(path: Path) -> dict[str, tuple[str, str]]:
    """Map unit name -> (sha256-16, body) for one leaf file."""
    text = path.read_text(encoding="utf-8")
    out: dict[str, tuple[str, str]] = {}
    for name, kind, body in units(text):
        if name in out:
            print(f"  WARN: duplicate top-level unit {name!r} in {path}")
        digest = hashlib.sha256(body.encode("utf-8")).hexdigest()[:16]
        out[name] = (digest, kind)
    return out


def _is_stub(sub: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    """True for a declaration-only body: ``...`` or a bare ``raise``.

    Mixin state bases declare the cross-mixin method surface with
    ``raise NotImplementedError`` (or ``...``) bodies so mypy can type ``self``
    in every leaf; these are not implementations and are skipped.
    """
    if len(sub.body) != 1:
        return False
    stmt = sub.body[0]
    if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Ellipsis):
        return True
    return isinstance(stmt, ast.Raise)


def class_method_bodies(text: str, class_name: str) -> list[tuple[str, str, str]]:
    """Return (name, "method", body) for every method of `class_name` in `text`.

    Used for class-based splits (mixin leaves): the composed class's method
    bodies move verbatim into mixin classes, so each method is fingerprinted
    individually. Decorator lines (e.g. ``@staticmethod``) belong to the method.
    ``...`` stub declarations (used by the mixin state base for mypy typing) are
    not implementations and are skipped.
    """
    text = _normalize(text)
    tree = ast.parse(text)
    lines = text.splitlines()
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            out: list[tuple[str, str, str]] = []
            for sub in node.body:
                if isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef)) and not _is_stub(sub):
                    start = sub.decorator_list[0].lineno if sub.decorator_list else sub.lineno
                    body = "\n".join(lines[start - 1 : sub.end_lineno])
                    out.append((sub.name, "method", body))
            return out
    return []


def _all_class_methods(text: str) -> list[tuple[str, str, str]]:
    """Return (name, "method", body) for every method of every class in `text`."""
    text = _normalize(text)
    tree = ast.parse(text)
    lines = text.splitlines()
    out: list[tuple[str, str, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            for sub in node.body:
                if isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef)) and not _is_stub(sub):
                    start = sub.decorator_list[0].lineno if sub.decorator_list else sub.lineno
                    body = "\n".join(lines[start - 1 : sub.end_lineno])
                    out.append((sub.name, "method", body))
    return out


def fingerprint_methods(path: Path) -> dict[str, tuple[str, str]]:
    """Map method name -> (sha256-16, "method") for every class method in one leaf."""
    text = path.read_text(encoding="utf-8")
    out: dict[str, tuple[str, str]] = {}
    for name, kind, body in _all_class_methods(text):
        if name in out:
            print(f"  WARN: duplicate method {name!r} in {path}")
        digest = hashlib.sha256(body.encode("utf-8")).hexdigest()[:16]
        out[name] = (digest, kind)
    return out


def _match(
    ref_bodies: dict[str, str],
    leaf_fps: dict[str, dict[str, tuple[str, str]]],
) -> tuple[
    list[str],
    list[tuple[str, str, str, str]],
    list[tuple[str, list[tuple[str, str, str]]]],
    dict[str, str],
    list[tuple[str, str]],
]:
    """Match every ref body against a name -> (leaf, digest) index."""
    index: dict[str, list[tuple[str, str, str]]] = {}
    for leaf, fp in leaf_fps.items():
        for name, (digest, kind) in fp.items():
            index.setdefault(name, []).append((leaf, digest, kind))

    missing: list[str] = []
    diffs: list[tuple[str, str, str, str]] = []
    ambiguous: list[tuple[str, list[tuple[str, str, str]]]] = []
    owned: dict[str, str] = {}

    for name, body in ref_bodies.items():
        matches = index.get(name, [])
        if not matches:
            missing.append(name)
            continue
        if len(matches) > 1:
            ambiguous.append((name, matches))
            continue
        leaf, digest, _ = matches[0]
        ref_digest = hashlib.sha256(body.encode("utf-8")).hexdigest()[:16]
        owned[name] = leaf
        if digest != ref_digest:
            diffs.append((name, leaf, ref_digest, digest))

    extra = [(name, leaf) for leaf, fp in leaf_fps.items() for name in fp if name not in ref_bodies]
    return missing, diffs, ambiguous, owned, extra


def verify(
    original: str, leaf_paths: list[str], class_methods: str | None = None
) -> dict[str, Any]:
    ref_units = {name: (kind, body) for name, kind, body in units(original)}
    if class_methods is not None:
        # The composed class is verified method-by-method below; its (changed)
        # class body is excluded from the top-level unit comparison.
        ref_units = {name: value for name, value in ref_units.items() if name != class_methods}
        ref_methods = {
            name: body for name, _k, body in class_method_bodies(original, class_methods)
        }

    leaf_fps = {leaf: fingerprint(Path(leaf)) for leaf in leaf_paths}
    missing, diffs, ambiguous, owned, extra = _match(
        {name: body for name, (_kind, body) in ref_units.items()},
        leaf_fps,
    )

    ok = not missing and not diffs and not ambiguous
    report: dict[str, Any] = {
        "ok": ok,
        "original": None,
        "leaves": leaf_paths,
        "missing": missing,
        "diffs": [(n, leaf, ref, cur) for n, leaf, ref, cur in diffs],
        "ambiguous": [(n, [(leaf, digest) for leaf, digest, _k in m]) for n, m in ambiguous],
        "extra": extra,
        "owned_by": {name: leaf for name, leaf in owned.items()},
        "class_methods": None,
    }

    if class_methods is not None:
        leaf_methods = {leaf: fingerprint_methods(Path(leaf)) for leaf in leaf_paths}
        m_missing, m_diffs, m_ambiguous, m_owned, m_extra = _match(ref_methods, leaf_methods)
        report["class_methods"] = {
            "class": class_methods,
            "missing": m_missing,
            "diffs": [(n, leaf, ref, cur) for n, leaf, ref, cur in m_diffs],
            "ambiguous": [(n, [(leaf, digest) for leaf, digest, _k in m]) for n, m in m_ambiguous],
            "extra": m_extra,
            "owned_by": {name: leaf for name, leaf in m_owned.items()},
        }
        ok = ok and not m_missing and not m_diffs and not m_ambiguous
        report["ok"] = ok

    return report


def print_report(report: dict[str, Any]) -> None:
    print("=== SPLIT FIDELITY CHECK ===")
    for leaf in report["leaves"]:
        print(f"  leaf: {leaf}")
    if report["missing"]:
        print("MISSING (in original, not found in any leaf):")
        for n in report["missing"]:
            print(f"  - {n}")
    if report["diffs"]:
        print("DIFF (found but byte content changed):")
        for n, leaf, ref, cur in report["diffs"]:
            print(f"  - {n} in {leaf}: ref={ref} leaf={cur}")
    if report["ambiguous"]:
        print("AMBIGUOUS (same name in multiple leaves):")
        for n, matches in report["ambiguous"]:
            print(f"  - {n}: {matches}")
    if report["extra"]:
        print("EXTRA (in leaves but not in original) — informational:")
        for n, leaf in report["extra"]:
            print(f"  - {n} ({leaf})")
    # `owned_by` records where every found ref unit lives, including seam-diffed
    # ones, so the byte-identical count is owned minus the diffed units.
    total = len(report["owned_by"]) + len(report["missing"])
    identical = len(report["owned_by"]) - len(report["diffs"]) - len(report["ambiguous"])
    print(f"{identical}/{total} original units byte-identical")
    cm = report.get("class_methods")
    if cm:
        if cm["missing"]:
            print("MISSING METHODS (in original class, not found in any leaf):")
            for n in cm["missing"]:
                print(f"  - {n}")
        if cm["diffs"]:
            print("DIFF METHODS (found but byte content changed):")
            for n, leaf, ref, cur in cm["diffs"]:
                print(f"  - {n} in {leaf}: ref={ref} leaf={cur}")
        if cm["ambiguous"]:
            print("AMBIGUOUS METHODS (same name in multiple leaves):")
            for n, matches in cm["ambiguous"]:
                print(f"  - {n}: {matches}")
        m_total = len(cm["owned_by"]) + len(cm["missing"])
        m_identical = len(cm["owned_by"]) - len(cm["diffs"]) - len(cm["ambiguous"])
        print(f"{m_identical}/{m_total} methods of {cm['class']} byte-identical")
    print("ALL BYTE-IDENTICAL" if report["ok"] else "FIDELITY FAILURES PRESENT")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Verify per-definition byte fidelity of a module split against git HEAD.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("original", help="Path of the original module (relative to repo root)")
    ap.add_argument(
        "--leaves",
        nargs="+",
        help="Split leaves to check, including the coordinator. Defaults to just "
        "the original path (self-check against git HEAD).",
    )
    ap.add_argument(
        "--snapshot",
        help="Plain-text copy of the pre-split module, used instead of "
        "`git show HEAD:<original>` when the original is untracked.",
    )
    ap.add_argument(
        "--manifest-out",
        help="Write the JSON report to this path (e.g. for CI regression).",
    )
    ap.add_argument(
        "--class-methods",
        metavar="CLASS",
        help="Verify the named composed class method-by-method instead of as one "
        "top-level unit (for mixin-leaf splits); its class body is excluded from "
        "the top-level comparison.",
    )
    args = ap.parse_args()

    original = _read_ref(args.original, args.snapshot)
    leaves = args.leaves if args.leaves else [args.original]
    report = verify(original, leaves, class_methods=args.class_methods)
    report["original"] = args.original
    if args.manifest_out:
        Path(args.manifest_out).write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(f"manifest written: {args.manifest_out}")
    print_report(report)
    raise SystemExit(0 if report["ok"] else 1)


if __name__ == "__main__":
    main()
