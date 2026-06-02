from __future__ import annotations

import argparse
import shutil
import subprocess
import tarfile
from collections.abc import Callable, Sequence
from pathlib import Path

DEFAULT_TAG = "ghcr.io/deathuman/baluffo:local"


class CleanContextError(RuntimeError):
    pass


Runner = Callable[[Sequence[str], Path], None]
Extractor = Callable[[Path, Path], None]


def run_checked(command: Sequence[str], cwd: Path) -> None:
    subprocess.run(list(command), cwd=str(cwd), check=True)


def safe_ref_name(ref: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "-" for ch in ref.strip())
    return cleaned.strip("-._") or "HEAD"


def ensure_inside(parent: Path, candidate: Path) -> Path:
    parent_resolved = parent.resolve()
    candidate_resolved = candidate.resolve()
    try:
        candidate_resolved.relative_to(parent_resolved)
    except ValueError as exc:
        raise CleanContextError(
            f"refusing path outside {parent_resolved}: {candidate_resolved}"
        ) from exc
    return candidate_resolved


def extract_tar_safely(archive_path: Path, context_dir: Path) -> None:
    context_dir.mkdir(parents=True, exist_ok=True)
    base = context_dir.resolve()
    with tarfile.open(archive_path, "r") as archive:
        for member in archive.getmembers():
            target = (base / member.name).resolve()
            try:
                target.relative_to(base)
            except ValueError as exc:
                raise CleanContextError(f"archive member escapes context: {member.name}") from exc
        archive.extractall(base, filter="data")


def remove_context_path(path: Path, tmp_root: Path) -> None:
    resolved = ensure_inside(tmp_root, path)
    if resolved.is_dir():
        shutil.rmtree(resolved)
    elif resolved.exists():
        resolved.unlink()


def build_from_clean_context(
    *,
    repo_root: Path,
    ref: str,
    tag: str,
    tmp_root: Path | None = None,
    keep_context: bool = False,
    runner: Runner = run_checked,
    extractor: Extractor = extract_tar_safely,
) -> Path:
    repo = Path(repo_root).resolve()
    tmp = (tmp_root or repo / ".tmp").resolve()
    ensure_inside(repo, tmp)
    tmp.mkdir(parents=True, exist_ok=True)
    suffix = safe_ref_name(ref)
    context_dir = ensure_inside(tmp, tmp / f"docker-build-context-{suffix}")
    archive_path = ensure_inside(tmp, tmp / f"docker-build-context-{suffix}.tar")
    if context_dir.exists() or archive_path.exists():
        remove_context_path(context_dir, tmp)
        remove_context_path(archive_path, tmp)
    try:
        runner(["git", "archive", "--format=tar", f"--output={archive_path}", ref], repo)
        extractor(archive_path, context_dir)
        runner(["docker", "build", "--progress=plain", "-t", tag, str(context_dir)], repo)
        return context_dir
    finally:
        if not keep_context:
            remove_context_path(context_dir, tmp)
            remove_context_path(archive_path, tmp)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Baluffo's Docker image from a clean git archive context."
    )
    parser.add_argument("--ref", default="HEAD", help="Git ref to archive. Defaults to HEAD.")
    parser.add_argument(
        "--tag", default=DEFAULT_TAG, help=f"Docker image tag. Defaults to {DEFAULT_TAG}."
    )
    parser.add_argument(
        "--repo-root",
        default=Path(__file__).resolve().parents[1],
        type=Path,
        help="Repository root. Defaults to this script's parent repository.",
    )
    parser.add_argument(
        "--tmp-root",
        default=None,
        type=Path,
        help="Temporary directory for the archive context. Defaults to <repo>/.tmp.",
    )
    parser.add_argument(
        "--keep-context",
        action="store_true",
        help="Keep the temporary context for debugging. Default removes it after the build.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    context_dir = build_from_clean_context(
        repo_root=args.repo_root,
        ref=str(args.ref),
        tag=str(args.tag),
        tmp_root=args.tmp_root,
        keep_context=bool(args.keep_context),
    )
    if args.keep_context:
        print(f"Kept clean Docker build context at {context_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
