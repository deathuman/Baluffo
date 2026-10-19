import shutil
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TEST_TMP_ROOT = ROOT / ".tmp" / "pytest"

# A leftover is only a leftover once no live process can still own it. The sweep
# below runs at conftest import, which happens in every xdist worker and in the
# child pytest processes tests spawn, so a younger directory belongs to a test
# some process is executing right now.
STALE_TMPDIR_MIN_AGE_S = 30 * 60.0


def make_workspace_tmpdir(prefix: str, *, root: Path = TEST_TMP_ROOT) -> Path:
    path = root / f"{prefix}-{uuid.uuid4().hex}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def remove_workspace_tmpdir(path: Path) -> None:
    shutil.rmtree(path, ignore_errors=True)


def cleanup_stale_workspace_tmpdirs(
    *roots: Path, min_age_s: float = STALE_TMPDIR_MIN_AGE_S
) -> None:
    """Remove ``pytest-*`` leftovers from earlier runs, never a live directory.

    Running this at conftest import means every xdist worker and every child
    pytest process a test launches sweeps the same root while other workers are
    still writing to it. ``tmp_path`` directories are named ``pytest-tmp-*``, so
    an unguarded sweep deleted the working directory of a test running in
    another worker -- which is why the suite failed only when sharded (`-n 2`
    stayed green, `-n 4` lost a different test each run). A directory's age is
    the only signal a separate process can rely on, so young directories are
    left alone and a crashed run's leftovers are collected on a later start.
    """
    cutoff = time.time() - min_age_s
    for stale_root in roots:
        if not stale_root.exists():
            continue
        for child in stale_root.iterdir():
            if not (child.is_dir() and child.name.startswith("pytest-")):
                continue
            try:
                if child.stat().st_mtime > cutoff:
                    continue
            except OSError:
                continue
            remove_workspace_tmpdir(child)


@contextmanager
def workspace_tmpdir(prefix: str):
    root = make_workspace_tmpdir(prefix)
    try:
        yield root
    finally:
        remove_workspace_tmpdir(root)
