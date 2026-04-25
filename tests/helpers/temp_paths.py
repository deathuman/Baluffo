import shutil
import uuid
from contextlib import contextmanager
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TEST_TMP_ROOT = ROOT / ".tmp" / "pytest"


def make_workspace_tmpdir(prefix: str, *, root: Path = TEST_TMP_ROOT) -> Path:
    path = root / f"{prefix}-{uuid.uuid4().hex}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def remove_workspace_tmpdir(path: Path) -> None:
    shutil.rmtree(path, ignore_errors=True)


def cleanup_stale_workspace_tmpdirs(*roots: Path) -> None:
    for stale_root in roots:
        if not stale_root.exists():
            continue
        for child in stale_root.iterdir():
            if child.is_dir() and child.name.startswith("pytest-"):
                remove_workspace_tmpdir(child)


@contextmanager
def workspace_tmpdir(prefix: str):
    root = make_workspace_tmpdir(prefix)
    try:
        yield root
    finally:
        remove_workspace_tmpdir(root)
