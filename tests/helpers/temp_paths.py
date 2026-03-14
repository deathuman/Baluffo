import shutil
import uuid
from contextlib import contextmanager
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TEST_TMP_ROOT = ROOT / ".codex-test-tmp"


@contextmanager
def workspace_tmpdir(prefix: str):
    root = TEST_TMP_ROOT / f"{prefix}-{uuid.uuid4().hex}"
    root.mkdir(parents=True, exist_ok=True)
    try:
        yield root
    finally:
        shutil.rmtree(root, ignore_errors=True)