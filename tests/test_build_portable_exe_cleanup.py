from pathlib import Path
from unittest import mock

import pytest

from scripts import build_portable_exe

pytestmark = pytest.mark.packaging


def test_remove_path_with_retry_preserves_directory_when_rename_is_locked(tmp_path: Path) -> None:
    output_dir = tmp_path / "baluffo-portable"
    child = output_dir / "ship" / "app" / "versions" / "0.2.71" / "src" / "scrapers"
    child.mkdir(parents=True)
    runner = child / "runner.py"
    runner.write_text("print('runner')\n", encoding="utf-8")

    with mock.patch.object(
        Path,
        "replace",
        side_effect=PermissionError("locked output"),
    ):
        with pytest.raises(RuntimeError, match="Could not clear path"):
            build_portable_exe._remove_path_with_retry(output_dir)

    assert runner.read_text(encoding="utf-8") == "print('runner')\n"
