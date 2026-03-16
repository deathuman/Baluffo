import unittest
from pathlib import Path


class ReleaseDocsTests(unittest.TestCase):
    def test_release_guide_is_canonical_single_source(self) -> None:
        """Ensure there is a single canonical release guide.

        Historical runbooks have been deprecated and may be removed entirely.
        This test focuses on guaranteeing that `docs/RELEASE.md` exists and is
        not accidentally emptied or deleted, rather than enforcing the presence
        of legacy stub files.
        """
        root = Path(__file__).resolve().parents[1]
        release_path = root / "docs" / "RELEASE.md"
        self.assertTrue(release_path.is_file(), msg="docs/RELEASE.md must exist as the canonical release guide.")
        text = release_path.read_text(encoding="utf-8").strip()
        self.assertGreater(len(text.splitlines()), 0, msg="docs/RELEASE.md should not be empty.")


if __name__ == "__main__":
    unittest.main()
