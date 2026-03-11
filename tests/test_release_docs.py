import unittest
from pathlib import Path


class ReleaseDocsTests(unittest.TestCase):
    def test_deprecated_release_docs_are_stubs_pointing_to_release(self) -> None:
        root = Path(__file__).resolve().parents[1]
        expected_lines = [
            "This document is deprecated.",
            "Use the canonical release guide instead:",
            "- `docs/RELEASE.md`",
        ]
        for relative in (
            "docs/ship-bundle-runbook.md",
            "docs/deployment-and-update-guide.md",
            "docs/versioning-policy-and-release-checklist.md",
            "docs/portable-executable-runbook.md",
        ):
            text = (root / relative).read_text(encoding="utf-8").strip()
            self.assertIn(expected_lines[0], text, msg=f"{relative} should stay deprecated.")
            self.assertIn(expected_lines[1], text, msg=f"{relative} should point to docs/RELEASE.md.")
            self.assertIn(expected_lines[2], text, msg=f"{relative} should point to docs/RELEASE.md.")
            self.assertLessEqual(
                len(text.splitlines()),
                7,
                msg=f"{relative} should remain a short stub and not regrow release procedures.",
            )


if __name__ == "__main__":
    unittest.main()
