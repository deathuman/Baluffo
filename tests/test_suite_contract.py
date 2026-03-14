import re
import unittest
from pathlib import Path


class TestSuiteContractTests(unittest.TestCase):
    def test_discovered_python_test_files_define_real_tests(self) -> None:
        root = Path(__file__).resolve().parent
        test_files = sorted(root.glob("test_*.py"))
        pattern = re.compile(r"^(class\s+\w+Tests\s*\(|def\s+test_)", re.MULTILINE)

        for path in test_files:
            text = path.read_text(encoding="utf-8")
            self.assertRegex(
                text,
                pattern,
                msg=f"{path.name} matches test discovery but does not define a real test case.",
            )

    def test_frontend_test_patterns_reserve_generated_manifest_as_only_aggregator(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        tests_root = repo_root / "tests"
        frontend_unit_root = tests_root / "frontend" / "unit"
        allowed_manifest = frontend_unit_root / "all.test.mjs"
        aggregator_paths = sorted(tests_root.rglob("all.test.mjs"))
        self.assertEqual(
            aggregator_paths,
            [allowed_manifest],
            msg="tests/frontend/unit/all.test.mjs must remain the only all.test.mjs-style test aggregator.",
        )

        real_frontend_test_pattern = re.compile(r"\btest\s*\(")
        for path in sorted(frontend_unit_root.glob("*.test.mjs")):
            if path == allowed_manifest:
                continue
            text = path.read_text(encoding="utf-8")
            self.assertRegex(
                text,
                real_frontend_test_pattern,
                msg=(
                    f"{path.relative_to(repo_root)} matches the frontend discovered test pattern "
                    "but does not define a real test."
                ),
            )
