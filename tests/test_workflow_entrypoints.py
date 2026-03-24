from pathlib import Path


def test_release_workflow_uses_canonical_test_entrypoints() -> None:
    root = Path(__file__).resolve().parents[1]
    workflow_path = root / ".github" / "workflows" / "build-portable-exe.yml"
    workflow_text = workflow_path.read_text(encoding="utf-8")

    for expected_command in (
        "npm run test:frontend:unit",
        "npm run test:py",
        "npm run test:frontend:packaged",
    ):
        assert expected_command in workflow_text, (
            f"{workflow_path.name} should invoke `{expected_command}`."
        )

    for forbidden_command in (
        "node tests/frontend/unit/all.test.mjs",
        'py -3.13 -m unittest discover -s tests -p "test_*.py" -v',
        "scripts\\run_py_tests.cmd",
        "py -3.13 scripts/packaged_desktop_smoke.py",
        "python scripts/packaged_desktop_smoke.py",
    ):
        assert forbidden_command not in workflow_text, (
            f"{workflow_path.name} should route release-gate test lanes through their canonical npm scripts instead of duplicating raw commands."
        )
