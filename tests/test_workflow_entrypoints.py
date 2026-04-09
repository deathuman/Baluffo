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


def test_lint_workflow_uses_full_precommit_entrypoint() -> None:
    root = Path(__file__).resolve().parents[1]
    workflow_path = root / ".github" / "workflows" / "lint.yml"
    workflow_text = workflow_path.read_text(encoding="utf-8")

    assert "npm run lint:precommit:ci" in workflow_text, (
        f"{workflow_path.name} should use the CI pre-commit npm entrypoint."
    )
    assert "pre-commit run --all-files" not in workflow_text, (
        f"{workflow_path.name} should not embed the raw full-repo pre-commit command."
    )


def test_lint_workflow_uses_split_precommit_entrypoints() -> None:
    root = Path(__file__).resolve().parents[1]
    workflow_path = root / ".github" / "workflows" / "lint.yml"
    workflow_text = workflow_path.read_text(encoding="utf-8")
    package_path = root / "package.json"
    package_text = package_path.read_text(encoding="utf-8")

    assert "lint:precommit:ci" in workflow_text, (
        f"{workflow_path.name} should run the CI pre-commit entrypoint."
    )
    assert "--exclude-root data" in package_text, (
        f"{package_path.name} should route the CI pre-commit entrypoint through the data exclusion."
    )


def test_pre_push_hook_runs_python_and_smoke_gates() -> None:
    root = Path(__file__).resolve().parents[1]
    hook_path = root / ".githooks" / "pre-push"
    hook_text = hook_path.read_text(encoding="utf-8")

    for expected_command in ("npm run lint:precommit:ci", "npm run test:py", "npm run test:smoke"):
        assert expected_command in hook_text, (
            f"{hook_path.name} should invoke `{expected_command}` before pushing to main."
        )


def test_pre_commit_hook_runs_lint_gate() -> None:
    root = Path(__file__).resolve().parents[1]
    hook_path = root / ".githooks" / "pre-commit"
    hook_text = hook_path.read_text(encoding="utf-8")

    assert "npm run lint:precommit:changed" in hook_text, (
        f"{hook_path.name} should invoke the changed-file lint gate before every commit."
    )
