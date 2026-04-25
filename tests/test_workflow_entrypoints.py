import shutil
import subprocess
import sys
from pathlib import Path


def test_dev_pipeline_targeted_npm_entrypoint_starts_without_relative_import_failure(
    repo_root: Path, tmp_path: Path
) -> None:
    npm_command = shutil.which("npm.cmd") or shutil.which("npm")
    assert npm_command, "npm must be available for the pipeline entrypoint smoke test."
    completed = subprocess.run(  # noqa: S603
        [
            npm_command,
            "run",
            "dev:pipeline",
            "--",
            "--only-sources",
            "missing-dummy-source",
            "--output-dir",
            str(tmp_path),
            "--max-workers",
            "1",
            "--no-preserve-previous-on-empty",
            "--quiet",
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )

    combined = "\n".join(part for part in (completed.stdout, completed.stderr) if part)
    report_path = tmp_path / "jobs-fetch-report.json"
    assert completed.returncode in (0, 2), combined
    assert report_path.exists(), combined
    assert "attempted relative import with no known parent package" not in combined


def test_location_unknown_country_manifest_script_runs_from_repo_root(
    repo_root: Path, tmp_path: Path
) -> None:
    input_csv = tmp_path / "jobs-unified.csv"
    input_csv.write_text(
        "title,company,city,country,source,jobLink\n"
        "Environment Artist,Studio,Hong Kong,Unknown,google_sheets,https://example.com/job\n",
        encoding="utf-8",
    )
    output_json = tmp_path / "manifest.json"
    completed = subprocess.run(  # noqa: S603
        [
            sys.executable,
            "scripts/location_unknown_country_manifest.py",
            "build",
            "--input-csv",
            str(input_csv),
            "--output-json",
            str(output_json),
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
    )

    combined = "\n".join(part for part in (completed.stdout, completed.stderr) if part)
    assert completed.returncode == 0, combined
    assert output_json.exists(), combined
