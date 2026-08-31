"""Guardrail: every ``.github/workflows/*.yml`` must pass actionlint validation.

The container workflow's ``paths-ignore`` deliberately excludes ``.github/**``
and ``tools/**`` so pure-tooling commits stop triggering no-op container
republishes. The cost of that alignment is that a push which only touches
workflow files no longer triggers *any* build that would validate the YAML —
a syntax error in a workflow could land on ``main`` silently and only surface
on the next real run. This guardrail closes that gap by running actionlint over
every workflow file in the repo-guardrail ``workflow`` group, which runs in the
pre-commit and pre-push gates locally and in CI Lint.

actionlint is a static checker for GitHub Actions workflow files: it validates
the ``on`` triggers, job/step structure, expression syntax, ``if:`` conditions,
and action pins. It is the semantic complement to the ``check-yaml`` pre-commit
hook, which only verifies the file parses as YAML.

The binary is not a repo dependency. The check prefers an ``actionlint`` on
PATH (honoring the toolbelt ``~/.local/bin`` convention), then provisions a
pinned, checksum-verified release binary into ``.tmp/actionlint/`` (gitignored)
on first use. If the binary cannot be located or provisioned, the gate fails
with a clear message instead of silently skipping — "always checked before
push" means the check cannot be bypassed by an offline environment.
"""

from __future__ import annotations

import hashlib
import platform
import shutil
import subprocess
import tarfile
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

ACTIONLINT_VERSION = "1.7.12"
RELEASE_BASE = f"https://github.com/rhysd/actionlint/releases/download/v{ACTIONLINT_VERSION}"
CHECKSUMS_FILENAME = f"actionlint_{ACTIONLINT_VERSION}_checksums.txt"
CACHE_SUBDIR = ".tmp/actionlint"

_ARCH_ALIASES = {
    "x86_64": "amd64",
    "amd64": "amd64",
    "aarch64": "arm64",
    "arm64": "arm64",
}


def _platform_os() -> str:
    system = platform.system().lower()
    if system in ("windows", "linux", "darwin"):
        return system
    raise OSError(f"unsupported platform for actionlint provisioning: {system!r}")


def _platform_arch() -> str:
    machine = platform.machine().lower()
    try:
        return _ARCH_ALIASES[machine]
    except KeyError:
        raise OSError(
            f"unsupported architecture for actionlint provisioning: {machine!r}"
        ) from None


def asset_filename(
    version: str = ACTIONLINT_VERSION,
    *,
    os_name: str | None = None,
    arch: str | None = None,
) -> str:
    os_name = os_name or _platform_os()
    arch = arch or _platform_arch()
    extension = "zip" if os_name == "windows" else "tar.gz"
    return f"actionlint_{version}_{os_name}_{arch}.{extension}"


def asset_url(asset: str) -> str:
    return f"{RELEASE_BASE}/{asset}"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _checksum_for(checksum_text: str, asset: str) -> str | None:
    """Return the published sha256 for ``asset`` from actionlint's checksums file."""
    for line in checksum_text.splitlines():
        digest, separator, filename = line.strip().partition("  ")
        if separator and filename == asset and digest:
            return digest
    return None


def _download(url: str, dest: Path) -> None:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "Baluffo-repo-guardrails (workflow-syntax gate)"},
    )
    with urllib.request.urlopen(request, timeout=60) as response:  # noqa: S310 - pinned https release host
        payload = response.read()
    dest.write_bytes(payload)


def _extract_archive(archive: Path, cache_dir: Path, os_name: str) -> None:
    """Extract only the actionlint binary member from a verified release archive."""
    binary_name = "actionlint.exe" if os_name == "windows" else "actionlint"
    if os_name == "windows":
        with zipfile.ZipFile(archive) as handle:
            for member in handle.infolist():
                if Path(member.filename).name == binary_name:
                    handle.extract(member, cache_dir)
                    return
        raise zipfile.BadZipFile(f"{archive.name} does not contain {binary_name}")
    with tarfile.open(archive, "r:gz") as handle:
        for member in handle.getmembers():
            if member.isfile() and Path(member.name).name == binary_name:
                handle.extract(member, cache_dir)
                return
    raise tarfile.TarError(f"{archive.name} does not contain {binary_name}")


def _install_cached_binary(
    cache_dir: Path,
    checksum_text: str,
    archive: Path,
    *,
    os_name: str,
    arch: str,
) -> tuple[Path | None, str | None]:
    """Verify and extract a local release archive; return (binary, error)."""
    asset = asset_filename(os_name=os_name, arch=arch)
    expected = _checksum_for(checksum_text, asset)
    if expected is None:
        return None, f"checksums file does not list {asset}"
    actual = _sha256(archive)
    if actual != expected:
        return (
            None,
            f"checksum mismatch for {asset}: downloaded {actual[:16]}..., "
            f"expected {expected[:16]}... (refusing to run an unverified binary)",
        )
    binary = cache_dir / ("actionlint.exe" if os_name == "windows" else "actionlint")
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
        _extract_archive(archive, cache_dir, os_name)
    except (OSError, tarfile.TarError, zipfile.BadZipFile) as exc:
        return None, f"could not extract {asset}: {exc}"
    if not binary.exists():
        return None, f"extracted {asset} but {binary.name} is missing"
    return binary, None


def _provision_actionlint(repo_root: Path) -> tuple[Path | None, str | None]:
    """Download the pinned actionlint binary into the repo-local cache."""
    os_name = _platform_os()
    arch = _platform_arch()
    asset = asset_filename(os_name=os_name, arch=arch)
    cache_dir = repo_root / CACHE_SUBDIR
    checksums_path = cache_dir / CHECKSUMS_FILENAME
    archive_path = cache_dir / asset

    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
        _download(asset_url(CHECKSUMS_FILENAME), checksums_path)
        _download(asset_url(asset), archive_path)
    except (OSError, urllib.error.URLError) as exc:
        return None, (
            f"could not download actionlint {ACTIONLINT_VERSION}: {exc} "
            "(network is required once to cache the binary)"
        )
    binary, error = _install_cached_binary(
        cache_dir,
        checksums_path.read_text(encoding="utf-8"),
        archive_path,
        os_name=os_name,
        arch=arch,
    )
    if binary is None:
        try:
            archive_path.unlink()
        except OSError:
            pass
        return None, error
    return binary, None


def _find_workflow_files(repo_root: Path) -> list[Path]:
    workflows_dir = repo_root / ".github" / "workflows"
    if not workflows_dir.is_dir():
        return []
    return sorted(workflows_dir.glob("*.yml"))


def _locate_actionlint(repo_root: Path) -> str | None:
    found = shutil.which("actionlint")
    if found:
        return found
    local_bin = Path.home() / ".local" / "bin" / "actionlint"
    if local_bin.is_file():
        return str(local_bin)
    cached = repo_root / CACHE_SUBDIR
    cached_binary = cached / ("actionlint.exe" if _platform_os() == "windows" else "actionlint")
    if cached_binary.is_file():
        return str(cached_binary)
    return None


def _run_actionlint(
    command: list[str],
    workflow_files: list[Path],
    repo_root: Path,
) -> tuple[int, str]:
    completed = subprocess.run(
        [
            *command,
            "-no-color",
            "-oneline",
            "-shellcheck",
            "",
            "-pyflakes",
            "",
            *(str(path.resolve()) for path in workflow_files),
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    output = "\n".join(part for part in (completed.stdout, completed.stderr) if part).strip()
    return completed.returncode, output


def check_workflow_syntax(
    repo_root: Path = ROOT,
    *,
    actionlint_bin: str | list[str] | None = None,
) -> list[str]:
    """Fail when any ``.github/workflows/*.yml`` fails actionlint validation.

    ``actionlint_bin`` overrides binary discovery: pass the executable path as a
    string, or a command prefix (e.g. ``["python", "/path/to/actionlint"]`` or a
    wrapper like ``["docker", "run", "--rm", "rhysd/actionlint:1.7.12"]``).
    When omitted, PATH is searched first and then the pinned release binary is
    provisioned into ``.tmp/actionlint/``.
    """
    repo_root = Path(repo_root).resolve()
    workflows_dir = repo_root / ".github" / "workflows"
    if not workflows_dir.is_dir():
        return [f"GitHub workflows directory is missing: {workflows_dir.relative_to(repo_root)}"]
    workflow_files = _find_workflow_files(repo_root)
    if not workflow_files:
        return [f"no workflow files found under {workflows_dir.relative_to(repo_root)}"]

    binary: str | list[str] | None = actionlint_bin
    if binary is None:
        located = _locate_actionlint(repo_root)
        if located is not None:
            binary = located
        else:
            cached, error = _provision_actionlint(repo_root)
            if cached is None:
                return [
                    "actionlint is not installed and could not be provisioned "
                    f"({error}). Install it yourself (`brew install actionlint`, "
                    "`scoop install actionlint`, `go install "
                    "github.com/rhysd/actionlint/cmd/actionlint@v1.7.12`, or put the "
                    "binary on PATH) and re-run the gate."
                ]
            binary = str(cached)

    command = [binary] if isinstance(binary, str) else list(binary)
    returncode, output = _run_actionlint(command, workflow_files, repo_root)
    if returncode == 0:
        return []
    if not output:
        return [f"actionlint exited with code {returncode} without output"]
    lines = output.splitlines()
    return [f"actionlint found {len(lines)} issue(s) in .github/workflows:\n" + "\n".join(lines)]
