"""The site, bridge, and browser must not outlive the launcher on Linux.

POSIX has no kill-on-close job object, so ``start_child_process`` arms
``PR_SET_PDEATHSIG`` in the child before exec. These tests exercise the real
kernel path: a parent process spawns a child through the production helper, the
parent is killed outright (no chance to run cleanup), and the child must be gone.
"""

from __future__ import annotations

import contextlib
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from src.ship.desktop_app import _linux as linux_platform

pytestmark = pytest.mark.skipif(sys.platform != "linux", reason="PR_SET_PDEATHSIG is Linux-only")

_ROOT = Path(__file__).resolve().parents[2]
# Windows typeshed has no signal.SIGKILL; this module is skipped there anyway.
_SIGKILL = getattr(signal, "SIGKILL", 9)


def _child_env() -> dict[str, str]:
    """Probe environment with the repo importable (a script's own dir is on sys.path)."""
    env = dict(os.environ)
    existing = [part for part in env.get("PYTHONPATH", "").split(os.pathsep) if part]
    env["PYTHONPATH"] = os.pathsep.join([str(_ROOT), *existing])
    return env


_PARENT_SCRIPT = """
import sys
import time

from src.ship.desktop_app import process as process_mod

child = process_mod.start_child_process([sys.executable, "-c", "import time; time.sleep(120)"])
print(child.pid, flush=True)
time.sleep(120)
"""


def _pid_is_running(pid: int) -> bool:
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False
    fields = stat[stat.rfind(")") + 2 :].split()
    return bool(fields) and fields[0] not in {"Z", "X", "x"}


def test_child_dies_when_its_parent_is_killed(tmp_path: Path) -> None:
    script = tmp_path / "parent.py"
    script.write_text(_PARENT_SCRIPT, encoding="utf-8")
    parent = subprocess.Popen(
        [sys.executable, str(script)],
        cwd=str(_ROOT),
        stdout=subprocess.PIPE,
        text=True,
        env=_child_env(),
    )
    child_pid = 0
    try:
        assert parent.stdout is not None
        line = parent.stdout.readline().strip()
        assert line.isdigit(), line
        child_pid = int(line)
        assert _pid_is_running(child_pid)

        os.kill(parent.pid, _SIGKILL)
        parent.wait(timeout=30)

        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline and _pid_is_running(child_pid):
            time.sleep(0.02)
        assert _pid_is_running(child_pid) is False, "child outlived its killed parent"
    finally:
        if parent.poll() is None:
            parent.kill()
        if child_pid > 0:
            with contextlib.suppress(OSError):
                os.kill(child_pid, _SIGKILL)


def test_pdeathsig_helper_is_a_no_op_without_a_parent_pid() -> None:
    assert linux_platform._linux_pdeathsig_preexec(0) is None


def test_pdeathsig_helper_returns_a_callable_for_a_real_parent() -> None:
    preexec = linux_platform._linux_pdeathsig_preexec(os.getpid())

    assert callable(preexec)


def test_spawned_child_is_parented_to_the_spawning_process(tmp_path: Path) -> None:
    """The preexec hook must not reparent or detach the child."""
    script = tmp_path / "one.py"
    script.write_text(
        "import sys\n"
        "from src.ship.desktop_app import process as process_mod\n"
        "child = process_mod.start_child_process(\n"
        "    [sys.executable, '-c', 'import time; time.sleep(30)']\n"
        ")\n"
        "print(child.pid, flush=True)\n"
        "import time; time.sleep(30)\n",
        encoding="utf-8",
    )
    parent = subprocess.Popen(
        [sys.executable, str(script)],
        cwd=str(_ROOT),
        stdout=subprocess.PIPE,
        text=True,
        env=_child_env(),
    )
    try:
        assert parent.stdout is not None
        child_pid = int(parent.stdout.readline().strip())
        stat = Path(f"/proc/{child_pid}/stat").read_text(encoding="utf-8", errors="replace")
        ppid = int(stat[stat.rfind(")") + 2 :].split()[1])
        assert ppid == parent.pid
    finally:
        parent.kill()
        parent.wait(timeout=30)
