"""A finished process must not look alive, and SIGTERM must unwind launch cleanup.

Two failures are pinned here because both left the app unusable in a different way:

* ``os.kill(pid, 0)`` succeeds for a zombie, so a crashed launcher the system had
  not reaped yet made the next start report "Baluffo is already running" and stop;
* ``SIGTERM``'s default disposition ends the process inside the kernel, so the
  ``finally`` blocks that stop the children and release the instance lock never ran.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, cast

import pytest

from src.ship.desktop_app import _linux as linux_platform
from src.ship.desktop_app import process as process_mod
from src.ship.desktop_app import session

pytestmark = pytest.mark.skipif(
    sys.platform != "linux", reason="procfs process states are Linux-only"
)

_ROOT = Path(__file__).resolve().parents[2]


def _wait_for_process_state(pid: int, states: set[str], *, timeout_s: float = 5.0) -> str:
    deadline = time.monotonic() + timeout_s
    state = ""
    while time.monotonic() < deadline:
        try:
            stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8", errors="replace")
        except OSError:
            return ""
        fields = stat[stat.rfind(")") + 2 :].split()
        state = fields[0] if fields else ""
        if state in states:
            return state
        time.sleep(0.01)
    return state


def test_pid_has_exited_is_true_for_an_unreaped_child() -> None:
    # Windows typeshed has no os.fork; this module is skipped there anyway.
    fork = cast(Any, os).fork
    pid = fork()
    if pid == 0:  # pragma: no cover - child process
        os._exit(0)
    try:
        assert _wait_for_process_state(pid, {"Z"}) == "Z"
        assert linux_platform._pid_has_exited(pid) is True
        # kill(pid, 0) still succeeds here; that is the bug being fixed.
        os.kill(pid, 0)
        assert session.is_process_alive(pid) is False
    finally:
        os.waitpid(pid, 0)


def test_pid_has_exited_is_false_for_a_running_process() -> None:
    assert linux_platform._pid_has_exited(os.getpid()) is False
    assert session.is_process_alive(os.getpid()) is True


def test_launcher_installs_a_termination_handler_that_raises_system_exit() -> None:
    probe = (
        "import signal\n"
        "from src.ship.desktop_app import process as process_mod\n"
        "process_mod._install_termination_handlers()\n"
        "signal.raise_signal(signal.SIGTERM)\n"
        "raise SystemExit(0)\n"
    )
    completed = subprocess.run(
        [sys.executable, "-c", probe],
        cwd=str(_ROOT),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )

    assert completed.returncode == 128 + int(signal.SIGTERM), completed.stderr


def test_termination_handler_turns_a_real_sigterm_into_exit(tmp_path: Path) -> None:
    script = tmp_path / "waiter.py"
    script.write_text(
        "import time\n"
        "from src.ship.desktop_app import process as process_mod\n"
        "process_mod._install_termination_handlers()\n"
        "print('ready', flush=True)\n"
        "try:\n"
        "    time.sleep(60)\n"
        "finally:\n"
        "    print('cleanup', flush=True)\n",
        encoding="utf-8",
    )
    process = subprocess.Popen(
        [sys.executable, str(script)],
        cwd=str(_ROOT),
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout is not None
        assert process.stdout.readline().strip() == "ready"
        process.send_signal(signal.SIGTERM)
        remaining, _ = process.communicate(timeout=30)
    finally:
        if process.poll() is None:
            process.kill()

    assert process.returncode == 128 + int(signal.SIGTERM)
    assert "cleanup" in remaining


def test_install_termination_handlers_leaves_an_existing_handler_alone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A host that already handles SIGTERM keeps it; only the default is replaced."""
    installed: list[tuple[int, object]] = []
    monkeypatch.setattr(process_mod.signal, "getsignal", lambda _signum: object())
    monkeypatch.setattr(
        process_mod.signal,
        "signal",
        lambda signum, handler: installed.append((int(signum), handler)),
    )

    process_mod._install_termination_handlers()

    assert installed == []
