"""Unit tests for the /proc-based TCP port-listener helpers in ``_linux.py``.

The helpers are exercised against an in-memory stand-in for ``/proc`` that is
wired in by swapping ``_linux.Path``, so the tests need no real ``/proc``
filesystem and no symlink privileges (Windows-safe).
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from src.ship.desktop_app import _linux

_TCP_HEADER = (
    "  sl  local_address rem_address   st tx_queue rx_queue tr "
    "tm->when retrnsmt   uid  timeout inode"
)


class _FakeProcFs:
    """In-memory /proc tree: text files, dirs, symlinks, and flaky paths."""

    def __init__(self) -> None:
        self.files: dict[str, str] = {}
        self.read_versions: dict[str, list[str]] = {}  # path -> contents served in order
        self.dirs: dict[str, list[str]] = {}
        self.links: dict[str, str] = {}
        self.flaky_dirs: set[str] = set()
        self.broken_links: set[str] = set()


class _FakePath:
    """pathlib.Path stand-in rooted at an in-memory /proc tree."""

    fs: _FakeProcFs | None = None

    def __init__(self, raw: str) -> None:
        self._raw = str(raw)
        self.name = self._raw.rsplit("/", 1)[-1]

    def __str__(self) -> str:
        return self._raw

    def __truediv__(self, child: str) -> _FakePath:
        return _FakePath(f"{self._raw.rstrip('/')}/{child}")

    def read_text(self, encoding: str = "utf-8", errors: str | None = None) -> str:
        fs = self._require_fs()
        versions = fs.read_versions.get(self._raw)
        if versions is not None:
            if not versions:
                raise FileNotFoundError(self._raw) from None
            return versions.pop(0)
        try:
            return fs.files[self._raw]
        except KeyError:
            raise FileNotFoundError(self._raw) from None

    def iterdir(self) -> Iterator[_FakePath]:
        fs = self._require_fs()
        if self._raw in fs.flaky_dirs:
            raise OSError(2, f"No such file or directory: {self._raw!r}")
        for entry in fs.dirs.get(self._raw, []):
            yield _FakePath(f"{self._raw.rstrip('/')}/{entry}")

    def readlink(self) -> str:
        fs = self._require_fs()
        if self._raw in fs.broken_links:
            raise OSError(40, "Too many levels of symbolic links")
        try:
            return fs.links[self._raw]
        except KeyError:
            raise FileNotFoundError(self._raw) from None

    def _require_fs(self) -> _FakeProcFs:
        if self.fs is None:
            raise RuntimeError("_FakePath.fs must be set via the proc_fs fixture")
        return self.fs


@pytest.fixture
def proc_fs(monkeypatch: pytest.MonkeyPatch) -> _FakeProcFs:
    """Point ``_linux.Path`` at an in-memory /proc tree for the test duration."""
    fs = _FakeProcFs()
    monkeypatch.setattr(_linux, "Path", _FakePath)
    monkeypatch.setattr(_FakePath, "fs", fs)
    return fs


def _tcp_line(local: str, state: str, inode: int | str) -> str:
    """One /proc/net/tcp{,6} data row; inode is the 10th field (parts[9])."""
    return (
        f"   0: {local} 00000000:0000 {state} 00000000:00000000 "
        f"00:00000000 00000000 0 0 {inode} 1 0000000000000000 100 0 0 10 0"
    )


def _tcp_table(*lines: str) -> str:
    return "\n".join((_TCP_HEADER, *lines)) + "\n"


def test_listening_socket_inodes_collects_listen_entries_on_port(proc_fs: _FakeProcFs) -> None:
    proc_fs.files["/proc/net/tcp"] = _tcp_table(
        _tcp_line("0100007F:1F90", "0A", 1001),  # 127.0.0.1:8080 LISTEN
        _tcp_line("0100007F:1F90", "01", 1002),  # same port, ESTABLISHED -> skipped
        _tcp_line("0100007F:2328", "0A", 1003),  # 127.0.0.1:9000 LISTEN -> skipped
    )
    proc_fs.files["/proc/net/tcp6"] = _tcp_table(
        _tcp_line("00000000000000000000000000000001:1F90", "0A", 2001),  # [::1]:8080 LISTEN
    )
    assert _linux._listening_socket_inodes_for_port(8080) == {1001, 2001}


def test_listening_socket_inodes_tolerates_missing_and_malformed_rows(
    proc_fs: _FakeProcFs,
) -> None:
    proc_fs.files["/proc/net/tcp"] = _tcp_table(
        "   0: garbage no-port 0A 0 0 0 0 0 0 999",  # local address without a colon
        "   0: 0100007F:1F90 00000000:0000 0A 0",  # too few fields
        _tcp_line("0100007F:1F90", "0A", "not-an-int"),  # inode not an int
        _tcp_line("0100007F:ZZZZ", "0A", 1004),  # port not hex
        _tcp_line("0100007F:1F90", "0A", 1005),
    )
    # /proc/net/tcp6 is absent: read_text raises OSError and is skipped.
    assert _linux._listening_socket_inodes_for_port(8080) == {1005}


def test_listening_socket_inodes_rejects_non_positive_port(proc_fs: _FakeProcFs) -> None:
    proc_fs.files["/proc/net/tcp"] = _tcp_table(_tcp_line("0100007F:1F90", "0A", 1001))
    assert _linux._listening_socket_inodes_for_port(0) == set()
    assert _linux._listening_socket_inodes_for_port(-1) == set()


def test_fd_socket_inode_parses_socket_targets_and_rejects_others(proc_fs: _FakeProcFs) -> None:
    proc_fs.links["/proc/1/fd/3"] = "socket:[63684]"
    proc_fs.links["/proc/1/fd/0"] = "pipe:[111]"
    proc_fs.links["/proc/1/fd/1"] = "anon_inode:[eventfd]"
    proc_fs.links["/proc/1/fd/2"] = "socket:[abc]"
    proc_fs.broken_links.add("/proc/1/fd/4")

    assert _linux._fd_socket_inode(_FakePath("/proc/1/fd/3")) == 63684
    assert _linux._fd_socket_inode(_FakePath("/proc/1/fd/0")) is None
    assert _linux._fd_socket_inode(_FakePath("/proc/1/fd/1")) is None
    assert _linux._fd_socket_inode(_FakePath("/proc/1/fd/2")) is None
    assert _linux._fd_socket_inode(_FakePath("/proc/1/fd/4")) is None


def test_fd_links_for_pid_lists_fds_and_tolerates_vanished_pid(proc_fs: _FakeProcFs) -> None:
    proc_fs.dirs["/proc/100/fd"] = ["0", "3"]
    proc_fs.flaky_dirs.add("/proc/999/fd")

    links = _linux._fd_links_for_pid(_FakePath("/proc/100"))
    assert [str(link) for link in links] == ["/proc/100/fd/0", "/proc/100/fd/3"]
    assert _linux._fd_links_for_pid(_FakePath("/proc/999")) == []


def test_socket_inode_holders_maps_inodes_to_pids(proc_fs: _FakeProcFs) -> None:
    proc_fs.dirs["/proc"] = ["net", "self", "1", "100", "101"]
    proc_fs.dirs["/proc/1/fd"] = ["0", "3"]
    proc_fs.links["/proc/1/fd/0"] = "pipe:[1]"
    proc_fs.links["/proc/1/fd/3"] = "socket:[63684]"
    proc_fs.dirs["/proc/100/fd"] = ["3", "5"]
    proc_fs.links["/proc/100/fd/3"] = "socket:[99999]"
    proc_fs.links["/proc/100/fd/5"] = "socket:[63684]"
    proc_fs.dirs["/proc/101/fd"] = ["7"]
    proc_fs.links["/proc/101/fd/7"] = "socket:[55555]"

    assert _linux._socket_inode_holders({63684}) == {1: 63684, 100: 63684}


def test_socket_inode_holders_returns_empty_without_inodes(proc_fs: _FakeProcFs) -> None:
    proc_fs.dirs["/proc"] = ["100"]
    assert _linux._socket_inode_holders(set()) == {}


def test_socket_inode_holders_skips_pid_vanishing_mid_scan(proc_fs: _FakeProcFs) -> None:
    proc_fs.dirs["/proc"] = ["100", "200", "300"]
    proc_fs.dirs["/proc/100/fd"] = ["3"]
    proc_fs.links["/proc/100/fd/3"] = "socket:[63684]"
    proc_fs.flaky_dirs.add("/proc/200/fd")  # pid 200 disappears while scanning
    proc_fs.dirs["/proc/300/fd"] = ["4"]
    proc_fs.links["/proc/300/fd/4"] = "socket:[63684]"

    assert _linux._socket_inode_holders({63684}) == {100: 63684, 300: 63684}


def test_socket_inode_holders_tolerates_proc_iteration_failure(proc_fs: _FakeProcFs) -> None:
    proc_fs.flaky_dirs.add("/proc")
    assert _linux._socket_inode_holders({63684}) == {}


def test_pids_listening_on_tcp_port_via_proc_full_flow(proc_fs: _FakeProcFs) -> None:
    proc_fs.files["/proc/net/tcp"] = _tcp_table(
        _tcp_line("0100007F:1F90", "0A", 63684),  # 8080 LISTEN
        _tcp_line("0100007F:2328", "0A", 77777),  # 9000 LISTEN, different port
    )
    proc_fs.files["/proc/net/tcp6"] = _tcp_table(
        _tcp_line("00000000000000000000000000000001:1F90", "0A", 63685),  # [::1]:8080
    )
    proc_fs.dirs["/proc"] = ["net", "100", "200"]
    proc_fs.dirs["/proc/100/fd"] = ["3"]
    proc_fs.links["/proc/100/fd/3"] = "socket:[63684]"
    proc_fs.flaky_dirs.add("/proc/200/fd")  # vanishes mid-scan, must not abort

    assert _linux._pids_listening_on_tcp_port_via_proc(8080) == {100}


def test_pids_listening_on_tcp_port_via_proc_rejects_non_positive_port(
    proc_fs: _FakeProcFs,
) -> None:
    assert _linux._pids_listening_on_tcp_port_via_proc(0) == set()


def test_pids_listening_on_tcp_port_via_proc_keeps_pid_still_listening(
    proc_fs: _FakeProcFs,
) -> None:
    proc_fs.files["/proc/net/tcp"] = _tcp_table(_tcp_line("0100007F:1F90", "0A", 63684))
    proc_fs.dirs["/proc"] = ["100"]
    proc_fs.dirs["/proc/100/fd"] = ["3"]
    proc_fs.links["/proc/100/fd/3"] = "socket:[63684]"

    assert _linux._pids_listening_on_tcp_port_via_proc(8080) == {100}


def test_pids_listening_on_tcp_port_via_proc_drops_pid_whose_listener_exited(
    proc_fs: _FakeProcFs,
) -> None:
    # First read sees inode 63684 LISTENing on 8080 (owned by pid 100); by the
    # fresh re-read the listener is gone, so the pre-walk check bails out and
    # the pid must never be reported.
    proc_fs.read_versions["/proc/net/tcp"] = [
        _tcp_table(_tcp_line("0100007F:1F90", "0A", 63684)),
        _tcp_table(_tcp_line("0100007F:2328", "0A", 77777)),
    ]
    proc_fs.dirs["/proc"] = ["100"]
    proc_fs.dirs["/proc/100/fd"] = ["3"]
    proc_fs.links["/proc/100/fd/3"] = "socket:[63684]"

    assert _linux._pids_listening_on_tcp_port_via_proc(8080) == set()


def test_pids_listening_on_tcp_port_via_proc_skips_fd_scan_when_port_vanished_early(
    proc_fs: _FakeProcFs,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The pre-walk re-read finding the port already gone must bail out before
    # the O(all processes) fd scan runs at all -- that skip is the measured
    # win (~380ms at 2.5k-process scale), so pin it with a call recorder.
    proc_fs.read_versions["/proc/net/tcp"] = [
        _tcp_table(_tcp_line("0100007F:1F90", "0A", 63684)),
        _tcp_table(_tcp_line("0100007F:2328", "0A", 77777)),
    ]
    proc_fs.dirs["/proc"] = ["100"]
    proc_fs.dirs["/proc/100/fd"] = ["3"]
    proc_fs.links["/proc/100/fd/3"] = "socket:[63684]"
    fd_scan_calls: list[set[int]] = []
    original_holders = _linux._socket_inode_holders

    def spy(inodes: set[int]) -> dict[int, int]:
        fd_scan_calls.append(set(inodes))
        return original_holders(inodes)

    monkeypatch.setattr(_linux, "_socket_inode_holders", spy)

    assert _linux._pids_listening_on_tcp_port_via_proc(8080) == set()
    assert fd_scan_calls == []


def test_pids_listening_on_tcp_port_via_proc_filters_recycled_inode_only(
    proc_fs: _FakeProcFs,
) -> None:
    # Both pids match at scan time; the middle re-read keeps the gate open
    # (inode 63685 survives), but only 63685 is still LISTening when the
    # post-walk re-read verifies, so the pid pinned to the recycled inode
    # is dropped. Three served versions model all three table reads.
    proc_fs.read_versions["/proc/net/tcp"] = [
        _tcp_table(
            _tcp_line("0100007F:1F90", "0A", 63684),
            _tcp_line("0100007F:1F90", "0A", 63685),
        ),
        _tcp_table(_tcp_line("0100007F:1F90", "0A", 63685)),
        _tcp_table(_tcp_line("0100007F:1F90", "0A", 63685)),
    ]
    proc_fs.dirs["/proc"] = ["100", "200"]
    proc_fs.dirs["/proc/100/fd"] = ["3"]
    proc_fs.links["/proc/100/fd/3"] = "socket:[63684]"
    proc_fs.dirs["/proc/200/fd"] = ["4"]
    proc_fs.links["/proc/200/fd/4"] = "socket:[63685]"

    assert _linux._pids_listening_on_tcp_port_via_proc(8080) == {200}
