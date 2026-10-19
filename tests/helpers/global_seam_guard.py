"""Guards for the process-global transport seams a test can hand to the next one.

The corpus capture layer patches ``urlopen``/``build_opener``/``httpx.Client`` for the length of a
session, and restores them by name. Restoring the names is not the whole contract: ``urllib``
keeps its default opener in its *own* module state, and ``urlopen`` builds it lazily *through*
``build_opener``. A build that happened while a session was live therefore installed an opener
whose ``open`` was the session's shadow, and that object stayed in ``urllib.request._opener`` for
the rest of the process. Both patched names read as restored, and the failure appeared in an
unrelated file: a container-gateway ETag assertion that received ``_ReplayHeaders`` instead of
``HTTPMessage``, only under ``-n 4``, where a corpus file and the gateway file shared a worker.

:func:`leaked_global_seams` is that detector, and it deliberately does **not** compare the opener
by identity. Snapshotting ``_opener`` before every test and demanding the same object afterwards
would fail a great many innocent tests: urllib builds the opener lazily on the first ``urlopen``
(``None`` -> a real ``OpenerDirector``), and a comparison of ``_opener.open`` by identity is
always unequal because each access makes a new bound method. Neither is a leak. What a leak looks
like is a watched callable or class that is not the object it was, or an installed opener whose
``open`` is *not* ``OpenerDirector.open`` -- a shadow, whoever installed it. Both are state, not
identity, so the guard can be strict without being noisy.

``global_seam_guard`` lives here rather than in ``tests/conftest.py`` so the self-test in
``tests/test_global_seam_guard.py`` can load the *same* fixture into a child pytest process (as a
plugin) and watch it fail a leaking test: a guard nothing exercises is a guard that can go
silently blind.
"""

from __future__ import annotations

import importlib
import unittest.mock
from collections.abc import Iterator, Mapping
from typing import Any

import pytest

# Seams compared by identity: a test that patches one must put back exactly the object it found.
# ``urllib.request._opener`` is handled separately -- see the module docstring for why.
IDENTITY_SEAMS: tuple[str, ...] = (
    "urllib.request.urlopen",
    "urllib.request.build_opener",
    "urllib.request.OpenerDirector.open",
    "httpx.Client",
)

_OPENER = "urllib.request._opener"

_GUARD_MESSAGE = (
    "process-global seam left changed by {test}: {seams}. Restore it in the test that changed"
    " it -- a context manager, a ``finally``, or ``monkeypatch`` -- so the change cannot outlive"
    " the test."
)

_MISSING = object()


def _resolve(name: str) -> tuple[bool, Any]:
    """Resolve a dotted name to its value, walking module *and* instance attributes.

    The walk starts at the longest importable prefix, so ``urllib.request.OpenerDirector.open``
    lands on the class attribute rather than stopping at the module. The modules are imported here
    rather than at module import, so this guard never makes an optional dependency mandatory.

    A path that resolves through ``None`` -- no default opener built yet -- is a *resolved* name
    whose value is ``None``, not a missing one: the snapshot has to record ``None`` so a later
    build can be told apart from a replacement.
    """

    parts = name.split(".")
    value: Any = None
    consumed = 0
    for index in range(len(parts) - 1, 0, -1):
        try:
            value = importlib.import_module(".".join(parts[:index]))
        except ImportError:
            continue
        consumed = index
        break
    if value is None:
        return False, None
    for part in parts[consumed:]:
        value = getattr(value, part, None)
    return True, value


def _describe(value: object) -> str:
    """A short name for a seam's value: what a reader would grep for."""

    if value is None:
        return "None"
    if isinstance(value, unittest.mock.Mock):
        return type(value).__name__
    qualname = getattr(value, "__qualname__", "")
    module = getattr(value, "__module__", "")
    if isinstance(qualname, str) and qualname:
        return f"{module}.{qualname}" if isinstance(module, str) and module else qualname
    kind = type(value)
    return f"{kind.__module__}.{kind.__qualname__}"


def current_global_seams() -> dict[str, Any]:
    """Snapshot the watched seams, skipping any this process cannot resolve."""

    seams: dict[str, Any] = {}
    for name in IDENTITY_SEAMS:
        resolved, value = _resolve(name)
        if resolved:
            seams[name] = value
    resolved, opener = _resolve(_OPENER)
    if resolved:
        seams[_OPENER] = opener
    return seams


def _shadowed_opener() -> str:
    """Name the installed opener's ``open`` when it is a shadow rather than the real method.

    This is the leak's own signature, and it is state: the opener object may be exactly the one
    urllib built, with only its ``open`` replaced. Hidden behind an identity comparison either
    way -- same object, different method, or different object, real method (a lazy build).
    """

    opener = getattr(importlib.import_module("urllib.request"), "_opener", None)
    if opener is None:
        return ""
    open_attr = getattr(opener, "open", None)
    real = getattr(importlib.import_module("urllib.request").OpenerDirector, "open", None)
    if getattr(open_attr, "__func__", open_attr) is not real:
        return f"{_OPENER}.open: {_describe(getattr(open_attr, '__func__', open_attr))}"
    return ""


def leaked_global_seams(before: Mapping[str, Any]) -> list[str]:
    """Name every watched seam that came back changed, in seam order."""

    changed: list[str] = []
    after = current_global_seams()
    for name in IDENTITY_SEAMS:
        if name not in before:
            continue
        current = after.get(name, _MISSING)
        if current is not before[name]:
            shown = "unresolved" if current is _MISSING else _describe(current)
            changed.append(f"{name}: {_describe(before[name])} -> {shown}")

    # An opener that did not exist when the test began and now does is urllib's own lazy build,
    # which is normal. An opener that *replaced* a built one is a test's doing, and a shadowed
    # ``open`` is a leak whether the object is new or the same one with a swapped method.
    if _OPENER in before:
        previous = before[_OPENER]
        current = after.get(_OPENER, _MISSING)
        if previous is not None and current is not previous:
            shown = "unresolved" if current is _MISSING else _describe(current)
            changed.append(f"{_OPENER}: {_describe(previous)} -> {shown}")
    shadow = _shadowed_opener()
    if shadow:
        changed.append(shadow)
    return changed


def global_seam_guard_message(before: Mapping[str, Any], *, test: str = "") -> str:
    """The failure text for :func:`global_seam_guard`, or ``""`` when no seam came back changed."""

    changed = leaked_global_seams(before)
    if not changed:
        return ""
    return _GUARD_MESSAGE.format(test=test or "this test", seams="; ".join(changed))


@pytest.fixture(autouse=True)
def global_seam_guard(request: pytest.FixtureRequest) -> Iterator[None]:
    """Fail the test that leaves a process-global transport seam changed.

    Reported as a teardown error, the same way ``compat_seam_guard`` reports, because the
    comparison has to happen after the test's own teardown restored what it meant to restore.
    Registered for the whole suite by ``tests/conftest.py``, which imports this fixture by name;
    autouse fixtures are set up before a test's own, so this runs after ``monkeypatch`` and any
    ``addfinalizer`` have put their patches back.

    The check runs at *setup* as well: a leak installed by an earlier test in the same worker
    must be named too, or the first test to notice it is the one blamed for it.
    """

    before = current_global_seams()
    inherited = global_seam_guard_message(before, test=f"{request.node.nodeid} (inherited)")
    if inherited:
        pytest.fail(inherited)
    yield
    message = global_seam_guard_message(before, test=request.node.nodeid)
    if message:
        pytest.fail(message)


__all__ = [
    "IDENTITY_SEAMS",
    "current_global_seams",
    "global_seam_guard",
    "global_seam_guard_message",
    "leaked_global_seams",
]
