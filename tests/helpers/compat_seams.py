"""Guards for the leaf bindings ``src.jobs.fetcher_compat_runtime`` installs per call.

The compat shims copy a binding from the ``src.jobs_fetcher`` root onto a leaf module for the
duration of one call. Two of them restore only a value they *found* there, and ``src.jobs.pipeline``
never binds ``build_redirect_resolver`` itself, so the copy is what installs the name: when the
first pipeline run in a process happens while a test has the facade mocked, the fake keeps that
name for the rest of the process -- every later file an xdist worker runs included. A fake
resolver that maps every link to one URL then rewrites another file's rows, which is how
``tests/tools/test_corpus_parity.py`` failed in CI while passing on its own.

Two things live here: :func:`leaked_compat_seams`, which the suite-wide autouse guard in
``tests/conftest.py`` uses to fail a test that leaves a test double installed, and
:func:`patched_root_redirect_resolver`, which restores the seam exactly for tests that mock the
facade on purpose.
"""

from __future__ import annotations

import importlib
import unittest.mock
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

# One entry per binding a ``fetcher_compat_runtime`` shim writes onto a leaf module:
# ``run_pipeline`` -> build_redirect_resolver, ``run_scrapy_static_source`` -> registry_entries,
# ``build_redirect_resolver`` -> httpx, ``maybe_fetch_job_listing_html`` -> urlopen.
COMPAT_SEAM_LEAVES: tuple[tuple[str, str], ...] = (
    ("src.jobs.pipeline", "build_redirect_resolver"),
    ("src.jobs.adapters.static_scrapy", "registry_entries"),
    ("src.jobs.transport", "httpx"),
    ("src.jobs.adapters.html_parsers", "urlopen"),
)


def _is_test_double(value: object) -> bool:
    """Whether a seam holds something no product run could have put there.

    A mock is the obvious case (a copied fixture fake). A class defined under ``tests`` is the
    same leak in hand-rolled form: ``DummyRedirectResolver()`` copied off the facade. The real
    bindings -- the transport builder, ``registry_entries``, ``httpx``, ``urlopen`` -- match
    neither test.
    """

    if isinstance(value, unittest.mock.NonCallableMock):
        return True
    return type(value).__module__.startswith("tests")


def leaked_compat_seams() -> list[str]:
    """Name every compat seam currently holding a test double, in seam order."""

    leaked: list[str] = []
    for module_name, attribute in COMPAT_SEAM_LEAVES:
        module = importlib.import_module(module_name)
        if not hasattr(module, attribute):
            continue
        if _is_test_double(getattr(module, attribute)):
            leaked.append(f"{module_name}.{attribute}")
    return leaked


@contextmanager
def patched_root_redirect_resolver(resolver: Any) -> Iterator[Any]:
    """Patch the facade's resolver factory and leave ``src.jobs.pipeline`` as it was found.

    ``mock.patch.object(jf, "build_redirect_resolver", ...)`` alone is not enough: the shim above
    copies whatever the facade holds onto the pipeline module and keeps the copy when it found no
    previous value.
    """

    facade = importlib.import_module("src.jobs_fetcher")
    pipeline_mod = importlib.import_module("src.jobs.pipeline")
    missing = object()
    previous = pipeline_mod.__dict__.get("build_redirect_resolver", missing)
    with unittest.mock.patch.object(
        facade, "build_redirect_resolver", return_value=resolver
    ) as builder:
        try:
            yield builder
        finally:
            if previous is missing:
                pipeline_mod.__dict__.pop("build_redirect_resolver", None)
            else:
                pipeline_mod.__dict__["build_redirect_resolver"] = previous
