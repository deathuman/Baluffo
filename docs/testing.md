# Testing

## Python tests (pytest)

Run the Python test suite:

```bash
npm run test:py
```

**Quick local runs (exclude slow tests):** To skip long-running tests (e.g. timeout/retry tests) and finish faster:

```bash
python -m pytest tests -q -m "not slow" --color=no
```

Slow tests are marked with `@pytest.mark.slow` in the codebase. The full suite includes them.

Run a quick timing sanity check (prints the slowest tests at the end):

```bash
npm run test:py:timing
```

Notes:
- `--durations=25` prints the 25 slowest tests.
- `--durations-min=0.2` only prints tests slower than 0.2s (adjust as needed).

If you want a full per-test breakdown once (noisy):

```bash
python -m pytest tests -q --durations=0 --color=no
```

## Test layout and fixtures

The Python suite is fully pytest (no `unittest.TestCase`). All tests are plain `def test_*` functions.

**Targeted runs:**

| Goal | Command |
|------|---------|
| Full suite | `npm run test:py` |
| One file | `python -m pytest tests/<path/to/test_*.py> -q` |
| Admin bridge | `python -m pytest tests/admin/ -q` |
| Exclude slow | `python -m pytest tests -q -m "not slow" --color=no` |

**Shared fixtures (where they are defined):**

| Fixture | Location |
|---------|----------|
| `repo_root`, `codex_tmp_root`, `make_test_root`, `source_sync_test_root` | `tests/conftest.py` |
| `admin_bridge_ops_root` | `tests/admin/conftest.py` |
| `workspace_tmpdir(prefix)` (context manager) | `tests/helpers/temp_paths.py` |

**Test-to-source map:**

| Area | Test path |
|------|-----------|
| Jobs pipeline / jobs_fetcher | `tests/test_jobs_fetcher.py` |
| Source discovery | `tests/test_source_discovery.py` |
| Admin bridge (registry, runtime, static fallback, sync) | `tests/admin/test_admin_bridge_ops_*.py` |
| Desktop app / launcher | `tests/test_desktop_app.py` |
| Source sync | `tests/test_source_sync.py` |
| Local data store, backup, config, etc. | `tests/test_<module>.py` |

## Frontend smoke tests (Playwright)

Playwright smoke tests are run by `npm run test:smoke` / `npm run test`.

`playwright.config.js` starts a local web server using `python -m http.server`.
Make sure `python` on your machine resolves to Python 3 (not Python 2), otherwise the web server will fail to start.

If needed for local development, set `PLAYWRIGHT_PYTHON=py` (or any Python 3 launcher) to override what Playwright uses.
