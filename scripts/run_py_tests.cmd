@echo off
setlocal

set "REPO_ROOT=%~dp0.."
for %%I in ("%REPO_ROOT%") do set "REPO_ROOT=%%~fI"
set "TEST_TMP=%REPO_ROOT%\.codex-tmp-tests"
if not exist "%TEST_TMP%" mkdir "%TEST_TMP%"
set "TMP=%TEST_TMP%"
set "TEMP=%TEST_TMP%"

set TEST_MODULES=tests.test_jobs_fetcher tests.test_source_discovery tests.test_source_registry tests.test_source_sync tests.test_admin_bridge_ops tests.test_ship_update_manager tests.test_fetcher_metrics tests.test_admin_bridge_fetcher_metrics
set TEST_MODULES=%TEST_MODULES% tests.test_build_ship_bundle tests.test_runtime_launcher tests.test_build_portable_exe tests.test_packaged_desktop_smoke

where python >nul 2>nul
if %ERRORLEVEL% EQU 0 (
  python "%REPO_ROOT%\scripts\check_python_version.py"
  if %ERRORLEVEL% NEQ 0 (
    exit /b %ERRORLEVEL%
  )
  python -m unittest -v %TEST_MODULES%
  exit /b %ERRORLEVEL%
)

echo ERROR: Python not found. Install Python 3.13 and add it to PATH.
exit /b 1
