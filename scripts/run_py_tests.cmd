@echo off
setlocal

set TEST_MODULES=tests.test_jobs_fetcher tests.test_source_discovery tests.test_source_registry tests.test_source_sync tests.test_admin_bridge_ops tests.test_ship_update_manager tests.test_fetcher_metrics tests.test_admin_bridge_fetcher_metrics

where py >nul 2>nul
if %ERRORLEVEL% EQU 0 (
  py -3 -m unittest -v %TEST_MODULES%
  if %ERRORLEVEL% EQU 0 (
    exit /b 0
  )
)

where python >nul 2>nul
if %ERRORLEVEL% EQU 0 (
  python -m unittest -v %TEST_MODULES%
  exit /b %ERRORLEVEL%
)

echo ERROR: Python not found. Install Python 3 or add it to PATH.
exit /b 1
