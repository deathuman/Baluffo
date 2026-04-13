@echo off
setlocal

set "REPO_ROOT=%~dp0.."
for %%I in ("%REPO_ROOT%") do set "REPO_ROOT=%%~fI"
set "TEST_TMP=%REPO_ROOT%\.tmp\pytest"
if not exist "%TEST_TMP%" mkdir "%TEST_TMP%"
set "TMP=%TEST_TMP%"
set "TEMP=%TEST_TMP%"
set "PYTEST_BASETEMP=%TEST_TMP%\basetemp"
set "PYTHON_CMD=python"

%PYTHON_CMD% --version >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
  echo ERROR: Python 3.13 launcher target not found. Install Python 3.13 and ensure the py launcher is available.
  exit /b 1
)

%PYTHON_CMD% "%REPO_ROOT%\scripts\check_python_version.py"
if %ERRORLEVEL% NEQ 0 (
  exit /b %ERRORLEVEL%
)

%PYTHON_CMD% -m pytest tests -q -m "not slow and not packaging and not release" --color=no --basetemp="%PYTEST_BASETEMP%"
exit /b %ERRORLEVEL%
