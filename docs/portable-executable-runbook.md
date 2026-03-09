# Portable Executable Runbook (Windows)

Baluffo supports a Windows-first portable executable wrapper built with `pywebview` and `PyInstaller`.

## Build prerequisites

```powershell
py -3.13 -m pip install -r requirements-desktop.txt
```

## Build

```powershell
py -3.13 scripts/build_portable_exe.py --bundle-version 1.2.3
```

Optional custom icon:

```powershell
py -3.13 scripts/build_portable_exe.py --bundle-version 1.2.3 --icon C:\path\to\Baluffo.ico
```

Current baseline:

- build with Python 3.13 on Windows
- keep the normal ship bundle on `py -3`
- do not build desktop EXE with Python 3.14 in this environment: `pywebview` install fails because `pythonnet` wheel build fails, which produces an EXE without desktop webview support

Outputs:

- `dist\baluffo-portable`
- `dist\baluffo-portable-1.2.3.zip`

By default the build now generates a branded `.ico` automatically and embeds it into `Baluffo.exe`.

## Portable layout

- `Baluffo.exe`: desktop window entrypoint
- `ship\`: embedded versioned runtime bundle
- `ship\data\`: local persistent runtime/user data
- `ship\data\local-user-data\`: desktop-only local profiles, saved jobs, notes, activity, and attachment files

## Runtime behavior

- the executable starts the local static site in the background
- the executable starts the local admin bridge in the background
- the desktop site now uses a fixed local port instead of a random one so desktop browser-origin state stays stable across relaunches
- readiness checks:
  - site: `jobs.html`
  - bridge: `/ops/health`
- after both are ready, the app opens in a dedicated window
- child processes stop when the desktop window closes
- desktop pages use a bridge-backed file store for core local user data instead of WebView-local IndexedDB/localStorage
- on Windows, startup checks for Microsoft Edge WebView2 Runtime and offers to open the installer if it is missing

## Notes

- v1 executable packaging is Windows-only
- the underlying ship/update model remains zip-first
- PowerShell launchers remain available for operator/debug usage
- future macOS/Linux support requires replacing remaining Windows-specific update/launcher assumptions
