# Ship Bundle Runbook (Windows Zip-First)

## Build

```powershell
python scripts/build_ship_bundle.py
```

Default output:

- `dist/baluffo-ship`

Optional output override:

```powershell
python scripts/build_ship_bundle.py --output-dir C:\temp\baluffo-ship
```

## Start from bundle

From bundle root:

```powershell
.\run-site.ps1 -Port 8080
.\run-bridge.ps1 -Host 127.0.0.1 -Port 8877
```

Or launch both:

```powershell
.\run-all.ps1 -SitePort 8080 -BridgeHost 127.0.0.1 -BridgePort 8877
```

## Runtime overrides

Bridge supports CLI and env configuration with precedence:

- `CLI > env > defaults`

CLI options:

- `--host`
- `--port`
- `--data-dir`
- `--log-format` (`human|jsonl`)
- `--log-level` (`info|debug`)
- `--quiet-requests`

Env options:

- `BALUFFO_BRIDGE_HOST`
- `BALUFFO_BRIDGE_PORT`
- `BALUFFO_DATA_DIR`
- `BALUFFO_BRIDGE_LOG_FORMAT`
- `BALUFFO_BRIDGE_LOG_LEVEL`

Example:

```powershell
$env:BALUFFO_DATA_DIR="C:\baluffo\data"
python scripts/admin_bridge.py --port 9988
```

## Troubleshooting

- `Address already in use`:
  - change `--port` or stop previous bridge process.
- Admin page shows bridge unavailable:
  - verify URL/port and local firewall prompts.
  - check bridge startup logs for bind or path errors.
- No files under expected data dir:
  - confirm `--data-dir` or `BALUFFO_DATA_DIR` and CLI/env precedence.
- Discovery/fetch launched from bridge writes unexpected location:
  - verify bridge `data_dir` in startup banner and child process args in task logs.
