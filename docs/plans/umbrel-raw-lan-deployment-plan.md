# Umbrel Raw-LAN Deployment Plan

> - **Status:** Implemented in repo; image publication and Umbrel smoke pending
> - **Use this when:** preparing, validating, or operating Baluffo as a private Umbrel community app-store install with raw LAN access
> - **Canonical for:** container runtime scope, same-origin UI/API behavior, Umbrel raw-LAN exposure, GHCR image target, and validation checklist
> - **Not canonical for:** official Umbrel store submission, public Internet exposure, or desktop packaged updater behavior
> - **Then inspect:** [`../admin-bridge-api.md`](../admin-bridge-api.md), [`../RELEASE.md`](../RELEASE.md), [`../testing.md`](../testing.md), [`../storage-contract.md`](../storage-contract.md), and [`../architecture-ai-map.md`](../architecture-ai-map.md)
> - **Last updated:** 2026-06-01

## Implementation Status

The original planning gate is closed. A separate explicit implementation request was given on 2026-06-01, and the repository now contains the container runtime, Docker packaging, GHCR workflow, and private Umbrel community app-store metadata.

This does not mean a release tag, GHCR image, or Umbrel deployment has already been published. Publication still requires the GitHub workflow or an operator build/push, followed by an Umbrel smoke test on `192.168.50.61`.

## Summary

Baluffo can run on the local Umbrel at `192.168.50.61` from this same GitHub repo as a private community app store. The target deployment is one container, one same-origin HTTP service, public multi-arch GHCR image `ghcr.io/deathuman/baluffo`, persistent `/data`, and raw LAN access at:

```text
http://192.168.50.61:8877/
```

The Umbrel app uses the standard `app_proxy` service with `PROXY_AUTH_ADD: "false"`. This preserves the Umbrel app format while intentionally disabling Umbrel auth for raw LAN access.

## Implemented Changes

- Container entrypoint: `python -m src.container_server --host 0.0.0.0 --port 8080 --data-dir /data`.
- Combined server routing: API prefixes dispatch first, then static UI assets, `favicon.ico`, images, CSS, JS, runtime data, and unknown page routes are served from the UI root.
- Cache policy: API, runtime config, runtime data, and HTML responses stay `no-store`; static CSS/JS/images use `Cache-Control: public, max-age=3600`.
- CORS policy: container mode is same-origin only and does not emit browser CORS allow headers. Desktop/non-container bridge serving keeps its localhost split-origin CORS behavior.
- Static `/data` serving is allowlisted to public runtime reports, registry/discovery exports, contracts, and defaults. Local profiles, saved jobs, attachments, backups, and other user files stay behind bridge routes.
- Container runtime config:
  - `bridge.sameOrigin: true`
  - `runtime.mode: "container"`
  - `runtime.localDataMode: "bridge"`
- Frontend bridge resolution treats explicit same-origin mode as relative URLs instead of falling back to `http://127.0.0.1:8877`.
- Frontend runtime mode is split into browser, desktop, and container paths. Container mode uses bridge-backed local data without desktop lifecycle, updater UI, `?desktop=1` navigation params, owner-session heartbeat, close beacons, or host-browser open behavior.
- Container mode disables desktop-only routes with `{ "ok": false, "error": "not available in container mode" }`:
  - `/desktop-local-data/open-url`
  - `/app/update-status`
  - `/app/check-for-update`
  - `/app/download-update`
  - `/app/install-update`
  - `/app/desktop-session-lifecycle`
- Runtime profiles, saved jobs, attachments, reports, logs, source registries, and SQLite `baluffo-runtime.db` resolve under `/data`.
- First-run seeding copies defaults and creates required runtime JSON skeletons only when missing; existing user data is never overwritten.
- Container port comes from CLI/env/Compose (`8080` inside, `8877` outside through Umbrel), not desktop `baluffo.config.json` ports.
- Umbrel Compose publishes host port `8877` to container port `8080` while keeping `app_proxy` with `PROXY_AUTH_ADD: "false"` for standard Umbrel app integration.
- Docker packaging uses Python 3.13, `requirements-lock.txt`, baked Playwright Chromium, a non-root user, `VOLUME /data`, and a healthcheck against `/ops/health`.
- Image hygiene is protected by `.dockerignore` rules excluding local secrets, sync config, local profiles, DBs, logs, `_out`, and fetched artifacts.
- GHCR workflow builds `linux/amd64` and `linux/arm64` with Docker buildx and QEMU, publishing `ghcr.io/deathuman/baluffo`.
- Umbrel community app-store files live in this repo:
  - `umbrel-app-store.yml`
  - `deathuman-baluffo/umbrel-app.yml`
  - `deathuman-baluffo/docker-compose.yml`
  - `deathuman-baluffo/exports.sh`

## Exposure Contract

Raw LAN access means anyone who can reach `192.168.50.61:8877` can reach Baluffo UI, Admin, and local-data routes. Do not expose this port to the Internet, public Wi-Fi, or broad VPN peers.

Umbrel proxy auth is intentionally disabled for this raw-LAN app, but the container service remains browser same-origin: arbitrary external web origins should not receive CORS allow headers for API or static responses.

Only the combined UI/API service should be exposed. The bridge must not be exposed as a second unauthenticated host port.

Fresh Umbrel installs start with a new `/data` volume. There is no automatic desktop data migration; backup/import can be used later if migration is desired.

## Validation Plan

- Python: combined API/static routing, same-origin runtime config, `/ops/health`, route fallbacks, cache headers, disabled desktop-only routes, `/data` seeding, and SQLite location.
- Frontend unit: same-origin bridge base, container bridge-backed local data without `?desktop=1`, disabled desktop updater/lifecycle, and normal container link behavior.
- Docker smoke: build, run with temp `/data`, poll health, load UI, create profile, save job, restart, verify persistence, and verify no desktop data paths are used.
- Image hygiene: assert local secrets, sync config, profiles, SQLite DBs, logs, `_out`, and fetched artifacts are excluded.
- Umbrel smoke on `192.168.50.61`: add the private app store, install Baluffo, open `http://192.168.50.61:8877/`, run bootstrap/fetch, save a job, restart app, and verify persistence.

## Assumptions

- Raw LAN access is intentional and accepted.
- Target is a private community app store in this repo, not official Umbrel store submission.
- Image distribution is public GHCR.
- Umbrel data starts fresh unless an operator later imports a backup.
- No new Python or Node dependency files were added.
- Sources checked during planning: [Umbrel App Framework](https://github.com/getumbrel/umbrel-apps#readme) and [Umbrel Community App Store template](https://github.com/getumbrel/umbrel-community-app-store#readme).
