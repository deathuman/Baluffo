# Umbrel Raw-LAN Deployment Plan

> - **Status:** Active plan, planning-only implementation gate
> - **Use this when:** preparing Baluffo for a future private Umbrel community app-store install with raw LAN access
> - **Canonical for:** proposed Umbrel deployment scope, non-implementation gate, raw LAN risk acceptance, container runtime requirements, and future validation plan
> - **Not canonical for:** current Baluffo runtime behavior, implemented Docker or Umbrel manifests, official Umbrel store submission, or released deployment support
> - **Then inspect:** [`../LOCAL_SETUP.md`](../LOCAL_SETUP.md), [`../admin-bridge-api.md`](../admin-bridge-api.md), [`../architecture-ai-map.md`](../architecture-ai-map.md), [`../testing.md`](../testing.md), and [`../RELEASE.md`](../RELEASE.md)
> - **Last updated:** 2026-05-31

## Planning Gate

This is a planning document only. Do not implement the deployment work yet.

The only immediate implementation that was requested for this phase is to write this plan under `docs/plans` and link it from [`../INDEX.md`](../INDEX.md). Do not create Dockerfiles, Umbrel app-store files, workflows, runtime code, images, tags, releases, or deployments until a separate explicit implementation request is given after this plan is accepted.

## Summary

Make full Baluffo installable on the local Umbrel at `192.168.50.61` from this same GitHub repo as a private Umbrel community app store. The future deployment should publish a public multi-arch GHCR image, run Baluffo as one container with one same-origin HTTP service, and expose it directly on the LAN at `http://192.168.50.61:8877/`.

Planning checks performed on 2026-05-31:

- The Umbrel root responded with HTTP 200 and page title `Umbrel`.
- Port `8877` on `192.168.50.61` refused connections, so it appeared unused at planning time.
- Current Baluffo has no checked-in Dockerfile, Compose file, or Umbrel app-store metadata.

## Future Implementation Changes

- Add a container runtime entrypoint, for example `python -m src.container_server --host 0.0.0.0 --port 8080 --data-dir /data`, that serves static UI and existing bridge API routes from one origin.
- Route API prefixes through the existing bridge handlers: `/desktop-local-data`, `/app`, `/registry`, `/sources`, `/discovery`, `/tasks`, `/fetcher`, `/ops`, `/sync`, `/source-policy`, and `/dedup`; all other `GET` requests should fall back to static/runtime data serving.
- Add frontend runtime config for container mode:
  - `bridge.sameOrigin: true`
  - `runtime.mode: "container"`
  - `runtime.localDataMode: "bridge"`
- Keep desktop-window semantics separate from container semantics. Container mode should use bridge-backed local data without enabling desktop owner-session lifecycle, desktop updater, browser heartbeat, or host-browser behavior.
- Keep bridge-backed local profiles, saved jobs, attachments, Admin actions, fetch/discovery/sync tasks, and runtime reports under `/data`.
- Seed `/data/defaults` and required first-run runtime JSON from checked-in defaults only when missing; never overwrite user data.
- Add Docker packaging using Python 3.13, existing locked requirements, Playwright Chromium/runtime dependencies, a non-root runtime user, `VOLUME /data`, and a healthcheck against `/ops/health`.
- Add image hygiene protection with `.dockerignore` rules for `.git`, `dist`, `_out`, `.tmp`, `node_modules`, runtime `data/*`, `data/local-user-data`, `*.db`, and ignored `packaging/github-app-sync-config*.json`.
- Add a GHCR workflow for `linux/amd64` and `linux/arm64`, publishing `ghcr.io/deathuman/baluffo`.
- Add Umbrel community app-store files in this repo when implementation is later approved:
  - `umbrel-app-store.yml` with store id `deathuman`
  - `deathuman-baluffo/umbrel-app.yml`
  - `deathuman-baluffo/docker-compose.yml`
  - `deathuman-baluffo/exports.sh`
  - reuse `packaging/baluffo.png` as the app icon.
- Use these Umbrel defaults:
  - app id/folder: `deathuman-baluffo`
  - app name: `Baluffo`
  - host port: `8877`
  - container port: `8080`
  - volume: `${APP_DATA_DIR}/data:/data`
  - no separate bridge port.
- Because raw LAN access was chosen, omit `app_proxy` for the Baluffo web route and document that `http://192.168.50.61:8877/` is intentionally not protected by Umbrel auth.

## Loopholes To Close

- Raw LAN access means anyone who can reach `192.168.50.61:8877` can reach Baluffo UI, Admin, and local-data routes. Do not expose this port to the Internet, public Wi-Fi, or broad VPN peers.
- Same-origin mode must not leave frontend calls pointing at the visitor's own `127.0.0.1:8877`.
- Container mode must not start desktop owner-session lifecycle, desktop updater, browser heartbeat, or host-browser `/desktop-local-data/open-url` behavior.
- Docker builds must not include local sync config secrets, local profiles, SQLite/runtime databases, logs, or fetched job artifacts from the developer machine.
- Fresh install means no automatic desktop data migration; backup/import can be used later.
- The bridge must not be exposed as a second unauthenticated host port. Raw LAN access is for the single combined UI/API port only.

## Test Plan

- Python tests for the combined server: static page load, dynamic `frontend-runtime-config.js`, `/ops/health`, `/desktop-local-data/session`, sign-in/save-job routes, unknown route handling, and static/runtime data precedence.
- Frontend unit tests for same-origin bridge base, bridge-backed local-data mode without `?desktop=1`, disabled desktop updater, and normal browser-native job links in container mode.
- Docker smoke: build image, run with a temp `/data`, poll `/ops/health`, load `jobs.html`, create a profile, save a job, restart the container, and verify persistence.
- Image hygiene check: assert the image does not contain `packaging/github-app-sync-config.json`, `packaging/github-app-sync-config.localkey.json`, `data/local-user-data`, local DBs, or fetched job artifacts.
- Umbrel smoke on `192.168.50.61`: add this repo as a private app store, install `Baluffo`, open `http://192.168.50.61:8877/`, run first bootstrap/fetch, save a job, restart the app, and verify data persists.

## Assumptions

- No deployment implementation should occur until explicitly requested after this plan is accepted.
- Target is a private app store in this repo, not official Umbrel store submission.
- Image distribution is public GHCR; the app-store metadata can still stay private.
- Data starts fresh on Umbrel.
- Raw LAN exposure is intentional and accepted.
- No new Python or Node dependencies are required.
- Sources checked during planning: [Umbrel App Framework](https://github.com/getumbrel/umbrel-apps/blob/master/README.md) and [Umbrel Community App Store template](https://github.com/getumbrel/umbrel-community-app-store/blob/master/README.md).
