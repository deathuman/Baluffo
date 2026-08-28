# Optional Playwright Browser Download Plan

> - **Status:** Parked — deferred until the next desktop portable release; recent release work is container/Umbrel-side and unaffected by this plan
> - **Use this when:** moving Playwright browser binaries out of the portable ZIP, changing packaged browser fallback install behavior, or revisiting first-start browser-support UX
> - **Canonical for:** proposed optional browser payload download behavior, packaging invariants, user-facing tradeoffs, and validation plan
> - **Not canonical for:** current v0.2.01 portable ZIP contents, released updater behavior, or implemented browser fallback runtime state
> - **Then inspect:** [`../RELEASE.md`](../RELEASE.md), [`../testing.md`](../testing.md), [`../TROUBLESHOOTING.md`](../TROUBLESHOOTING.md), [`../scraping-pipeline.md`](../scraping-pipeline.md), and [`../../scripts/build_portable_exe.py`](../../scripts/build_portable_exe.py)
> - **Last updated:** 2026-08-28 (status review — parked until the next desktop portable release)

## Summary

Move Playwright browser binaries out of the portable ZIP and make browser fallback an optional first-start download. The portable package should keep Playwright Python and driver support, including packaged `browsers.json`, `node.exe`, and `cli.js`, but should not embed `_internal/playwright/driver/package/.local-browsers/`.

On desktop start, Baluffo should show a nonblocking browser-support banner when the browser payload is missing. The banner explains the tradeoff and offers `Download browser support`, `Skip for now`, and `Don't ask again`.

The default implementation should use Playwright's native installer for `chromium-headless-shell`. Playwright may also download small `ffmpeg-*` and `winldd-*` companion payloads; accept those siblings because they simplify the implementation and avoid owning custom CDN URL and extraction logic.

## Key Changes

- **Packaging:** Change the portable build invariant from "embed exactly one `chromium_headless_shell-*`" to "embed no Playwright browser cache." Add a pre-zip guard that rejects any `_internal/playwright/driver/package/.local-browsers` payload while keeping the Playwright driver files needed for later install.
- **Runtime install:** Add a browser-payload service that reads the required revision from packaged `browsers.json`, stores browser downloads under `ship/data/playwright-browsers/`, and invokes the packaged Playwright driver command equivalent to `playwright install chromium-headless-shell`.
- **Environment:** Set `PLAYWRIGHT_BROWSERS_PATH` to `ship/data/playwright-browsers/` before any Playwright launch or child task process so browser fallback uses the portable data-owned cache.
- **Availability:** Treat `chromium_headless_shell-<revision>` as the required success marker. Allow Playwright-created `ffmpeg-*` and `winldd-*` siblings, but keep full Chromium, Firefox, and WebKit out of the app-owned cache.
- **Failure behavior:** If the payload is missing, browser fallback stays disabled with a clear actionable message while normal startup, jobs browsing, saved jobs, sync, updates, and non-browser fetch paths continue to work.

## User UX And Interfaces

Add bridge endpoints:

- `GET /app/browser-payload-status`
- `POST /app/download-browser-payload`
- `POST /app/browser-payload-preference`

Use this stable status payload shape:

```json
{
  "status": "missing",
  "requiredBrowser": "chromium-headless-shell",
  "revision": "1208",
  "cacheRootKind": "ship-data",
  "promptDisabled": false,
  "lastError": ""
}
```

Status values are `missing`, `downloading`, `available`, and `failed`. Persist retryable failures and non-secret install logs under `ship/data/browser-payload/`. `Skip for now` hides the banner only for the current session; `Don't ask again` persists through the preference endpoint.

The banner copy should be direct:

- Pros: smaller portable ZIP, faster app/update downloads, and browser fallback works after install.
- Cons: requires internet, adds browser files under `ship/data`, browser fallback is unavailable until installed, and failed downloads may need retry.

## Test Plan

- **Unit tests:** Cover browser revision resolution from `browsers.json`, missing vs available status, installer command construction with `PLAYWRIGHT_BROWSERS_PATH`, accepted `ffmpeg-*` and `winldd-*` siblings, persisted retryable failures without secrets, and `promptDisabled` persistence.
- **Build tests:** Update portable build tests so `.local-browsers` is rejected and generated ZIP inspection confirms no embedded Playwright browser cache.
- **Route/frontend tests:** Cover the new GET/POST routes, first-start banner visibility, and Download / Skip / Don't ask again actions.
- **Packaged smoke:** Launch portable with no browser cache and confirm startup succeeds, status is `missing`, and the banner is visible. Mock the installer path in CI to prove route/UI/state behavior without a large network download.
- **Manual network smoke:** Keep a manual or optional gate that runs the real Playwright installer and verifies a browser fallback launch succeeds afterward.

## Assumptions

- No new Python or Node dependencies.
- Browser support is optional; the app remains useful without it.
- The portable ZIP should prioritize smallest initial user download over fully offline browser fallback.
- Playwright installer companion downloads `ffmpeg-*` and `winldd-*` are acceptable because they are small and keep the implementation simple.
- Existing `v0.2.01` release behavior remains unchanged; this is a future-release plan.
