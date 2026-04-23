# Desktop Update Cross-Stack Boundary Cleanup

> Historical refactor record preserved for archive/reference use. For current routing, start with [`../../INDEX.md`](../../INDEX.md), [`../../AI_ASSISTANT_GUIDE.md`](../../AI_ASSISTANT_GUIDE.md), and [`../../architecture-ai-map.md`](../../architecture-ai-map.md).

## What Landed

- `src/ship/desktop_updater.py` stayed as the stable updater helper executable and patch surface.
- Updater implementation ownership moved into `src/ship/desktop_updater_{ui,release,install}.py`.
- `frontend/jobs/app/desktop-update.js` stayed as the stable Jobs desktop-update export surface while the implementation moved into `frontend/jobs/app/desktop-update-{model,dom,controller}.js`.

## Final Owning Surfaces

- Stable roots: `src/ship/desktop_updater.py`, `frontend/jobs/app/desktop-update.js`
- Owning leaves: `src/ship/desktop_updater_{ui,release,install}.py` and `frontend/jobs/app/desktop-update-{model,dom,controller}.js`

## Current Routing

Current routing lives in the active wiki, especially [`../../AI_ASSISTANT_GUIDE.md`](../../AI_ASSISTANT_GUIDE.md), [`../../architecture-ai-map.md`](../../architecture-ai-map.md), [`../../RELEASE.md`](../../RELEASE.md), and [`../../admin-bridge-api.md`](../../admin-bridge-api.md).

## Historical Notes

- `src/ship/update_manager.py` remained the canonical updater-capability owner.
- This pass intentionally kept both roots tiny and patch-friendly so existing updater tests and Jobs runtime imports did not need to move.
