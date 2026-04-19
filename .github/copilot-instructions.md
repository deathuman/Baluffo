<!-- .github/copilot-instructions.md -->
# Copilot / AI agent instructions for Baluffo

Purpose
- Brief: Help contributors and AI agents work productively on this small static web project.

Big picture
- This is a Windows-first, local-first jobs app with multiple HTML entrypoints: `index.html`, `jobs.html`, `saved.html`, and `admin.html`.
- Shared frontend logic lives under `frontend/` as ES modules. The app is still served as static HTML/CSS/JS without a bundler.
- Styles are split under `styles/`: `base.css` for tokens and page foundations, `components.css` for shared UI, and page-scoped files such as `jobs.css`, `saved.css`, and `admin.css`.

Key files (examples)
- `jobs.html`, `saved.html`, `admin.html`: primary UI entrypoints; keep their stylesheet includes aligned with the split `styles/` layout.
- `frontend/`: shared and page-specific ES modules; prefer editing the narrow owner module instead of reviving root-level script files.
- `styles/`: shared and page-scoped CSS; keep shared primitives in `base.css` / `components.css` and page-specific polish in the page stylesheet.

Project-specific patterns & conventions
- No build system or dependencies — edits are reflected by reloading the HTML entrypoints in a browser.
- Preserve the current split stylesheet ownership; do not collapse page CSS back into a root `styles.css` shim.
- Buttons and shared UI primitives should stay in the shared CSS layer unless they are clearly page-owned.
- IDs and `data-ui` hooks are primary selectors for JS wiring. Prefer updating markup and the owning frontend module together.

Developer workflows (how to run / test)
- Quick preview: open `index.html` in a browser or run a local static server from the repo root.
  - Recommended (Node): `npx serve .` (or any static server)
  - Recommended (Python): `python -m http.server 8000` then open `http://localhost:8000`
- Debugging: use browser DevTools. Inspect elements referenced by `app.js` and watch console for uncaught errors.

What to look for when editing
- Keep DOM IDs and `data-ui` hooks in sync with the corresponding frontend modules.
- Keep styles modular: shared rules belong in `styles/base.css` or `styles/components.css`; page-only rules belong in the matching page stylesheet.
- Prefer editing the smallest owning frontend module rather than introducing new root-level scripts.

Integration points & external deps
- None currently. There are no external APIs, packages, or build steps.

Examples (common tasks)
- Adjust Jobs page presentation: edit `jobs.html`, the owning `frontend/jobs/...` module, and `styles/jobs.css` if the change is page-specific.
- Adjust shared UI styling: update `styles/components.css`, and only touch page CSS when the behavior or polish is page-specific.

Agent guidance (do / don't)
- Do: make minimal, focused edits; update both HTML and JS for ID/name changes.
- Do: include a short rationale in PR descriptions for behavior changes.
- Don't: add large frameworks or new build tooling without explicit human approval.

If this file needs expansion
- Mention more complex workflows if the project grows (tests, build, CI). For now, keep instructions minimal and concrete.

Feedback
- If any sections are unclear or you want additional examples (tests, CI, or packaging), say which area to expand.
