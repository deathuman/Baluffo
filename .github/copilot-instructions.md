<!-- .github/copilot-instructions.md -->
# Copilot / AI agent instructions for Baluffo

Purpose
- Brief: Help contributors and AI agents work productively on this small static web project.

Big picture
- This is a tiny static site delivered from three primary files: `index.html`, `styles.css`, and `app.js`.
- `index.html` contains the DOM structure and loads `app.js`.
- `app.js` implements small, event-driven UI logic (button -> popup). The data source is the in-file `descriptions` array.

Key files (examples)
- `index.html`: root markup and IDs used by JS (`learn-more-btn`, `popup-overlay`, `popup-description`, `popup-close-btn`).
- `app.js`: single-file DOM scripting; modify here for behavior changes.
- `styles.css`: visual styles including the `.hidden` utility class used to toggle the popup.

Project-specific patterns & conventions
- No build system or dependencies — edits are reflected by reloading `index.html` in a browser.
- UI state is toggled with the `hidden` class on `#popup-overlay`. Respect this pattern when changing show/hide behavior.
- Buttons use the `.btn` utility class; close actions use `.btn-close`.
- IDs are primary selectors for JS wiring. Prefer updating IDs consistently across `index.html` and `app.js`.

Developer workflows (how to run / test)
- Quick preview: open `index.html` in a browser or run a local static server from the repo root.
  - Recommended (Node): `npx serve .` (or any static server)
  - Recommended (Python): `python -m http.server 8000` then open `http://localhost:8000`
- Debugging: use browser DevTools. Inspect elements referenced by `app.js` and watch console for uncaught errors.

What to look for when editing
- Keep DOM IDs in sync: when renaming an ID in `index.html`, update `app.js` references.
- Keep styles modular: prefer class-based styling (`.btn`, `.popup`) rather than inline styles.
- Small JS only: if a feature grows, suggest introducing a lightweight module structure (ES modules) and document it here.

Integration points & external deps
- None currently. There are no external APIs, packages, or build steps.

Examples (common tasks)
- Change popup wording: edit `descriptions` array in `app.js`.
- Add another button that shows a fixed message: add element to `index.html`, add `getElementById` and `addEventListener` in `app.js`, and reuse `.btn` styling.

Agent guidance (do / don't)
- Do: make minimal, focused edits; update both HTML and JS for ID/name changes.
- Do: include a short rationale in PR descriptions for behavior changes.
- Don't: add large frameworks or new build tooling without explicit human approval.

If this file needs expansion
- Mention more complex workflows if the project grows (tests, build, CI). For now, keep instructions minimal and concrete.

Feedback
- If any sections are unclear or you want additional examples (tests, CI, or packaging), say which area to expand.
