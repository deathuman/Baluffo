> - **Class:** enhance-stable
> - **Trigger:** UI design pass of 2026-10-19 (interior.dev micro-interaction patterns applied to Jobs, Saved and Admin) reached package: pagination, copy button and loading button were accepted as shared primitives; the live evidence below reproduces on portable 0.3.021
> - **Verified against:** 7f144667 (repo), portable 0.3.021 `ship/app/versions/0.3.021` running on `127.0.0.1:8080` (live), 2026-10-19
> - **Status:** S0 baseline complete; S1–S5 not started

# Shared UI primitives: pager, pending button, copy button

This plan replaces three pieces of hand-rolled UI with shared vanilla-JS primitives in
`frontend/shared/ui/`, so the "nothing moves but ink" rule interior.dev calls
*the row never moves* is enforced by the component that owns the widget instead of by
per-page CSS reservations. It is a presentation change only: no bridge route, payload,
persisted-data or busy-flag contract changes.

Source material: [interior.dev](https://www.interior.dev/docs/) — Pagination, Loading
Button, Copy Button, Tooltip Group. Those are React + Motion components; the design
principles transfer, the code does not. Baluffo takes the behaviour notes, not the library.

## Decisions already taken (2026-10-19)

- **Shared primitives**, not page-local changes — the user chose this explicitly.
- **No native `disabled` for in-flight work.** `aria-busy` + `aria-disabled` only; native
  `disabled` stays for control locks (pipeline lock, source filters mid-task).
- **Ellipsis is a jump-to-page input**, not a static marker.
- **Shorter pending labels** (`Discovery running…`, not `Discovery Running...`).
- **Verified against a local instance**; the site is never public.
- **Desktop 11-slot pager, 9-slot mobile** — approved.
- **Collapsible banner is out of this slice** (next slice).
- **Outcome faces:** success face 0.9 s, error face 1.4 s, then revert. The busy face lasts
  as long as the busy flag (a run can legitimately be 15–20 min; nothing animates beyond
  the ring). Run-end results stay with the existing toasts.

## Baseline measured on the live instance (2026-10-19, portable 0.3.021)

Harness: `.tmp/ui-review/harness.mjs` (scratch; rebuild from this section if it is gone).
It loads the live pages, runs the real `syncAdminBusyUi()` and the real
`getVisiblePages()` from the served modules, and measures geometry with
`getBoundingClientRect()`.

| Measurement | Value |
|---|---|
| Live catalogue | 41,916 jobs, 4,192 pages at 10/page |
| Full-feed page count (pager signature from the 41,916 set) | 10,479 pages |
| Pager cells, page 1 / 4 / 5 / 300 / 10,475 / 10,476 | **8 / 9 / 11 / 11 / 11 / 9** |
| First-cell left edge, page 1 → 4 → 5 | **523 → 548 → 517 px** |
| Active-number left edge, page 4 → 5 | **652 → 649 px** (slides under the pointer) |
| `aria-current` elements / `<nav>` landmark | **0 / false** |
| Pager at 430 px viewport | right edge **400 px** vs card edge **380 px** — **overflows** |
| Admin busy buttons that change width | **9 of 18**; Run Discovery **121 → 161 px**, Uncapped R… **122 → 231 px**, Force Ignore Circuit **155 → 133 px** |
| Admin busy buttons that lose focus (native `disabled`) | **10** task buttons |
| Discovery run shape (last real report) | 1,191 survive → 240 probed → 88 failed → 133 healthy → 20 queued; GameDevMap audit **1,036 s** before queueing |

## Slices

Each slice is one commit (or one small commit series), independently shippable, with its
own gate. S1 must land first because `pager.js` is the template the other two follow.

### S1 — `pager.js` + Jobs adoption

Files: `frontend/shared/ui/pager.js` (new), `frontend/shared/ui/index.js` (export),
`frontend/jobs/app/runtime/list-view.js` (drop inline `renderPagination`), keep
`frontend/jobs/app/pagination.js` as the pure `getVisiblePages` (re-exported or replaced;
decide in-slice and delete whichever loses), `styles/components.css` `.pager*` rules,
`styles/jobs.css` `.jobs-page .pager` and the mobile block.

Behaviour:

- Desktop layout, constant at every page: `Prev 1 2 3 4 5 … N Next` (11 cells).
  Mobile (≤ 520 px): `Prev 1 … 4 … N Next` or `Prev 1 2 3 … N Next` (9 cells).
  `Prev`/`Next` are always mounted; at a limit they carry `aria-disabled="true"`,
  `tabindex="-1"` and refuse the click in the handler (no UA grey).
- `aria-current="page"` on the active number; active number is a `<button>` with
  `aria-disabled="true"` (keeps its slot, refuses the click).
- One always-mounted thumb inside a CSS grid track, moved with `transform`; the
  numbers are not animated individually except a 60–180 ms roll on cells whose text
  changed when the window slides.
- Jump: `…` is a `<button aria-label="Jump to page">`; activating it swaps that one
  cell for `<input inputmode="numeric" pattern="[0-9]*">` (identical slot), commits on
  Enter/blur (clamped 1..count), Escape cancels and restores `…`. Focus moves to the
  page button that is `aria-current` after a jump.
- Keyboard: `←/→` move focus within the pager, `Home/End` go to first/last page.
- `role="navigation" aria-label="Pagination"` on the container.
- Writes the same `data-page` attribute contract on buttons so `displayJobs`'s
  `contentSignature` guard and the existing click delegation keep working; pager
  `destroy()` removes the delegation on re-render.

Gate: unit tests `tests/frontend/unit/shared-pager.test.mjs` (fixed slot count at
first/middle/last; `aria-current`; arrow `aria-disabled` at limits; jump clamp + Escape
+ commit; mobile slot count; a 10,479-page case); `jobs-css-guards` and
`startup-css-performance` updated for the new `.pager` class names; live harness shows
**0 px** change in first-cell/active-cell position across pages 1/4/5/10478/10479 and no
overflow at 430 px.

### S2 — `pending-button.js` + `busy-state.js`

Files: `frontend/shared/ui/pending-button.js` (new), `frontend/shared/ui/index.js`,
`frontend/admin/app/busy-state.js` (`setButtonBusy` delegates), label strings in
`busy-state.js` / `fetcher.js` / `discovery` busy labels, `styles/components.css`
`.pending-btn*`, `styles/admin.css` + `styles/jobs.css` scoped overrides,
`frontend/jobs/app/pipeline.js` (in-flight `disabled` → `aria-disabled`).

Behaviour:

- Faces share one grid cell: idle · busy · success · error. Width = inherent widest
  face + 24 px slack, reserved before first interaction.
- Busy: `aria-busy="true"` + `aria-disabled="true"`, focus retained, 13 px ring
  appears after a 150 ms grace (no flash for fast settles), no other animation.
- Re-entrancy guard in the controller (second click while busy is ignored); the
  Jobs pipeline controller keeps its own `abortRequested`/`active` guards.
- Success face 0.9 s, error face 1.4 s, then revert to idle (error face copy names the
  next action, e.g. "Try again").
- `aria-live="polite"` sr-only status: one announcement per settle.
- Shorter labels: `Discovery running…`, `Uncapped discovery…`, `Fetching…`,
  `Incremental…`, `Force running…`, `Retry…`, `Loading report…`, `Testing…`,
  `Pulling…`, `Pushing…`, `Adding…`. Prefix-preserving so "Run Discovery" and
  "Run Uncapped Discovery" remain distinguishable.
- Genuinely-unavailable controls (registry bulk buttons during pipeline lock, source
  filters) keep native `disabled`, so they never use the busy face; `aria-busy` is not
  set on them.

Gate: unit tests `shared-pending-button.test.mjs` (width reserved, focus retained,
second click ignored, faces and revert timings with fake timers); `admin-busy-state`
and `jobs-pipeline` assertions move from `disabled === true` to `aria-disabled === "true"`
plus "second click ignored"; live harness (a short-running task, e.g. *Load Discovery
Report* on the live Admin) shows 0 px width change on all five fetcher presets and zero
lost focus in a keyboard walk.

### S3 — `copy-button.js` + migration

Files: `frontend/shared/ui/copy-button.js` (new), `frontend/shared/ui/index.js`,
`frontend/admin/app/action-center.js` (delete `copyTextToClipboard`, call the module),
`frontend/admin/app/ops/health-actions.js` (two handlers),
`frontend/admin/app/fetcher.js` (replace silent autopaste with an explicit copy
button next to the error; keep the VS Code task launch unchanged),
`frontend/admin/render/ops-run-detail.js` and `ops-summary-sections.js` (button markup
gains `data-copy-face` children), `styles/components.css` `.copy-btn*`.

Behaviour:

- `copyText(value)`: async clipboard when available; otherwise `execCommand` textarea
  with the prior selection restored; returns `{ok, reason}`. Empty string is a no-op.
- Faces: `Copy · Copied ✓ · Failed ✕`, fixed accessible name "Copy", success held
  2 s then revert, new copy restarts the timer, polite live region announces once.
- Tick draws via `stroke-dashoffset` 260 ms; reduced motion collapses to 0 ms.
- Plain-HTTP LAN deployments gain a working copy path (today only the Action Center
  falls back).

Gate: unit tests `shared-copy-button.test.mjs` (fallback used when clipboard refuses,
selection restored, error face on double refusal, timer restart); admin ops render
tests updated for the new markup; live harness: click a section "Copy diagnostics",
assert the copied face appears and the toast is not duplicated, then paste-read the
clipboard in the Playwright context.

### S4 — Tooltip delay (Tooltip Group pattern)

Files: `frontend/shared/ui/tooltip-controller.js`, `tests/frontend/unit/tooltip-controller.test.mjs`.

- First tooltip on contact; afterwards a 350 ms delay per trigger.
- Focus opens only when the element matches `:focus-visible`.
- One active tooltip at a time (already true); Escape + dismissal lock retained.
- Reposition via `transform`, not `left`/`top`.

Gate: unit tests for delay, `:focus-visible` gating, Escape lock, and repositioning;
live harness sweep across a toolbar shows one tooltip at a time.

### S5 — Docs, screenshots, closeout

- `docs/architecture-ai-map.md` styles sentence: `frontend/shared/ui/` now owns the UI
  primitives (pager, pending button, copy button, tooltip controller).
- Carry a durable "shared UI primitives" paragraph into `docs/testing.md` (how to run the
  focused unit tests) if the section does not already exist.
- Refresh `docs/screenshots/{jobs,saved,admin}.png` from the live instance at
  1365×900 (dark) — they are stale today (the Jobs one still shows a removed *Reload*
  button).
- `docs/INDEX.md`: add this plan under Operational / Process Docs with scope and
  routing.
- Delete this plan once S1–S4 land (DOCS_WORKFLOW plan lifecycle); git history keeps it.

## Non-goals

Saved segmented control, collapsible banner, value flash, task steps, progress-bar
consolidation, ripple, any Motion/npm dependency, React port, Admin registry-conflicts
pager, and any change to a bridge route, payload, or persisted shape.

## Verification stack (run per slice)

```bash
node --test tests/frontend/unit/shared-pager.test.mjs tests/frontend/unit/shared-pending-button.test.mjs tests/frontend/unit/shared-copy-button.test.mjs tests/frontend/unit/tooltip-controller.test.mjs
npm run test:frontend:unit
npm run test:frontend
npm run lint:repo-guardrails
```

Packaged gates when Admin or pipeline chrome is touched:
`npm run test:frontend:packaged:admin-startup`, `npm run test:frontend:packaged:jobs-pipeline`.

## Reproduction harness

The scratch harness lives at `.tmp/ui-review/harness.mjs` and is not committed. It needs
the portable instance (or any site build) on `REVIEW_SITE` (default
`http://127.0.0.1:8081`), a global Playwright install, and produces `shots/` plus
`report.json` with the measurement shape in the Baseline table. Rebuild it from that
table if the scratch directory is gone; the assertions are in S1's gate either way.
