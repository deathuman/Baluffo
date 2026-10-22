## [0.3.024] - 2026-10-22

### Changed

- **Job rows paint ~50 ms sooner on a slow machine because the save button's plus sign stopped pulling in a
  second typeface.** The button used a fullwidth `＋`, which the UI font does not have, so every row rendered
  that glyph from a CJK fallback font. It is now a plain `+`. Nothing looks different; the rows' first painted
  frame moves from 592 → 552 ms at quarter speed.
- **Every remaining icon the pages drew as a character now draws as a path or a border.** Audited against the
  font files themselves: Segoe UI has no glyph for `✓`, so the jobs save button's saved state and the saved
  page's completed phase step were painted from Segoe UI Symbol, and the admin discovery-log's `▸`/`▾` marker
  came from Cambria Math — a serif math face shaping a chevron in a sans-serif panel. The tick is now the
  shared inline SVG path the action center already used, and the disclosure marker is a drawn triangle. Nothing
  looks different; two fewer typefaces are resolved to paint the UI.
- **The admin page shows its header before it lays out panels you have not scrolled to.** The discovery and
  source-sync sections start below the fold but were laid out on the first frame; they now skip layout until
  the shell has painted, so first paint lands 128 ms sooner on a quarter-speed machine (306 → 178 ms) and the
  first layout is 82 ms cheaper. Once the page is up nothing moves: those sections, their controls and their
  data appear exactly where they were.
- **Buttons, dropdowns and search boxes now use the app's font on all three pages.** Form controls
  never inherited it, so they rendered in the browser's default Arial (and monospace on the custom-job
  notes field) while the text around them used Segoe UI. They now match the rest of the page, which
  also makes every page's first paint cheaper — the browser no longer prepares a second typeface for
  the controls. Controls sit a few pixels taller: the jobs page grows by four pixels, the admin content
  panel by nine, the Add Custom Job panel by sixteen, and buttons with long labels get up to eleven
  pixels narrower.
- **No page downloads the storage or window handling it cannot use.** Hosted pages keep saved jobs,
  attachments, backups and sign-in on the server, and the packaged desktop app keeps them in its
  local bridge — neither ever used the browser's offline database, yet every page shipped it, and
  the hosted pages also shipped the desktop window's liveness and close handling. Each runtime now
  loads its own local-data client: the hosted jobs page's script is 245 KB instead of 295 KB, saved
  212 KB instead of 268 KB, admin 292 KB instead of 347 KB; the packaged pages are 46 to 52 KB
  lighter per page (jobs 236 KB instead of 284, saved 216 KB instead of 268, admin 296 KB instead of
  348). Saved jobs, backups, attachments, sign-in, the admin panels and the desktop app's window
  behaviour all work as before. Hosted pages appear at the same moment on a fast connection and
  save about a third of a second on a very slow one; packaged pages finish loading 15 to 25 ms
  sooner on a machine running at a quarter of this one's CPU speed.

- **The packaged pages stop carrying each other's UI handles, and one copy of the sign-in dialog
  instead of four.** Every page shipped the jobs, saved and admin handle tables plus the whole
  dialog module, whether or not it used them; each page now takes only its own handles, and the
  dialog's four near-identical builders share one frame. Packaged scripts are 7 to 10 KB smaller
  per page (jobs 236 KB, saved 216 KB, admin 296 KB). Every dialog and handle behaves exactly as
  before.

- **Every desktop page now loads as one script instead of dozens.** A packaged launch fetched each
  page module as its own file before the page could settle, and it did that on all three pages. The
  ship build now bundles admin, jobs and saved the same way the hosted version already was. Against
  the packaged site's own file server, alternating nine runs per variant: the jobs feed paints its
  rows in 101 ms instead of 182 ms, the admin page settles in 162 ms instead of 260 ms, and the saved
  page finishes booting in 58 ms instead of 122 ms. At a quarter of the CPU it is 488 vs 686 ms,
  573 vs 730 ms and 284 vs 374 ms — the slower the machine, the more those 118, 169 and 97 requests
  cost. First paint stays within about 10 ms either way, and the jobs page keeps the data-request
  head start it had before.

- **The admin page no longer waits for operations health before it settles.** Its bundled script is
  split: the page shell (stored profiles, action center, bridge badge) loads first, and the
  operations-health tabs arrive in a second file as soon as the admin data has been read. The shell
  script is 347 KB instead of 607 KB, and the ops half (273 KB) is fetched only after the page's own
  data settles. On a machine running at a quarter of this one's CPU speed, the admin page settles
  534 ms after launch instead of 596 ms; at full speed the difference is inside the run-to-run noise.
  The operations tabs render the same content either way — checked against a live bridge on both
  builds — and a page whose data never settles still loads them a moment later.

- **The jobs page shows its list before it wires up the controls.** The saved snapshot used to wait
  behind the filter presets, every click handler and the sign-in handshake. Those now start right
  after the first jobs appear: the list comes up about 7 ms sooner at full speed and about 38 ms
  sooner on a machine running at a quarter of this one's CPU speed, and the filter presets are ready
  roughly 20 ms after the rows. Filter values, saved jobs and the sign-in state settle exactly as
  before, and a page whose list never appears still wires its controls a moment later.

- **The jobs page no longer carries the update panel and run controls while it draws its list.**
  Those two parts now arrive in their own files once the list is on screen: 42 KB taken out of the
  page's own script, with the update panel and the job-run controls following it. The list comes up
  about 5 ms sooner at full speed and about 10 ms sooner at a quarter of this machine's CPU speed —
  small, because that wait is the saved snapshot and the first paint rather than the script — and the
  two deferred parts behave exactly as before against a live bridge. A page whose list never appears
  still loads the update panel a moment later.

- **The hosted jobs page starts fetching its list sooner.** The hosted page loads as one script, so
  the request for the saved list used to wait until that script had downloaded and run. It now goes
  out as soon as the page is parsed, and the desktop update panel — which can never appear on the
  hosted site — is no longer part of that script (33 KB smaller). Against the hosted server with the
  saved list regenerated for every run, the first rows appeared 701 ms after navigation instead of
  760 ms, and 921 ms instead of 1,150 ms on a machine running at a quarter of this one's CPU speed.
  With a saved list already on disk the gain is smaller (92 ms instead of 106 ms) because there the
  script itself is what the rows wait on. Nothing else about the page changed.

- **The hosted admin page no longer loads its operations half before it can show anything.** On the
  hosted site each page is a single script, so the operations-health tabs used to be parsed before
  the admin page could settle. They are now their own file: the page's shell is 347 KB instead of
  622 KB, and the 273 KB operations half is fetched when that section loads. The operations panels
  render the same content either way, and the files the shell does need are preloaded rather than
  discovered one round trip later. The hosted jobs page deliberately keeps one script: splitting off
  its job-run controls measured even — the first rows arrived 394 ms after navigation on both builds
  over ten alternating runs at a quarter of this machine's CPU speed — because those controls are
  only 5% of that page, so a second request would buy nothing.

### Notes

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.023:
  the same-origin Linux container,
  GHCR multi-arch image publishing,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
