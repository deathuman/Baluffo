## [0.3.001] - 2026-09-30
> Shared Desktop + Umbrel rollup: the rebuilt Admin surface, the bridge
> connection fixes that make it usable in the packaged app, and the release
> automation repairs. Anyone upgrading from 0.2.x gets the whole 0.3 line here.

### Changed

- **The Admin status panel is rebuilt and now actually tells you something.** It was a large, mostly empty card that could only ever show "everything is fine" or "signals unavailable" as visually identical text, using emoji icons. It now shows a clear, distinct state for each situation — all good, partly working, something needs attention, checks unavailable, or work is running — with matching colours and simple line icons. When everything is fine it collapses to a single tidy line instead of taking over the top of the page. You also get a "checked N seconds ago" stamp so you know how current the answer is, and a plain-language summary of any storage or sync problem rather than a restatement of its own headline.

- **Clicking a job run now opens a proper detail view.** Previously it expanded an inline panel — and the side drawer meant to show run detail could not open at all, because of a styling bug that kept it permanently hidden. Run details now open in that drawer with a clearer layout: what ran, key numbers, the slowest sources, one combined timeline, and suggested next steps. Empty sections are left out rather than shown blank. Two related bugs are fixed: rows inside the "Recent runs" and "Older runs" lists were unclickable, and on narrow screens the Stop button was drawn but clipped so it could not be pressed — it is reachable again from the drawer.

- **The job-runs table no longer cuts off its own content.** Columns were sized from guesswork, so "completed with warnings", the finish timestamp and the progress bar were all clipped, while two other columns sat mostly empty. Every column is now sized to what it actually has to show, with nothing cut off at any window width. The section labels also stay put when you scroll sideways instead of scrolling out of view.

- **The source list shows the web address instead of repeating the studio name.** The Studio column almost always repeated what the Name column already said, while the address is the only thing that tells two same-named boards apart. Column widths are measured from the real data, so nothing is squeezed.

- **Empty log boxes no longer hold open a large blank space.** When the fetch and discovery logs had nothing to show, they still reserved roughly 200px each. They now shrink to fit and expand again as soon as real lines arrive, so the page is about 300px shorter in the common case.

- **You no longer see the same fetch warning twice.** Warnings about a stale or never-run job update appeared both in the top status panel and again further down the Operations section. Each is now shown once, in the place designed for it.

- **Admin buttons and headings are sized correctly.** Action buttons were stretched far wider than their labels, leaving large gaps of empty space. Section headings used a single font size for every level, so nested sub-headings could render larger than the section they sat under. Headings now follow a proper descending scale and buttons fit their text.

- **Tab counters now resolve instead of spinning forever.** A counter that could never be filled kept showing "..." indefinitely; it now shows "..." only while genuinely loading, and a dash once it has failed.

- Smaller consistency work: the info glyph and page icons now use the same clean line-icon style as the rest of the page, source ID icons are the right size and colour in both light and dark themes and stay keyboard-reachable, and the advanced bulk-actions and run-trends controls are spaced more clearly.

### Added

- **Source health is now monitored continuously instead of only at release time.** A standing report flags duplicate board addresses, boards that have quietly gone offline, and drift between the tracked source list and the one the pipeline actually fetches. It is strictly read-only — it never removes, disables or rewrites a source — and any count is a prompt to review, not a failure. Two new repository checks also prevent sources from being registered with a duplicate identity or a malformed page address.

- **Dead job boards are flagged only when there is real evidence, and never automatically.** A board is only suggested for repair after repeated failures or a long outage, is never removed or changed on its own, and each suggestion shows the failure count, how long it has been down, and a short error sample so you can judge it without re-running anything. Temporary problems such as rate limiting, bot blocks or timeouts are correctly not treated as dead. Right now the honest answer is zero boards needing repair. A companion record lets you mark findings as reviewed, approved or snoozed; approving one is a human decision that never applies itself, and an approval stops applying if the underlying problem later changes, so it cannot quietly approve a different fault.

### Fixed

- **The Admin page no longer stalls behind refused bridge connections in the packaged app.** Opening Admin could leave the page apparently broken for a long stretch, with every request failing before recovering on a later poll. Three causes, all in the connection-accept path: local listeners were accepting only a handful of queued connections at once, while a single Admin visit issues around two dozen requests in a few hundred milliseconds; the bridge answered in a mode that forced a brand-new connection per request, multiplying exactly the burst that overflowed the queue; and the bridge's event log was being re-read and re-parsed in full on every single request, by every concurrent request at once. The queue is now sized for a burst, connections are reused, and the log is only tidied when it is actually over a limit. A burst of thirty connections now completes with no refusals. This was specific to the packaged Windows app because that platform refuses new connections when the queue is full, where other systems quietly retry.

- **When the bridge is slow to answer, Admin now shows the wait instead of a silently broken page.** Previously the panels were simply blanked with no explanation and the tab counters sat at an endless "...", so the page looked broken, gave no reason, and then appeared to recover on its own about ten seconds later. Admin now shows a reconnecting badge, a banner naming the wait with an elapsed timer and a **Retry now** button, and keeps the content behind a bounded gate that reveals the page anyway at the deadline — so a bridge that never comes back can no longer hold the page blank forever. The banner is display-only: it never changes which operations are attempted.

- **A degraded start-up no longer reports itself as healthy.** If the page could not reach the bridge during start-up it fell back to a reduced set of data, but the badge immediately claimed the bridge was online — so a broken connection was reported as a working one. It now reports a distinct **Degraded** state whenever the start-up fell back. The badge also no longer flickers between online and offline on a single unlucky request; it takes two consecutive failures before it reports a problem, which is what stops a healthy page oscillating.

- **The reconnect banner now appears when it should.** It was meant to explain the offline badge, but it read its answer from an internal status that nothing in the Admin page ever updated, so it stayed hidden precisely when it was needed — the badge would say "Bridge Offline" while the banner explaining it was invisible. Reachability is now handed to the banner by the same call that sets the badge, so the two read one signal and cannot disagree. The banner has also moved into the page header, where it no longer covers the bridge badge or the page title.

- **The fetch and discovery log boxes no longer show chopped-up half lines.** During a busy run the two log panels interleaved intact lines with fragments such as `325/612 pages.` or `ages.` — leftovers from a line whose start had been cut off. Because those fragments carried no timestamp of their own, they were stamped with the time the page happened to redraw, so they also appeared out of order among the correct entries. Both boxes now show whole lines in the right order, and a line appears only once the run has finished writing it.

- **The Admin status panel now reports problems it was silently dropping.** Two of its four warning types could never appear, because the page was asking the server for a summary that does not contain the information they need. Stale-job-data and failing-source warnings now appear correctly. The same fix removes a message that could read "Last successful fetch was at an unknown time" or show a nonsense "Infinityd".

- **The Admin page no longer logs an internal error on every load.** An idle-refresh error appeared in the console once per page load; it is fixed and covered by a regression test.

- A release that fails partway through no longer requires deleting and re-creating its tag. The release workflow can be re-run from the main branch to rebuild the files for an existing release, and the recovery steps are documented in [`RELEASE.md`](RELEASE.md).

- Release automation no longer stalls on a missing browser or loses its diagnostics: it installs both browser runtimes the release needs, includes the recovery download in the update manifest, and cannot hang indefinitely. If a browser fails to start, the test runner now shuts down cleanly instead of leaving a stuck process.

- The release workflow's browser-cache check no longer fails every run. It looked for the test-runner browser using a filter-and-exclude combination that silently discarded every match, so it reported the browser missing even when it was installed correctly. The check now selects the two browser caches properly, and a repository test drives the shipped check both ways — confirming it passes on a complete browser cache and still fails when the test-runner browser is genuinely absent.

- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.
