## [0.3.025] - 2026-10-23

### Changed

- **Restarting the app right after quitting keeps it on the same local ports.** Opening the app again
  within about a minute of closing it could land it on a different address than the usual
  `127.0.0.1:8080` and `:8877`, because the startup check counted the previous session's connections —
  still winding down after the window closed — as a port already in use, and quietly moved to a random
  one. The check now treats that brief window the way the app's own servers do, so the usual addresses
  hold. A port another program is genuinely listening on is still reported as busy.
- **The Linux AppImage now updates itself, the way the Windows build already did.** The jobs page's
  update panel used to hide on Linux, because only the Windows build shipped an installer. The
  AppImage now carries its own updater: it downloads the next release, verifies it, replaces the
  file you launched, and reopens it — keeping the old build aside until the new one reports it
  started, and putting it back if it does not. The AppImage has to sit in a folder you can write to;
  when it does not, the panel says so instead of failing halfway through. AppImages released before
  this change still report updates as unavailable, so download the newest one from the release page
  once.
- **Source discovery stops asking a studio the same question four more times.** When a studio
  homepage had no jobs link, discovery guessed `/careers` and `/jobs`, and then — even after those
  answered, or answered that they do not exist — guessed four more spellings on the same site. On a
  full directory scan that second round was most of the page fetches and almost every fetch failure,
  and it produced no studios worth adding. It now runs only for sites that could not be read at all.
  The discovery log also stops calling failed probes "fetched": each recovery round reports how many
  pages it actually read and how many failed.
- **Every failed discovery probe is now listed with its URL and reason.** The directory adapters
  counted hundreds of failed recovery fetches per run and then dropped them before writing the
  report, so the discovery failure view could show the count but never a cause. Each miss now
  appears as its own row — which page was tried, whether it was guessed or linked from the
  homepage, and what the site answered — and the admin's task-failure view groups them as expected
  404s rather than actionable errors.
- **The fetcher metrics panel shows how long a run actually took.** It reported the sum of every
  source's fetch time, capped at 24 hours, so a run that finished in eight hours read as a full
  day. The panel now shows wall-clock time and reports total source time separately.
- **A page the browser had to re-read no longer counts as a lost studio.** When a site answered the
  first page fetch with a block or a challenge, discovery retried it in the browser and often got
  the page. The earlier failure stayed in the report with no sign that it had been rescued, so the
  failure count read as studios discovery never saw. Those rows are now marked as resolved by the
  browser and counted separately, and the admin's task-failure view treats them as expected rather
  than something to fix. The discovery report's own summary carries that rescued total, so the
  admin's report and run detail show it next to the failure count instead of making you open the
  adapter audit that recorded it.
- **Every page now loads as one script, and paints what you are looking at first.** The packaged
  pages used to fetch each of their modules as a separate file, so a launch waited on roughly a
  hundred small requests before the page could settle; they ship bundled now — about 118 fewer
  requests on the jobs page and 169 fewer on the admin page. The admin page lays out its
  below-the-fold panels after the shell has painted, its operations half and the jobs page's update
  panel arrive in their own files once the page is up, and the hosted pages got the same split. Form
  controls finally use the app's font instead of the browser's default, the icons the pages drew as
  characters (the save tick, the fullwidth plus, the discovery disclosure marker) are drawn as paths
  or borders, and each runtime loads only its own local-data client. Nothing moves or looks
  different.

### Fixed

- **On Linux, the app's background servers now stop with the app.** Quitting from a terminal,
  logging out, or stopping the launcher by hand used to leave its local site and admin bridge
  running: the bridge kept answering on its port and the site kept serving the last page until
  their own timers expired, and a crash could leave them behind for good. Both now end when the
  app ends, including when it is killed outright.
- **A process that has already exited is no longer treated as running.** On Linux, a crashed
  launcher that the system had not cleaned up yet counted as alive, so the next start could report
  that Baluffo was already running and stop — with no window left to close. An exited process is
  recognised as gone now.

### Notes

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.023:
  the same-origin Linux container,
  GHCR multi-arch image publishing,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
