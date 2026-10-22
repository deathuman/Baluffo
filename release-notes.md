## [0.3.024] - 2026-10-22

### Changed

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

### Notes

- Distribution surfaces are unchanged. Each behaves as it did in 0.3.023:
  the same-origin Linux container,
  GHCR multi-arch image publishing,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
