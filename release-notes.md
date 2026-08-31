## [0.2.141] - 2026-08-31
### Changed

- Runtime↔seed registry reconcile (WP24 runbook, jobs-coverage plan): converges the live
  container's runtime registry (`data/source-registry-active.json.gz` + journal) with the
  twin-reconciled tracked seeds, deferring to the seeds as the source of truth. Applies the
  verified `POST /registry/demote-active` batch demoting **10** real runtime-only twins to
  pending (`static:listing_url` rows for `www.scopely.com/en/join-us`, `www.hugecalf.com/careers`,
  `bandainamcoent.com/careers`, `www.roshkastudios.com/jobs.html`, `www.joinplaygames.com/jobs.php`,
  `www.ninerocksgames.com/careers`, `www.skybound.com/careers`, `sybogames.com/careers/`,
  `www.nocodestudio.com/jobs`, `www.volleygames.com/careers`) — **active 2301 → 2291, pending
  850 → 860**. Deliberately left
  in place: `careers.playstation.com`'s two distinct rows (a marketing redirect + the
  `playstation.com/jobs` Global-website board, 50 jobs — not a twin) and the 16 single-row
  reconcile keys (e.g. bytedance holding `jobs.bytedance.com/en/position`). Every demote is
  reversible via `POST /registry/approve`.
- Release compatibility remains aligned with the same-origin Linux container for Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility.
