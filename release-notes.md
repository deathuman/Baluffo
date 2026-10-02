## [0.3.007] - 2026-10-03
### Fixed

- **Voodoo's job openings show up again.** Voodoo moved its careers board from Lever to Ashby, and the old board was simply deleted. Baluffo was still watching the deleted one, so it collected nothing and the studio's openings vanished from the Jobs page - 120 of them, including roles posted as recently as the day before. Baluffo now watches Voodoo's Ashby board, which is verified live and serving all of them.

- **The Ashby board list no longer throws away working studios.** A maintenance routine that checks whether Ashby boards are still alive could not read the board address back out of the saved source list, so it treated nine healthy boards as broken and deleted them - studios such as thatgamecompany, k-ID and Sleeper, along with roughly 80 live openings between them. It now reads the address correctly, and it also treats a board written two different ways as one board instead of two, which was double-counting openings.

- **Deleted job boards are no longer hidden behind a healthy-looking group.** When one app fetches many companies' boards at once - Lever, Greenhouse, Workable and the rest - a single dead board was reported only as part of a long text field on an otherwise successful run, so the run looked fine and nothing drew attention to it. Twelve boards had been quietly dead this way, including one studio's after they moved to a different hiring system. Board-level failures are now listed separately with the company and the reason, so they can actually be cleaned up.

- **Distribution surfaces are unchanged.** The same-origin Linux container, Umbrel raw-LAN installs, GHCR multi-arch image publishing, private community app-store metadata, wildcard browser CORS allow headers, and desktop localhost bridge compatibility all behave exactly as they did in 0.3.006. The only differences are the job-source fixes above.
