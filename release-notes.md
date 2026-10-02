## [0.3.007] - 2026-10-03
### Fixed

- **Voodoo's job openings show up again.** Voodoo moved its careers board from Lever to Ashby, and the old board was simply deleted. Baluffo was still watching the deleted one, so it collected nothing and the studio's openings vanished from the Jobs page - 120 of them, including roles posted as recently as the day before. Baluffo now watches Voodoo's Ashby board, which is verified live and serving all of them.

- **The Ashby board list no longer throws away working studios.** A maintenance routine that checks whether Ashby boards are still alive could not read the board address back out of the saved source list, so it treated nine healthy boards as broken and deleted them - studios such as thatgamecompany, k-ID and Sleeper, along with roughly 80 live openings between them. It now reads the address correctly, and it also treats a board written two different ways as one board instead of two, which was double-counting openings.
