## [0.3.017] - 2026-10-11
### Changed

- **A fetch run does far less repeated work, for the same jobs.** Building a job's public record was
  the single largest cost in a run: the app rebuilt that record in full — about 69 times per job —
  almost always just to read one or two fields out of it. Reading now happens without building the
  record, and a full record is built only for a step that actually needs one. Measured by replaying
  the same recorded responses through one workload, four paired runs: **median engine wall-clock
  time 22.8% lower** (95% confidence interval 18.9% to 25.3% lower), with every pair producing the
  same jobs, the same identities and the same per-board outcomes.

- **Job web addresses are no longer re-parsed every time they are mentioned.** Cleaning a job link,
  deriving its duplicate-detection identity and deciding whether the address is publicly reachable
  are each remembered per address, so the same parsing is not repeated for the same job across a
  run: **median engine time 15.1% lower** on the same workload (95% confidence interval 13.9% to
  15.2% lower), again with identical output.

- **The availability and lifecycle pass reads the same way.** Deciding a job's lifecycle state no
  longer builds a full record for every job before it knows whether it needs one, which removes
  about 91,000 record builds from a run and roughly half a second from that stage. It is the
  smallest of the three steps, and its effect on the run as a whole was too small for this method
  to resolve — so it is described as what it is: less wasted work, not a faster run.

### Notes

- Nothing about a collected job changes. The same titles, locations, links, identities and counts
  come out of the same responses; what changed is how much work the app does to produce them.
- Every figure above is **engine time only**, measured by replaying recorded responses on one
  representative workload. A live run also waits on the network, so the gain there will be smaller,
  and the third step's effect is below what that method can resolve.
- Distribution surfaces are unchanged. Each behaves as it did in 0.3.016:
  the same-origin Linux container,
  Umbrel raw-LAN installs,
  GHCR multi-arch image publishing,
  private community app-store metadata,
  wildcard browser CORS allow headers,
  desktop localhost bridge compatibility.
