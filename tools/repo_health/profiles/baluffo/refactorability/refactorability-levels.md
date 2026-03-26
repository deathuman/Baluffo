# Refactorability Levels

This document defines maturity levels for **AI-oriented refactorability** in Baluffo.

These levels do not measure generic polish.
They measure how safely an AI coding agent can:
- find the right files
- understand the relevant contracts
- make local changes
- choose the right verification step
- avoid collateral damage in high-risk paths

## Level 1: Fragile (0-20%)

AI-assisted refactoring is high risk.

Typical characteristics:
- subsystem boundaries are unclear
- important contracts must be guessed from code
- changes frequently spill across unrelated files
- verification paths are unclear or too expensive
- docs, commands, and code maps are weakly connected

What work feels like:
- broad repo exploration before every edit
- repeated shallow fixes
- easy regressions in adjacent areas
- high chance of "fix one path, break another"

## Level 2: Emerging (21-40%)

Some useful structure exists, but routine refactors are still noisy and error-prone.

Typical characteristics:
- some architecture/testing docs exist
- some commands are discoverable
- a few canonical docs or registries exist
- there are still major hotspot files and drift risks
- build/runtime/config boundaries are not consistently isolated

What work feels like:
- small edits are possible
- medium edits still require wide search
- high-risk paths require manual caution and tribal knowledge

## Level 3: Workable (41-60%)

AI can perform many routine refactors safely with moderate guidance.

Typical characteristics:
- subsystem boundaries are mostly visible
- common commands are easy to find
- contracts are documented for important paths
- targeted verification exists for major subsystems
- hotspot files and drift risks still exist, but are identifiable

What work feels like:
- ordinary fixes and focused refactors are realistic
- AI can usually choose a sensible test path
- large or cross-cutting changes still need strong supervision

## Level 4: Reliable (61-80%)

AI can usually refactor with good locality, explicit contracts, and predictable verification.

Typical characteristics:
- clear module boundaries
- low duplication/drift in critical paths
- build/package/config concerns are reasonably isolated
- docs cross-link well and act as navigation aids
- failures are diagnosable without guesswork
- high-risk paths are visibly marked and documented

What work feels like:
- most routine refactors stay local
- AI can navigate quickly and verify cheaply
- risky areas are still risky, but not mysterious

## Level 5: Refactor-Ready (81-100%)

The repository is intentionally optimized for safe AI-assisted refactoring.

Typical characteristics:
- changes are highly local
- contracts are explicit and canonical
- boundary violations are rare
- verification routing is obvious
- packaging/build/runtime/config paths are isolated cleanly
- duplication and drift are actively minimized
- hotspot files are rare, known, and monitored

What work feels like:
- AI can make focused changes with low collateral risk
- common change types have obvious file/test/doc routes
- refactor cost is predictable
- maintainers can trust the tool to point at the right next improvements

## Scoring Guidance

Scores should reward:
- evidence over assumption
- locality over broad coupling
- explicit contracts over inferred behavior
- targeted verification over only broad end-to-end checks
- canonical registries/docs over repeated literals and guesses
- isolated high-risk paths over mixed-concern code

Scores should not be inflated just because:
- many config files exist
- there are lots of tests but no routing guidance
- there is a large amount of documentation but no canonical path
- CI exists but does not help choose safe refactor paths

## Interpretation Rule

When score and hotspots disagree, trust the hotspots.

A repository may score moderately well overall and still contain a few files or subsystems that are dangerous for AI refactoring.
Those hotspot findings should drive the next improvements first.

---

*Last updated: 2026-03-25*
