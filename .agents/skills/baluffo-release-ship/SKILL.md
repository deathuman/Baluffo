---
name: baluffo-release-ship
description: Ship Baluffo desktop and container releases; handle shared desktop/container public releases, release readiness reviews, release CI or tag failures, packaged asset verification, GHCR image checks, live deployment smoke, update manifest checks, changelog or version checks, and release handoff or gotcha closeout. Use for Baluffo release publication work that needs source-aware local gates, GitHub Actions evidence, packaged or container smoke evidence, asset/image checks, and Basic Memory closeout.
---

# Baluffo Release Ship

## Overview

Use this skill to run Baluffo release work without rediscovering the release path or repeating known CI/tag mistakes. Treat repo docs, source, tests, and `AGENTS.md` as canonical; use Basic Memory only for recent release gotchas and handoff continuity.

## Workflow

1. Start from repo instructions.
   - Read `AGENTS.md`, `docs/INDEX.md`, `docs/AI_ASSISTANT_GUIDE.md`, `docs/RELEASE.md`, `docs/testing.md`, and `docs/TROUBLESHOOTING.md`.
   - Read relevant Basic Memory release notes, especially `release-publication-ci-parity-and-tag-recovery` and the latest `baluffo-v*-shipped-*` handoff.
   - Confirm the requested version, branch, release scope, and whether the user is asking for planning, implementation, tagging, publishing, or recovery.

2. Verify release state before mutating anything.
   - Check `git status`, current branch, `HEAD`, `origin/main`, existing release tags, and current `src/app_version.py`.
   - Check `docs/CHANGELOG.md`, `release-notes.md`, and release docs policy expectations.
   - If a tag or release already exists, inspect GitHub release state, workflow runs, and published assets before proposing tag movement.
   - For desktop release work after container patch cycles, compare the latest public desktop tag with current `main`; if shared fixes accumulated, prefer a new rollup version and desktop-facing changelog entry instead of tagging a narrow container patch note.

3. Run local gates in increasing risk order.
   - Prefer the documented narrow gates first when investigating a release blocker.
   - For a ready release, use the repo release preflight from `docs/RELEASE.md`.
   - Build local portable artifacts only when the docs call for it or when artifact inspection is needed.
   - For packaged failures, inspect the smoke report JSON and runtime artifacts before changing tests or retrying.
   - For container releases, follow the dedicated lane in `docs/RELEASE.md`: focused gates, Docker build or clean-context fallback, published-image sync readiness, live route checks when a host is in scope, and a manual or scheduled pipeline smoke before closeout.

4. Publish or recover carefully.
   - Never move, delete, or recreate release tags without explicit user approval.
   - Treat local release preflight as necessary but not sufficient for GitHub secret-backed packaged sync generation.
   - After pushing a tag, watch all tag-triggered workflows that publish release artifacts or images: `build-portable-exe`, `Build Container`, and `build-linux`.
   - Verify the release is published, not draft, not prerelease, with portable ZIP, ship ZIP, desktop update manifest, and Linux AppImage when that workflow publishes it.
   - After pushing a container release to `main`, watch the normal main checks plus `Build Container`; verify the GHCR tag, multi-arch digest/platforms, and packaged `/sync/status` readiness from the published image.
   - If a live deployment smoke exposes a blocker, bump to the next patch version instead of reusing the failed image identity, keep existing tags untouched, and document the intermediate failed patch in the closeout.
   - Remember that default-branch and tagged pushes can trigger container image publication; account for GHCR tag side effects when a single app version is shared, even for docs-only release-process commits.
   - Verify desktop manifest version, channel, schema, key id, signature, minimum updater version, rollback flag, portable asset URL, checksum, and size against the published release asset.
   - For main-branch ship or remote CI closeout, inspect GitHub Actions before declaring shipped. If remote CI fails after local gates passed, fetch the failing job logs first, reproduce with the closest local command, and account for remote OS, path, import, or secret-backed differences.

5. Close out continuity.
   - Update release docs only when behavior, commands, contracts, or workflows changed.
   - Write or update Basic Memory with release version, commit, workflow run, assets, validation commands, and durable gotchas.
   - If Basic Memory wrote a Git-backed note, curate, commit, and push the BaluffoMemory repo unless the user explicitly keeps memory local or unrelated memory drift blocks a safe push.
   - Final response must name the version, commit, tag/release URL or GHCR digest if published, validation run results, live smoke status when applicable, and any residual risk.
   - When the user asked to ship to `main`, include final remote run ids or check statuses in the closeout.

## Shared Desktop + Container Public Release Lane

Use this lane when one version is intended to become both the public desktop release and the container release.

1. Confirm the release identity.
   - Verify `main`, `origin/main`, `src/app_version.py`, and `docs/CHANGELOG.md` all name the same version.
   - Confirm the target tag and GitHub release do not already exist. If either exists, inspect it before proposing recovery.
   - **Know what the version gate will demand before you touch shipped code.** `check_container_shipped_code_version_gate` reads the version at **HEAD**, not the working tree. While the current version is unreleased, `Release-tag: v<current>` intent is enough and republishing the tag is inert. Once it is **released**, intent is no longer consulted: a **newer** `Release-tag:` is still rejected, because overwriting a published image requires the version to actually move. Either bump `src/app_version.py` in the same change as the shipped code, or keep shipped code on a branch and release it by tagging. The gate blocks at **pre-push**, so the offending commit always lands locally first — that is by design, not a bypass point.

2. Run local confidence.
   - Run `rtk npm run release:preflight` on the exact commit to tag, then confirm the repo stays clean.
   - Build or inspect local portable and ship ZIPs only as artifact confidence; the signed desktop manifest is verified from the GitHub workflow because signing secrets live there.

3. Publish the tag.
   - Create an annotated `v<version>` tag on the verified commit.
   - Push only that tag, then watch `build-portable-exe`, `Build Container`, and `build-linux` to completion with bounded polling.

4. Verify published artifacts.
   - Check the GitHub release is latest, published, not draft, and not prerelease.
   - Confirm release notes are the matching changelog section.
   - Confirm the portable ZIP, ship ZIP, desktop update manifest, and AppImage assets are present.
   - Download and inspect the desktop update manifest fields and compare portable artifact size/hash with the release asset.
   - Reconfirm the final GHCR digest and `linux/amd64` plus `linux/arm64` platforms after tag-side container workflows finish.

5. Recheck the deployment and memory.
   - When a host deployment is in scope, reconfirm `/app/ready`, `/ops/health`, `/tasks/run-jobs-pipeline-status`, `/ops/task-state?view=summary`, and `/sync/status?view=summary` on it.
   - Write a durable Basic Memory closeout with commit, tag, release URL, workflow IDs, assets, manifest evidence, GHCR digest/platforms, deployment status, and residual risks.
   - Push curated BaluffoMemory notes at closeout when the memory repo has no unrelated or ambiguous drift.

## Container Patch Lane

Use this lane for container-only Baluffo patches where the desktop tag remains unchanged.

1. Confirm scope and idleness.
   - Verify the requested version, that no desktop tag or desktop GitHub release is intended, and that existing tags must not be moved.
   - Before updating a live deployment, read `/ops/health`, `/ops/task-state?view=summary`, and `/tasks/run-jobs-pipeline-status`; wait if a pipeline or critical task is active unless the user explicitly asks to interrupt it.

2. Prepare the version commit.
   - Run `python scripts/bump_version.py <next>` to update `src/app_version.py`, `docs/CHANGELOG.md`, and `dist/baluffo-ship/app/current.txt`. If the command reports version drift, an earlier bump reached only some artifacts: finish or revert it rather than bumping past it.
   - Run the focused gates named in the release plan before committing. Add broader gates only when the touched surface warrants them.
   - Keep release commits narrow. Do not add dependencies, change desktop packaging, or repair unrelated issues during a container version bump.

3. Publish and verify.
   - Push `main`, monitor normal checks plus `Build Container`, and record workflow run IDs.
   - Confirm `ghcr.io/deathuman/baluffo:<version>` exists with `linux/amd64` and `linux/arm64` manifests and record the index digest.
   - If a pushed image is degraded, publish a forward patch version instead of reusing that image identity.

4. Verify a live deployment when the task asks for one.
   - Pull the new image on the host and restart the service, then confirm `appVersion` moved.
   - Verify `/ops/health.appVersion`, `admin.html`, `jobs.html`, startup feed availability when relevant, `/sync/status.config.ready`, and `credentialsPackaged`.
   - For performance releases, prefer Browser/Developer-mode user-visible evidence over backend profile-only evidence before declaring the user-visible issue fixed.

5. Close out with one durable evidence note.
   - Record version, commit SHA, workflow run IDs, GHCR digest/platforms, deployed version, smoke result, trace paths when used, and residual risk.

## Guardrails

- Do not use `--no-verify`.
- Do not treat console logs alone as packaged smoke evidence when artifacts are available.
- Do not assume a failed tagged workflow means no assets were published; inspect the release first.
- Do not add dependencies or broaden release scope without explicit user approval.
- If the same release failure repeats twice, stop retrying and diagnose from docs, source, tests, CI logs, and artifacts.
- Do not declare a container release shipped on CI and GHCR evidence alone when the task asks for live deployment verification; live `/ops/health`, registry, sync, jobs, and pipeline evidence are required.
