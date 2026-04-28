# Security Policy

## Supported Versions

Baluffo is maintained on a best-effort basis.

We recommend always running the latest official release.

| Version | Supported |
| ------- | --------- |
| Latest release | ✅ |
| `main` branch | Best effort |
| Older releases | ❌ |

## Reporting a Vulnerability

**Please do not report suspected vulnerabilities through public GitHub issues.**

If you discover a security issue in Baluffo, please report it privately:

1. **Private report**: Use GitHub's [Private vulnerability reporting](https://github.com/deathuman/Baluffo/security/advisories/new) feature, if it is enabled for this repository
2. **Maintainer contact**: If you already have a trusted private contact path to the maintainer, you may use that instead

When reporting, please include:

- a clear description of the issue
- affected files, scripts, or release artifacts
- steps to reproduce
- expected impact
- proof of concept, if safe to share
- whether the issue affects source checkout, packaged releases, or both

Reports are handled on a best-effort basis.

## Security Model

### Local-First Architecture

Baluffo is designed as a **local-first** application.

- **No user accounts** — the application does not require a Baluffo login
- **Data stays local by default** — saved jobs, notes, and related runtime data are intended to remain on the user's machine
- **No hosted Baluffo cloud** — Baluffo is not built around a hosted account or cloud profile model

The project README describes Baluffo as local-first, portable, and optimized for local/personal operation. See `README.md` for product-level details.

### Data Handling

Baluffo works with:

- **public job listing data** collected from external job sources
- **local user data** such as saved jobs, notes, and preferences
- **local runtime/configuration data** needed to run the app and related tooling

Baluffo should not commit, bundle, or publish private user runtime data, local secrets, or developer-specific environment artifacts.

### External Sources

Baluffo may interact with public job boards and other configured external sources as part of its job aggregation workflows.

Because third-party sources are outside this repository's control:

- source reliability may vary
- anti-bot or rate-limit behavior may change
- externally sourced content should be treated as untrusted input
- user-visible data should be sanitized and validated before display or persistence

## What to Report

Please report issues such as:

- remote code execution
- arbitrary file read/write outside intended app paths
- path traversal in import/export, backup/restore, packaging, or update flows
- manifest, checksum, or update-integrity bypass
- weaknesses in rollback or migration safety
- exposure of secrets or signing material
- accidental inclusion of local runtime state or private data in release artifacts
- cross-site scripting or unsafe rendering of externally sourced job data

You generally do **not** need to report:

- ordinary scraping breakage caused by third-party site changes
- general parser failures with no security impact
- local setup mistakes on a developer machine
- feature requests for future hardening, unless tied to a concrete vulnerability

## Secrets and Credentials

Do not commit secrets, tokens, signing keys, or private credentials to the repository.

In particular:

- update signing material such as `BALUFFO_UPDATE_SIGNING_KEY` must never be committed
- local configuration secrets must not be added to tracked files
- production packaged sync config must stay out of tracked files as `packaging/github-app-sync-config.json`
- generated local sync key material must stay out of tracked files as `packaging/github-app-sync-config.localkey.json`
- machine-local config overrides must stay out of tracked files as `baluffo.config.local.json`
- release workflow secrets such as `BALUFFO_SYNC_BUILD_PRIVATE_KEY_PEM` and `BALUFFO_DESKTOP_UPDATE_PRIVATE_KEY_B64` belong in GitHub encrypted secrets
- release artifacts should not contain developer-local credentials, logs, or private runtime state

The contributor and CI lint gates run `gitleaks` through pre-commit to catch likely secrets before merge. If you believe a secret has been exposed in repository history, CI logs, or published artifacts, report it immediately, rotate the credential first, and treat any history rewrite as a separate high-risk follow-up.

## Release and Update Security

Baluffo's release and update model includes versioning, manifest validation, checksum/signature expectations, migration rules, and rollback behavior.

These rules are documented in `docs/RELEASE.md` and should be treated as part of the project's security boundary.

If you discover a weakness in:

- manifest signing or verification
- artifact integrity checks
- version comparison logic
- migration safety
- rollback protections
- release packaging or support bundle handling

please report it as a security issue.

## Developer Security Expectations

If contributing to Baluffo:

1. **Never commit secrets**
2. **Treat external data as untrusted input**
3. **Validate and sanitize data before displaying it in the UI**
4. **Avoid widening file-system access or data export/import behavior without review**
5. **Keep release artifacts and build outputs free of local-only runtime state**
6. **Run `npm run lint:precommit:changed` before committing security-sensitive changes**

## Dependency and Tooling Hygiene

When updating dependencies or build tooling:

- review dependency changes carefully
- prefer trusted, maintained packages
- run the project's configured validation and test workflows
- use vulnerability scanning tools where appropriate

Examples:

```bash
npm audit
pip-audit
```
