# Environments

This page documents the release-path separation for source-sync writes.

## Private transport repo

BaluffoSync is intentionally private. It is not a collaborative development branch; it is a sync transport repo for local apps and machines that need to sync `active` and `pending` sources in and out.

For that model, the baseline controls are:

- GitHub App auth for production writes, or a deploy key for a lightweight single-writer setup
- the `validate-source-sync` CI check on each write path
- GitHub notifications for failed `validate-source-sync.yml` runs

Branch protection, repository rulesets, and required reviewers are useful hardening, but they are optional for this transport-first model and may remain unavailable on the current GitHub plan.

## Production path

Use GitHub App auth for production writes. That remains the preferred path because it keeps the write actor explicit and auditable.

Production writes should also run through:

- the `validate-source-sync` CI check
- signed commits on the write path
- protected `main` branch rules or repository rulesets when available
- required reviewers for the production deployment environment when available

## Failure notifications

The repository owner or release maintainer is responsible for enabling GitHub notifications for failed `validate-source-sync.yml` runs.

That GitHub notification path is the baseline. If the team also wants Slack or webhook mirrors, those can be configured separately, but they do not replace the GitHub notification setting.

Use the same policy for any failed validation run, regardless of whether it was triggered by `push` or `pull_request`.

## Staging path

Use a separate staging environment when you want to rehearse the write path before production.

Recommended staging defaults:

- separate deployment branch or branch scope
- no required reviewers
- the same snapshot validation check as production
- no production write credentials

## Lightweight / local path

For single-repo or development-only setups, a deploy key with write access can work.

That option is lighter weight than a GitHub App, but it should stay out of the production path unless the team explicitly accepts the reduced audit model.

## What stays in the repository

This repository should document and validate:

- the snapshot schema
- the `validate-source-sync` workflow
- the production/staging split
- the writer auth model that the code expects

GitHub-side controls that are configured outside the repository, such as branch protection, rulesets, and required reviewers, should be documented here but applied in the repository settings UI.
