# GitHub App Sync Packaging

The source sync runtime now expects a packaged GitHub App config file instead of user-entered PAT settings.

## Packaged file

Ship a file named `github-app-sync-config.json` with the app bundle, or set `BALUFFO_SYNC_APP_CONFIG_PATH` to its location.

Expected JSON shape:

```json
{
  "schemaVersion": 1,
  "appId": "123456",
  "installationId": "98765432",
  "repo": "your-org/job-sources-backup",
  "branch": "main",
  "path": "baluffo/source-sync.json",
  "keySalt": "base64url-random-salt",
  "privateKeyPemEnc": "base64url-encrypted-private-key"
}
```

## Encryption format

The encrypted key uses the same machine-bound derivation implemented in `scripts/source_sync.py`:

- key material is derived from machine identity, app id, installation id, and `keySalt`
- the private key PEM is XOR-encrypted with a SHA-256 keystream
- plaintext private keys should not be shipped in production bundles

For local tests only, `privateKeyPem` may be supplied instead of `privateKeyPemEnc`.

## Build-time notes

- Do not commit production `github-app-sync-config.json`
- Generate a unique `keySalt` per distribution target
- Encrypt the GitHub App private key PEM before bundling
- Bundle the config next to the app or point `BALUFFO_SYNC_APP_CONFIG_PATH` to the deployed file

## Helper command

You can generate the packaged config locally with:

```powershell
py -3 scripts/build_sync_app_config.py `
  --app-id 123456 `
  --installation-id 98765432 `
  --repo owner/repo `
  --private-key C:\path\to\github-app-private-key.pem
```

For local-only testing, add `--plaintext` to write `privateKeyPem` directly instead of the encrypted form.
