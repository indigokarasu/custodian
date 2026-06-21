# Google OAuth Client Deleted Pattern

**Fingerprint:** `oc_google_oauth_client_deleted`

When a Google OAuth client is deleted from Google Cloud Console, token refresh attempts return `deleted_client` error. This is distinct from `oc_google_token_invalid` (token expired/revoked but client still exists) and `oc_google_token_missing` (token files absent).

## Diagnostic

1. Check credential file: `python3 -c "import json; d=json.load(open('/root/.google_workspace_mcp/credentials/google-workspace-user.json')); print(d.get('expiry'))"`
2. Attempt token refresh — if error contains `deleted_client`, the OAuth client is gone
3. Check if other Google jobs are failing with auth errors

## Why Auto-Fix Cannot Work

The `oc_google_token_invalid` auto-fix (`cp <hermes-root>-indigo/google_token.json <hermes-root>/google_token.json`) restores a backup token, but if the OAuth client itself was deleted, no token will work. A new OAuth client must be created in Google Cloud Console.

## Required User Action

1. Create a new OAuth client in Google Cloud Console
2. Update `config.yaml` with new `GOOGLE_OAUTH_CLIENT_ID` and `GOOGLE_OAUTH_CLIENT_SECRET`
3. Re-authorize via `python3 <hermes-root>/skills/infrastructure/google-workspace-auth/scripts/google_oauth_init.py`

## Affected Jobs

- `email:check`
- `sands:*` (morning-brief, evening-brief, conflict-scan, travel-check, update)
- `taste:*` (scan, sync-spotify, historical-email, historical-calendar, update, daily-styx-enrichment)
- `vesper:*` (morning, evening, update, deliver-morning, deliver-evening)

## Tier: 3 (Escalate — Cannot Be Automated)

## Match Patterns

- `"deleted_client"`
- `"OAuth client was deleted"`
- `"client was deleted.*Google Cloud"`

## History

- 2026-06-04: First detected. Credential file at `/root/.google_workspace_mcp/credentials/google-workspace-user.json` exists (1686 bytes) with token, refresh_token, client_id, client_secret. Token expired 2026-06-04T21:33. OAuth client deleted from Google Cloud Console.
- 2026-06-05: Still open. Awaiting user re-authorization.
