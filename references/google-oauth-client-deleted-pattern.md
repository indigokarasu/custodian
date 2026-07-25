# Google OAuth Client Deleted Pattern

**Fingerprint:** `oc_google_oauth_client_deleted`

When a Google OAuth client is deleted from Google Cloud Console, token refresh attempts return `deleted_client` error. This is distinct from `oc_google_token_invalid` (token expired/revoked but client still exists) and `oc_google_token_missing` (token files absent).

## Diagnostic

1. Check credential file: `python3 -c "import json; d=json.load(open('<gworkspace-creds>/credentials/<user-google-email>.json')); print(d.get('expiry'))"`
2. Attempt token refresh — if error contains `deleted_client`, the OAuth client is gone
3. Check if other Google jobs are failing with auth errors

## Why Auto-Fix Cannot Work

The `oc_google_token_invalid` auto-fix (`cp <hermes-home>-indigo/google_token.json <hermes-home>/google_token.json`) restores a backup token, but if the OAuth client itself was deleted, no token will work. A new OAuth client must be created in Google Cloud Console.

## Required User Action

1. Create a new OAuth client in Google Cloud Console
2. Update `config.yaml` with new `GOOGLE_OAUTH_CLIENT_ID` and `GOOGLE_OAUTH_CLIENT_SECRET`
3. Re-authorize via `python3 <hermes-home>/skills/infrastructure/google-workspace-auth/scripts/google_oauth_init.py`

## Affected Jobs

- `email:check`
- `monitor:list` (cascades via `tasks_monitor.py` which uses the same Google OAuth credentials)
- `sands:*` (morning-brief, evening-brief, conflict-scan, travel-check, update)
- `taste:*` (scan, sync-spotify, historical-email, historical-calendar, update, daily-styx-enrichment)
- `vesper:*` (morning, evening, update, deliver-morning, deliver-evening)

## `oc_google_oauth_token_revoked` — Refresh Token Revoked (Distinct from Client Deleted)

When the OAuth client exists but the refresh token is revoked/expired by Google:

**Error**: `google.auth.exceptions.RefreshError: ('invalid_grant: Token has been expired or revoked.', {'error': 'invalid_grant', 'error_description': 'Token has been expired or revoked.'})`

**Distinguished from `oc_google_oauth_client_deleted`** — `deleted_client` means the OAuth client entry was deleted from Google Cloud Console. `invalid_grant` means the client exists but its refresh token was revoked (e.g., due to Google's 7-day refresh token limit for apps testing status, or explicit revocation).

**Why Auto-Fix Cannot Work**: No token refresh, backup, or package install can restore a revoked refresh token. A new refresh token must be obtained via OAuth re-authorization flow.

**After fixing a missing-dependency (e.g., `googleapiclient` not installed)**: If a Google-auth job previously failed with `ModuleNotFoundError` and the package was installed, the NEXT run will likely hit `invalid_grant` if the token was already dead. Install the package (Tier 1) → immediately re-check for token revocation (Tier 3).

## Live Verification of Token Revocation (2026-06-29)

Before concluding an issue is user-gated, **verify the token is actually revoked** by attempting a direct refresh against Google's OAuth endpoint. This distinguishes a truly revoked token from a stale error, network blip, or clock skew issue.

```python
import requests, json

# Load the stored credentials
with open('<gworkspace-creds>/credentials/<user-google-email>.json') as f:
    creds = json.load(f)

# Client config from google_auth_mcp.py (_CLIENTS dict)
client_id = <GOOGLE_OAUTH_CLIENT_ID>
client_secret = <GOOGLE_OAUTH_CLIENT_SECRET>

resp = requests.post(
    'https://oauth2.googleapis.com/token',
    data={
        'client_id': client_id,
        'client_secret': client_secret,
        'refresh_token': creds['refresh_token'],
        'grant_type': 'refresh_token',
    },
    headers={'Content-Type': 'application/x-www-form-urlencoded'},
)
print('Status:', resp.status_code)
print('Response:', resp.text)
```

**Interpretation:**
- `400 + "invalid_grant: Token has been expired or revoked."` → Token is definitively revoked. User-gated. Pause affected jobs.
- `200 + access_token` → Token is actually fine. The job error is stale or caused by something else (clock skew, transient network, etc.). Do NOT classify as user-gated.
- `400 + "deleted_client"` → OAuth client deleted (distinct pattern — `oc_google_oauth_client_deleted`). Requires new client creation.
- `403 + "access_denied"` → Client exists but permissions changed. May be recoverable without full re-auth.

**Why this matters**: A naive scan that only reads `last_error` from `jobs.json` can be fooled by stale errors. The live test is ground truth. Confirmed 2026-06-29: the live test returned `400 invalid_grant` while the credential file showed a token with expiry of `2026-06-28T21:15:46` (already past but not obviously "revoked" from metadata alone).

## Subprocess Cascade Masking (2026-07-06)

When a Google-auth job wraps a subprocess (e.g., `monitor:list` → `tasks_monitor.py`), the cron job's `last_error` shows only **"Script exited with code 1"** — the actual OAuth error (`invalid_grant`, HTTP 400 on token endpoint) is masked by the subprocess exit code. 

**Diagnostic**: Run the subprocess directly (`python3 tasks_monitor.py --mode check`) to see the real error. Do NOT classify as `oc_cron_no_agent_exit_1_noop` — the exit 1 is a real subprocess failure, not a no-op.

See `references/subprocess-cascade-oauth-masking.md` for full pattern and diagnostic procedure.

## Tier: 3 (Escalate — Cannot Be Automated)

## Match Patterns

- `"deleted_client"` (client deleted)
- `"OAuth client was deleted"` (client deleted)
- `"client was deleted.*Google Cloud"` (client deleted)
- `"invalid_grant: Token has been expired or revoked"` (token revoked)

## History

- 2026-06-04: First detected. Credential file at `<gworkspace-creds>/credentials/<user-google-email>.json` exists (1686 bytes) with token, refresh_token, client_id, client_secret. Token expired 2026-06-04T21:33. OAuth client deleted from Google Cloud Console.
- 2026-06-05: Still open. Awaiting user re-authorization.
- 2026-06-29: Token revocation confirmed via live API test. `email:check` and `monitor:list` paused. Other Google-auth jobs (sands, taste, vesper) currently running OK — they use different auth flows or haven't hit token expiry yet.
- 2026-07-06: Token revocation confirmed again (new instance). Client secret stored, auth URL generated. `monitor:list` subprocess masking pattern documented. Awaiting user auth code.