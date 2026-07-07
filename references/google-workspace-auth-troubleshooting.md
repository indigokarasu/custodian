# Google Workspace MCP Authentication - Correct Procedure

## Architecture

The `workspace-mcp` server (v1.20.4) at `/usr/local/bin/workspace-mcp`:
- Runs as a stdio subprocess spawned by the Hermes gateway
- Uses OAuth 2.0 with PKCE flow
- Stores credentials in `/root/.google_workspace_mcp/credentials/` as `<email>.json`
- Each credential file contains: `token`, `refresh_token`, `client_id`, `client_secret`, `scopes`, `expiry`
- In `--single-user` mode, it picks the correct credential file based on the `user_google_email` parameter

## Credential Files

Each account uses its OWN OAuth client in its OWN Google Cloud project (both in Testing mode):

- **Indigo**: client `550801240087-vmc47b1gflj2biblqdr6bkekl7qqm8ls`, creds at `/root/.google_workspace_mcp/credentials/mx.indigo.karasu@gmail.com.json`
- **owner**: client `112292610034-1revbmnkves56ago2c2t5dul5mj9bc17`, creds at `/root/.google_workspace_mcp/credentials/google-workspace-user.json`

Both files must have all 41 scopes pre-populated. The MCP checks stored scopes against required scopes — if any are missing, it triggers a re-auth loop.

## Re-Authorization Procedure (when refresh tokens die)

1. Generate the auth URL using the correct client_id for the account:
   - Indigo: `client_id=550801240087-vmc47b1gflj2biblqdr6bkekl7qqm8ls`
   - owner: `client_id=112292610034-1revbmnkves56ago2c2t5dul5mj9bc17`
   - Redirect URI: `http://localhost:1` (Google blocks raw IPs)
   - All 41 scopes in the scope parameter
   - `access_type=offline`, `prompt=consent`

2. User opens the URL in their browser and grants permission

3. Google redirects to `http://localhost:1/?code=...&state=...` (won't load locally)

4. User copies the full redirect URL from their browser bar and pastes it back

5. Exchange the code for tokens using the matching `code_verifier` from the state file

6. Save to the correct credential file (`/root/.google_workspace_mcp/credentials/<email>.json`)

## Critical Rules

- NEVER use one OAuth client for both accounts — each account has its own Google Cloud project in Testing mode
- NEVER use raw IP in redirect_uri — always use `http://localhost:1`
- NEVER let the MCP overwrite credential files with incomplete scopes — pre-populate all 41 scopes
- tinyurl.com is the only working URL shortener from this network
- Always use `installed` (Desktop) client type in the OAuth flow, NOT `web` type
- The credential store uses per-file client_id for refresh — the ENV client is only for server-initiated auth flows
- Credential file corruption (0 bytes) happens when `store_credential` uses non-atomic write during a failed refresh — the atomic write patch in `credential_store.py` prevents this
- NEVER write credential files manually without using atomic write (temp file + rename)

## Scripts

The old reauth scripts (`google_reauth_url.py`, `google_oauth_finish.py`) have been removed from `<hermes-root>/scripts/`. They are no longer needed — the MCP server handles OAuth flows directly. Credentials are stored at `/root/.google_workspace_mcp/credentials/<email>.json`. To re-authorize, generate an auth URL using the correct client_id for each account (see Critical Rules above) and complete the OAuth flow in browser.

## Common Errors

| Error | Cause | Fix |
|-------|-------|-----|
| `invalid_grant: Bad Request` | Refresh token expired or revoked | Re-authorize using the procedure above |
| `invalid_client: client secret is invalid` | Client secret rotated since tokens were obtained | Use current client_secret from credential file |
| `invalid_scope` | Typo in scope URL (e.g. missing `.com`) | Use the canonical scope list from the MCP server |
| `403: access_denied` | Account not in OAuth consent screen test users (Testing mode) | Add email to test users in Google Cloud Console |
| File is 0 bytes | Non-atomic write during failed refresh | Apply atomic write patch to `credential_store.py` |

## Cascade Pattern: When Google OAuth Tokens Die

When a Google refresh token is revoked (`invalid_grant`), it affects not just the obvious email jobs but cascades through subprocess calls:

- `email:check` → directly imports `googleapiclient` → fails on token refresh
- `monitor:list` → calls `tasks_monitor.py` as subprocess → that script also uses Google credentials → fails with same `invalid_grant`
- `sands:*`, `taste:*`, `vesper:*` → all use Gmail/Calendar/Tasks APIs via the same OAuth client

**Detection**: During scan, if `email:check` shows `invalid_grant`, immediately check `monitor:list` and other Google-dependent jobs. The error may appear as `Script exited with code 1` on wrapper scripts that call subprocesses — run the wrapper manually to see the actual subprocess error before classifying.

**Confirmed 2026-06-28**: `email:check` and `monitor:list` both hit `invalid_grant` from the same dead refresh token. `monitor:list` showed `Script exited with code 1` which would be misclassified as `oc_cron_no_agent_exit_1_noop` without subprocess inspection.

```
