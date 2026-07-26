# email_check.py `invalid_grant` — Diagnostic Procedure

When email_check.py fails with `google.auth.exceptions.RefreshError: invalid_grant`, the refresh tokens are permanently revoked. This diagnostic procedure:

## Step 1: Check which token store email_check.py uses

email_check.py imports from `google_auth_mcp` which reads from:
```
<gworkspace-creds>/credentials/<user-google-email>.json
```

**NOT** `<hermes-home>/google_token.json` — that's a different OAuth client.

## Step 2: Test both stores independently

```bash
python3 << 'PYEOF'
import json, urllib.request, urllib.parse
from pathlib import Path

stores = {
    'MCP (owner)': '<gworkspace-creds>/credentials/<user-google-email>.json',
    'google_token.json': '<hermes-home>/google_token.json',
}

for name, path in stores.items():
    try:
        d = json.loads(Path(path).read_text())
        data = urllib.parse.urlencode({
            'client_id': d['client_id'], 'client_secret': d['client_secret'],
            'refresh_token': d['refresh_token'], 'grant_type': 'refresh_token',
        }).encode()
        try:
            resp = urllib.request.urlopen('https://oauth2.googleapis.com/token', data=data, timeout=10)
            print(f'{name}: REFRESH OK')
        except urllib.error.HTTPError as e:
            body = json.loads(e.read())
            print(f"{name}: FAILED - {body.get('error_description', '?')}")
    except Exception as e:
        print(f'{name}: PARSE ERROR - {e}')
PYEOF
```

## Step 3: Decision matrix

| MCP Result | google_token.json Result | Action |
|------------|-------------------------|--------|
| OK | OK | Not an auth issue — look elsewhere |
| OK | Failed | Rare (different client). email_check.py should work. Failed store affects other tools. |
| Failed | OK | email_check.py broken. Copying google_token.json token to MCP creds won't help (different client_id). User must re-auth. |
| Failed | Failed | Both revoked. User must re-auth via paste-back OAuth for both clients. |

## Step 4: If re-auth needed

```bash
# For <operator>'s MCP client (112292610034...)
python3 <hermes-home>/skills/infrastructure/google-workspace-auth/scripts/paste_back_oauth.py

# Follow the re-auth flow in google-workspace-auth SKILL.md
```

## Gotcha: `creds.valid` False Positive

The google-auth library can report `creds.valid: True` and `creds.expired: False` even when the refresh token is revoked. This happens when:

1. The stored access token's `expiry` field is in the past (e.g., `2026-05-07`)
2. But the library's internal `expired` check uses a 5-minute buffer — if the expiry is "close enough," it may still report as valid
3. When `creds.refresh()` is actually called, Google returns `invalid_grant: Token has been expired or revoked.`

**Always test the actual refresh, never trust `creds.valid` alone:**
```python
creds.refresh(Request())  # This is the ground truth
```

Confirmed 2026-06-01: Token file had `expiry: 2026-05-07T08:00:24Z`, library reported `valid: True`, but forced refresh returned `invalid_grant`.

## Important

- **Do NOT copy `google_token.json` into MCP credentials** — Different `client_id` means the MCP server will try to refresh using the wrong client, causing additional failures.
- **Do NOT run `google_oauth_init.py` for MCP credential setup** — It starts its own callback server on port 8000, which competes with the MCP server.
- Use `paste_back_oauth.py` instead.
- When both credential stores show `invalid_grant`, the re-auth requires `access_type=offline&prompt=consent` to get a new refresh token (not just a new access token).