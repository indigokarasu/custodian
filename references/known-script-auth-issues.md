# Known Script Auth Issues — Reference for Custodian Scans

## `email_check.py` — Auth Flow

<<<<<<< Updated upstream
**File**: `<hermes-home>/profiles/indigo/scripts/email_check.py` (moved from `<hermes-home>/scripts/email_check.py` on 2026-06-03)
**Import**: Line 20 does `from google_auth_mcp import get_gmail_service`
**Path trick**: Line 19 does `sys.path.insert(0, str(AGENT_ROOT / "scripts"))` — this adds the OLD scripts dir to sys.path, so `google_auth_mcp` resolves from `<hermes-home>/scripts/google_auth_mcp.py` (4.8K, exists).
=======
**File**: `~/.hermes/profiles/indigo/scripts/email_check.py` (moved from `~/.hermes/scripts/email_check.py` on 2026-06-03)
**Import**: Line 20 does `from google_auth_mcp import get_gmail_service`
**Path trick**: Line 19 does `sys.path.insert(0, str(AGENT_ROOT / "scripts"))` — this adds the OLD scripts dir to sys.path, so `google_auth_mcp` resolves from `~/.hermes/scripts/google_auth_mcp.py` (4.8K, exists).
>>>>>>> Stashed changes
**Status**: Import works. The `google_auth_mcp.py` module exists at the old path.

### Stale Error Detection — Script Path Mismatch

When a cron job's `last_error` traceback shows a different script path than the current `script` field in jobs.json, the error is **stale** (from before a previous fix):

```
<<<<<<< Updated upstream
Traceback path: <hermes-home>/scripts/email_check.py     ← OLD
jobs.json script field: <hermes-home>/profiles/indigo/scripts/email_check.py  ← NEW
=======
Traceback path: ~/.hermes/scripts/email_check.py     ← OLD
jobs.json script field: ~/.hermes/profiles/indigo/scripts/email_check.py  ← NEW
>>>>>>> Stashed changes
```

**Diagnostic**: Extract paths from traceback with `re.findall(r'File "(<fs-root>/\.hermes[^"]+)"', last_error)` and compare against `jobs.json` `script` field. If they differ, the error predates the path fix.

**Caveat**: `consecutive_failures > 0` may still be real even if the traceback path is stale — the counter accumulates across runs. Check `last_run_at` age to determine if failures are ongoing or historical.

<<<<<<< Updated upstream
**As of 2026-06-04**: `email:check` has `consecutive_failures=2` and `last_run_at=2026-06-01T17:33`. The error traceback references the OLD script path (`<hermes-home>/scripts/email_check.py`) while the job's `script` field was updated to `<hermes-home>/profiles/indigo/scripts/email_check.py` on 2026-06-03. Since `last_run_at` (2026-06-01) predates the fix (2026-06-03), the error is **definitively stale**. The MCP credentials at `<gworkspace-creds>/credentials/<user-google-email>.json` are valid with refresh_token. The `google_auth_mcp.py` import resolves correctly via `sys.path.insert(0, str(AGENT_ROOT / "scripts"))`. **Issue resolved — no user action needed.** The job should succeed on its next scheduled run.
=======
**As of 2026-06-04**: `email:check` has `consecutive_failures=2` and `last_run_at=2026-06-01T17:33`. The error traceback references the OLD script path (`~/.hermes/scripts/email_check.py`) while the job's `script` field was updated to `~/.hermes/profiles/indigo/scripts/email_check.py` on 2026-06-03. Since `last_run_at` (2026-06-01) predates the fix (2026-06-03), the error is **definitively stale**. The MCP credentials at `<gworkspace-creds>/credentials/<user-google-email>.json` are valid with refresh_token. The `google_auth_mcp.py` import resolves correctly via `sys.path.insert(0, str(AGENT_ROOT / "scripts"))`. **Issue resolved — no user action needed.** The job should succeed on its next scheduled run.
>>>>>>> Stashed changes

### Actual Failure Mode

Import succeeds, but `creds.refresh(Request())` returns `invalid_grant` because the refresh token was revoked by Google. This is NOT a code bug — it requires OAuth re-auth via browser. Do NOT treat `invalid_grant` as an import error.

---

## Manifest.build API Key Verification

<<<<<<< Updated upstream
**Config**: `fallback_model.api_key` in `<hermes-home>/config.yaml` (key starts with `mnfst_`)
=======
**Config**: `fallback_model.api_key` in `~/.hermes/config.yaml` (key starts with `mnfst_`)
>>>>>>> Stashed changes
**Quick test**: `curl -s -o /dev/null -w "%{http_code}" https://app.manifest.build/v1/models -H "Authorization: Bearer <key>"`
- 200 = key valid
- 401 = key invalid or expired

When `finch:weekly` fails with HTTP 401 from manifest.build, test the key with curl before escalating. The 401 may be transient (provider-side) even with a valid key. Confirmed 2026-06-01: API key returns 200 via curl, so the finch:weekly 401 was transient.

---

## Other Known Script Auth Patterns

See `google-workspace-auth` skill for the full OAuth credential management procedures.