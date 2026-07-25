# Subprocess Cascade OAuth Error Masking Pattern

**Confirmed 2026-07-06**

## The Pattern

When a monitor/wrapper script runs a subprocess that performs Google OAuth, the wrapper job's `last_error` shows **"Script exited with code 1"** — the actual OAuth error (`invalid_grant`, `deleted_client`, HTTP 400 on token endpoint) is **masked** by the subprocess exit code.

## Real-World Instance

**2026-06-28/29**: `monitor:list` wraps `tasks_monitor.py` as a subprocess:
```python
# monitor_list.py
subprocess.run([sys.executable, str(SCRIPT), "--mode", "check"])
```

The cron job `monitor:list` shows:
```
last_error: "Script exited with code 1"
```

But the **actual error** (visible only by running the subprocess directly):
```
HTTPError: 400 Client Error: Bad Request for url: https://oauth2.googleapis.com/token
  -> google.auth.exceptions.RefreshError: invalid_grant: Token has been expired or revoked
```

## Same Root Cause, Different Manifestations

| Job | Script | Auth Mechanism | Error Shown in `last_error` |
|-----|--------|----------------|----------------------------|
| `email:check` | `email_check.py` → `google_auth_mcp.py` | Direct API call in process | Full traceback with `RefreshError: invalid_grant` |
| `monitor:list` | `monitor_list.py` → `tasks_monitor.py` (subprocess) | Subprocess uses `CREDS_FILE = ".../<user-google-email>.json"` | **"Script exited with code 1"** (OAuth error masked) |

Both jobs failed from the **same revoked refresh token** on the same account (`<user-google-email>`). Only `email:check` revealed the root cause directly.

## Diagnostic Procedure

When a `no_agent` cron job shows `"Script exited with code 1"` and uses a subprocess wrapper:

1. **Identify the wrapped script** — check the wrapper's `subprocess.run()` call
2. **Run the subprocess directly**:
   ```bash
<<<<<<< Updated upstream
   python3 <hermes-home>/profiles/indigo/scripts/tasks_monitor.py --mode check
=======
   python3 ~/.hermes/profiles/indigo/scripts/tasks_monitor.py --mode check
>>>>>>> Stashed changes
   ```
3. **Observe the actual stderr** — this time (not masked by wrapper)
4. **Cross-reference** with other jobs using the same credentials

## Classification Trap

Do NOT classify as `oc_cron_no_agent_exit_1_noop` (Tier 2, surface-only no-op exit). The exit 1 here is a **real subprocess failure**, not a no-op monitor. The subprocess error IS the root cause.

## Affected Job Pattern (2026-06-29 Confirmation)

- `email:check` — direct auth, shows OAuth error in traceback → clearly `oc_google_oauth_token_revoked`
- `monitor:list` — subprocess wrapper, shows "Script exited with code 1" → **masked** `oc_google_oauth_token_revoked`
- `sands:*`, `taste:*`, `vesper:*` — **unaffected** — use different auth flows or different account credentials (the agent's account, not <operator>'s)

**Key insight**: The same token revocation does NOT necessarily cascade to all Google-auth jobs. Only jobs using the revoked account's credentials directly fail. Check each job's credential source before assuming cascade.