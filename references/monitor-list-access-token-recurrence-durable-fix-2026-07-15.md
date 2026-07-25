# monitor:list access_token Recurrence + Durable Fix (2026-07-15)

## Symptom
`monitor:list` (no_agent, job `39b7edc44b35`, script `monitor_list.py` → wraps `ocas-tasks/scripts/tasks_monitor.py --mode check`) fails masked as `Script exited with code 1`. Live `tasks_monitor.py --mode check` → `KeyError: 'access_token'` at `get_access_token()` (tasks_monitor.py:88).

## Discriminator (PRESENT vs ABSENT access_token) — critical
Inspect the creds file `<gworkspace-creds>/credentials/<user-google-email>.json`:
- **`access_token` PRESENT (non-empty):** transient credential-refresh RACE (file was mid-rewrite when probed). Re-run worker 1–2×; if it exits 0, resolve any `user_gated` issue for this fingerprint as a FALSE ESCALATION. Do NOT persist. (See `monitor-list-keyerror-transient-creds-race-2026-07-14.md`.)
- **`access_token` ABSENT (only `token` + valid `refresh_token` + future `expiry`):** PERSISTENT code defect, NOT a race. Proceed to fix below.

## Root cause (why it recurs)
The upstream Google Workspace MCP credential store periodically REWRITES the creds file with only the key `token` (no `access_token`) and a *future* string `expiry`. `tasks_monitor.get_access_token()` trusted the future `expiry` and never checked whether `access_token` actually existed — so it crashed on the missing key instead of refreshing. A one-off non-interactive refresh held only ~5.5h before the store stripped `access_token` again.

## Fix (two parts)
1. **Immediate (non-interactive, no <operator> re-auth):** `refresh_token()` in tasks_monitor.py POSTs to `https://oauth2.googleapis.com/token` using the valid `refresh_token` + `client_secret`. It mints a fresh `access_token` and writes it back. Reuse the module's own function:
   ```python
   import importlib.util
   spec = importlib.util.spec_from_file_location("tm", "<hermes-home>/profiles/indigo/skills/ocas-tasks/scripts/tasks_monitor.py")
   mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
   mod.refresh_token(mod.load_creds())
   ```
2. **Durable (code fix in `tasks_monitor.py` `get_access_token()`):** replace the body so it falls back to `token` and triggers a refresh when no usable token exists:
   ```python
   def get_access_token():
       creds = load_creds()
       token = creds.get("access_token") or creds.get("token")
       expiry_ts = parse_expiry(creds.get("expiry", 0))
       if not token or time.time() > expiry_ts - 300:
           creds = refresh_token(creds)
           token = creds.get("access_token") or creds.get("token")
       return token
   ```
   This eliminates the crash-on-stripped-creds recurrence.

## Verification recipe
- `tasks_monitor.py --mode check` → EXIT 0
- `scripts/monitor_list.py` (run from profile `scripts/` dir, NOT `skills/`) → EXIT 0
- `hermes cron run 39b7edc44b35` → succeeded
- `jobs.json` `monitor:list`: `last_status=ok`, `last_error=None`
- Self-heal test: strip `access_token` (keep `token` + future expiry) in the creds file, re-run worker → must exit 0 and repopulate `access_token`. (Also test the deepest case: strip BOTH `access_token` and `token` → refresh must fire and heal the file.)

## Escalation-loop note (Step 8d false-resolution)
This fingerprint recurred AFTER a prior loop marked `oc_google_tasks_access_token_missing_20260714` `resolved` (18:10Z) via a one-off non-interactive refresh. The job re-failed live at 23:39Z with the identical signature → that resolution was FALSE under Step 8d. When you see this recurrence, REOPEN the issue, apply the DURABLE code fix (not just another refresh), verify, and record `recurrence_resolved_code: true`. The proper classification is a code defect fixable by the escalation loop (non-interactive), NOT `user_gated`.
