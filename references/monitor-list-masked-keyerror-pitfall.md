# monitor:list masked error — local KeyError access_token (not OAuth API error)

**Confirmed:** 2026-07-12 light scan (`monitor:list` job `39b7edc44b35`).

## Symptom
`monitor:list` (`no_agent: true`, script `monitor_list.py`) shows `last_error: "Script exited with code 1"`.
The cron output file shows only `Script exited with code 1` — **no captured stderr**.
Running `monitor_list.py` manually also exits 1 with empty stdout/stderr.

## Why the error is invisible (two layers of masking)
1. `monitor_list.py` runs the real monitor as a subprocess and **swallows its stderr**:
   ```python
   result = subprocess.run([sys.executable, str(SCRIPT), "--mode", "check"], capture_output=True, ...)
   if result.returncode != 0:
       sys.exit(1)   # does NOT print result.stderr
   ```
2. So the no_agent wrapper reports only "exit 1"; the actual failure is hidden inside the subprocess.

## Real error this occurrence
Running the wrapped script directly revealed:
```
KeyError: 'access_token'
  File ".../ocas-tasks/scripts/tasks_monitor.py", line 88, in get_access_token
    return creds["access_token"]
```
This is a **local credential-store failure** (the token file is missing the `access_token` key) — NOT the
OAuth API `400/403/invalid_grant` the historical subprocess-cascade pattern documented. Different surface,
same job family (Google Tasks auth for that account).

## Diagnostic recipe (reuse)
1. Read the job's `script` field (e.g. `monitor_list.py`).
2. Read the wrapper source; locate `subprocess.run([..., "--mode", "check"])` → the real script (e.g. `tasks_monitor.py`).
3. Run the wrapped script **directly, bypassing the wrapper**, to surface the real error:
   `python3 <hermes-home>/skills/ocas-tasks/scripts/tasks_monitor.py --mode check`
4. Classify the real error:
   - `KeyError: 'access_token'` → local credential store missing key (repair token file / re-run OAuth flow).
   - `400/403/invalid_grant` on `oauth2.googleapis.com/token` → OAuth API failure.
   Both are **user-gated**, NOT `oc_cron_no_agent_exit_1_noop`. Leave the monitor running (no pause).

## Attribution
Belongs to the Google Tasks auth issue (`oc_google_tasks_api_403_forbidden` / `oc_google_oauth_token_revoked`
family). Do NOT create a new fingerprint — attribute to the existing issue.
