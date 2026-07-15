name: monitor-list-keyerror-noninteractive-refresh-fix-2026-07-15
license: MIT
description: Third case of monitor:list masked `KeyError: 'access_token'` — creds file lacks the `access_token` key but has a valid `refresh_token`; fixable non-interactively by calling `tasks_monitor.refresh_token()` directly. Do NOT label it user_gated.

# monitor:list `KeyError: 'access_token'` — Case C: persistent, but non-interactively fixable

## Context
`monitor:list` (no_agent, `script: monitor_list.py`) wraps
`ocas-tasks/scripts/tasks_monitor.py --mode check`. The wrapper masks the real
traceback, so `jobs.json` shows bare `Script exited with code 1`. The real error is
`KeyError: 'access_token'` at `tasks_monitor.get_access_token()`.

There are THREE distinct cases (the custodian SKILL.md OAuth section covers A = transient
race and B = genuinely revoked). This file is Case C, discovered + fixed 2026-07-15.

## Discriminator (check the KEY, not just any token)
Inspect `/root/.google_workspace_mcp/credentials/google-workspace-user.json`:
- **Case A (transient race):** key `access_token` present + non-empty → re-runs succeed → resolve false-escalation.
- **Case B (user-gated):** `refresh_token` absent/revoked or OAuth client deleted → interactive re-auth needed → leave `user_gated`.
- **Case C (THIS FILE):** NO `access_token` key, but `refresh_token` present + valid AND a `token` key + a **future** `expiry`.

In Case C, `tasks_monitor.get_access_token()` (lines 83-88) does:
```python
expiry_ts = parse_expiry(creds.get("expiry", 0))
if time.time() > expiry_ts - 300:
    creds = refresh_token(creds)   # line 86-87 — NEVER REACHED
return creds["access_token"]      # line 88 — KeyError, key absent
```
Because `expiry` is in the future, the `if` is false, the native `refresh_token()`
call is skipped, and line 88 throws `KeyError: 'access_token'`. Both the wrapper
and a direct `tasks_monitor.py --mode check` re-run fail PERSISTENTLY (this is NOT
a mid-write race — the token is simply never populated).

## Fix (non-interactive — NO owner re-auth required)
Invoke the script's own `refresh_token()` directly. It POSTs the valid `refresh_token`
to `https://oauth2.googleapis.com/token`, mints a fresh `access_token`, writes it to
the creds file, and returns:

```python
# /tmp/trigger_refresh.py
import sys
sys.path.insert(0, "<hermes-home>/skills/ocas-tasks/scripts")
import tasks_monitor as tm
creds = tm.load_creds()
refreshed = tm.refresh_token(creds)   # uses valid refresh_token
print("access_token now present:", "access_token" in refreshed)
```
Run: `python3 /tmp/trigger_refresh.py` → expect `REFRESH OK; access_token now present: True`.

## Verification (authoritative)
1. `python3 <hermes-home>/skills/ocas-tasks/scripts/tasks_monitor.py --mode check` → exit **0** (was `KeyError`).
2. `python3 <hermes-home>/scripts/monitor_list.py` → exit **0**.
3. `hermes cron run <job_id>` (e.g. `39b7edc44b35`) → **"succeeded"**.
4. `jobs.json`: the job's `last_status` flips to `ok`, `last_error` → `None`.

## Reconcile the issue (race-safe, survives top-of-hour custodian:light rewrite)
Use `scripts/race_safe_issue_patch.py`:
```bash
python3 scripts/race_safe_issue_patch.py \
  --issue-id oc_google_tasks_access_token_missing_20260714 \
  --set status=resolved --set user_gated=false --set escalation_needed=false \
  --set resolved_at=2026-07-15T18:10:00Z \
  --set resolved_by="escalation-execution-loop" --retries 4
```
Then add `fix_applied` + `resolved_note` (whole-file rewrite is fine; the patcher
guards the scalar fields against the concurrent-rewrite race). Set `verified: true`.

## Pitfall — do NOT mislabel Case C as user_gated
A prior scan (2026-07-14) saw this same `KeyError`, declared it a transient race,
resolved it, then a later re-open labeled it `user_gated` (claiming the `refresh_token`
was dead). Both were WRONG: the fault was real AND fixable non-interactively. Labeling
it `user_gated` hides a self-fixing defect behind "needs owner" for 18h. When the
creds file has a valid `refresh_token`, always try the direct `refresh_token()` call
before concluding user-gated.

## Pitfall — false-resolution slip-through
`reopen_false_resolutions.py` only matches `token_expired` / `402 credits` /
`owl-alpha 404` signatures. A `resolved` `oc_google_tasks_access_token_missing`
whose "resolution" was a false transient-race claim is NOT re-detected by it. When
triaging a Google-Tasks `KeyError` issue that already carries a `resolved_at` +
`false_escalation: true`, RE-RUN the wrapped worker live before trusting the note —
the note may describe a recovery that never actually happened.
