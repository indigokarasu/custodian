# Subprocess Cascade Re-verification Pitfall

**Confirmed 2026-06-29**

When a monitor/wrapper script runs a subprocess (e.g., `monitor:list` → `tasks_monitor.py`), a single manual run exiting 0 does **NOT** prove the error is stale.

## The Trap

1. Job shows `last_status=error` with "Script exited with code 1"
2. You run the wrapper script manually → exit 0
3. You classify the error as "stale — script runs fine now"
4. The next scheduled run fails again with the same error

## Why It Happens

Subprocess failures can be:
- **Persistent but timing-dependent**: OAuth token refresh fails at certain times but succeeds at others (token expiry window, API rate limit reset)
- **Intermittent**: Network timeouts, upstream API flakiness
- **State-dependent**: The subprocess reads cached credentials/tokens that expire between runs

## Correct Verification Procedure

Before classifying a subprocess-wrapped error as stale:

1. Run the **subprocess directly**: `python3 <subprocess_script> --mode check`
2. Run the **wrapper script**: `python3 <wrapper_script>`
3. Run **both at least twice** with a few seconds between runs
4. If **any** run reproduces the error → it is ACTIVE, not stale
5. Only classify as stale if ALL runs succeed AND the error timestamp predates the last known fix

## Real-World Instance

**2026-06-29**: Light scan at 03:01 UTC ran `monitor_list.py` directly, got exit 0, classified the error as "stale — script runs fine now." The 21:55 PDT run failed again with the same OAuth `invalid_grant` on `tasks_monitor.py`. The subprocess (`tasks_monitor.py --mode check`) still failed consistently — the wrapper's earlier exit 0 was a transient success (possibly cached token state or timing), not actual resolution.

**Lesson**: Always test the subprocess directly, not just the wrapper. And test multiple times.
