# verify_plugin_defect_postrestart.py — False-LIVE Bug (2026-07-24)

## Symptom
`verify_plugin_defect_postrestart.py` reported `post_restart_total=12/9 → LIVE (escalate / keep open)` for the Chronicle `actor_check` and `seq_unique` signatures, even though the fixes were committed (10:40) BEFORE the most recent gateway restart (12:42:57) and the defects had stopped.

## Root cause
`scan_log()` maintained a single **running** `last_restart` that updated on every `SIGTERM` / `Starting Hermes Gateway` line, and bucketed each signature hit as `post` if its timestamp was `>=` that *nearest-preceding* restart. On a log with ~20 restarts, every historical error falls after *some* earlier restart, so `post_restart_total` was **always > 0** and the SUMMARY always printed `LIVE (escalate)`. The script answered "did this error occur after *a* restart?" instead of "did it occur after the *most recent* restart?"

## Ground-truth check (the detection recipe)
Before trusting any `LIVE` verdict, independently confirm with a tool that scopes to the **most recent** restart line:

```bash
LOG=$HERMES_HOME/../indigo/logs/gateway.log
# real last restart line number + time
grep -nE "Starting Hermes Gateway" "$LOG" | tail -1
# any actor/seq constraint errors AFTER that line number?
awk 'NR>=<last_restart_lineno>{print}' "$LOG" | grep -cE "CHECK constraint failed: actor|UNIQUE constraint failed: events.seq"
# expect 0 on a healthy post-fix runtime
```

If the independent count is 0 but the verifier says LIVE, the verifier is wrong — do NOT reopen resolved issues.

## Fix (applied 2026-07-24)
`scan_log()` now does two passes: Pass 1 finds the **MAX** restart timestamp in the whole file (`overall_last_restart`); Pass 2 buckets each hit as `post` only if `t >= overall_last_restart`. The SUMMARY verdict (`post > 0 → LIVE`) is now correct.

## Why this matters
A chronic FALSE-LIVE verdict re-escalates already-`resolved` issues (e.g. `oc_chronicle_event_actor_check_constraint_20260722`, `oc_chronicle_event_seq_unique_constraint_20260722`) on every light scan, creating data-integrity regressions that a later scan must reverse. Always cross-check the verifier against the raw log before acting on its LIVE verdict.

## Live-vs-stale log caveat
The script's DEFAULT scans BOTH `$HERMES_HOME/../<profile>/logs/gateway.log` (LIVE) and `~/.hermes/logs/gateway.log` (often STALE). Prefer running with `--log <live path>` to avoid counting stale-pre-restart noise. (The stale root log had 0 Chronicle errors, so it did not contribute post-fix, but the live-path flag is the safe default.)
