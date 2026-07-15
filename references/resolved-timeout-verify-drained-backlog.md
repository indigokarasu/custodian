# Resolved-Timeout Verify Against Real Volume (not drained backlog)

**Confirmed:** 2026-07-13 light scan caught `oc_script_timeout_chronicle_embed_20260713` as a FALSE CLOSE via Step 8e.

## Principle
A timeout/throughput code-defect issue must be verified against the **actual production data volume**, not a queue that was just drained by a prior run. A script that finishes in ~163s on an empty queue will hang past the 600s cron hard limit when the daily backlog rebuilds.

## Detection recipe
1. Read the job's progress/state file (`commons/db/chronicle/embed_state.json` → `last_run`). If `last_run` is within seconds of the claimed fix/resolution time, the verification run executed on a cleared queue — inconclusive.
2. Inspect real volume:
   `python3 -c "import sqlite3;c=sqlite3.connect('<hermes-home>/commons/db/chronicle/chronicle.db');print(c.execute('SELECT COUNT(*) FROM facts').fetchone())"`
   (facts table held 35,486 rows in the failing run.)
3. Re-run the actual script against that full volume with a hard cap. The foreground terminal cap is 60s — insufficient for a script that may run 590s. Use:
   `terminal(background=true, notify_on_complete=true)` running `timeout 590 python3 scripts/<script>.py > /tmp/<script>_test.log 2>&1`, then `process(wait/poll)`. For a steady-state run it must complete (exit 0, under 600s) with the full backlog present.
4. If it is still embedding past ~85s on an 8,000-row pass with more tables pending → the timeout is ACTIVE. Reopen (`status: open`, `escalation_needed: true`, `user_gated: false`, clear `resolved_at`, set `reopened_at` + `reopen_note`).

## What a proper resolution looks like
- The fix reduces work per run (lower `DAILY_DOC_LIMIT`), raises the cron timeout, or swaps to a faster embedding endpoint — AND a live full-backlog run completes under 600s.
- `last_error` on the job clears on a post-fix scheduled run with normal volume.

## Related
- `references/chronicle-daily-embed-timeout-pattern.md` — fingerprint + sibling-script isolation.
- `references/resolved-codefix-regression-verify.md` — general verify-before-accepting-prior-resolution (stale-error direction).
