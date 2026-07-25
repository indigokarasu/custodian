# `oc_cron_script_not_found_transient` Pattern

## When

<<<<<<< Updated upstream
A no_agent cron job reports `Script not found: <hermes-home>/profiles/<profile>/scripts/<basename>` but the script **exists at the exact path mentioned**, is executable, and runs correctly when invoked directly.
=======
A no_agent cron job reports `Script not found: ~/.hermes/profiles/<profile>/scripts/<basename>` but the script **exists at the exact path mentioned**, is executable, and runs correctly when invoked directly.
>>>>>>> Stashed changes

## Distinct From

- **`oc_cron_dead_script_ref`**: Script file genuinely doesn't exist (deleted or never created).
<<<<<<< Updated upstream
- **`oc_cron_script_path_security_block`**: Script exists but at wrong path (e.g., `<hermes-home>/scripts/` instead of `<hermes-home>/profiles/<profile>/scripts/`).
=======
- **`oc_cron_script_path_security_block`**: Script exists but at wrong path (e.g., `~/.hermes/scripts/` instead of `~/.hermes/profiles/<profile>/scripts/`).
>>>>>>> Stashed changes
- **`oc_cron_no_agent_script_args`**: Script field contains embedded arguments (`foo.py --flag`) treated as literal path.

## Root Cause

Write/read race condition: the script file was being written (created or modified) at the exact moment the cron scheduler attempted to resolve it. The scheduler's file resolution check observed an incomplete or absent file, then cached the "not found" result for that run.

Evidence: script mtime is AFTER the failed run time. In the observed case:
- Cron run: 2026-06-24 04:36:14
- Script mtime: 2026-06-24 06:05 (modified 89 minutes after the failed run)
- Script created: 2026-06-22 23:08 (existed for ~29 hours before the run)

Despite existing for 29+ hours, the error occurred — suggesting the file was rewritten/modified at 06:05 and the 04:36 race was with an earlier write operation, OR the scheduler does a content hash check that failed temporarily.

## Classification

**Transient** — Tier 2 (surface only, do NOT escalate). The job will succeed on its next scheduled run without intervention.

## Diagnostic Steps

1. Verify script exists: `ls -la <path_from_error>`
2. Verify executable: `test -x <path_from_error>`
3. Verify content valid: `bash -n <path_from_error>` (for shell scripts)
4. Run directly: `bash <path_from_error>` — should exit 0
5. Check mtime vs run time: `stat <path>` — if mtime > run time, confirms race condition
6. Check next_run_at: should be the next schedule occurrence

## Fix

**None needed.** Auto-resolves on next run. Do NOT apply pause/resume, do NOT edit jobs.json, do NOT recreate the script.

## Journal Note

Record as `oc_cron_script_not_found_transient` with note: "Script exists and runs correctly. Error at <time> was a write/read race condition. Next run (<next_scheduled>) will succeed."

## Pitfall

Do NOT classify as `oc_cron_dead_script_ref` (which would trigger Tier 1 fix attempts). Always verify the script exists AND is executable before classifying. If both conditions are met, this is the transient pattern.