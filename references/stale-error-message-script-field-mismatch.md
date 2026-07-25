# Stale Error Message vs Current Script Field

When a cron job's `last_error` traceback shows a different command or script path than the current `script` field in jobs.json, the error is **stale** — it reflects a previous configuration, not the current state.

## Distinct From

- **`oc_cron_script_not_found_transient`**: Script exists, error is from write/read race during the same run cycle. The error message matches the current script path.
- **`oc_cron_no_agent_script_args`**: The `script` field contains embedded arguments (`foo.py --flag`) that resolve as a literal path. The error message matches the current (broken) field value.
- **`oc_cron_dead_script_ref`**: The script file no longer exists at all.

## Pattern

The job's `last_error` shows a command string (often compound `&&` chains or a different script path) that does NOT match the current `script` field. The current `script` field points to a valid, executable file. The error is from a prior configuration that has since been corrected externally (e.g., a wrapper script was created, or the compound command was split into a single script).

## Confirmed Cases

**dispatch:triage-morning** (fixed 2026-06-20, stale error confirmed 2026-06-25):
- `script` field: `triage_morning.sh` (single file, exists, executable)
<<<<<<< Updated upstream
- `last_error`: `Script not found: <hermes-home>/profiles/indigo/scripts/triage.py && python3 <hermes-home>/skills/ocas-dispatch/scripts/...`
=======
- `last_error`: `Script not found: ~/.hermes/profiles/indigo/scripts/triage.py && python3 ~/.hermes/skills/ocas-dispatch/scripts/...`
>>>>>>> Stashed changes
- The error is from the pre-2026-06-20 configuration when the script field was a compound `&&` command. The fix (wrapper script `triage_morning.sh`) was applied on 2026-06-20, but the scheduler's `last_error` still shows the old failure.
- `consecutive_failures=None` — the job has been running successfully since the fix; the error is residual.

**dispatch:triage-evening** (fixed 2026-06-25, stale error confirmed 2026-06-25):
- `script` field: `triage_evening.sh` (single file, exists, executable)
- `last_error`: Same stale compound `&&` command as morning
- Same pattern — wrapper script created 2026-06-25, but `last_error` references the pre-fix command

## Sub-Pattern: "Script not found" on no_agent jobs (stale symlink fix)

When `no_agent: true` and the `script` field is a bare basename, the error may be "Script not found" at the profile path even though the script exists at the system path. After a symlink fix (profile path → system path), the error persists in jobs.json until the next successful run.

**Confirmed 2026-06-27**: `brief:email-morning` and `brief:email-evening`
<<<<<<< Updated upstream
- Error: `Script not found: <hermes-home>/profiles/indigo/scripts/email_morning_brief.py`
- Fix: Symlink created at `<hermes-home>/profiles/indigo/scripts/email_morning_brief.py` → `<hermes-home>/scripts/email_morning_brief.py` (2026-06-27T05:33)
=======
- Error: `Script not found: ~/.hermes/profiles/indigo/scripts/email_morning_brief.py`
- Fix: Symlink created at `~/.hermes/profiles/indigo/scripts/email_morning_brief.py` → `~/.hermes/scripts/email_morning_brief.py` (2026-06-27T05:33)
>>>>>>> Stashed changes
- Error predates fix (from 2026-06-26T13:40 for morning, 2026-06-27T05:27 for evening)
- `consecutive_failures=None` in both cases

**Diagnosis**: Compare error timestamp to script file mtime. If file exists and error predates it → stale.

## Diagnosis

1. Compare `last_error` traceback to current `script` field.
2. If they differ AND the current script exists + is executable → stale error.
3. For "Script not found" errors: check if script exists at the exact error path. If YES (and executable) → stale (error predates file creation).
4. Verify: `ls -la <current_script>` and `head -1 <current_script>` to confirm it's a valid script.
5. **Run the script** to confirm it works: `bash <current_script>` and check exit code.
6. Check `consecutive_failures` — if `None` or `0`, the job is healthy.

## Classification

`oc_cron_stale_error_script_mismatch` — Tier 2, surface only. No fix needed. The job will self-verify on next successful run (scheduler clears `last_error` on success).

## Journal Notation

```json
{
  "name": "dispatch:triage-morning",
  "error": "Script not found (triage_morning.sh exists at profile path, error is stale from previous compound-command config)",
  "pattern": "oc_cron_stale_error_script_mismatch"
}
```