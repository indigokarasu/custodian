# Light Scan Triage: Stale Error on Fixed No-Agent Job

When a light scan encounters a `no_agent: true` job with `status=error` and `consecutive_failures=None`, and the `last_error` shows a compound `&&` command but the current `script` field is a single wrapper file — the error is stale. The fix has already been applied.

## Confirmed Case: `dispatch:triage-evening` (2026-06-26)

- `script`: `triage_evening.sh` (exists, executable, runs clean with exit 0)
- `last_error`: `Script not found: <hermes-home>/profiles/indigo/scripts/triage.py && python3 <hermes-home>/skills/ocas-dispatch/scripts/journal.py`
- `consecutive_failures`: None
- `last_status`: error (stale — scheduler hasn't run the job since the fix)

## Diagnostic Sequence

1. Parse `jobs.json` → find error jobs
2. For each `no_agent: true` error job: compare `last_error` to current `script` field
3. If `last_error` contains `&&`, `;`, `|`, or embedded arguments AND current `script` is a single filename → stale error
4. Verify: `ls -la <script>` exists + executable; `bash <script>` exits 0
5. Classify as `oc_cron_stale_error_script_mismatch` (Tier 2, surface only)
6. Do NOT re-apply wrapper fix, do NOT escalate, do NOT create issues.jsonl entry
7. Note in journal's `error_classification` with `transient: true`

## Journal Notation Template

```json
{
  "oc_cron_no_agent_script_args_stale": {
    "count": 1,
    "description": "Stale error: <job> compound && was fixed with wrapper script <wrapper>. Wrapper verified running successfully. Error is from pre-fix run.",
    "transient": true,
    "by_design": false,
    "jobs": ["<job_name>"]
  }
}
```

## Why This Isn't In `oc_cron_no_agent_script_args`

The `oc_cron_no_agent_script_args` fingerprint applies when the current `script` field is STILL the broken compound command (active error). Once the wrapper fix is applied, the fingerprint transitions to `oc_cron_stale_error_script_mismatch` because the current field is valid but the error record is residual.

## Key Insight

The scheduler clears `last_error` on the next successful run. Until the job runs again (next schedule cycle), the stale error persists in `jobs.json`. A light scan that checks between the fix and the next run will see the stale error. **Do not re-fix.** The wrapper script is in place and working.

## See Also

- `references/stale-error-message-script-field-mismatch.md` — general stale error pattern
- `references/no-agent-script-argument-pattern.md` — active no_agent compound command pattern
- `references/dispatch-triage-evening-no-agent-script-args.md` — fix details
- `references/post-fix-wrapper-script-verification.md` — how to verify wrapper scripts