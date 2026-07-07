# Deep Scan: Fix-Loop Pre-Handled Silent Verdict

**Pattern confirmed 2026-06-25:** When all error jobs are transient AND a fix-loop RCA already exists from a prior esc-run in the same day, the scan returns `[SILENT]` without producing a new escalation report.

## Trigger Conditions

All three must be true:
1. Every error job has `consecutive_failures` in (None, 0) AND matches a known transient pattern
2. An RCA record exists for the fix-loop fingerprint with `pattern: "B"` and `fix_loop_detected: true`
3. The RCA's occurrence chain already includes the current recurrence

## Rationale

The escalation runner that processed the fix-loop earlier in the day already:
- Created the RCA record with the full occurrence chain
- Tagged the journal `escalation_needed: true`
- Classified the issue as Tier 3

A subsequent deep scan finding the same null keys should NOT:
- Write a second escalation journal for the same occurrence chain
- Produce a report when there is genuinely nothing new to report
- Apply the fix again (fix-loop policy: do NOT re-apply 3rd+ time)

## Journal Write

Still write an observation journal with `not_activity_reason` explaining:
- All error jobs are transient (list the classifications)
- The fix-loop RCA already exists (cite `rca_id`)
- No new action needed

## Example (2026-06-25)

```
Run: deep-scan at 03:04 UTC
Error jobs: 13 (all transient — certifi SSL, futures shutdown, 429, monitor no-op, script-not-found race, compound cmd)
Fix-loop: oc_config_empty_section occurrence #5
  - RCA: rca-oc_config_empty_section_fix_loop-20260624 (created 21:08 UTC by esc-run)
  - Pattern B confirmed, fix_loop_detected: true
  - Action: none (do NOT re-apply per fix-loop policy)
  - Note: 5th occurrence includes nested keys (cron.max_parallel_jobs, kanban.max_in_progress_per_profile)
Result: [SILENT] — journal written, no report produced
```

## Distinction from Escalation Required

If the fix-loop is a **new** occurrence (not in the existing RCA's chain) OR if the RCA doesn't exist yet, proceed with escalation per Step 10. This pattern only applies when the RCA already covers the current state.
