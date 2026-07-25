# Stale Error State: Pause/Resume Fix Pattern

## Pattern

A cron job shows `status=error` with `consecutive_failures=None` (literal null) or `consecutive_failures=0`, and `last_error` references a provider/path that no longer exists in the current configuration.

## Root Cause

The Hermes scheduler doesn't always update `consecutive_failures` or `last_status` when an underlying issue is resolved externally (e.g., a broken fallback provider is removed from config.yaml by a prior escalation run). The job's scheduler state remains stuck on the old error.

## Diagnosis Steps

1. **Check `last_error` content**: Look for provider references (e.g., `kepler.ai.cloud.ovh.net`, specific provider names)
<<<<<<< Updated upstream
2. **Verify current config.yaml**: `grep -i <provider> <hermes-home>/profiles/<profile>/config.yaml` — if the provider is no longer listed, the error is stale
=======
2. **Verify current config.yaml**: `grep -i <provider> ~/.hermes/profiles/<profile>/config.yaml` — if the provider is no longer listed, the error is stale
>>>>>>> Stashed changes
3. **Check `next_run_at`**: If the job is still being scheduled (next_run_at is future), the scheduler is running it but stuck on the old error state
4. **Check `consecutive_failures`**: `None` (null) means the scheduler never updated the counter; `0` with `status=error` means it reset but didn't clear the status

## Fix

```bash
hermes cron pause <job_id>
hermes cron resume <job_id>
```

This forces the scheduler to recalculate internal state from jobs.json. On resume, `next_run_at` will be recalculated and `status` will reset.

## Verification Before Fix

**Always verify the underlying cause is gone before resetting:**
- If `last_error` references a provider still in config.yaml → error is ACTIVE, not stale. Do NOT pause/resume; fix the provider instead.
- If `last_error` references a provider that was removed → error is STALE. Safe to pause/resume.

## Example (2026-06-18)

`genie:update` and `soul:sync` both had `status=error` with `consecutive_failures=None` and `last_error` referencing `kepler.ai.cloud.ovh.net` (OVH provider). Investigation showed:
1. OVH providers were already removed from config.yaml by prior escalation run (2026-06-18-0925)
2. Both jobs had `next_run_at` in the future (still being scheduled)
3. Both had `provider=openrouter` and `model=openrouter/owl-alpha` explicitly set

Fix: `hermes cron pause de59a77614e9 && hermes cron resume de59a77614e9` (same for soul:sync). Both resumed with clean state and next_run_at recalculated.

## Variant — Stale PAUSE (resolved issue + frozen job, paused_reason=None)

**Inverse of the pause-loop guidance.** A job may be left `paused` (paused_at set) with `paused_reason=None` while its corresponding issue is already `resolved` AND a live re-run confirms the fix holds. The job is healthy but frozen — a silent monitoring gap, the opposite of the problem above.

**Detection (Step 1c inverse gotcha):** cross-check every `paused` job against its issue's status. If the issue is `resolved` and the live fix is re-verified (re-run the actual script → EXIT=0 on real volume, not a drained queue), the pause is stale and should be cleared.

**Diagnosis:**
1. Issue for the job is `resolved` (or `user_gated` but the underlying failure is actually fixed).
2. Job `paused_at` is set but `paused_reason=None` (or references a now-resolved root cause).
3. Live re-run of the job's script succeeds on REAL backlog volume.

**Fix:** clear the pause so the job resumes its schedule:
```python
# in jobs.json, set for the job:
j["paused_at"] = None
j["paused_reason"] = None
# then json.dump back
```
(Equivalent to `hermes cron resume <job_id>` if the CLI honors the state; direct jobs.json edit is reliable in cron contexts.)

**Verification:** re-read jobs.json — `paused_at=None`, `paused_reason=None`, `last_status` still error but will clear on next run. Do NOT mark the issue unresolved; the fix is genuine, the pause was just never lifted.

**Example (2026-07-14):** `chronicle:daily-embed` (id `f7fb5ff15067`) was `paused` with `paused_reason=None`; its timeout issue `oc_script_timeout_chronicle_embed_20260713` was `resolved`. Live re-run completed in 13.6s/EXIT=0 against real volume (46,616 unembedded events confirmed via `chronicle_embed_backlog_probe.py` — not a drained queue). Pause cleared; job resumed. NOTE: this is the mirror-image of the Step 8e drained-backlog false-close trap — there, a *claimed* resolve was false; here, a *real* resolve left the job frozen. Always re-verify the fix live before lifting a pause.