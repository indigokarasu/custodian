# Cron Output Verification Gap

## The Problem

Custodian checks cron job health via `jobs.json` — `last_status`, `last_run_at`, `last_error`, `consecutive_failures`. But it does NOT verify that cron jobs are actually producing their expected output files or side effects.

## What Happened (June 8, 2026)

- `ocas-autobio-distill` (monthly, 1st of month): Last ran June 1. Next scheduled July 1. jobs.json showed `last_status=ok`. But the profile SOUL.md hadn't been updated since May 31 because the distillation was writing to the repo, not the profile. Custodian never flagged this because the job *executed* successfully — it just didn't do what mattered.
- `soul:sync` (23:00 daily): `last_status=error` for multiple runs. Custodian correctly flagged this as an error job. The timing collision with the 23:00 observation (which writes files the sync needed to commit) was the root cause, but Custodian's scans did not diagnose the *pattern* — just that the job was erroring.

## The Pattern

A cron job can:
1. Execute successfully (`last_status=ok`) but produce no meaningful output
2. Execute successfully but write to the wrong target path
3. Execute successfully on the wrong schedule (e.g., monthly when daily is needed)
4. Error repeatedly due to timing collisions that the scan doesn't diagnose

## What Custodian Should Add

When a cron job's prompt contains keywords like "write", "update", "distill", "sync" AND the job has a known output path:
- After the job's `last_run_at`, check if the expected output file was modified (via `stat` mtime)
- If the output file's mtime predates `last_run_at` by more than the schedule interval, flag as `oc_cron_no_output`
- This is a lightweight check: one `stat` call per output-producing job

## Why This Wasn't Caught

The autobio distillation pipeline was designed to write to the repo SOUL.md, and it did so successfully. The failure mode was *target selection* (repo vs. profile), not *execution failure*. No existing Custodian fingerprint covers "job ran correctly but target is wrong."

## Related

See `references/jobs-not-running-diagnostic.md` — addresses jobs that don't run at all. This is the complement: jobs that run but don't produce output.