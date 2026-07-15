# Light-Scan False-Resolution Gotcha (inverse gotcha, light-scan variant)

## What it is
A light scan (`custodian.scan.light` / heartbeat `custodian:light`) marked a
`user_gated` provider/auth/credit issue `resolved` on a "forward-stale / provider
recovered" theory derived from an OLD `last_run_at` timestamp — WITHOUT confirming
the live `jobs.json` error state. Because the jobs hadn't re-run since the outage,
their `last_status` was still `error` and `last_error` still showed the live
outage. The scan inferred "recovered" from the timestamp alone and closed the
issue. Downstream consumers (Mentor, dashboards) then saw the systemic outage as
RESOLVED while jobs kept failing.

## Why light scans are susceptible
- The escalation-runner path runs `scripts/verify_escalation_state.py` (both
  staleness directions) and the execution-loop Step 1 explicitly checks "issue
  claims resolved but job still enabled+erroring" (the inverse gotcha).
- The light-scan checklist only had Step 8c (verify self-resolved `ModuleNotFoundError`)
  and Step 9 (verify-before-acting on error jobs). Nothing re-verified a PRIOR
  `resolved` classification against live job state. A light scan that merely
  accepted a prior `resolved` state as still-valid never re-checked the live errors.
- `last_run_at` lag is real (`references/scheduler-state-lag-vs-execution-failure.md`)
  — but here the lag was used as EVIDENCE of recovery, which is backwards.
  An old `last_run_at` means the job has NOT re-run; it says nothing about whether
  the underlying fault cleared.

## Reproduction (2026-07-13)
- ~16:10Z light scan marked `oc_provider_auth_token_expired_20260712T040120`,
  `oc_nous_api_key_invalid_20260712T040120`,
  `oc_openrouter_402_credits_exhausted_20260712T040120` -> `resolved`,
  reason: "forward-stale provider recovered", based on `last_run_at` 2026-07-12.
- 19:00Z light scan: 18 jobs (17 Nous `token_expired` + 2 OpenRouter 402) STILL
  showed `last_status=error` + `token_expired`/402 `last_error`, no re-auth evidence.
- Fix: reopened the 2 issues to `user_gated` + `escalation_needed=true`, cleared
  `resolved_at`. See `scripts/reopen_false_resolutions.py`.

## Detection recipe (run in EVERY light scan, after reading prior journal/issues.jsonl)
1. For each `resolved` issue with a provider/auth/credit fingerprint, collect its fingerprint.
2. In live `jobs.json`, count enabled jobs where `last_status == "error"` AND
   `last_error` still contains the outage signature
   (e.g. `token_expired`, `402 ... credits`, `No endpoints found for ...owl-alpha`).
3. If count >= 1 -> the resolution was FALSE. Reopen:
   `status: "user_gated"`, `escalation_needed: true`, clear `resolved_at`,
   add `reopened_at` (UTC) + `reopen_note` citing the live job count.
4. Honesty rule: never mark a provider outage `resolved` until a post-fix run shows
   `last_status=ok` + cleared `last_error`. Stale timestamp != recovery.

## Distinguish from genuine stale-resolution
- GENUINE: the job re-ran AFTER the fix and now shows `last_status=ok`.
- FALSE: the job never re-ran, or re-ran and STILL errors with the same fingerprint.

## Automation
Run `scripts/reopen_false_resolutions.py` (dry-run by default; `--write` to persist)
as part of the light-scan Step 8d guard. It brace-depth-parses `issues.jsonl`,
counts live erroring jobs per known outage fingerprint, and reopens matches.

## Related
- `references/escalation-loop-pitfalls.md` (execution-loop inverse gotcha)
- `references/escalation-execution-loop.md` Step 1 (verify live state BOTH directions)
- `references/provider-recovery-forward-stale-confirmation.md`
- `scripts/verify_escalation_state.py` (escalation-runner staleness probe)
- `scripts/reopen_false_resolutions.py` (this gotcha's light-scan guard)
