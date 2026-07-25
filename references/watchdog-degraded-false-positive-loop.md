# Watchdog DEGRADED message can be a false positive (escalation-loop trap)

When the escalation loop relies on `hermes cron run <id>` returning `Ran now: failed.`
as proof a job is still broken, **a monitoring/watchdog job whose `last_error` is a
`DEGRADED <invariant>` message (not a traceback) may be failing due to a bug in the
watchdog logic itself, not a real system fault.**

## 2026-07-22 case (rally:pipeline-watchdog)
- Issue `oc_rally_pipeline_no_staged_rebalance_20260722T1407Z` was opened by a light
  scan; Mentor correctly judged it a FALSE POSITIVE (watchdog step-6 counted only
  `status=="staged"`; a rebalance that advanced to `in_progress` makes STAGED=0).
- It was later REOPENED because `hermes cron run 13eadbca14e4` returned
  `Ran now: failed.` with the `no_staged_rebalance` DEGRADED message.
- Root cause: the watchdog's step-6 only counted `status=="staged"`. The 2026-07-22
  rebalance (`pa_20260722_090716_rebalance`) was staged 09:07, submitted 8/8 orders at
  12:30 (→ `in_progress`), so by the 15:18Z+ watchdog runs STAGED=0 → benign false flag.
- **Fix (Code-defect, executable by the escalation loop):** patch
  `rr_rally_pipeline_watchdog.sh` step-6 to count `staged`/`in_progress`/`complete`.
  Applied to both `scripts/` and `skills/ocas-rally/wrappers/` copies. Verified: direct
  run exit 0 → `status ok`; `hermes cron run` → succeeded; registry flipped to `ok`.

## How to disambiguate before trusting a watchdog failure
1. Read the job's `last_error`. If it is a `DEGRADED <invariant>` / `no_<x>` message
   (not a Python traceback), treat it as a *candidate* false positive.
2. Open the state file the watchdog reads (e.g. `pending_actions.jsonl`) and check the
   actual invariant. A rebalance `in_progress`/`complete` means the pipeline is healthy —
   the watchdog's `staged`-only count is the bug.
3. If the watchdog logic is wrong → classify as **Code-defect fixable by THIS loop**
   (skill-owned script), patch it, then `hermes cron run <id>` to confirm and flip the
   registry.
4. Only reopen/rely on the `last_error` as proof of a real fault AFTER verifying the
   underlying invariant state — never from `hermes cron run` exit code alone when the
   error is a DEGRADED message.

Complements `references/no-agent-monitor-exit1-upstream-degraded-pitfall.md` (which
covers the inverse: a real `UNHEALTHY` message that IS a genuine failure).
