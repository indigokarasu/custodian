# consecutive_failures=None vs 0 -- Diagnostic Distinction

## The Difference

| Value | Meaning | Action |
|---|---|---|
| cf=None (JSON null) | Scheduler never updated its internal failure counter | Check systemic vs isolated |
| cf=0 | Scheduler counted and reset the counter | Transient, already resolved |
| cf>=1 | Scheduler counted consecutive failures | Investigate root cause |

## Why cf=None Happens

1. Null-provider jobs with provider errors: provider-level errors (401/429) on null-provider jobs may not be counted as consecutive failures even though last_status=error.

2. no_agent: true jobs: Script exit codes may not be tracked as consecutive failures.

3. Post-pause/resume state: Scheduler resets internal state but last_status/last_error in jobs.json remain from pre-pause run.

## Diagnostic Heuristic

ALL error jobs with cf=None: Suspect systemic routing issue. Check config.yaml fallback_model, null-provider routing, common upstream provider.

Single job with cf=None: Likely one-off transient. Monitor only.

cf=0 with last_status=ok: Stale counter. Job healthy. Do NOT escalate.

## Example (2026-06-21)

16 error jobs, all cf=None: 5 manifest.build 401 (systemic), 5 transient 429, 5 monitor script no-ops, 1 futures shutdown. No genuine failure loops.

## Related Patterns

- references/null-provider-fallback-routing-2026-06-18.md
- references/transient-401-self-resolution-pattern.md
- references/stale-error-state-pause-resume-fix.md
