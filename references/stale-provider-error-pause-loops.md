# Stale provider errors and pause loops

Custodian must not convert provider incidents into permanent cron disablement.

## Rule

Provider/model failures are not automatically user-gated. A job's frozen `last_error` is not proof the provider is still broken. Before pausing or keeping paused a batch of jobs for provider/auth/credits/model errors, verify the provider/model live.

Minimal probe:

```bash
hermes chat -q 'Reply OK' --provider <provider> --model <model> -Q --toolsets safe
```

If the probe succeeds, resume/leave the jobs running and let cron overwrite stale error state on the next tick.

## What to pause

Pause only if retry is genuinely futile without a specific external fix:

- revoked domain OAuth for the job's actual data source
- missing script/path
- blocked `execute_code` in cron requiring redesign
- deterministic job-local configuration failure

When pausing, always write:

- `pause_reason`
- matching `issues.jsonl` entry with `jobs_paused`
- action journal evidence
- re-enable-on-recovery condition

## What not to pause

Do not pause solely for:

- 401/402 from a provider that later probes successfully
- 429/rate limit
- `ResourceExhausted`
- old model endpoint failures after re-pointing
- any error inherited from a global/default model when a per-job re-point is available

## Honesty rule

Pausing is mitigation, not a fix. Never report “fixed” when the root cause remains provider funds/auth/config. Report: “mitigated by pause” or “resumed after provider verified live,” then verify with later runs.