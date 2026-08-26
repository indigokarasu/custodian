# Wrong path prefix in skill scripts

## When to read
A skill script raises FileNotFoundError in cron context on a commons/config path.

Scripts that hardcode `<hermes-home>/commons/...` instead of `<hermes-home>/profiles/<profile>/commons/...` crash in cron context because the flat commons path does not exist for the executing profile. Why: Hermes resolves per-profile data dirs, so the profile prefix must always be included. Fix: rewrite the hardcoded prefix to `$HERMES_HOME/profiles/<profile>/commons/...` (or derive from `Path.home()`), then re-run the job.
