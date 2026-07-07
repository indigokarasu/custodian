# Escalation Runner: Pause Affected Jobs Pattern

**Confirmed 2026-06-29**

When the escalation runner identifies a user-gated issue (Tier 3, cannot be auto-fixed) that is causing a job to fail on **every scheduled run**, the correct action is to **pause the job** — not just classify it as "user-gated" and leave it burning cycles.

## The Trap

1. Escalation runner finds an open issue classified as `user_gated`
2. It writes a journal noting "no actionable issues" and returns `[SILENT]`
3. The affected job continues to fail every N minutes, generating:
   - Log noise (clutters gateway log, makes real errors harder to spot)
   - Wasted compute (each failed run consumes resources)
   - Inflated error counts on subsequent scans
   - False urgency (scans keep flagging the same error as "new")

## When to Pause

Pause a job when ALL of these are true:
- The root cause is user-gated (requires human action: OAuth re-auth, credential renewal, etc.)
- The job is actively failing on every run (`last_status=error`, `consecutive_failures` increasing or `None` with persistent `last_error`)
- The job has no chance of success until the user intervenes
- The issue is already tracked in `issues.jsonl` (so pausing doesn't lose visibility)

## How to Pause (in cron context)

Since `hermes cron pause` doesn't work from cron (it reads the wrong path), edit `jobs.json` directly:

```python
import json
from datetime import datetime, timezone

with open('<hermes-root>/profiles/<profile>/cron/jobs.json') as f:
    data = json.load(f)

for j in data.get('jobs', []):
    if j.get('id') == '<job_id>':
        j['enabled'] = False
        j['state'] = 'paused'
        j['paused_at'] = datetime.now(timezone.utc).isoformat()
        j['paused_reason'] = '<root cause>. Will resume after <user action>.'

with open('<hermes-root>/profiles/<profile>/cron/jobs.json', 'w') as f:
    json.dump(data, f, indent=2, default=str)
```

## How to Pause (interactive/agent context)

```bash
hermes cron pause <job_id>
# Later, after user resolves the root cause:
hermes cron resume <job_id>
```

## After Pausing

1. **Update `issues.jsonl`**: Add `jobs_paused: [<job_id>, ...]` to the issue entry and append a note to the description with the pause timestamp and reason.
2. **Write an action journal** (not just observation): Document what was paused, why, and that it's reversible.
3. **Do NOT mark the issue as resolved**: The underlying problem (e.g., revoked OAuth token) is still open. Only the symptom (repeated failures) is mitigated.

## Resuming After User Action

When the user resolves the root cause (e.g., re-authorizes Google OAuth):
1. Verify the fix works by running the script manually
2. Resume the job: `hermes cron resume <job_id>` or set `enabled: true` in jobs.json
3. Mark the issue as `status: resolved` in `issues.jsonl`
4. Clear `escalation_needed: false`

## Real-World Instance

**2026-06-29**: Google OAuth refresh token revoked by Google. `email:check` was paused at 10:34 PDT by a prior scan. But `monitor:list` (which wraps `tasks_monitor.py` as a subprocess) was still running every 5 min and failing every time with the same OAuth error. The escalation runner at 17:31 UTC classified it as "user-gated, no actionable issues" — but the job kept burning cycles. The 18:33 UTC escalation run finally paused `monitor:list` to stop the waste.

**Lesson**: "User-gated" does not mean "no action." If a job is failing every run due to a known user-gated issue, pausing it IS the action.

## Partial Impact Assessment (2026-06-29)

Not all Google-auth jobs necessarily fail simultaneously when one token is revoked. After identifying the affected account:

1. **Check each Google-auth job's `last_status`** — jobs showing `ok` despite the token revocation are using a different auth flow or a different account's credentials.
2. **Inspect the script directly** — grep for `CREDS_FILE`, `credentials/`, or `account=` to determine which token each script actually uses.
3. **Beware subprocess wrapper masking** — jobs that wrap subprocesses (like `monitor:list` → `tasks_monitor.py`) show only `"Script exited with code 1"` in `last_error`. The actual OAuth error is masked. Run the subprocess directly to diagnose. See `references/subprocess-cascade-oauth-masking.md`.
4. **Only pause confirmed-failing jobs** — pausing a working job wastes functionality.

**Confirmed instance (2026-06-29)**: google-workspace-user token revoked. `email:check` (uses `google_auth_mcp.py` with owner's account) and `monitor:list` (wraps `tasks_monitor.py` with `CREDS_FILE = ".../google-workspace-user.json"`) both failed. But `sands:*`, `taste:*`, `vesper:*` all showed `last_status=ok` — they use different auth paths (Indigo's account, different scopes, or agent-mode auth that pulls from a different credential store). Only the two confirmed-failing jobs were paused.

## Distinguishing From Stale Errors

| Scenario | Action |
|----------|--------|
| Job failed once, error is old, job now OK | Do nothing (stale error) |
| Job failing every run, root cause is transient | Wait for self-resolution |
| Job failing every run, root cause is user-gated | **Pause the job** |
| Job failing every run, root cause is auto-fixable | Apply Tier 1 fix |
