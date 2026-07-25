# Direct Cron Health Check via jobs.json Parsing

When the `custodian_cron_health` MCP tool is unavailable (not registered, or Composio search returns unrelated tools), perform the health check directly by parsing `jobs.json`.

## When to Use

- The `custodian_cron_health` tool slug doesn't resolve
- Running in a context where MCP plugin tools aren't loaded
- You need a lightweight check without importing the full plugin

## jobs.json Structure

<<<<<<< Updated upstream
Located at `<hermes-home>/profiles/<profile>/cron/jobs.json`.
=======
Located at `~/.hermes/profiles/<profile>/cron/jobs.json`.
>>>>>>> Stashed changes

```json
{
  "jobs": [
    {
      "id": "e8f7818fbcac",
      "name": "finch:memory-guard-floor",
      "enabled": true,
      "last_status": "ok",
      "last_error": null,
      "last_run_at": "2026-06-27T18:04:38.927748-07:00",
      "next_run_at": "2026-06-28T00:00:00-07:00",
      "schedule": {"kind": "cron", "expr": "0 */6 * * *"},
      "script": null,
      "no_agent": false,
      "model": null,
      "provider": null,
      "paused_at": null,
      "repeat": {"completed": 39}
    }
  ],
  "updated_at": "..."
}
```

**Critical:** The top-level structure is `{"jobs": [...]}`, NOT a bare list. Always extract `data.get("jobs", [])`.

Jobs can also be nested inside group entries (`j.get("jobs", [])`), though most are flat.

## Parsing Pattern

```python
import json
from datetime import datetime, timezone, timedelta

<<<<<<< Updated upstream
with open('<hermes-home>/profiles/indigo/cron/jobs.json') as f:
=======
with open('~/.hermes/profiles/indigo/cron/jobs.json') as f:
>>>>>>> Stashed changes
    data = json.load(f)

jobs = data.get('jobs', [])
if not isinstance(jobs, list):
    jobs = []

failures = []
for j in jobs:
    if not isinstance(j, dict):
        continue
    jname = j.get('name', j.get('id', ''))
    last_status = j.get('last_status', 'unknown')
    last_error = j.get('last_error')
    enabled = j.get('enabled', False)
    
    if last_status == 'error' or last_error:
        failures.append({
            'name': jname,
            'error': last_error,
            'enabled': enabled,
            'last_run': j.get('last_run_at', '')
        })
    
    # Check nested jobs
    for sub in j.get('jobs', []):
        if isinstance(sub, dict):
            # same checks as above
            ...
```

## Error Classification Quick Reference

| `last_error` Pattern | Classification | Action |
|---------------------|----------------|--------|
| `RuntimeError: Provider returned error` + `consecutive_failures=None` | Transient provider error | No fix; self-resolves next run |
| `Script not found: <path>` + file exists at path | Transient race condition | No fix |
| `Script not found: <path>` + file missing | Dead script reference | Tier 1 fix or disable |
| `Script not found: <path> && <path2>` | Compound command in script field | Create wrapper script |
| `Blocked: script path resolves outside scripts dir` | Path security block | Move script to profile scripts dir |
| `RuntimeError: cannot schedule new futures` | Interpreter shutdown (transient) | No fix |
| `HTTP 429: Rate limited` | Provider rate limit | Transient |
| Job disabled (`enabled: false`) + `last_status: error` | Stale error on disabled job | Filter out; non-actionable |

## Memory Guard Floor Check

Combined with cron health in a single `terminal()` call:

```bash
python3 << 'PYEOF'
import json, os
from datetime import datetime, timezone, timedelta

# 1. Parse jobs.json
<<<<<<< Updated upstream
with open('<hermes-home>/profiles/indigo/cron/jobs.json') as f:
=======
with open('~/.hermes/profiles/indigo/cron/jobs.json') as f:
>>>>>>> Stashed changes
    data = json.load(f)

jobs = data.get('jobs', [])
failures = []
for j in jobs:
    if not isinstance(j, dict):
        continue
    enabled = j.get('enabled', False)
    last_status = j.get('last_status', '')
    last_error = j.get('last_error')
    if last_status == 'error' and enabled:
        failures.append(j.get('name', j.get('id','')))

# 2. Memory guard
<<<<<<< Updated upstream
mem_path = '<hermes-home>/profiles/indigo/memories/MEMORY.md'
=======
mem_path = '~/.hermes/profiles/indigo/memories/MEMORY.md'
>>>>>>> Stashed changes
mem_size = os.path.getsize(mem_path) if os.path.exists(mem_path) else 0

# 3. Finch job recency
finch_run = None
for j in jobs:
    if isinstance(j, dict) and j.get('id') == 'e8f7818fbcac':
        finch_run = j.get('last_run_at')
        break

now = datetime.now(timezone(timedelta(hours=-7)))
finch_ok = False
if finch_run:
    fr = datetime.fromisoformat(finch_run)
    finch_ok = (now - fr).total_seconds() < 8 * 3600

# 4. over_cap_after check
over_cap = False
<<<<<<< Updated upstream
decisions_path = '<hermes-home>/profiles/indigo/commons/data/ocas-finch/decisions.jsonl'
=======
decisions_path = '~/.hermes/profiles/indigo/commons/data/ocas-finch/decisions.jsonl'
>>>>>>> Stashed changes
if os.path.exists(decisions_path):
    with open(decisions_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            ts = rec.get('timestamp', '')
            if ts >= '2026-06-23' and rec.get('over_cap_after') == True:
                over_cap = True
                break

print(json.dumps({
    'cron_failures': len(failures),
    'failed_jobs': failures,
    'memory_bytes': mem_size,
    'memory_over_limit': mem_size > 2200,
    'finch_ran_8h': finch_ok,
    'finch_last_run': finch_run,
    'recent_over_cap_after': over_cap,
}, indent=2, default=str))
PYEOF
```

## Output Interpretation

- `cron_failures > 0`: List failed job names and check error patterns
- `memory_over_limit: true`: Memory guard should run or has not run recently
- `finch_ran_8h: false`: Memory guard floor hasn't checked in — WARNING
- `recent_over_cap_after: true`: Memory grew past cap since last guard run — WARNING

## Output Directory Error Inspection

When `jobs.json` shows a job with `last_status=error` but `last_error` is truncated or unclear, inspect the latest output file for the full error context:

```bash
# Find the latest output for a job
<<<<<<< Updated upstream
latest=$(ls -t <hermes-home>/cron/output/{job_id}/ | head -1)
cat <hermes-home>/cron/output/{job_id}/$latest
```

Output files are at `<hermes-home>/cron/output/{job_id}/{YYYY-MM-DD_HH-MM-SS}.md` and contain the full prompt, response, and any error tracebacks. This is more reliable than `last_error` in `jobs.json`, which is truncated to ~300 chars.
=======
latest=$(ls -t ~/.hermes/cron/output/{job_id}/ | head -1)
cat ~/.hermes/cron/output/{job_id}/$latest
```

Output files are at `~/.hermes/cron/output/{job_id}/{YYYY-MM-DD_HH-MM-SS}.md` and contain the full prompt, response, and any error tracebacks. This is more reliable than `last_error` in `jobs.json`, which is truncated to ~300 chars.
>>>>>>> Stashed changes

**Batch error scan** — check all recent job outputs for error markers:

```bash
<<<<<<< Updated upstream
for dir in $(ls -t <hermes-home>/cron/output/ | head -20); do
    latest=$(ls -t <hermes-home>/cron/output/$dir/ 2>/dev/null | head -1)
    if [ -n "$latest" ] && [ -f "<hermes-home>/cron/output/$dir/$latest" ]; then
        errors=$(grep -ci -E '(error|failed|exception|traceback|CRITICAL)' "<hermes-home>/cron/output/$dir/$latest" 2>/dev/null || echo 0)
=======
for dir in $(ls -t ~/.hermes/cron/output/ | head -20); do
    latest=$(ls -t ~/.hermes/cron/output/$dir/ 2>/dev/null | head -1)
    if [ -n "$latest" ] && [ -f "~/.hermes/cron/output/$dir/$latest" ]; then
        errors=$(grep -ci -E '(error|failed|exception|traceback|CRITICAL)' "~/.hermes/cron/output/$dir/$latest" 2>/dev/null || echo 0)
>>>>>>> Stashed changes
        if [ "$errors" -gt 0 ]; then
            echo "ERRORS ($errors): $dir ($latest)"
        fi
    fi
done
```

Note: grep error counts from custodian/haiku/scanner output files often flag **non-fatal mentions** (pitfall warnings, reference text about errors). Always read the actual file before escalating — a count of 7 "error" mentions might be a skill's pitfalls section, not 7 actual failures.

## Stale Counter Anomaly

A job can show `consecutive_failures=1` (or more) while `last_status=ok` and `last_error=null`. This means the counter is stale from a prior transient failure that has since resolved. **No fix needed** if the job is running on schedule and producing expected output. The counter resets to 0 on the next successful run but can lag if the scheduler's state update is delayed. Confirmed 2026-06-28: `weave-enrichment-health-check` had `consecutive_failures=1` + `last_status=ok` after a transient provider error followed by successful completion.

## Pitfalls

1. **Don't trust `hermes cron list` from cron context** — it reads from `~/.hermes/cron/`, not the profile-scoped path. Always read `jobs.json` directly.
2. **execute_code is blocked** — use `terminal()` with inline Python.
3. **Disabled jobs inflate error counts** — filter `enabled: false` before reporting failure totals.
4. **Stale errors on disabled jobs** — Disabled jobs with `last_status: error` should be classified as `oc_cron_disabled_stale_error`. If the job is confirmed redundant (replaced by another pipeline), remove it entirely with the full cleanup sequence below. Confirmed 2026-06-28: `brief:email-morning` and `brief:email-evening` were removed.
5. **Google OAuth cascade** — When `email:check` fails with `RefreshError`/`invalid_grant`, check ALL Google-dependent jobs: `monitor:list`, `sands:*`, `taste:*`, `vesper:*`. One revoked token cascades to all Google API consumers. Classify as `oc_google_oauth_token_revoked` (Tier 3 escalation).

### Full cleanup sequence for redundant disabled jobs

When a disabled cron job is confirmed redundant (e.g., replaced by another pipeline, scripts never worked, or paused indefinitely):

1. **Delete the job**: `hermes cron delete <id>` — removes from jobs.json and the scheduler
<<<<<<< Updated upstream
2. **Remove orphaned scripts**: Check `<hermes-home>/profiles/<profile>/scripts/` AND `<hermes-home>/scripts/` for script files referenced by the deleted job. Delete if no other job references them (grep `jobs.json` for the script filename first).
=======
2. **Remove orphaned scripts**: Check `~/.hermes/profiles/<profile>/scripts/` AND `~/.hermes/scripts/` for script files referenced by the deleted job. Delete if no other job references them (grep `jobs.json` for the script filename first).
>>>>>>> Stashed changes
3. **Remove stale reference docs**: If a custodian reference file was created to document the now-resolved error (e.g., `light-scan-YYYY-MM-DD-*.md` about the disabled job), delete it — the issue no longer exists and the reference is misleading.
4. **Update task list**: Mark the corresponding finch task as `done` with a resolution note listing what was deleted.