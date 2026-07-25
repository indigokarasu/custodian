# `dispatch:triage-evening` No-Agent Script Argument Pattern — RESOLVED

## Summary

The `dispatch:triage-evening` job failed every night at 02:45 with `Script not found` because it used `no_agent: true` with a compound `&&` command in the `script` field. **Fixed on 2026-06-25** by replacing the compound command with a wrapper script (`triage_evening.sh`). This is the evening counterpart to the `dispatch:triage-morning` case (fixed 2026-06-20).

## Job Configuration (Current — Post Fix 2026-06-25)

```json
{
  "name": "dispatch:triage-evening",
  "no_agent": true,
  "script": "triage_evening.sh",
  "prompt": "Run dispatch evening triage...",
  "schedule": {"kind": "cron", "expr": "45 2 * * *"}
}
```

The `script` field is now a single wrapper file. The wrapper exists at `<hermes-home>/profiles/indigo/scripts/triage_evening.sh` and is executable.

## Historical Configuration (2026-05-28 to 2026-06-25)

```json
{
  "name": "dispatch:triage-evening",
  "no_agent": true,
  "script": "triage.py && python3 <hermes-home>/skills/ocas-dispatch/scripts/journal.py",
  "prompt": "Run dispatch evening triage...",
  "schedule": {"kind": "cron", "expr": "45 2 * * *"}
}
```

## Root Cause

When `no_agent: true`, the `script` field is treated as a **literal file path**. The entire string `triage.py && python3 <hermes-home>/skills/ocas-dispatch/scripts/journal.py` is resolved as a path, which does not exist.

## Why It Persisted

The morning counterpart (`dispatch:triage-morning`) was fixed 2026-06-20, but the evening job was missed — its `consecutive_failures=None` (the scheduler doesn't count no_agent script-path errors as consecutive failures since the agent never ran), making it invisible to the "high consecutive_failures" heuristic. It was only caught by the 2026-06-25 light scan which iterated all jobs.json entries.

## Impact

- The 02:45 evening triage run never executed (failing every night since ~2026-05-28)
- No evening triage journal was produced
- The error produced noise in every scan

## Classification

- **Fingerprint**: `oc_cron_no_agent_script_args`
- **Tier**: 1 (auto-fixed — wrapper script + jobs.json edit)
- **Escalation**: No
- **Status**: Resolved 2026-06-25

## Fix Applied (2026-06-25)

```bash
# 1. Create wrapper script
cat > <hermes-home>/profiles/indigo/scripts/triage_evening.sh << 'EOF'
#!/usr/bin/env bash
set -e
cd <hermes-home>/profiles/indigo/skills/ocas-dispatch/scripts
python3 triage.py
python3 <hermes-home>/skills/ocas-dispatch/scripts/journal.py
EOF
chmod +x <hermes-home>/profiles/indigo/scripts/triage_evening.sh

# 2. Update cron job script field (Python via terminal — execute_code blocked in cron)
python3 << 'PYEOF'
import json
with open("<hermes-home>/profiles/indigo/cron/jobs.json") as f:
    data = json.load(f)
jobs = data.get("jobs", data) if isinstance(data, dict) else data
for job in jobs:
    if job.get("name") == "dispatch:triage-evening" and job.get("no_agent") == True:
        job["script"] = "triage_evening.sh"
        break
with open("<hermes-home>/profiles/indigo/cron/jobs.json", "w") as f:
    json.dump(data, f, indent=2, ensure_ascii=False)
```

## Diagnosis Steps for Future Reference

1. Check `no_agent: true` in job config
2. Check `script` field for `&&`, `;`, `|`, or spaces (indicates compound command or arguments)
3. If compound command found → active error (not stale), apply wrapper fix
4. Verify wrapper script exists and is executable after fix

## See Also

- `references/no-agent-script-argument-pattern.md` — general no_agent script argument pattern
- `references/dispatch-triage-morning-no-agent-script-args.md` — morning counterpart (fixed 2026-06-20)
