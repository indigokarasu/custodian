# `dispatch:triage-morning` No-Agent Script Argument Pattern — RESOLVED

## Summary

The `dispatch:triage-morning` job failed from **2026-05-28 to 2026-06-20** (~3 weeks) with `Script not found` because it used `no_agent: true` with a compound command in the `script` field. **Fixed on 2026-06-20** by replacing the compound command with a wrapper script (`triage_morning.sh`). Residual `last_error` in jobs.json still shows the old compound command — classify as `oc_cron_stale_error_script_mismatch`, not active.

## Job Configuration (Current — 2026-06-25)

```json
{
  "name": "dispatch:triage-morning",
  "no_agent": true,
  "script": "triage_morning.sh",
  "prompt": "Run dispatch morning triage...",
  "schedule": {"kind": "cron", "expr": "45 12 * * *"},
<<<<<<< Updated upstream
  "workdir": "<hermes-home>/profiles/indigo/skills/ocas-dispatch/scripts"
}
```

The `script` field is now a single wrapper file (relative path, resolved via `HERMES_HOME/scripts/`). The wrapper exists at `<hermes-home>/profiles/indigo/scripts/triage_morning.sh` and is executable.
=======
  "workdir": "~/.hermes/profiles/indigo/skills/ocas-dispatch/scripts"
}
```

The `script` field is now a single wrapper file (relative path, resolved via `HERMES_HOME/scripts/`). The wrapper exists at `~/.hermes/profiles/indigo/scripts/triage_morning.sh` and is executable.
>>>>>>> Stashed changes

## Historical Configuration (2026-05-28 to 2026-06-20)

```json
{
  "name": "dispatch:triage-morning",
  "no_agent": true,
<<<<<<< Updated upstream
  "script": "triage.py && python3 <hermes-home>/skills/ocas-dispatch/scripts/journal.py",
  "prompt": "Run dispatch morning triage...",
  "schedule": {"kind": "cron", "expr": "45 12 * * *"},
  "workdir": "<hermes-home>/profiles/indigo/skills/ocas-dispatch/scripts"
=======
  "script": "triage.py && python3 ~/.hermes/skills/ocas-dispatch/scripts/journal.py",
  "prompt": "Run dispatch morning triage...",
  "schedule": {"kind": "cron", "expr": "45 12 * * *"},
  "workdir": "~/.hermes/profiles/indigo/skills/ocas-dispatch/scripts"
>>>>>>> Stashed changes
}
```

## Root Cause

<<<<<<< Updated upstream
When `no_agent: true`, the `script` field is treated as a **literal file path**. The entire string `triage.py && python3 <hermes-home>/skills/ocas-dispatch/scripts/journal.py` is resolved as a path, which does not exist.

The `triage.py` script exists individually at `<hermes-home>/profiles/indigo/skills/ocas-dispatch/scripts/triage.py` and the `journal.py` exists individually. The compound command works in a shell but NOT as a literal path lookup.
=======
When `no_agent: true`, the `script` field is treated as a **literal file path**. The entire string `triage.py && python3 ~/.hermes/skills/ocas-dispatch/scripts/journal.py` is resolved as a path, which does not exist.

The `triage.py` script exists individually at `~/.hermes/profiles/indigo/skills/ocas-dispatch/scripts/triage.py` and the `journal.py` exists individually. The compound command works in a shell but NOT as a literal path lookup.
>>>>>>> Stashed changes

## Why It's Not Auto-Fixable by Custodian

This requires creating a wrapper script that bakes in the two-part command, then updating the cron job's `script` field to point at the wrapper. This is a deliberate job configuration decision (which wrapper to use, where to put it) — not an operational failure Custodian can resolve autonomously.

## Impact

- The 12:45 triage run never executes
- No triage journal is produced
- The error produces noise in every scan (visible but not actionable)

## Classification

- **Fingerprint**: `oc_cron_no_agent_script_args` (historical) → `oc_cron_stale_error_script_mismatch` (current)
- **Tier**: 2 (non-fatal, surface only)
- **Escalation**: No
- **Status**: Resolved 2026-06-20. Wrapper script `triage_morning.sh` is in place. Residual `last_error` is stale.

## Diagnosis Steps for This Variant

1. Check `no_agent: true` in job config
2. Check `script` field for `&&`, `;`, `|`, or other shell metacharacters
3. Verify individual command parts exist at their paths
4. Confirm: the issue is the compound-string-as-path problem, not missing files

## Fix Template (for user execution)

```bash
# 1. Create wrapper script
<<<<<<< Updated upstream
cat > <hermes-home>/profiles/indigo/scripts/triage-morning-wrapper.sh << 'EOF'
#!/bin/bash
set -e
cd <hermes-home>/profiles/indigo/skills/ocas-dispatch/scripts
python3 triage.py
python3 journal.py
EOF
chmod +x <hermes-home>/profiles/indigo/scripts/triage-morning-wrapper.sh
=======
cat > ~/.hermes/profiles/indigo/scripts/triage-morning-wrapper.sh << 'EOF'
#!/bin/bash
set -e
cd ~/.hermes/profiles/indigo/skills/ocas-dispatch/scripts
python3 triage.py
python3 journal.py
EOF
chmod +x ~/.hermes/profiles/indigo/scripts/triage-morning-wrapper.sh
>>>>>>> Stashed changes
```

Then update the cron job's `script` field to `triage-morning-wrapper.sh`.

## Resolution Status — Fixed 2026-06-20

<<<<<<< Updated upstream
As of 2026-06-24/25 light scans, the job's `script` field is now `triage_morning.sh` (a single wrapper file at `<hermes-home>/profiles/indigo/scripts/triage_morning.sh`). The script exists and is executable. However, `last_error` still shows the old compound command `triage.py && python3 ...` — this is a **stale error** (`oc_cron_stale_error_script_mismatch`), not an active failure. The job is healthy.
=======
As of 2026-06-24/25 light scans, the job's `script` field is now `triage_morning.sh` (a single wrapper file at `~/.hermes/profiles/indigo/scripts/triage_morning.sh`). The script exists and is executable. However, `last_error` still shows the old compound command `triage.py && python3 ...` — this is a **stale error** (`oc_cron_stale_error_script_mismatch`), not an active failure. The job is healthy.
>>>>>>> Stashed changes

When scanning this job in the future:
- Check current `script` field vs `last_error` content
- If `script` points to an existing executable AND `last_error` shows a different (older) command → `oc_cron_stale_error_script_mismatch` (Tier 2, surface only)
- Do NOT re-escalate as `oc_cron_no_agent_script_args` — the compound-command configuration has been replaced.

## See Also

- `references/no-agent-script-argument-pattern.md` — general no_agent script argument pattern
- `references/dispatch-triage-evening-no-agent-script-args.md` — evening counterpart (same pattern, fixed 2026-06-25)