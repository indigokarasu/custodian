# Escalation Runner Clean Verdict Pattern

When the escalation runner finds no actionable issues, write the observation journal with `not_activity_reason` and return `[SILENT]`. This is the expected steady-state.

## Classification Decision Tree

For each open issue found during escalation runner, classify into exactly one bucket:

### Bucket A — Actionable (execute fix)
- `status: escalated` or `fix_attempted_failed`
- `escalation_needed: true`
- Root cause is active (not stale, not already resolved)
- Fix is within the known auto-fix registry
- Does NOT require user confirmation

### Bucket B — Note but skip (user-gated)
- `status: open` with `tier: 4` (skill library hygiene, stub directories)
- `status: monitoring` (Tier 2, no action needed)
- Requires user confirmation to resolve (skill removal, directory cleanup)
- These are NOT failures — they are maintenance suggestions. Note count in journal, do not auto-fix.

### Bucket C — Ignore (legacy/inactive)
- Issues in config files of inactive profiles (no cron jobs, no recent activity)
- YAML debris in profiles like `braun` that have no `cron/jobs.json`
- These are historical artifacts, not active system issues

### Bucket D — Already resolved (verify and close)
- Issue references a provider/path that no longer exists in config
- Config.yaml verified clean via PyYAML (0 null keys)
- Job `last_status: ok` with `consecutive_failures: 0`
- Close by setting `status: resolved`, `escalation_needed: false`

## Clean Verdict Journal Template

```json
{
  "run_id": "esc-run-{timestamp}",
  "run_type": "escalation-runner",
  "timestamp": "{ISO8601}",
  "escalation_needed": false,
  "not_activity_reason": "No escalated issues requiring action. {details}.",
  "issues_checked": N,
  "issues_resolved": N,
  "issues_flagged": 0,
  "fixes_applied": 0,
  "config_yaml_verification": {
    "main_config": "CLEAN (0 null keys)",
    "profile_config_indigo": "CLEAN (0 null keys)",
    "method": "yaml.safe_load + recursive null-key detection"
  },
  "paths_checked": [
    "<hermes-home>/commons/data/ocas-custodian/issues.jsonl",
    "<hermes-home>/profiles/indigo/commons/data/ocas-custodian/issues.jsonl",
    "<hermes-home>/profiles/indigo/commons/journals/ocas-custodian/issues.jsonl",
    "<hermes-home>/commons/journals/ocas-custodian/{date}/"
  ]
}
```

## Inactive Profile Detection

To determine if a profile is inactive (Bucket C):
1. Check for `cron/jobs.json` — if absent, profile has no scheduled work
2. Check for recent journal activity — if last entry >90 days ago, profile is dormant
3. Null keys in inactive profile config are YAML debris, not active issues
4. Do NOT escalate config issues from inactive profiles

## Confirmed 2026-06-25

- Main config (`<hermes-home>/config.yaml`): CLEAN (0 null keys)
- the agent profile config: CLEAN (0 null keys)
- Braun profile config: 3 null keys (legacy, inactive — no cron jobs, no action needed)
- All issues.jsonl files: 0 open escalated issues
- Remaining open: `skill_library_stubs` (user-gated), `skill_hygiene_followup_20260601` (user-gated), `oc_cron_no_agent_exit1_noop_20260622` (Tier 2 monitoring-only)
- `oc_config_empty_section` fix-loop: DORMANT (8th occurrence avoided)
