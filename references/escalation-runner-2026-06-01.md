# Escalation Runner — 2026-06-01

Multiple autonomous escalation runs completed. Consolidated learnings below.

## Runs Completed

| Run | Time (UTC) | Issues Reviewed | Auto-Resolved | User Action Needed |
|-----|-----------|-----------------|---------------|-------------------|
| 1 | 11:39 | 9 | 5 | 4 |
| 2 | 19:00 | 7 | 3 | 4 (+1 new tracking) |
| 3 | ~20:10 | 8 | 4 | 5 |
| 4 | 22:06 | 35 | 6 | 5 |
| 5 | 23:40 | 6 | 0 | 5 |

## What is scanned

Three sources checked for escalated issues:
1. `{agent_root}/commons/journals/ocas-custodian/` — journals tagged `escalation_needed: true` (last 24h)
2. `{agent_root}/commons/data/ocas-custodian/issues.jsonl` — open issues
3. `{agent_root}/commons/data/ocas-custodian/proposals/` — unresolved InsightProposals

## Key technique: issue evolution via `superseded_by`

Issues can evolve — e.g., "token file missing" → "token revoked". Mark old issues `status: superseded` with `superseded_by: <new_issue_id>` instead of leaving duplicates open.

## Key technique: skill-stub-with-data

Some skill directories have operational data but no SKILL.md. Before removing stubs, grep `jobs.json` for references to the skill's data paths. If data is consumed by other skills, it's a broken dependency, not just dead weight.

## Key technique: MCP server process lifecycle

MCP servers are **spawned on-demand by the gateway**, not persistent daemons. When `ps aux` shows no MCP process, it does NOT mean they're down. The diagnostic sequence:

1. Check if the MCP entry in `config.yaml` `mcp_servers` has `enabled: false` — if so, connection failures are expected noise.
2. If `enabled: true` but process not running, this is **normal** — the process will be spawned when a skill/job first connects.
3. If connection failures persist after a real job attempt, test the server command directly: run the `command` + `args` from the config entry manually to check for import errors.
4. **Python path gotcha**: `command: "python3"` resolves to whatever `python3` is on PATH (often a venv Python). If the MCP server's module is installed under a different Python (e.g., system python3.13), the import will fail. Fix: use the full path to the correct Python binary.

## Auto-fixes the escalation runner CAN apply

| Pattern | Fix |
|---------|-----|
| job `last_status: ok` but issue open | Resolve with timestamp |
| Issue A evolved into issue B | Mark A `superseded`, link via `superseded_by` |
| `consecutive_failures: 0` + old error | Transient; verify then resolve |
| Skill gained `.git` since issue filed | Resolve |
| Disk recovered since issue filed | Resolve |
| MCP `enabled: false` in config but issue open | Resolve — failures expected |
| MCP `command: python3` but module under different Python | Fix command to full path of correct Python |
| MCP "process not running" but on-demand spawn | Resolve — normal behavior |
| Job paused (`enabled: false`, `state: paused`) due to transient HTTP 429/502/503 | **Re-enable:** set `enabled: true`, `state: idle`, remove `paused_at`/`paused_reason`. Tier 1 auto-fix. |
| email_check.py `invalid_grant` with BOTH token stores revoked | No autonomous fix. User must re-auth via paste-back OAuth. |

### Re-enabling paused jobs (Tier 1 auto-fix)

When a job is paused with `last_status: error` and the error is a transient provider issue (HTTP 429, 502, 503), re-enable it. The system auto-pauses jobs on rate limits, but these self-resolve.

<<<<<<< Updated upstream
**jobs.json location:** `<hermes-home>/cron/jobs.json` (NOT `<hermes-home>/jobs.json`). Always use this path.

```python
# In terminal() heredoc for cron mode
jobs_path = Path('<hermes-home>/cron/jobs.json')
=======
**jobs.json location:** `~/.hermes/cron/jobs.json` (NOT `~/.hermes/jobs.json`). Always use this path.

```python
# In terminal() heredoc for cron mode
jobs_path = Path('~/.hermes/cron/jobs.json')
>>>>>>> Stashed changes
data = json.loads(jobs_path.read_text())
jobs = data if isinstance(data, list) else data.get('jobs', [])
for job in jobs:
    if job.get('id') == '<job_id>':
        job['enabled'] = True
        job['state'] = 'idle'
        job.pop('paused_at', None)
        job.pop('paused_reason', None)
jobs_path.write_text(json.dumps(data, indent=2, default=str))
```

**Verification:** After re-enabling, the job's `next_run_at` should be recalculated on the next scheduler tick. No need to manually trigger.

## Issues requiring user action (cannot auto-fix)

- `invalid_grant` / OAuth revocation — interactive browser re-auth. **NOTE:** email_check.py reads from MCP credentials dir (`<gworkspace-creds>/credentials/`), NOT from `google_token.json`. Both stores can drift. When both are revoked, re-auth via `google-workspace-auth` skill paste-back OAuth.
- Provider API key errors (HTTP 401) — user must update keys
- Nous subscription/payment — user must check billing
- Skill library stubs with data — user confirmation required

## Script-Level Diagnostics

### `email_check.py` — auth status (updated 2026-06-01)

<<<<<<< Updated upstream
The script at `<hermes-home>/scripts/email_check.py` imports `get_gmail_service` from `google_auth_mcp`. The import works correctly.

The **primary** failure mode is `invalid_grant` (token revoked). Two token stores exist:
- `<hermes-home>/google_token.json` (client: `550801240087...`) — NOT used by email_check.py
=======
The script at `~/.hermes/scripts/email_check.py` imports `get_gmail_service` from `google_auth_mcp`. The import works correctly.

The **primary** failure mode is `invalid_grant` (token revoked). Two token stores exist:
- `~/.hermes/google_token.json` (client: `550801240087...`) — NOT used by email_check.py
>>>>>>> Stashed changes
- `<gworkspace-creds>/credentials/<user-google-email>.json` (client: `112292610034...`) — **this is what email_check.py reads**

When diagnosing, test the MCP credentials directly (not google_token.json). When both stores have revoked tokens, no local recovery is possible — requires user re-auth.

**Do not copy google_token.json into MCP credentials** — different client_id causes refresh failures.

### `manifest.build` 401 — provider key rejected

The `fallback_model` provider in config.yaml uses `https://app.manifest.build/v1/` with API key `mnfst_897...`. Both `spot:watch-sweep` and `ocas-finch:weekly` hit this provider. If both fail with 401 simultaneously, the key itself is invalid/expired.

## Cron-mode JSONL mutation pattern

When running as a cron job, all `issues.jsonl` and journal mutations must use `terminal()` with heredoc. Never `read_file` (corrupts JSONL) and never `execute_code` (blocked in cron). The reliable pattern:

```bash
python3 << 'PYEOF'
import json
from pathlib import Path
from datetime import datetime, timezone

<<<<<<< Updated upstream
issues_path = Path('<hermes-home>/commons/data/ocas-custodian/issues.jsonl')
=======
issues_path = Path('~/.hermes/commons/data/ocas-custodian/issues.jsonl')
>>>>>>> Stashed changes
issues = [json.loads(l) for l in issues_path.read_text().strip().split('\\n') if l.strip()]

# ... modify issues ...

issues_path.write_text('\\n'.join(json.dumps(i) for i in issues) + '\\n')
print(f"Wrote {len(issues)} lines")
PYEOF
```

Write journals the same way. Always verify with follow-up `terminal(command="wc -l /path")`.