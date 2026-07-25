# Platform Compatibility

The `scripts/custodian.py` script was originally written for the `openclaw` CLI. On Hermes, the CLI is `hermes` (not `openclaw`), and some commands differ:

| OpenClaw command | Hermes equivalent | Status |
|---|---|---|
| `openclaw cron add --name X --cron S --message M` | `hermes cron add --name X --skill SKILL S 'PROMPT'` | Different syntax and flags |
| `openclaw cron edit ID --enabled true` | `hermes cron resume ID` | Different subcommand |
| `openclaw cron run ID` | `hermes cron run ID` | Same |
| `openclaw doctor` | `hermes doctor` | May differ |

**The script's `CronRegistry.add_cron_job()` calls `openclaw` which does not exist on Hermes.** The `init` command will fail with `FileNotFoundError: 'openclaw'`. Path references also use `~/openclaw/` instead of `{agent_root}/commons/`.

**Workaround:** Execute deep scans manually by reasoning directly. For cron registration, use `hermes cron add` with `--skill` and `--name` flags. For data operations, manipulate JSONL files directly using `read_file`/`write_file`/terminal tools. The data directory is `{agent_root}/commons/data/ocas-custodian/` (not `~/openclaw/data/`).

## Hermes-Specific Execution Patterns (Deep Scan)

On Hermes, the deep scan must be executed manually by reasoning through each step. Here are the exact patterns:

**Agent root path:** `~/.hermes` (not `~/openclaw/`). Commons dirs: `{agent_root}/commons/data/` and `{agent_root}/commons/journals/`.

**Cron registration (Tier 1 — oc_background_task_missing):**
```bash
hermes cron add --name 'skill:taskname' --skill ocas-skillname '0 0 * * *' 'Human-readable prompt describing what the task does'
```
- `--name`: The background task name from SKILL.md (e.g., `mentor:deep`, `sands:morning-brief`)
- `--skill`: The skill package name (e.g., `ocas-mentor`, `ocas-sands`)
- Next arg: cron schedule expression
- Final arg: descriptive prompt for the agent executing the task

**Cron listing and removal:**
```bash
hermes cron list                    # List all jobs with IDs, schedules, and status
hermes cron remove <job_id>         # Remove a job by ID (use for duplicates)
hermes cron pause <job_id>          # Pause without removing
hermes cron resume <job_id>         # Resume a paused job
```

**⚠️ `hermes cron list` crash bug:** Some cron jobs have `schedule` as a plain string (e.g., `"0 3 * * *"`) instead of the expected dict `{"kind": "cron", "expr": "0 3 * * *", "display": "0 3 * * *"}`. This causes an `AttributeError: 'str' object has no attribute 'get'` crash in `hermes_cli/cron.py` line 61 when calling `hermes cron list`. **Workaround:** Read `{agent_root}/cron/jobs.json` directly instead of using the CLI. The file contains the full job objects and is always available.

**Duplicate cron job detection:** `hermes cron list` output includes job IDs. When multiple entries share the same name/schedule, the earliest-registered ID is canonical — remove the later ones. This happens when `init` commands are run multiple times.

**All task registration is via cron.** Hermes has no heartbeat mechanism — all background tasks use `cronjob(action='create', ...)`.
```json
{
  "proposal_id": "prop-<8charhex>",
  "type": "anomaly_alert",
  "priority": "high|medium|low",
  "title": "Short title",
  "description": "Detailed description of the issue",
  "fingerprint": "oc_fingerprint_name",
  "tier": 3,
  "recommendation": "Suggested action",
  "created_at": "ISO timestamp"
}
```
Written to `{agent_root}/commons/data/ocas-custodian/proposals/{proposal_id}.json`.

**Skill initialization:** Create three directories minimally (never overwrite existing):
1. `{agent_root}/commons/data/{skill-name}/` (if missing)
2. `{agent_root}/commons/data/{skill-name}/config.json` (default `{"skill_name": "...", "version": "1.0.0", "initialized_at": "..."}` — only if absent)
3. `{agent_root}/commons/journals/{skill-name}/` (if missing)

**Background task scan:** Read each `{agent_root}/skills/ocas-*/SKILL.md`, find background task declarations from BOTH sources:
1. `## Background tasks` section — parse the table rows for Job name/Mechanism/Schedule
2. YAML frontmatter `metadata.hermes.cron` array — extract `name:` fields from `cron:` entries

Cross-reference declared tasks against the cron registry (read `{agent_root}/cron/jobs.json` directly — see control char pitfall below). All tasks use cron — Hermes has no heartbeat mechanism.

**Cron registry health sub-pass** (runs after Background task scan): Using the same jobs.json data, run three additional checks:
1. **Dead skill references**: For each job with a `skills` array, verify every listed skill exists as a directory under `{agent_root}/skills/`. Remove dead entries or delete the job.
2. **Dead script references**: For each job with a `script` field, verify the file exists. Check both the literal path and `{agent_root}/<script>`.
3. **Duplicate detection**: Group jobs by script path, prompt prefix (200 chars), and display name. Keep the canonical one (matches SKILL.md name or earliest ID), delete the rest.
All three are Tier 1 auto-fixes. Log all changes in the action journal.

**⚠️ Stale open issues:** When verifying issues during a scan, always re-check the actual system state before assuming an issue persists. Issues from previous cycles may have been silently resolved (e.g., a cron job that was timing out may now be running OK, a config error may have been fixed by another process). Only keep `status: open` if the underlying condition still exists. Resolve stale issues and record the resolution method in `issues.jsonl`.

**⚠️ Prematurely closed issues (critical — 429 cascade):** Issues with `status: resolved` and `resolution_method: cascade_self_resolved` may have been closed prematurely. The deep scan's "self-resolve" heuristic (no new errors in X hours) can trigger even when the underlying rate limit has not actually reset — only the surge temporarily paused. **Do not trust `self_resolved` as final.** Always verify by grepping `errors.log` for the fingerprint with TODAY's date. If ANY match is found, re-open the issue. The safety threshold: require a full 24-hour clean window before accepting a `self_resolved` closure on rate-limit-related fingerprints. Document re-opened issues with `status: reopened` and `reopened_at` in `issues.jsonl`.

**⚠️ Jobs with `last_status: error` from yesterday (or older) may not have run yet today:** A job with `last_status: error` and `last_run_at` from the previous day (or even weeks ago) is NOT necessarily failing today. Check the job's schedule — if the scheduled time hasn't occurred yet today, the job simply hasn't run. Do NOT re-open or re-escalate based on old error status alone. Verify by checking today's date in `errors.log` for the job's fingerprint before treating it as an active issue. This is especially common with weekly jobs (e.g., `0 16 * * 1` for Monday-only) that ran successfully on their last scheduled day but whose `last_status` was never updated, or that failed on their last scheduled day and won't run again until next week. Example: `haiku:follow-maintenance` (Monday-only) shows `last_status: error` from 2026-05-11 but is not failing today (Thursday) — it simply isn't scheduled to run. Some cron jobs with weekly schedules (e.g., `0 1 * * 0` for Sunday-only) may have `next_run_at: None` in the registry. This appears to be a scheduler bug where it fails to compute the next occurrence. Fix by pausing and resuming the job via `hermes cron pause <id>` then `hermes cron resume <id>`, which forces the scheduler to recalculate `next_run_at`.

**⚠️ Jobs with `last_status: error` from today but no error in logs:** A job with `last_status=error` and `last_run_at` from today may show no corresponding error in gateway.log. This happens when the job's session reports failure internally but the error is caught and not logged as a gateway ERROR. Before escalating: (1) Is the script file executable and present? (2) Is this a weekly job that just ran for the first time in a week? If the script exists and the job ran recently, the error may be transient — mark as Tier 2 observation and re-verify on next run. Do NOT escalate based solely on `last_status=error` without corroborating log evidence. The cron registry does NOT automatically clear or update `last_status` when a job runs successfully again — it only updates when the job's session completes and reports back. If the job's session is long-running, interrupted, or the gateway restarts mid-run, the status may never update. Always check the `last_run_at` timestamp AND grep `gateway.log` (use `strings <hermes-home>/logs/gateway.log | grep "job_name"`) for the most recent run outcome before treating an error status as current. Example: voyage:update had `last_status: error` from April 8 (Unknown provider 'nvidia'), but the job ran again on May 14 — the error was stale, not current.

**⚠️ Stale `last_status: error` diagnostic pattern:** Jobs with `last_status: error` from a previous day/week may not have run yet today. Always verify against primary sources — check `last_run_at` date, grep today's logs, check if it's a weekly job whose day hasn't arrived. See `references/light-scan-2026-05-20.md` for the full diagnostic pattern and concrete examples.

**⚠️ Cron name matching pitfall:** The cron registry may use display names that differ from SKILL.md canonical names (e.g., `"Vesper: Morning Briefing"` in cron vs `vesper:morning` in SKILL.md). When checking conformance, do fuzzy matching — a cron job with a different display name but matching skill tag and schedule is likely the same task. Only flag as Tier 1 `oc_background_task_missing` if no cron job exists with the same skill tag AND schedule pattern. Name mismatches with matching functionality are Tier 2 (surface only).

**⚠️ jobs.json control character corruption:** The `{agent_root}/cron/jobs.json` file may contain control characters (0x00–0x1F) that break standard JSON parsers. Always clean before parsing:
```python
import re, json
with open('{agent_root}/cron/jobs.json', 'r') as f:
    raw = f.read()
cleaned = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', raw)
data = json.loads(cleaned)
jobs = data['jobs']  # list of job objects, NOT a dict
```
The file structure is `{"jobs": [...], "updated_at": "..."}`, not a bare list. The `hermes cron list` CLI may crash with `AttributeError: 'str' object has no attribute 'get'` when `schedule` is a plain string instead of a dict — always read the file directly.

**⚠️ Dead skill reference detection:** When checking a job's `skills` array, verify each skill directory exists under `{agent_root}/skills/`. Common dead references found in the wild:
- `google-workspace` — does not exist as a skill directory; remove from skills array
- `weave-enrichment-proper` — does not exist; the correct skill is `ocas-weave`
- `mcp-mempalace` — does not exist; was likely confused with the MCP server name
A job with a dead skill reference may still function if its `prompt` contains standalone instructions. Fix by removing the dead entry from the `skills` array (preserving other valid entries), not by deleting the entire job.

**⚠️ Duplicate cron job detection:** Duplicates arise when init commands are run multiple times or when a job is re-created under a slightly different name. Detection method:
1. Group jobs by `script` path — identical scripts = duplicate function
2. Group jobs by prompt prefix (first 200 chars) — identical prompts = duplicate function
3. Group jobs by display `name` — identical names = exact duplicate
When duplicates are found, keep the job whose name matches the SKILL.md canonical name (e.g., `vesper:morning` over `Vesper: Morning Briefing`). If neither matches, keep the one with the earliest/smallest ID. Delete the rest using `cronjob(action='remove', job_id=...)`. Always log deletions in the action journal.

**⚠️ Script path resolution:** Cron job `script` fields may be stored as relative paths or as paths relative to the skill directory. When verifying script existence, check both the literal path and the path relative to `~/.hermes/`. The Hermes cron runner resolves relative paths from the agent working directory, not from the skill directory. Before flagging a script as dead, verify the job's `last_status` — if it's `ok`, the runner is resolving the path correctly and the job should NOT be flagged.

**⚠️ Pitfall — Request dump files contain plaintext API keys:** Session files matching `request_dump_*.json` in `~/.hermes/sessions/` contain full API request payloads including `Authorization: Bearer *** tokens in plaintext. These are created when API calls fail with `max_retries_exhausted`. They are debug artifacts with no operational value. During deep scans, check for these files and flag as `oc_request_dump_key_exposure` (Tier 2 — surface with recommendation to delete old files and disable request_dump in production). Files older than 7 days can be safely deleted. As of 2026-05-17, 5,704 such files (1.2GB) were cleaned up, leaving 780 recent files. See `references/request-dump-key-exposure.md`.

**⚠️ Pitfall — Prompt-based jobs with dead script references (2026-05-17):** A cron job with `script: null` (prompt-based) may have a `prompt` field that instructs the agent to run a specific Python script — but that script doesn't exist on disk. The job's `last_status` may still be `ok` because the agent catches the `FileNotFoundError` and reports success, but the actual work is never performed. During cron registry health checks, also scan the `prompt` field for `python3 /path/to/script.py` patterns and verify the referenced scripts exist. This is a Tier 2 issue — cannot auto-fix without knowing the correct script. Example: `weave:enrichability-recalc` references `recalculate_enrichability.py` which doesn't exist in `ocas-weave/scripts/`. See `references/prompt-based-job-dead-script-ref.md`.

**Activity model:** On Hermes, `message.processed` events may not be labeled in gateway.log. Gateway log files at `{agent_root}/logs/agent-YYYY-MM-DD.log` may not exist (no files were found there). Use `{agent_root}/state.db` instead — it's a SQLite database with a `sessions` table containing `started_at` (Unix timestamp REAL, NOT `created_at` which does not exist) and `source` columns.

**⚠️ Pitfall — Filter out cron/heartbeat sessions for activity model:** The state.db includes ALL sessions — but on a production Hermes system, cron/heartbeat jobs typically account for 95%+ of sessions (e.g., 4437 of 4573). If you don't filter them out, every hour appears equally "active" at medium confidence, making quiet-hour detection useless. The activity model must only consider **user-initiated sessions** (`source IN ('telegram', 'cli', 'test', 'user')`) for quiet-hour computation. Cron sessions are continuous and don't indicate user activity.

**⚠️ Pitfall — state.db `sessions` table column names:** The `sessions` table does NOT have a `session_id` column. The actual columns include `started_at` (Unix timestamp REAL), `source` (text), and `last_run_at` — but NOT `session_id` or `created_at`. Always use `PRAGMA table_info(sessions)` or `SELECT * FROM sessions LIMIT 1` to verify column names before writing queries. Using a non-existent column name (e.g., `session_id`) will crash with `sqlite3.OperationalError: no such column`.

Query pattern:
```python
import sqlite3, os, time
conn = sqlite3.connect(os.path.expanduser('~/.hermes/state.db'))
fourteen_days_ago = time.time() - (14 * 86400)
cursor = conn.execute(
    "SELECT started_at, source FROM sessions WHERE started_at > ?",
    (fourteen_days_ago,)
)
user_sessions = [row for row in cursor.fetchall()
                 if row[1] not in ('cron', 'dojo-seed')]
# Now bucket user_sessions by hour, compute active_days/total_days per hour
```

Build hourly confidence from `active_days / total_days` per hour using only user sessions. Common user sources: `telegram`, `cli`. Common system sources: `cron`, `dojo-seed`. Then convert `started_at` from Unix timestamp to datetime for hourly bucketing.

**⚠️ state.db is very large (14GB+ as of 2026-05):** Do NOT attempt to read `state.db` via `execute_code` — the sandbox will OOM. Use the `terminal` tool with the `sqlite3` CLI for targeted queries:
```bash
sqlite3 {agent_root}/state.db "SELECT started_at, source FROM sessions WHERE started_at > strftime('%s', 'now', '-14 days') ORDER BY started_at DESC LIMIT 100;"
```
Or use `execute_code` with Python's `sqlite3` module (which runs in a separate process and can handle large DBs):
```python
import sqlite3, time
conn = sqlite3.connect('{agent_root}/state.db')
cutoff = time.time() - 14*86400
rows = conn.execute("SELECT started_at, source FROM sessions WHERE started_at > ?", (cutoff,)).fetchall()
```

**Schedule scoring:** Score each slot -2 to +2 based on quietness (lower activity = higher score). Quiet slots score +2, moderate +1, high activity -2. Total max = 8. If score < 6 and confidence >= med, shift each slot max 30 minutes toward the target.
