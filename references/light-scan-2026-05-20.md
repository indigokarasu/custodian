# Light Scan: Stale Error Status & Category Dir Patterns

## Stale `last_status: error` Pattern

### What Happens
Cron jobs retain `last_status: error` indefinitely after a failure. The status does NOT auto-clear when the job runs successfully again. This means a job can show `last_status: error` from weeks ago while running fine today.

### Concrete Example (2026-05-20 Light Scan)
Three jobs showed `last_status: error`:
- `voyage:update` — last error April 8 ("Unknown provider 'nvidia'"), ran today at 07:54 PT. Script field is `update_voyage.sh` but file not found at literal path. Job still runs (agent resolves path or catches error). Error status is stale from April.
- `rally:update` — last error April 8 (same "Unknown provider 'nvidia'"), weekly Sunday job. Next run May 24.
- `praxis:review` — last error May 19, no corresponding error in today's logs. Transient.

### voyage:update Script Path Note
The job's `script` field is `update_voyage.sh` (just filename, no directory). The file was not found at `<hermes-home>/skills/ocas-voyage/scripts/update_voyage.sh` or any obvious path. Despite this, the job's `last_run_at` shows it executed today. Either the agent resolves the path at runtime, or the job is prompt-based and the script field is vestigial. This is NOT a new issue — the error is stale from April.

### Diagnostic Steps
1. Check `last_run_at` — if from a previous day/week, the error is likely stale
2. Grep `errors.log` for the job name with today's date
3. If no matches, the error is stale — do NOT escalate or re-open issues
4. For weekly jobs, check if the scheduled day has arrived yet
5. Count of request_dump files: check `find ~/.hermes/sessions/ -name "request_dump_*.json" | wc -l`

### Key Insight
The cron registry does NOT automatically clear or update `last_status` when a job runs successfully again. It only updates when the job's session completes and reports back. If the job's session is long-running, interrupted, or the gateway restarts mid-run, the status may never update. Always check the `last_run_at` timestamp AND grep `gateway.log` for the most recent run outcome before treating an error status as current.

## Category/Meta Skill Directories Without SKILL.md

### What Happens
The skill initialization check counts any directory under `{agent_root}/skills/` as a "skill". Many directories are actually category/meta folders (e.g., `creative/`, `infrastructure/`, `ocas-bower/`) that contain sub-skill directories but have no SKILL.md themselves. These should NOT be flagged as "uninitialized skills".

### Heuristic
If a directory under `{agent_root}/skills/` has no `SKILL.md` file, it is a category/meta directory — not a skill that needs initialization. Do NOT create data dirs, config.json, or journal dirs for these.

### Concrete Example (2026-05-20)
25 directories were flagged as "uninitialized" but all lacked SKILL.md files. Examples: `.archive`, `.curator_backups`, `.hub`, `.templates`, `apple`, `autonomous-ai-agents`, `creative`, `data-science`, `infrastructure`, `mcp`, `media`, `mlops`, `note-taking`, `productivity`, `research`, `smart-home`, `social-media`, `youtube-reach`. None are actual skills.

## scout:sources-refresh Weekly Job Pattern

### What Happens
`scout:sources-refresh` has schedule `0 6 * * 0` (Sunday 06:00). On Wednesday, it correctly shows `last_status: None`, `last_run_at: None`, `next_run_at: None`. This is NOT an orphaned job — it's a weekly job whose day hasn't arrived.

### Key Insight
Always check the schedule before concluding a job with null status fields is orphaned. Weekly jobs (e.g., `0 16 * * 1` for Monday-only) will show null status on non-scheduled days. Only flag as orphaned if `next_run_at` is also null or in the past AND the job name doesn't match any SKILL.md declaration.