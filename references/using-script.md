# Using the script

All deterministic operations delegate to `scripts/custodian.py`. Call it via Bash tool:

```
python3 {skill_dir}/scripts/custodian.py <command> [args]
```

Where `{skill_dir}` is the path to this skill package (e.g. `{agent_root}/skills/ocas-custodian`).

**Known issue:** Commands that call `CronRegistry` methods (`init`, any operation registering/editing cron jobs) will fail on Hermes because they invoke the `openclaw` binary. Use `hermes cron` CLI instead. All other commands that only read/write JSONL files and logs should work.

| When to call the script | When to reason directly |
|---|---|
| JSONL reads/writes, log parsing, fingerprinting (scan, status, issues) | Cron registration (use `hermes cron add`) |
| Activity model data analysis | Web search pass (Step 9) |
| Fix verification, issue lifecycle tracking | Writing Vesper InsightProposals (Step 10) |
| **Cron registration/editing** (script calls `openclaw`, use `hermes cron add/remove/list`) | Interpreting novel/ambiguous findings |
| **Skill init** (script calls CronRegistry, create dirs/config manually, register tasks via `hermes cron add`) | Composing escalation summaries for Mentor |
| **Duplicate job cleanup** (use `hermes cron list` to find, `hermes cron remove <id>` to clean) | Determining which tasks should be heartbeat vs. cron |

**Output contract:** All commands print human-readable status to stdout and write structured state to JSONL files. `status` and `issues.list` emit JSON. Exit 0 on success, non-zero on failure.

**Web search handoff:** After `scan.deep`, if `{agent_root}/commons/data/ocas-custodian/search_candidates.json` exists, read it and execute the web search pass directly using the query mutation sequence in Web Search Protocol. Write actionable results to `learned_issues.jsonl`.

**Escalation handoff:** After `scan.deep` prints "Agent: run web search pass", also check open Tier 3/4 issues in `issues.list` output and write InsightProposals to `{agent_root}/commons/data/ocas-custodian/proposals/` if Vesper is installed. Vesper reads from this directory.

## Escalation Runner Execution Pattern (Hermes)

The escalation runner is a dedicated cron job (`custodian:escalation-runner`) that acts on escalated issues that the deep scan flagged but couldn't auto-fix. It must be self-contained (runs in an isolated cron session with no user present).

**Execution steps:**

1. **Check escalated journals** — scan `{agent_root}/commons/journals/ocas-custodian/` for entries tagged `escalation_needed: true` from the last 24 hours.
2. **Check open escalated issues** — read `issues.jsonl` for any with `status: escalated` or `status: fix_attempted_failed` or `escalation_needed: true`.
3. **Check proposals** — list files in `{agent_root}/commons/data/ocas-custodian/proposals/` that haven't been marked `resolved: true`.
4. **Load confidence model** — read `fix_effectiveness.jsonl`. For each open issue, check if the fingerprint now has a confidence-based fix recommendation. If `confidence_score >= 0.6` and `recommended_tier == 1`, auto-fix instead of escalating.
4. **Re-verify against current state** — for each open issue, check the actual system condition (cron job last_status, log error counts, provider availability) before assuming the issue persists. This is critical — stale issues waste cycles and mask real problems.
   - **Also check resolved issues with `self_resolved` or `cascade_self_resolved` status** — especially for rate-limit-related fingerprints. Grep `errors.log` for today's occurrences of the fingerprint. If ANY match is found, RE-OPEN the issue (set `status: reopened` and `reopened_at`). These are the most common type of prematurely-closed issue because rate limits pause between daily update waves.
5. **Apply known auto-fixes** if the root cause matches a known pattern:
    - "no delivery target resolved" → fix cron job `deliver` field to correct target
    - "platform not configured/enabled" → ensure `config.yaml` has platform enabled
    - Cron job disabled → re-enable via `hermes cron resume <id>`
    - Skill uninitialized → create data/journal dirs and default config.json
    - Missing cron job → register per SKILL.md declaration
    - Platform missing webhook → set `platforms.{name}.enabled: false` in config.yaml
    - **Platform auto-detected from env vars (no explicit config entry)** → add explicit `platforms.{name}.enabled: false` to config.yaml to suppress gateway auto-detection from `TWILIO_*`, `SMS_ENABLED`, etc. The gateway scans env vars for known service credentials and auto-starts platforms even without an explicit config entry. Adding an explicit `enabled: false` overrides this behavior.
    - **Email enabled without credentials** → set `platforms.email.enabled: false` (or provide EMAIL_ADDRESS, EMAIL_PASSWORD, EMAIL_IMAP_HOST, EMAIL_SMTP_HOST in .env)
    - Model context_length noise → set `model.context_length` and `fallback_model.context_length` in config.yaml
    - **Cron no_agent mismatch** (`no_agent=True but no script is set`) → Remove and re-create the job with `hermes cron add`. See `references/cron-no-agent-mismatch.md`.
    - **Cron `next_run_at: None`** → Pause and resume the job to force scheduler recalculation.
    - **Google OAuth 400** → Run OAuth refresh script.
    - **Vision model incompatible** → Set explicit `auxiliary.vision.provider`.
    - **Concurrent 429 errors** → Stagger cron schedules (see Tier 1 Fix: Cron Schedule Staggering below).
7. **Apply code-level fixes** for known Tier 4 bugs (see Known Code Fixes section below). These require patching gateway source and a user restart.
8. **Close resolved issues** — update `issues.jsonl` entries to `status: resolved` with `resolved_at` and `resolution` fields.
7. **Clean stale proposals** — mark InsightProposals as resolved when their underlying fingerprint's issue is closed.
8. **Record everything** — append fix records to `fixes.jsonl`, decision records to `decisions.jsonl`, write Action Journal with OKR metrics, write report to `reports/YYYY-MM-DD-HHMM.md`.
9. **Escalation outcome** — if issues cannot be auto-fixed and require user action, document clearly in the report and journal (escalation_held, recommended_action).
10. **Update confidence model** — for each fix applied, update `fix_effectiveness.jsonl`.
11. **Silent exit** — if no escalated issues found at all, return `[SILENT]` to suppress delivery.

**⚠️ Re-verification is critical:** Issues from previous cycles may have been silently resolved by other processes (provider rate limits reset, config fixed elsewhere, cron jobs now running OK). Always check `last_status` and `last_run_at` in `jobs.json`, grep logs for the specific error pattern with today's date, and confirm the condition still exists before keeping an issue open.

**⚠️ Don't trust evidence fields from the issue record — always re-grep primary sources:** Evidence fields in `issues.jsonl` (`last_429`, `last_seen`, `hours_since_last_429`, etc.) may be stale or incorrect from the previous run. The previous escalation run may have propagated stale data because it updated `verification_time` without actually re-grepping the raw log. **Always compute evidence from primary sources** (grep `errors.log` for the actual latest occurrence, query `jobs.json` for current `last_status`/`last_run_at`). If the previous run had stale data, simply updating `verification_time` will preserve the error. This is especially critical for recurring rate-limit issues where the cascade goes dormant between update waves — the last-seen timestamp drifts if not re-verified against the raw log each run.

**⚠️ Rate-limit cascade pattern:** The most common escalation pattern on Hermes is: primary provider hits 429 rate limit → credential pool exhausts → session summarization fails → auxiliary LLM calls timeout → cron jobs timeout. These are all the same root cause. Document the cascade clearly rather than treating each as independent. Cannot be auto-fixed — requires user to upgrade provider plan or reduce job frequency.

**⚠️ Proposal accumulation:** Each deep scan may generate new InsightProposals for the same fingerprint. Over time, the proposals/ directory fills with duplicates. The escalation runner should consolidate by marking older proposals as `resolved: true` when a newer proposal for the same fingerprint exists, or when the underlying issue is closed.

**⚠️ Rate-limit cascade re-verification:** When checking if a rate-limit cascade is still active, grep errors.log for the specific error codes (429, 403, "no available entries") with today's date. If no matches in the last 4+ hours, the cascade has likely self-resolved and issues can be closed. Do NOT keep issues open based on historical counts — always check recurrence recency.

**⚠️ Rate-limit cascade verification window pitfall:** The "4+ hours without recurrence" heuristic is NOT sufficient for definitive closure. A rate-limit cascade can pause for 10+ hours (e.g., overnight when no cron jobs fire) and then resume with the next daily update wave. This happened on 2026-04-26: No 429s from 00:33 to 06:50 UTC (6+ hours), issue marked `self_resolved`, then 429s resumed at 06:50 and 07:26 UTC. **Safe practice:** For rate-limit issues, require a full 24-hour clean window OR verify across at least one complete daily cron cycle (24h) before marking `self_resolved`. Until then, keep the issue `open` and document it as "dormant between daily update waves" rather than resolved.

**⚠️ 429 sub-pattern distinction:** HTTP 429 errors from LLM providers can have different root causes requiring different fingerprints and remediation:
- `oc_http_429_rate_limit` — "weekly usage limit" / plan-level rate limit. Self-resolves when limit resets. Remediation: wait or upgrade plan.
- `oc_http_429_concurrent` — "too many concurrent requests". Caused by peak concurrent API calls (e.g., 10+ cron jobs firing at the same minute). Remediation: stagger cron schedules to spread load, or upgrade plan for higher concurrency.
Do NOT merge these into a single fingerprint — they have different recurrence patterns and different fixes. When verifying a resolved 429 rate limit issue, check whether a NEW concurrent 429 pattern has emerged since the original was resolved.

**⚠️ Practical cascade pattern:** In practice, `oc_http_429_rate_limit` and `oc_http_429_concurrent` rarely occur in isolation. A concentrated cron wave (e.g., 17 jobs in 25 min at 07:00 UTC) creates concurrent spikes that accelerate consumption of plan-level rate limits, causing both patterns simultaneously. Document the issue as the dominant fingerprint (`oc_http_429_rate_limit`) but note the concurrent contribution. Staggering helps with the concurrent component; the plan-level limit needs separate remediation (upgrade).

<<<<<<< Updated upstream
**⚠️ Google OAuth token expiry field is stale after refresh:** The `expiry` field in `<user-google-email>.json` and `<third-party-or-user-email>.json` is set at token creation time and is NOT updated by the refresh script. After a successful refresh, the file modification time (mtime) is the ground truth for token freshness — NOT the `expiry` field. A token with `expiry: 2026-05-15T18:10:00Z` but mtime of `2026-05-15T17:17 UTC` is FRESH (refreshed after the old expiry). Always check `os.path.getmtime()` or `stat` to verify token freshness. Tokens refreshed within the last 24 hours are valid regardless of the JSON `expiry` field value.
=======
**⚠️ Google OAuth token expiry field is stale after refresh:** The `expiry` field in `<user-google-email>.json` and `<agent-email>.json` is set at token creation time and is NOT updated by the refresh script. After a successful refresh, the file modification time (mtime) is the ground truth for token freshness — NOT the `expiry` field. A token with `expiry: 2026-05-15T18:10:00Z` but mtime of `2026-05-15T17:17 UTC` is FRESH (refreshed after the old expiry). Always check `os.path.getmtime()` or `stat` to verify token freshness. Tokens refreshed within the last 24 hours are valid regardless of the JSON `expiry` field value.
>>>>>>> Stashed changes

**⚠️ Stale OAuth credential → confusing HTTP 400 error substitution:** When a credential pool entry has an expired `agent_key` (its `agent_key_expires_at` has passed) but `last_status: ok`, the round-robin strategy will still select it. Instead of returning HTTP 401 (expected for expired auth), the upstream provider may return HTTP 400 "This request is not valid. Check the model name and other parameters." — an error that looks like a model-name issue but is actually an auth issue.

The credential pool is stored at `{agent_root}/auth.json` under `credential_pool.{provider_name}`. Each entry has `agent_key_expires_at` (the agent key's expiry) and `expires_at` (the OAuth refresh token's expiry). An entry is stale when BOTH are in the past.

**Diagnostic pattern:**
- Error in logs: `HTTP 400 "This request is not valid"` from the LLM provider
- The model name IS valid and works with other credential entries
- The credential pool uses `round_robin` strategy
- Error is intermittent (only on some API calls)
- Inspect `{agent_root}/auth.json` → `credential_pool.{provider}` entries for expired `agent_key_expires_at` dates with `last_status: ok`

**Fix options:**
1. **Change strategy to `fill_first`** — set `credential_pool_strategies.{provider}: fill_first` in config.yaml. Prefers the first (fresh) credential exclusively.
2. **Remove stale entry** — delete the expired entry from `credential_pool.{provider}` in auth.json. ⚠️ Manual/AI action, not auto-fixable — modifying auth.json could break auth if done incorrectly.
3. **Code-level fix** — the credential pool module could be patched to check `agent_key_expires_at` and auto-exhaust expired entries.

**⚠️ Proposal directory accumulation:** InsightProposals marked `resolved: true` accumulate in the proposals/ directory over time. Each deep scan may generate new proposals for resolved fingerprints. The escalation runner should periodically consolidate: count resolved proposals, verify their underlying issues are still closed, and note accumulation in the report. Consider removing proposals older than 7 days that are already resolved. Track the count in the journal as `proposals_cleaned`.

<<<<<<< Updated upstream
**⚠️ Disk trend monitoring:** After disk cleanup (removing old state-snapshots), a new snapshot can regenerate quickly — the system creates `state-snapshots/YYYYMMDD-HHMMSS-pre-update/` directories during custodian:update runs. Each snapshot is ~14G (a full copy of state.db). If disk crosses 85% again, check for new snapshots before assuming the cleanup failed. The snapshot lifecycle is: created during update → should be cleaned after successful update → but cleanup doesn't always run automatically. Monitor `du -sh <hermes-home>/state-snapshots/` on each deep scan.
=======
**⚠️ Disk trend monitoring:** After disk cleanup (removing old state-snapshots), a new snapshot can regenerate quickly — the system creates `state-snapshots/YYYYMMDD-HHMMSS-pre-update/` directories during custodian:update runs. Each snapshot is ~14G (a full copy of state.db). If disk crosses 85% again, check for new snapshots before assuming the cleanup failed. The snapshot lifecycle is: created during update → should be cleaned after successful update → but cleanup doesn't always run automatically. Monitor `du -sh ~/.hermes/state-snapshots/` on each deep scan.
>>>>>>> Stashed changes

**⚠️ State-snapshot auto-cleanup gap:** The `custodian:update` job (schedule: `0 7 * * *`) creates a `state-snapshots/YYYYMMDD-HHMMSS-pre-update/` directory before updating. This snapshot is a full copy of `state.db` (~14GB). The update process does NOT automatically clean up the snapshot after completion. Every deep scan should check for and remove snapshots from completed updates (any snapshot older than 1 hour is safe to remove). This is a recurring source of disk pressure — the snapshot was created at 02:43 UTC on 2026-05-14 and was not cleaned up until the deep scan at 09:21 UTC, during which time disk was at 86%. On this system, 10+ cron jobs fire simultaneously at `0 0 * * *` (midnight UTC): custodian:update, weave:sync-contacts, corvus:update, vesper:update, scout:update, elephas:update, taste:sync-spotify, mentor:update, praxis:update, voyage:update, forge:update, sift:update, sands:update. This causes concurrent API request spikes leading to 429 errors and session summarization failures. When checking rate-limit cascade patterns, always check the cron schedule for simultaneous job triggers.

## Tier 1 Fix: Cron Schedule Staggering (for `oc_http_429_concurrent`)

When multiple cron jobs use the same shorthand pattern (e.g., `*/10 * * * *` or `0 7 * * *`), they all execute at the identical minute tick. If those jobs make LLM API calls, they create concurrency spikes that trigger `HTTP 429: too many concurrent requests`. The fix is staggering: offset each job's start minute so they fire sequentially instead of simultaneously.

**Diagnosis:** Query `jobs.json` for same-minute fire patterns:
```bash
python3 -c "
import json
from collections import Counter
<<<<<<< Updated upstream
with open('<hermes-home>/cron/jobs.json') as f:
=======
with open('~/.hermes/cron/jobs.json') as f:
>>>>>>> Stashed changes
    jobs = json.load(f)['jobs']
schedules = Counter()
for j in jobs:
    s = j.get('schedule','')
    if isinstance(s, dict): s = s.get('expr','')
    schedules[s] += 1
for s, count in schedules.most_common():
    if count > 1:
        names = [j['name'] for j in jobs if (j.get('schedule','') == s or (isinstance(j.get('schedule'), dict) and j['schedule'].get('expr','') == s))]
        print(f'{count}x | {s:20s} | {\" \".join(names)}')
"
```

**Staggering technique:** Replace `*/N` with `N-59/M` using unique offset minutes:

| Shorthand | Staggered form | Fires at minutes |
|---|---|---|
| `*/10 * * * *` | `0-59/10 * * * *` | :00, :10, :20, :30, :40, :50 |
| `*/10 * * * *` | `1-59/10 * * * *` | :01, :11, :21, :31, :41, :51 |
| `*/10 * * * *` | `3-59/10 * * * *` | :03, :13, :23, :33, :43, :53 |
| `*/15 * * * *` | `5-59/15 * * * *` | :05, :20, :35, :50 |
| `*/15 * * * *` | `12-59/15 * * * *` | :12, :27, :42, :57 |

**Procedure (idempotent):**
```bash
# Use the cronjob tool to update the schedule
cronjob action=update job_id=<job_id> schedule="1-59/10 * * * *"
```

**Concrete example (from 2026-04-26):** 6 high-frequency jobs (`dispatch:check`, `dispatch:draft`, `Gateway health monitor`, `weave-sync-10min`, `elephas:ingest`, `dispatch:briefing-deliver`) were all firing at `:00` of every 10/15-minute interval, hitting OpenRouter with 7 simultaneous requests. Each was staggered +1/+2/+3/+7/+5/+12 minutes off the hour. 429 errors dropped significantly.

**Verification:** After staggering, pick a time when the old schedule would have fired, check `error.log` for 429s at that minute:
```bash
grep "HTTP 429" {agent_root}/logs/errors.log | grep "$(date +%H:%M)" | head -5
```

**Large wave staggering (17+ jobs in a 25-min window):** When a concentrated wave of daily-update jobs (e.g., 17 jobs scheduled 07:00-07:25 UTC) all fire within minutes of each other, the 5-minute stagger found in the diagnosis step is insufficient — jobs overlap and their API calls stack. The fix is to spread the wave across a much wider window:

1. **Identify the wave** — Use the diagnosis script above to find all jobs in the same hour block
2. **Assign each job a unique minute** — With 17 jobs and a 60-minute hour, assign each job a distinct minute (e.g., 07:00, 07:06, 07:12, ..., 08:36). The formula: `base_hour + (index * (60 / count))` rounded to nearest minute offset
3. **Use two hours if needed** — For waves > 30 jobs or jobs that are resource-intensive, spill into the next hour. Example from 2026-04-26: 17 jobs spread from 07:00 to 08:36 (96-min window vs original 25-min window)
4. **Apply via `cronjob action=update`** — Use the tool for each job individually:
   ```bash
   cronjob action=update job_id=<job_id> schedule="12 7 * * *"
   ```
5. **Verify during the next wave** — Do NOT verify immediately. Wait for the next scheduled wave time, then check for 429s at the old conflict minutes. The verification window is the NEXT natural occurrence of the wave.

**Concrete large-wave example (2026-04-26, 17 jobs staggered from 07:00-07:25 to 07:00-08:36):**
| Original | Jobs | After | Jobs |
|---|---|---|---|
| 07:00 | custodian:update, corvus:update (2) | 07:00 | custodian:update only |
| 07:05 | vesper:update, scout:update, elephas:update (3) | 07:05 | corvus:update only |
| 07:10 | taste:sync-spotify, mentor:update, praxis:update (3) | 07:12 | vesper:update |
| 07:15 | voyage:update, forge:update, sift:update (3) | 07:18 | scout:update |
| 07:20 | sands:update, look:update, fellow:update (3) | 07:24 | elephas:update |
| 07:25 | weave:update, lucid:update, taste:update (3) | 07:30-08:36 | remaining 12 jobs at 6-min intervals |

**When to use this fix (trigger conditions):**
- Error log shows `HTTP 429: Provider returned error` with `provider=openrouter` (or any concurrent-rate-limited provider)
- Multiple cron jobs share the same schedule (check with the diagnosis script above)
- Jobs are `last_status: ok` despite 429 errors (they retry and succeed, but the retries waste API calls)
- 429 errors cluster at specific minutes (e.g., every 10 minutes on the :00, :10, :20 marks, or a dense wave like 07:00-07:25 UTC)
- **High-risk pattern:** A "daily update wave" where 10+ jobs fire within a 30-minute block AND all make LLM API calls — this is the most common source of 429 concurrency spikes

**Reversibility:** Change the schedule back to the original `*/N` shorthand.

**⚠️ Log file locations:** On Hermes, the date-stamped log pattern `agent-YYYY-MM-DD.log` may not exist. The actual log files are: `{agent_root}/logs/agent.log` (general), `{agent_root}/logs/errors.log` (errors/warnings), `{agent_root}/logs/gateway.log` (gateway platform events). Use `tail` and `grep` on these files rather than trying to construct date-stamped paths.

<<<<<<< Updated upstream
**⚠️ gateway.log is binary — use `strings` before `grep`:** The `gateway.log` file may be in a binary or non-text format that doesn't respond to standard `grep`. Always pipe through `strings` first: `strings <hermes-home>/logs/gateway.log | grep "pattern"`. This is especially important when searching for job run outcomes, error messages, or session IDs in gateway logs.
=======
**⚠️ gateway.log is binary — use `strings` before `grep`:** The `gateway.log` file may be in a binary or non-text format that doesn't respond to standard `grep`. Always pipe through `strings` first: `strings ~/.hermes/logs/gateway.log | grep "pattern"`. This is especially important when searching for job run outcomes, error messages, or session IDs in gateway logs.
>>>>>>> Stashed changes

**⚠️ errors.log format:** The errors.log format is `YYYY-MM-DD HH:MM:SS,mmm LEVEL [session_id] message` — NOT `| ERROR message`. When parsing with regex, use:
```python
# Correct pattern for errors.log:
match = re.search(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} (\w+) \[(\w+)\] (.*)', line)
# Groups: level, session_id, message
```
Do NOT use `r'\| ERROR\s+(.*)'` — it will match zero lines.

**⚠️ read_file 100K character limit:** The `read_file` tool has a hard limit of 100,000 characters. Files larger than this (e.g., `jobs.json` at 76K is fine, but `errors.log` at 1.4MB or `state.db` at 14GB are not) will fail with `"Read produced N characters which exceeds the safety limit"`. For large files, use:
- `terminal` with `grep`, `head`, `tail`, `