# Critical Pitfalls — Custodian

These are the most commonly-hit traps. Check this section before starting any scan.

1. **Pipe-to-interpreter AND execute_code both blocked in cron:** `cat file | python3 -c "..."` is ALWAYS blocked by security scan. In cron mode (no user present), `execute_code` is ALSO entirely blocked — `read_file` + `execute_code` does NOT work. The universal-safe substitute for ALL contexts:
   ```bash
   terminal(command='python3 -c "import json, re; ..."')
   ```
   If the code needs file I/O, write intermediate results to `/tmp/` and read them back. Prefer this pattern for any code that must also run in cron jobs.

2. **`hermes cron list` crashes on string schedules / jobs.json is a dict:** Read `{agent_root}/profiles/{profile}/cron/jobs.json` directly via `terminal` with `python3 -c` (not `execute_code` in cron). The top-level structure is `{"jobs": [...], "updated_at": "..."}`, NOT a raw list. Access jobs via `data.get('jobs', [])` or `data['jobs']`. Parsing it as a list directly will crash with `AttributeError: 'str' object has no attribute 'get'`.

   **jobs.json locations (both exist):**
   - `<hermes-root>/cron/jobs.json` — default profile jobs
   - `<hermes-home>/cron/jobs.json` — indigo profile jobs (authoritative for indigo sessions)
   
   Always use the profile-specific path when running under a profile.
3. **jobs.json control chars:** Always clean with chr-based builder before parsing: `[chr(i) for i in list(range(0,9))+[11,12]+list(range(14,32))+[127]]`. **NOTE: When running this via `terminal()` in cron mode, the `\x` escapes will crash** (`re.error: incomplete escape \x`). Use the CHR-based builder instead.
4. **read_file 100K limit:** For large files (errors.log, state.db), use `terminal` with grep/tail/python3. Do NOT use `execute_code` in cron context (see pitfall #1).

4a. **patch tool fails on non-standard JSON indentation:** When a JSON file has inconsistent or no indentation on certain lines, the `patch` tool's fuzzy matcher may fail. **Workaround:** Use `terminal` with python3 to parse the JSON via `json.load()`, modify in memory, then write back via `write_file` with `json.dump(indent=2)`.

   **⚠️ Pitfall — Writing JSONL files safely:** Use the `write_file` tool for JSONL files, NOT `terminal` with heredoc/`cat >>`. For JSONL files you must read, modify, and rewrite: read via `terminal` + Python (cron), modify in memory, then write back via `write_file`. **Never use `read_file` on files in `{agent_root}/commons/data/ocas-custodian/`.** The `read_file` tool's sandbox caching mechanism can overwrite the actual file on disk with a placeholder stub.

   | Tool | Safe for reading | Safe for writing |
   |---|---|---|
   | `execute_code` with `open()` | ✅ Always | ✅ Always |
   | `terminal` with `cat`/`grep` | ✅ Always | ⚠️ Use `write_file` tool |
   | `write_file` tool | N/A | ✅ Always |
   | `read_file` tool | ❌ **Corrupts JSONL files** | N/A |

4b. **gateway.log file format:** The file at `{agent_root}/logs/gateway.log` is plain text in current Hermes deployments. Use standard text tools.

4c. **gateway.log is tiny after restart (113 bytes) / STALE after --replace:** After a gateway restart (especially `--replace`), `gateway.log` may contain only the old shutdown sequence. The ACTUAL error data for the current day is in `{agent_root}/logs/errors.log`. Always check `errors.log` as the **primary error source** during light scans. Check `hermes-update.log` for restart timestamps and `ps -o etime` for process uptime to determine the actual restart time.

   **⚠️ Pitfall — `jobs.json` on-disk ≠ scheduler in-memory state:** After batch `hermes cron pause`/`resume` operations or `--replace` restart, the scheduler updates its **in-memory** `next_run_at` values but does NOT write them back to `jobs.json` on disk. The file will show stale `next_run_at` values even though the scheduler has correct future times. When checking skill journal completeness, a directory existing with 0 files does NOT mean the skill is uninitialized. Some OCAS skills run successfully via cron but do not write observation journals. Check the job's `last_status` — if it's `ok`, the skill is running fine.

4d. **Shell quoting: inline Python via terminal() crashes on complex expressions:** Any Python with `[`, `]`, `)`, `"`, `'`, `$`, `!`, or backslash escaping inside a `terminal(command=...)` call should use heredoc `<< 'PYEOF'` instead of inline `-c "..."`. The quoted delimiter `'PYEOF'` prevents all shell expansion.

5. **Gateway systemd service check:** On every deep scan, run `systemctl is-active hermes-gateway` AND `curl -s http://localhost:8080/health`. If health returns "ok" but systemd reports inactive/dead, the gateway is running fine — systemd lost track due to `--replace` takeover. Do NOT generate `oc_gateway_service_failed` proposals in this case. If health fails, check `journalctl -u hermes-gateway --since "7 days ago" --no-pager` for the root cause.

   **⚠️ Pitfall — Health endpoint down but system operational (2026-06-05):** After a `--replace` restart, the gateway process may be running (systemd active, jobs executing) but port 8080 health endpoint may not bind. Check `ss -tlnp` to see what ports ARE listening — port 9119 (pyroscope) and 8081 (uvicorn) may be up while 8080 is not. If `systemctl is-active` returns active AND cron jobs have recent `last_run_at` timestamps, the gateway is functional despite the health endpoint being unreachable. Do NOT escalate as `oc_gateway_service_failed`. Classify as `oc_gateway_health_endpoint_unreachable` (Tier 2 — surface only). The health endpoint loss is cosmetic; the scheduler and agent loop operate independently of it.

   **⚠️ Pitfall — TimeoutStopSec crash loop:** If the journal shows rapid "Failed with result 'signal'" entries (SIGKILL during shutdown), the systemd unit's `TimeoutStopSec` is too short. Fix: `hermes gateway install --force` then `systemctl --user reset-failed hermes-gateway.service` then `systemctl --user start hermes-gateway.service`. Do NOT use `restart` or `stop` when in this state.

   **⚠️ Pitfall — Rapid SIGTERM restart loop (2026-06-23):** When the gateway receives SIGTERM every ~3 minutes (5+ restarts in <15 min), systemd is repeatedly triggering shutdown. The gateway performs clean teardown each time (drain, disconnect, SessionDB close, exit code 1) and systemd `Restart=on-failure` revives it. This is NOT a gateway crash — it's an external signal loop. The loop can last 10-15 minutes before the triggering condition resolves (e.g., a systemd timer, a competing service, or a config reload). **Do NOT escalate as `oc_gateway_crash_loop`.** Classify as `oc_gateway_sigterm_restart_loop` (Tier 2, monitor only). The gateway is healthy — it's the external signal that's the problem. Verify stability by checking that the last log entry shows "Gateway running with N platform(s)" and no subsequent SIGTERM for >30 minutes. If the loop persists >1 hour, check `systemctl list-timers` and `journalctl -u hermes-gateway --since "1 hour ago"` for the triggering unit.

   **⚠️ Pitfall — Cron job inactivity timeout (finch:scan pattern):** Some cron jobs fail consistently because they exceed the 600s/1800s cron inactivity timeout while executing long-running tools (commonly `session_search` on large state.db). This is a structural issue — NOT a transient error. Surface as `oc_cron_job_inactivity_timeout` (Tier 2).

   **⚠️ Pitfall — Mass overdue jobs after gateway downtime:** When >85% of jobs simultaneously show overdue `next_run_at` AND `last_run_at` >48h old, this is a MASS EVENT (gateway downtime / scheduler divergence), not individual job failures. Do NOT triage each job individually. Use the Mass Event Procedure in `references/jobs-not-running-diagnostic.md`. The `jobs.json` `updated_at` timestamp can be fresh while `next_run_at` values are stale — the scheduler wrote the file but didn't recalculate schedules after `--replace` restart.

   **⚠️ Pitfall — Activity model must include telegram and cli as user sources:** The `state.db` `sessions` table `source` column uses `telegram` and `cli` for user-initiated sessions (not `source='user'`). Filter with `source NOT IN ('cron', 'heartbeat', 'dojo-seed')` to capture all user activity.

6. **⚠️ Pitfall — NoneType f-string crash:** When processing `jobs.json`, fields like `schedule`, `last_status`, `last_run_at` can be `None`. Using f-string format specs on `None` crashes. Always coerce: `f"{(sched or 'N/A'):20s}"` or `f"{str(sched):20s}"`.

   **⚠️ Pitfall — oc_google_token_invalid auto-fix assumes backup exists:** Before running the fix, verify the backup file exists and is non-empty. If BOTH tokens are missing, classify as `oc_google_token_missing` (Tier 3).

7. **⚠️ Pitfall — finch:work task-list.json path mismatch:** The correct path is `<hermes-root>/commons/data/ocas-finch/task-list.json`.

8. **⚠️ Pitfall — Stale git lock files in checkpoints/:** Check for stale lock files with mtime >1 day and size 0 bytes. Safe to `rm -f`.

9. **⚠️ Pitfall — MCP simultaneous failure (mempalace + stealth-browser):** When both fail with identical "unhandled errors in a TaskGroup" errors, this is a systemic/runtime issue. If gateway health is ok, the MCP failures are non-fatal noise.

9a. **⚠️ Pitfall — MCP server "process not running" is normal (on-demand spawn):** MCP server processes are spawned on-demand. If `ps aux` shows no MCP process, this is expected.

9b. **⚠️ Pitfall — MCP `command: python3` resolves to wrong Python:** Check which Python actually has the module. Fix: update the MCP server `command` in `config.yaml` to the full path of the correct Python binary.

9c. **⚠️ Pitfall — MCP `enabled: false` means connection failures are expected:** Before investigating MCP connection failures, check `config.yaml` `mcp_servers.<name>.enabled`.

9d. **⚠️ Pitfall — `Path.mkdir(parents_ok=True)` is wrong, use `parents=True`:** Python's `pathlib.Path.mkdir()` takes `parents=True` (not `parent_ok`).

9e. **⚠️ Pitfall — `state.db` sessions table uses `started_at`, not `created_at`:** Querying `WHERE created_at > ...` crashes. Use `started_at`.

9k. **⚠️ Pitfall — `started_at` stores Unix epoch floats, not ISO strings:** The `started_at` column stores Unix timestamps as floats (e.g., `1780592739.65`), NOT ISO 8601 strings. SQLite's `datetime('now', '-7 days')` returns an ISO string like `'2026-05-30 02:00:00'`. Comparing `started_at > datetime('now', '-7 days')` silently returns wrong results (epoch 1780592739 > string '2026-...' evaluates as false in SQLite's type affinity). Use epoch math instead: `WHERE started_at > (strftime('%s', 'now') - 7*86400)` to filter sessions from the last 7 days.

9l. **⚠️ Pitfall — Stale issues in issues.jsonl vs live cron state:** Before closing any issue in `issues.jsonl`, verify the affected job's live status with `hermes cron list`. Issues can have `last_run_at: null` or old error data even though the job has since run successfully and is healthy. This happens when issues are created from a previous scan's stale data. Cross-checking against `hermes cron list` (which queries the live scheduler state) prevents falsely keeping stale issues open OR prematurely closing real ones. See `references/escalation-runner-2026-06-08.md` for 8 examples of verified-stale issues.

9q. **⚠️ Pitfall — CWD may not be /root:** The session CWD can be a project directory (e.g., `/root/hermes-telegram-artifacts`) rather than `/root`. Relative paths in `terminal()` calls will fail. Always use absolute paths: prefix with `<hermes-root>/...` not `~/.hermes/...` (tilde expansion can also be unreliable in terminal()). Run `pwd` at the start of a session if path-related failures occur.

9r. **⚠️ Pitfall — read_file can return "File not found" for files that exist on disk:** If `read_file` reports "File not found" but `ls` confirms the file exists, use `terminal(command="/path/to/file")` as the fallback. This has been observed on JSON journal files specifically. Always verify with `ls` before concluding a file doesn't exist.

9s. **⚠️ Pitfall — "Dead script" false positive when job script field is null:** When a cron job's `script` field is `null`, the agent executes the task from the `prompt` text, which may reference a script path. Do NOT flag the job as `oc_cron_dead_script_ref` based solely on the `script` field being null. Before flagging: (1) check if the referenced path in the prompt actually exists with `ls`, (2) check if the job is `enabled: false` or `state: paused` (intentionally disabled jobs are not broken), (3) check `last_status` and `last_error` — if `last_status=ok` and `last_error=null`, the job is functioning correctly even if the script path in the prompt doesn't match the `script` field. A null `script` field with a prompt-referenced path that exists and a healthy job status is NOT a dead script issue. Confirmed false positive 2026-06-14: chronicle-outline-sync job had `script: null`, sync.sh existed at the referenced path, job was intentionally paused (Outline container down), and `last_status=ok`.

9t. **⚠️ Pitfall — Context engine "not found" warnings are expected when config uses a different engine:** When `config.yaml` has `context: engine: compressor` (or any engine other than `chronicle`), the "Context engine 'chronicle' not found — falling back to built-in compressor" warning is expected informational noise. Do NOT flag as `oc_context_engine_chronicle_missing` unless: (1) the config actually has `engine: chronicle` but the plugin fails to load, OR (2) the plugin directory is genuinely empty/missing. Before flagging: check `config.yaml` for the actual `context.engine` value, then verify the plugin code exists and is importable. If the config requests a different engine, the warnings are not errors — they're the system correctly falling back to the configured engine. Confirmed false positive 2026-06-14: 43 warnings over 2 days, config had `engine: compressor`, chronicle plugin was present and importable.

9u. **⚠️ Pitfall — `schedule_display=None` is cosmetic, not functional:** A cron job can have `schedule_display: null` in jobs.json while having a valid `schedule` object (`kind: cron, expr: ...`). The job runs correctly — `last_status=ok`, `last_run_at` is recent, `next_run_at` is set. The `schedule_display` field is a derived/cached value that can be null if the job was created via API or imported. Do NOT flag as a broken schedule or attempt to fix. Confirmed 2026-06-14: `dispatch:triage-morning` (id=3) and `dispatch:triage-evening` (id=4) both have `schedule_display=None` but valid cron schedules and run correctly.

   **⚠️ Pitfall — User-created jobs with `last_status=null` and `last_run_at=null` are NOT orphans:** A job that has never run but has `next_run_at` set to a future timestamp is a user-created job waiting for its first run. It is NOT orphaned. Do NOT remove it. The `menu-monitor-weekly` job (id=a6788bcd3411) is an example: created by the user, references ocas-taste, has `next_run_at=2026-06-15`, has never run. Leave it alone.

   **⚠️ Pitfall — Stale failure counter with last_status=ok is NOT an active error:** A job can show `consecutive_failures > 0` while having `last_status=ok` and `last_error=null`. The counter is stale from a previous transient failure that self-resolved. This is common after provider timeouts or brief network issues. If `last_status=ok` and the job ran recently, do NOT escalate. The counter resets to 0 on the next successful run. Confirmed pattern: elephas:ingest (cf=2, ok, ran 12m ago) and weave-enrichment-health-check (cf=1, ok, ran 53m ago) on 2026-06-14.

9v. **⚠️ Pitfall — `hermes cron create` syntax**: The command is `hermes cron create --name "job:name" "schedule" "prompt text"` — schedule and prompt are **positional arguments**, NOT flags. There is NO `--schedule`, `--prompt`, `--model`, or `--skill` flag. The `--name` flag is optional. Example: `hermes cron create --name "sands:chronicle-sync" "0 8 * * 0" "Run sands.chronicle.sync weekly."`. Using `--schedule "..."` or `--prompt "..."` will fail with `unrecognized arguments`. See the SKILL.md gotchas section for the full syntax reference.

9w. **⚠️ Pitfall — `oc_config_empty_section` fix-loop (2026-06-23, updated)**: The empty sections fix has been applied 2x (2026-06-17, 2026-06-18). Per the fix-loop detection rule, do NOT apply a 3rd time. Confirmed 3rd occurrence on 2026-06-23: `fallback_model: null` reappeared after gateway restart at 04:22. The fix-loop list is NOT limited to the originally-known keys (`custom_providers`, `fallback_providers`, `honcho`, `hooks`, `personalities`, `quick_commands`, `whatsapp`) — ANY null/empty top-level key that reappears after removal counts as recurrence. Escalate to Tier 3 with root cause "gateway config migration regenerates null/empty keys from template." Fix direction: add a post-startup hook that strips these automatically, not repeated manual removal.

9x. **⚠️ Pitfall — All jobs `status=pending` after gateway restart is normal (2026-06-23)**: After a gateway restart, `jobs.json` may show ALL jobs with `status: "pending"` — not `error`, not `ok`. This is the scheduler's default state on startup. The scheduler only updates `status` and `last_run_at` after a job completes its first post-restart run. Do NOT interpret this as a mass failure event or MassOverdueCondition. The "mass overdue" pattern (pitfall 5) requires overdue `next_run_at` AND old `last_run_at` — not just pending status. All-pending with fresh `updated_at` timestamp = normal post-restart state. Confirmed 2026-06-23: 132/132 jobs showed `status=pending` after restart at 04:22; no errors found.

9z. **⚠️ Pitfall — PyYAML stale-read returns phantom null keys (2026-06-24):** `yaml.safe_load()` via `terminal()` can return cached/stale file state, showing top-level keys as `None` when the actual file has values or the key doesn't exist. This triggers false-positive `oc_config_empty_section` detection and potential Tier 3 escalation. **Before acting on PyYAML null-key findings, ALWAYS verify with raw file read:** `grep -n "^key:" config.yaml` or `sed -n '/^key:/p' config.yaml`. If PyYAML and raw file disagree, raw file is authoritative. Confirmed 2026-06-24: PyYAML reported `fallback_model: None, mcp: None, max_concurrent_sessions: None` but raw file showed `mcp:` as a populated dict and the other two keys absent. See `references/config-null-key-verification.md`.

9s. **⚠️ Pitfall — Gateway "Another instance" errors are noise, not failures:**

 **⚠️ Pitfall — Duplicate systemd service as root cause of collision noise:** When "Another gateway instance" errors exceed ~100/day, check for a duplicate systemd service: `systemctl --user list-units --type=service | grep hermes-gateway`. If both `hermes-gateway.service` (default profile) and `hermes-gateway-indigo.service` (indigo profile) exist and the default one shows `activating (auto-restart)`, the default service is in a crash loop. The fix is `systemctl --user stop hermes-gateway.service && systemctl --user disable hermes-gateway.service`. This requires user confirmation (Tier 2). The two services can be distinguished by their `HERMES_HOME` environment variable: default uses `<hermes-root>`, indigo uses `<hermes-home>`.

   **⚠️ Pitfall — Health endpoint down but system operational (2026-06-05):**
```python
best = {}
for entry in all_entries:
    eid = entry.get('issue_id') or entry.get('id')
    if eid not in best or status_priority(entry) > status_priority(best[eid]):
        best[eid] = entry
with open(path, 'w') as f:
    for entry in best.values():
        f.write(json.dumps(entry) + '\\n')
```
Also remove non-issue entries (scan_complete, esc-run log entries) during dedup.

9n. **⚠️ Pitfall — Large state.db batch DELETE with timeout:** Deleting rows from a 14GB+ SQLite DB with FTS trigram indexes will exceed the 60s terminal timeout. Use batched DELETE with LIMIT in a loop:
```bash
for i in $(seq 1 20); do
    RESULT=$(sqlite3 <hermes-root>/state.db "DELETE FROM sessions WHERE id IN (SELECT id FROM sessions WHERE source='cron' AND started_at < (strftime('%s', 'now') - 30*86400) LIMIT 500); SELECT changes();")
    if [ -z "$RESULT" ] || [ "$RESULT" = "0" ]; then break; fi
done
```
Each batch of 500 takes ~5-10s. Total wall time for 3000 rows ≈ 30-60s. The session will timeout before completion — plan for partial cleanup across multiple runs.

9o. **⚠️ Pitfall — state.db VACUUM requires auto_vacuum=incremental at creation time:** `PRAGMA incremental_vacuum(N)` has no effect if the DB wasn't created with `PRAGMA auto_vacuum = incremental`. The only way to reclaim freelist pages is a full `VACUUM`, which requires ~2x the DB size in temporary disk space. On a 14GB DB with 24GB free disk, VACUUM is not feasible. Alternative: use `VACUUM INTO '/tmp/state.db.new'` (copy to a filesystem with more space), then swap. If no such filesystem exists, the freelist pages remain allocated until disk space is freed by other means (e.g., removing backups).

9p. **⚠️ Pitfall — spot:update git stash fix:** When `ocas-spot` skill directories accumulate local uncommitted changes that block `git pull` in the update script, run `git stash save "custodian-escalation-runner: auto-stash before update"` in BOTH `<hermes-root>/skills/ocas-spot` AND `<hermes-home>/skills/ocas-spot`. The update script (`update_spot.sh`) calls `update_skill.sh` which does a `git pull` — uncommitted changes cause merge conflicts. After stashing, the update script can pull cleanly. This is a Tier 1 auto-fix.

9f. **⚠️ Pitfall — HTTP 429 with `consecutive_failures: None` is truly transient:** The scheduler does not consider it a persistent failure. Do NOT escalate.

9y. **⚠️ Pitfall — Stale 401/403 errors on null-provider jobs:** When a null-provider job (`provider: null, model: null`) shows `status=error` with a 401/403 `last_error` but `consecutive_failures=None/0`, check the gateway log for recent matching errors. If the last 2000 lines of `gateway.log` contain NO 401/403 errors, the 401 on the job is **stale** — the provider issue was resolved externally (e.g., broken fallback provider removed from config.yaml by a prior escalation run). The job will succeed on its next scheduled run without intervention. Do NOT apply pause/resume or escalate. Confirmed 2026-06-23: `bower:weekly-deep` and `taste:scan` both showed 401 from June 21, no 401 in recent gateway log, both scheduled to run next on their regular schedule.

9g. **⚠️ Pitfall — VACUUM timeout on large state.db with FTS trigram indexes:** VACUUM on a state.db >10GB can exceed the 600s terminal timeout. FTS trigram indexes retain size even after VACUUM — accept as operational cost.

9h. **⚠️ Pitfall — `hermes cron edit` requires relative script paths:** The `hermes cron edit <id> --script <path>` command requires paths relative to `~/.hermes/scripts/`, NOT absolute paths. Passing an absolute path like `<hermes-home>/scripts/foo.py` fails with "Script path must be relative to ~/.hermes/scripts/". Use just the filename (e.g., `foo.py`) if the script exists at `~/.hermes/scripts/foo.py`. If the script only exists under the profile directory, create a symlink or copy to `~/.hermes/scripts/` first.

9i. **⚠️ Pitfall — Stale `last_status` after pause/resume is expected:** After applying the pause/resume fix to reset stale scheduler state, `jobs.json` will still show `status=error` for the affected jobs until they complete their next actual run. The scheduler's in-memory state is reset (verified by checking `next_run_at` is recalculated), but `last_status` in the on-disk file is not updated until the next run completes. Do NOT re-apply the fix — this is normal behavior. The fix is confirmed successful if `next_run_at` is recalculated to a future time.

9j. **⚠️ Pitfall — state.db >10GB is a Tier 2 issue:** When `state.db` exceeds 10GB (expected <1GB), flag as `oc_state_db_oversized` (Tier 2). VACUUM feasibility: free_disk >= db_size is sufficient (confirmed 2026-06-08: 13.1 GB DB, 17.1 GB free, VACUUM succeeded in 97s, reclaimed 6.96 GB freelist). The primary space consumer is the `system_prompt` column in old sessions, not FTS indexes. Before VACUUM, check freelist ratio: `PRAGMA freelist_count` / `PRAGMA page_count`. If >30% freelist, VACUUM is worthwhile. If disk is >80% full, free up space via backup/state-snapshot cleanup first.
