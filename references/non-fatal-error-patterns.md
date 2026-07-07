# Non-Fatal Error Patterns (Tier 2 — Surface Only)

These patterns are detected during scans but are NOT auto-fixed. They are logged for awareness.

## Quick Reference — Newly Added Patterns (2026-05-18)

| Fingerprint | Description | Action |
|---|---|---|
| `oc_cron_job_inactivity_timeout` | Cron job exceeds 600s inactivity timeout on long-running tools (commonly `session_search`). Recurring for finch:scan. | Surface in report. No auto-fix — structural issue. |
| `oc_cron_declared_but_unregistered` | Task declared in SKILL.md but no matching cron job. Check if another job covers the same function before registering. | Surface in report with analysis. |

## MCP Stdio Parse Errors

| Fingerprint | Description | Action |
|---|---|---|
| `oc_mcp_stdio_parse_error` | MCP server stdout contains non-JSON content (ANSI escape codes, colored text, non-English output) that fails JSONRPC parsing. Non-fatal — MCP client skips malformed messages. Common with google-search MCP server outputting Chinese ANSI-colored text. | Surface in report. No auto-fix. Known pattern. |

## MCP Cascade Failures

| Fingerprint | Description | Action |
|---|---|---|
| `oc_mcp_simultaneous_failure` | Multiple MCP servers (mempalace + stealth-browser) fail with identical "unhandled errors in a TaskGroup" errors simultaneously. Systemic issue, not independent failures. | Log as single observation. No auto-fix. If gateway health is ok, ignore. |

## Transient Provider Errors

| Fingerprint | Description | Action |
|---|---|---|
| `oc_http_429_rate_limit` | OpenRouter weekly usage limit. Self-resolves when limit resets. | Surface in report. No auto-fix. |
| `oc_http_429_concurrent` | Too many concurrent requests. Caused by peak concurrent API calls. | Stagger cron schedules if recurring. |
| `oc_http_502_provider_unavailable` | OpenRouter HTTP 502 `provider_unavailable` — upstream model provider temporarily down. Distinct from 429: no rate limit errors in logs, API key valid, model exists, credential pool ok. Self-resolving. See `references/openrouter-502-provider-unavailable.md`. | Surface in report. No auto-fix. |
| `oc_provider_error_transient` | Generic transient OpenRouter provider errors (catch-all for non-429/502 patterns). | Surface in report. No auto-fix. |

## Stale Error Patterns

| Fingerprint | Description | Action |
|---|---|---|
| `oc_cron_stale_error` | Job shows `last_status: error` but error is from previous day/week with no corresponding errors in today's logs. | Verify against primary sources before escalating. |

## Chronicle Plugin Directories Empty

| Fingerprint | Description | Action |
|---|---|---|
| `oc_chronicle_plugins_empty` | `plugins/memory/chronicle/` and/or `plugins/context_engine/chronicle/` directories contain only `__pycache__/` — no `.py` source files. Directories are not in git HEAD, so files were likely removed by a gateway update or separate cleanup. Affects Chronicle context engine and memory provider. Non-fatal (degrades gracefully). See `references/chronicle-plugin-dirs-empty-pattern.md`. | Surface in report. No auto-fix — files not in git history and original install method unknown. Re-install Chronicle plugin package appropriate for current hermes-agent version. |

## Next-AI-Drawio MCP Failures

| Fingerprint | Description | Action |
|---|---|---|
| `oc_mcp_drawio_connection_failure` | MCP server 'next-ai-drawio' persistently fails with "unhandled errors in a TaskGroup (1 sub-exception)" / "Connection closed". Occurs every session startup. Non-fatal — MCP client skips gracefully. Very high frequency (755+ occurrences per startup cycle). | Surface once in report. No auto-fix. Known pattern. |

## Request Dump Key Exposure

| Fingerprint | Description | Action |
|---|---|---|
| `oc_request_dump_key_exposure` | `request_dump_*.json` files contain plaintext API keys. | Surface with recommendation to delete old files. |

## Custom Provider Auth Failures

| Fingerprint | Description | Action |
|---|---|---|
| `oc_http_401_auth_failure` | HTTP 401 authentication failures affecting multiple agent-mode jobs. Check if transient (API key valid, consecutive_failures=None) before escalating. If transient, self-resolves. If persistent, provider API key may need rotation. See `references/transient-provider-errors.md` for diagnostic procedure. | Surface in report. If transient, no auto-fix. If persistent, check key validity. |
| `oc_spot_manifest_build_401` | `spot:watch-sweep` job fails with HTTP 401 from `manifest.build` custom provider. API key is present in config.yaml (`fallback_model.api_key`) but rejected by upstream. Key may be expired or revoked. | Surface in report. Check if key is still valid at manifest.build. No auto-fix — credential update requires user action. |
| `oc_finch_weekly_manifest_401` | `ocas-finch:weekly` job fails with HTTP 401 from `manifest.build` custom provider (`fallback_model` in config.yaml). Same root cause as spot:watch-sweep 401 — the API key (`mnfst_897...`) is rejected by upstream. Key may be expired or revoked. | Surface in report. Check if key is still valid at manifest.build. No auto-fix — credential update requires user action. If both spot and finch hit 401 simultaneously, it's a provider key issue, not a per-job issue. |\n\n## MCP Server Files Missing

| Fingerprint | Description | Action |
|---|---|---|
| `oc_mcp_server_files_missing` | MCP servers enabled in config.yaml but server script files don't exist on disk. MCP client tries 3 attempts per server, fails, gives up gracefully. Produces 380+ WARNINGS per startup cycle. Non-fatal. User decision required: install packages or disable in config.yaml. | Surface in report. Do NOT auto-disable — escalate for user decision. |

## Context Engine 'Chronicle' Not Found

| Fingerprint | Description | Action |
|---|---|---|
| `oc_context_engine_chronicle_not_found` | "Context engine 'chronicle' not found — falling back to built-in compressor" warnings in errors.log. The Chronicle context engine plugin is not being loaded despite the kwargs bug being fixed (issue `oc_chronicle_kwargs_get_20260604`, resolved). May indicate empty plugin directory (`plugins/context_engine/chronicle/` has no `.py` files), plugin not installed, or plugin discovery misconfiguration. Non-fatal — degrades gracefully to built-in compressor. | Surface in report. Check if `plugins/context_engine/chronicle/` has `.py` files. If empty, re-install Chronicle plugin. If present, check plugin discovery config. |
| `oc_context_engine_chronicle_session_lookup_noise` | **Sub-pattern** (2026-06-16): Plugin loads successfully at gateway startup (confirmed by "already registered by a plugin" messages in logs), plugin directory has `__init__.py` and code, but agent-session-level context engine lookup still falls back to compressor. Config has `context.engine: chronicle`. Occurs 10-20+ times per day during agent sessions. **Distinguished from empty-plugin-dir case by**: (1) plugin dir has `.py` files, (2) "already registered" messages confirm plugin loaded, (3) `import hermes_chronicle_plugin` may fail from system Python (not installed as editable package) even though the gateway's plugin loader finds it. Non-fatal — degrades gracefully. | Surface once in report with count. No auto-fix. Known pattern. |

## Kanban Dispatcher Stuck

| Fingerprint | Description | Action |
|---|---|---|
| `oc_kanban_dispatcher_stuck` | Kanban dispatcher reports "ready queue non-empty for N consecutive ticks but 0 workers spawned". Workers failing to stall — often correlated with gateway mass-restarts. | Surface with tick count. <15 ticks: monitor. >=15: investigate profile health (venv, PATH, credentials). See `references/kanban-dispatcher-stuck-pattern.md`. |

## Gateway Collision Noise

| Fingerprint | Description | Action |
|---|---|---|
| `oc_gateway_instance_collision_noise` | "Another gateway instance is already running (PID X)" errors in errors.log — very high frequency (1000+ occurrences). Caused by cron scheduler's internal gateway process checker detecting the already-running gateway. The gateway process itself is healthy. Non-fatal noise that floods the error log and can mask real issues. | Surface once in report with occurrence count. No auto-fix. If gateway PID is healthy and jobs are running, ignore. |

## Duplicate Gateway Systemd Service Crash Loop

| Fingerprint | Description | Action |
|---|---|---|
| `oc_gateway_duplicate_systemd_service` | "Another gateway instance is already running" errors at very high frequency (1000+/day) caused by a **duplicate systemd service** (`hermes-gateway.service` without `--profile`) in an auto-restart crash loop. The indigo profile gateway (`hermes-gateway-indigo.service`) runs fine. The default profile gateway has `Restart=always`, `RestartSec=5`, `StartLimitIntervalSec=0` and fails because the indigo gateway holds the PID file. **Diagnostic**: `systemctl --user status hermes-gateway.service` shows `activating (auto-restart)`. `systemctl --user status hermes-gateway-indigo.service` shows `active (running)`. The two services use different `HERMES_HOME` values (`<hermes-root>` vs `<hermes-home>`). **Fix**: `systemctl --user stop hermes-gateway.service && systemctl --user disable hermes-gateway.service`. Cannot auto-fix in cron: disabling a systemd service requires user confirmation per safety envelope. | Surface in report with occurrence count and the specific fix command. Escalate as Tier 2 — requires user confirmation to disable the duplicate service. |

## Skill Name Mismatch

| Fingerprint | Description | Action |
|---|---|---|
| `oc_cron_skill_name_mismatch` | Cron job's `skills` array references a name (e.g., `ocas-elephas`) that doesn't match any directory under `{agent_root}/skills/`. The scheduler logs "Skill 'X' not found" even though a similarly-named directory may exist (e.g., `elephas/`). Distinct from `oc_cron_dead_skill_ref` (which checks directory existence) — here the directory exists but under a different name. | Surface in report. Verify the correct skill directory name and update the job's `skills` array, or rename the directory. Requires investigation — do not auto-fix without confirming the intended name with the user. |

## Config Empty Sections

| Fingerprint | Description | Action |
|---|---|---|
| `oc_config_empty_section` | `tui_gateway.server` warnings: "config.yaml has empty section(s): `X`, `Y`. Remove the line(s) or set them to `{}` — empty sections silently drop nested settings." Caused by null-valued top-level keys (e.g. `max_concurrent_sessions: null`, `mcp: null`). Non-fatal — config still parses but warnings are noisy. **Tier 1 auto-fix**: remove null keys from config.yaml. Verify the key is truly null (not a valid empty dict/list) before removing. **⚠️ Pattern B (2026-06-18)**: Sections reappear after gateway restart. Fix applied 2x (2026-06-17, 2026-06-18). Do NOT re-apigate — escalate to Tier 3 with root cause "gateway config migration regenerates null keys from template." Fix direction: investigate config migration code, add post-startup null key strip hook. | Auto-fix: remove null keys (max 2 applications). If still recurring after 2x, escalate to Tier 3. |

## Subdirectory Hints Home Directory Resolution

| Fingerprint | Description | Action |
|---|---|---|
| `oc_subdirectory_hints_home_dir` | `RuntimeError: Could not determine home directory` in `subdirectory_hints.py` `_add_path_candidate`. Triggered when `Path(raw_path).expanduser()` fails because `$HOME` is unset in cron execution environment. Non-fatal — agent handles gracefully. **Fix (2026-06-17)**: Add `RuntimeError` to the except clause in `_add_path_candidate` — change `except (OSError, ValueError):` to `except (OSError, ValueError, RuntimeError):` on all 3 occurrences. **Patch BOTH the editable source** (`<hermes-install>/agent/subdirectory_hints.py`) **and the installed copy** (`/usr/local/lib/hermes-agent/agent/subdirectory_hints.py`), then clear stale `.pyc` caches. See `references/subdirectory-hints-home-dir-pattern.md`. | Fix: patch both copies + clear pyc. Promoted from Tier 2 surface-only to actionable fix. |

## Gateway Health Endpoint Unreachable

| Fingerprint | Description | Action |
|---|---|---|
| `oc_gateway_health_endpoint_unreachable` | Gateway process running but port 8080 health endpoint not listening. Port 9119 (pyroscope) and/or 8081 may still be bound. Systemd may report `inactive` due to `--replace` takeover. Non-fatal — scheduler and agent loop operate independently of health endpoint. **Diagnostic sequence** (5-second check): (1) `ss -tlnp` — confirm port 9119 listening, port 8080 NOT listening, (2) `ps -o pid,etime -p $(pgrep -f hermes)` — confirm process alive, (3) check jobs.json `updated_at` is recent (proving scheduler is active). If all three confirm operational + port 8080 down → cosmetic, no fix. | Surface in report. No auto-fix. Monitor — may resolve on next gateway restart. |

## Weekly Jobs Missing Runs After Gateway Downtime

| Fingerprint | Description | Action |
|---|---|---|
| `oc_cron_weekly_missed_after_downtime` | Weekly cron jobs (schedule `* * * N` where N=day of week) show last_run_at >7 days ago despite being enabled and healthy (last_status=ok, last_error=null). Usually caused by gateway downtime during the specific weekly window. The scheduler skips missed windows rather than catching up. Verify: (1) check if gateway was down during the expected run window, (2) confirm next_run_at is set for the next occurrence, (3) check if the job is otherwise healthy. Not a structural failure — no auto-fix. | Surface in report as informational. No auto-fix needed — job will run at next scheduled occurrence. Only escalate if the job misses >3 consecutive scheduled runs without a gateway downtime explanation. |

## Database Size\n\n| Fingerprint | Description | Action |\n|---|---|---|\n| `oc_state_db_oversized` | `state.db` exceeds 10GB (expected <1GB). VACUUM requires ~2x the DB size in temporary disk space. If disk >80%, VACUUM may fail — recommend message pruning instead. | Surface in report. No auto-fix. Track disk headroom before recommending VACUUM. |\n\n## Cron Interpreter Futures Shutdown\n\n| Fingerprint | Description | Action |\n|---|---|---|\n| `oc_cron_interpreter_futures_shutdown` | `RuntimeError: cannot schedule new futures after interpreter shutdown` — cron job uses `concurrent.futures` and the executor is reused across runs; the interpreter shuts down between runs. The job shows `status=error` with this message but `consecutive_failures=0`. **Self-resolving**: job succeeds on next run. **Fix if persistent**: `hermes cron pause <id>` then `hermes cron resume <id>`. Observed affecting: `thread-renamer:active`, `thread-renamer:backfill`, `finch:scan`, `gateway health monitor`, `weave:sync-google`. All showed CF=0 and status=ok after next run — confirmed transient (2026-06-17, 2026-06-19). | Surface in report with affected job count. No auto-fix for first occurrence (transient). If same job hits this 3+ times, apply pause/resume fix. |

## Vision Model Returns Invalid ChatCompletion

| Fingerprint | Description | Action |
|---|---|---|
| `oc_vision_model_invalid_response` | Vision model returns `ChatCompletion` with null `choices` (e.g., `nvidia/nemeron-nano-12b-v2-vl:free`). Distinct from `oc_vision_model_incompatible` (which is about `provider: auto`). Here the provider is correctly set but the specific free model returns malformed responses. Affects `vision_analyze` and `browser_vision` tools. Non-fatal — tools fail gracefully but user sessions lose vision capability. **First seen 2026-06-22**: 12 occurrences over 2 days, all from telegram user sessions. | Surface in report. No auto-fix — requires switching the vision model to a working one. If recurring, recommend changing `auxiliary.vision.model` in config.yaml. |

## Telegram Platform Noise

| Fingerprint | Description | Action |
|---|---|---|
| `oc_telegram_message_thread_not_found` | "Message thread not found" + "Fallback send also failed" in gateway log. Caused by Telegram bot token collision (gateway SIGTERM while old instance still holds the token). The new instance reconnects, sends to a thread the old instance created, and gets thread-not-found. Self-resolves on next message. | Surface in report if frequency >5/hour. No auto-fix. Correlates with gateway restart events. |
| `oc_telegram_send_timeout` | "Failed to send Telegram message: Timed out" — httpx network timeout to Telegram API. Non-systemic, single-occurrence. | Surface in report. No auto-fix. |
| `oc_telegram_flood_control` | "Flood control exceeded. Retry in N seconds" — Telegram rate limit on message edits. Recurring during high-volume send periods (201+ occurrences in 3 min). Self-resolves with built-in retry. | Surface in report with occurrence count. No auto-fix. Distinct from provider 429 rate limits. |

## no_agent Exit 1 No-Op Pattern

| Fingerprint | Description | Action |
|---|---|---|
| `oc_cron_no_agent_exit_1_noop` | A `no_agent: true` cron job's script exits with code 1 when there is no work to do (e.g., no new journals, no undelivered briefings, no new tasks). The script is functioning correctly — exit 1 means "no data to process." But the cron scheduler interprets any non-zero exit as an error. **Diagnostic**: check `last_error` for stdout content like "no undelivered briefings", "no new journals", or empty stderr. If the script's stdout/stderr indicates a no-op (not a real error), classify as this pattern. **Fix**: modify the script to exit 0 for no-op cases, or accept the false-positive error. Do NOT escalate. **Verified examples (2026-06-24)**: `monitor:journals` (exit 1, no new journals), `monitor:list` (exit 1, no new tasks), `dispatch:briefing-deliver` (exit 1, "no undelivered briefings"), `monitor:email` (exit 1, "no new actionable emails"), `monitor:koda-issues` (exit 1, no new), `monitor:spot` (exit 1, "no watches file"), `monitor:styx` (exit 1, "no new transactions"). All have `consecutive_failures=None/0`. | Surface in report with count. No auto-fix needed — this is a script design choice. If the false-positive noise becomes problematic, fix the script to exit 0 for no-op cases. |

## No-Agent Missing Dependency

| Fingerprint | Description | Action |
|---|---|---|
| `oc_no_agent_missing_dependency` | A `no_agent: true` cron job fails with `ModuleNotFoundError` because a Python package is missing from the hermes-agent venv (the system Python cron uses). Distinct from `oc_gateway_restart_import_window` (which affects agent-mode jobs and is transient). The package may exist in other venvs on the system. **Diagnostic**: (1) confirm `no_agent: true`, (2) identify missing module from traceback, (3) check if package exists elsewhere (`find /root -name "<module>" -type d`), (4) determine last success vs first failure from cron output files, (5) correlate with gateway restart events (mem-watchdog RSS drops), (6) trace import chain — the missing module may be imported transitively via a local helper module. **Fix**: install package into hermes-agent venv. **Tier 2** — requires package install confirmation. See `references/no-agent-missing-dependency-pattern.md`. | Surface in report with diagnostic details. Escalate as Tier 2. Do NOT auto-install. |
