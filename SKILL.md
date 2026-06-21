---
name: ocas-custodian
description: 'Monitors agent gateway logs, cron jobs, skill journals, and OCAS data directories for operational failures. Detects errors, applies safe non-destructive fixes autonomously during quiet hours, and escalates only what it cannot fix. Performs root cause analysis on recurring errors with fix-loop detection and confidence-tier auto promote/demote. NOT for OKR trend analysis, skill design evaluation, behavioral lesson extraction, briefing delivery, entity knowledge queries, or social graph queries.'
license: MIT
source: https://github.com/indigokarasu/hermes-custodian-plugin
includes:
- references/**
- scripts/**
metadata:
  author: Indigo Karasu (indigokarasu)
  version: 3.0.0+hermes
tags:
- monitoring
- system-health
- log-analysis
- cron
- OCAS-core
triggers:
- system health
- log errors
- cron failures
- skill journal errors
- operational monitoring
---

# Custodian

Enforces the recovery contract defined in `spec-ocas-recovery.md` across all OCAS skills: every scheduled run must write an evidence record (including no-op runs with `not_activity_reason`), schedule gaps must trigger remedial passes, degraded mode must be explicit (not silent skip), and self-repair must include re-validation.

## Interactive Menu

When invoked interactively, present a two-level menu. See `references/interactive-menu.md` for the full menu structure.

## When to Use

- System health monitoring and alerting
- Skill library audits (conformance, freshness, coverage)
- Cron job health checks
- Log compaction and disk space monitoring
- After any major system change — verify integrity
## When NOT to Use

- Real-time monitoring (use heartbeat instead)
- Skill creation or modification (use Forge)
- Content generation or research
- User-facing task execution


## Overview

Enforces the recovery contract defined in `spec-ocas-recovery.md` across all OCAS skills.

## Critical Pitfalls

See `references/critical-pitfalls.md` before any scan. Key traps: pipe-to-interpreter blocking, jobs.json control chars, read_file corruption on data files, gateway systemd false negatives, cron inactivity timeouts, execute_code import isolation, gateway.log post-restart truncation, and regex \x escape failures in terminal().

### Tool Quirks in Cron/Scheduled Context

- **read_file dedup blocking**: Calling `read_file` on the same path multiple times triggers "BLOCKED: already read" protection. In cron/scheduled sessions, use `terminal(command="cat /path/to/file")` or `terminal(command="tail -N /path/to/file")` to re-read files.
- **write_file silent failure**: `write_file` may fail with "missing required field: path" even when `path` is provided. Reliable fallback: `terminal(command="cat > /path/to/file << 'EOF'\n...\nEOF")` with heredoc.
- **execute_code blocked in cron**: The `execute_code` tool is categorically denied during cron execution. Use `terminal()` directly for all Python operations.
- **MCP server PIDs running but connection failing**: Processes can be alive yet fail TaskGroup connection handshake. Check process liveness before escalating.
- **state.db bloat pattern**: Expected size <1GB. If >1GB, check WAL size, then VACUUM or old message pruning.

### Escalation-Runner Cron-Mode JSONL Workflow

When running `custodian.escalation-runner` as a cron job, all `issues.jsonl` and journal file mutations must use `terminal()` with heredoc — never `read_file` (corrupts JSONL) and never `execute_code` (blocked in cron). See the skill body for the reliable Python heredoc pattern.

## Responsibility Boundary

**Owns:** gateway log scanning and error fingerprinting, cron job registry health, skill journal completeness, OCAS data directory health, skill initialization, background task conformance, Tier 1 auto-repair, activity model and schedule optimization, escalation signaling, fix effectiveness tracking, confidence-based tier management, skill library hygiene (detection of stubs, nested .git, orphaned files).

**Does not own:** OKR trend analysis (Corvus, Mentor), skill design evaluation (Mentor, Forge), behavioral lesson extraction (Praxis), briefing delivery (Vesper), entity knowledge (Elephas), social graph (Weave). Never modifies any file inside a skill package directory.

## Ontology types

Custodian operates on system health data (logs, config files, journal metadata, storage usage).

## Optional Skill Cooperation

- **Vesper** -- writes InsightProposals to proposals dir; Vesper reads from there.
- **Mentor** -- journals tagged `escalation_needed: true` are readable by Mentor heartbeat.
- **Corvus** -- if installed, reads Corvus observation journals. Blended 70/30.
- **Elephas** -- journal entity observations consumed during Chronicle ingestion

## Commands

- `custodian.init` -- create storage, register background tasks, build activity model
- `custodian.scan.light` -- tail gateway log, check cron registry, retry failed fixes, check uninitialized skills
- `custodian.scan.deep` -- full sweep (see references/deep-scan.md)
- `custodian.verify {fix_id}` -- verify fix outcome
- `custodian.repair.auto` -- apply all pending Tier 1 fixes
- `custodian.repair.plan` -- generate repair plan for Tier 2/3 issues
- `custodian.issues.list` -- list open issues
- `custodian.issues.resolve {issue_id}` -- mark resolved
- `custodian.status` -- emit SkillStatus JSON
- `custodian.schedule.show` -- display scan schedule
- `custodian.escalation-runner` -- process escalated Tier 3+ issues
- `custodian.update` -- self-update from GitHub
- `custodian.confidence.show` -- display confidence scores

## Confidence Model

See `references/confidence-model.md`. Key: `confidence_score = sample_confidence × success_rate`. Auto-promotes/demotes tiers based on fix history.

## Execution Loops

**Light Scan** (every heartbeat): Read jobs.json → tail log → fingerprint → check failed fixes → check uninitialized skills → **check error jobs for script path blocks and `Path.home()` resolution issues** → **check for jobs not running (stale `last_run_at` vs expected schedule)** → **for each fingerprint with `recurrence_count >= 2`, check `rca.jsonl` — if no RCA record exists, flag for deep scan RCA step; if Pattern B, skip fix and note in journal** → **verify-before-acting: for any error job, check current config.yaml and provider state to confirm the error is still active before attempting fix** → write journal.

**Cron silence protocol:** When running as a scheduled cron job, if the scan finds no actionable issues, respond with exactly `[SILENT]`. Only produce a report when there is genuinely new information.

**Journal-before-silent requirement:** The recovery contract (see `spec-ocas-recovery.md`) requires every scheduled run to write an evidence record. Even a no-op scan with no actionable issues MUST write an observation journal (with `not_activity_reason` set) before returning `[SILENT]`. The correct sequence is: (1) write the journal → (2) return `[SILENT]`. Do NOT skip the journal on silent runs. The journal proves the scan ran; `[SILENT]` prevents unnecessary delivery noise.

**Deep Scan** (optimized 6h cron): Full 13-step sweep. See `references/deep-scan.md`.

**Deep Scan early-exit shortcut (2026-06-20):** When error jobs are all transient (cf=0/None, last_run before a recent gateway restart, no new fingerprints, no jobs with consecutive_failures >= 1), skip Steps 3b (RCA), 4 (activity model rebuild), 5 (schedule optimization), and 9 (web search). Go directly to classification and Tier 1 fix pass. Running RCA/activity-model on 100% transient errors wastes 45-60 seconds. The trigger: after initial log scan, if every error job has `consecutive_failures` in (0, None) AND all `last_error` messages match a transient pattern (futures shutdown, exit 1 no-op, gateway collision), proceed to fix pass. Do NOT skip journal writing or skill conformance checks.

**Post-fix verification**: After applying any Tier 1 auto-fix, re-check the targeted log entry or config state to confirm the error no longer appears.

**Empty plugin directory detection**: During cron scanning, check for empty plugin directories. See `references/empty-plugin-dir-detection.md`. This is a Tier 2 issue (requires investigation, not auto-fixed). See `references/chronicle-plugin-dirs-empty-pattern.md` for the specific Chronicle plugin case.

## Script Path Security Block Pattern

See `references/script-path-security-block-pattern.md` for the `oc_cron_script_path_security_block` fingerprint — a distinct sub-pattern from `oc_cron_dead_script_ref` where the script exists but the path is rejected by the security model. The fix direction depends on `HERMES_HOME`: when running under a profile, scripts must be at `<hermes-root>/profiles/<profile>/scripts/<basename>`, NOT `<hermes-root>/scripts/`.

## Google OAuth Client Deleted Pattern

See `references/google-oauth-client-deleted-pattern.md` for the `oc_google_oauth_client_deleted` fingerprint — when the OAuth client itself is deleted from Google Cloud Console (distinct from token expiry/revocation). The `deleted_client` error cannot be fixed by token restore; requires new OAuth client creation + browser re-auth.

## Fix Safety & Tier Classification

See `references/fix-safety.md` for the safety envelope, tier definitions, and the full Tier 1 auto-fix registry.

## Skill Conformance & Initialization

See `references/conformance.md` for background task checking and cron registry health checks.

## Activity Model & Schedule Optimization

Activity model rebuilt each deep scan from 14-day window. See `references/deep-scan.md` and `references/schedule-optimization.md`.

## Non-Fatal Error Patterns

See `references/non-fatal-error-patterns.md` for Tier 2 patterns detected but not auto-fixed.
See `references/no-agent-script-argument-pattern.md` for the `oc_cron_no_agent_script_args` pattern — when `no_agent: true`, the script field is a literal path and arguments embedded in it cause "Script not found" (Tier 1 fix: wrapper script pattern).
See `references/browser-cdp-502-loop-pattern.md` for the browser CDP 502 Bad Gateway loop pattern.
See `references/provider-401-diagnosis.md` for diagnosing HTTP 401 errors.
See `references/oc-hook-post-tool-call-task-id-pattern.md` for the `oc_hook_post_tool_call_task_id` pattern — post_tool_call hook task_id kwarg mismatch (plugin code bug, non-fatal but noisy).
See `references/chronicle-session-lookup-noise-pattern.md` for the `oc_context_engine_chronicle_session_lookup_noise` pattern — when the plugin loads at gateway startup but agent-session-level lookup still falls back to compressor.
See `references/skill-reference-path-mismatch-pattern.md` for the `oc_skill_reference_path_mismatch` pattern — skill agent code reads references from `<hermes-root>/commons/data/<skill>/references/` but files exist at `<hermes-root>/profiles/<profile>/skills/<skill>/references/` (Tier 2, requires skill code fix).
See `references/transient-401-self-resolution-pattern.md` for the transient 401 self-resolution pattern — a null-provider job hits a first-occurrence 401 from the default upstream, then re-runs successfully without intervention. Non-fatal, monitor only.

## Context Engine Chronicle Session Lookup Noise

See `references/chronicle-session-lookup-noise-pattern.md` for the `oc_context_engine_chronicle_session_lookup_noise` pattern — when the plugin loads at gateway startup but agent-session-level lookup still falls back to compressor. Distinguished from empty-plugin-dir case by "already registered by a plugin" messages in logs.

## Known Code Fixes & MCP Cascade

See `references/known-code-fixes-and-cascade.md` for Tier 4 known code fixes and the MCP server cascade failure triage procedure.

## Escalation Path

Tier 3: write InsightProposal to proposals dir, tag journal `escalation_needed: true`. Confidence-gated: if `confidence_score >= 0.6` and `recommended_tier == 1`, auto-fix instead of escalating.

## Journal Outputs

- **Observation Journal** -- scan-only runs
- **Action Journal** -- runs with fixes or registrations

Path: `{agent_root}/commons/journals/ocas-custodian/YYYY-MM-DD/{run_id}.json`

## Background tasks

| Job | Mechanism | Schedule | Command |
|---|---|---|---|
| `custodian:light` | heartbeat | every heartbeat cycle | `custodian.scan.light` |
| `custodian:deep` | cron | optimized 6h | `custodian.scan.deep` |
| `custodian:escalation-runner` | cron | `*/30 9-17 * * 1-5` | Process escalated issues |
| `custodian:update` | cron | `0 0 * * *` (midnight) | Self-update |

## Storage & Platform

See `references/background-tasks.md` for storage layout and `references/platform-compatibility.md` for Hermes-specific execution patterns.

## Scripts

See `references/using-script.md` for script usage and cron schedule staggering procedure.

**Self-Update**

`custodian.update` pulls from the GitHub plugin repo at `https://github.com/indigokarasu/hermes-custodian-plugin`.

**Note:** Custodian v2.0.0+ runs as a Hermes plugin (not just a skill). The plugin is installed at `~/.hermes/plugins/custodian/`. The skill (this file) is retained for backward compatibility and reference, but the active code lives in the plugin package. Self-update pulls from the plugin repo.

**IMPORTANT:** Do NOT push changes to this skill directory to any GitHub repo. The skill is a local reference copy. The canonical source is the `hermes-custodian-plugin` repo at `https://github.com/indigokarasu/hermes-custodian-plugin`. Sync changes to the plugin directory (`~/.hermes/plugins/custodian/`) via `git pull`/`git push` in that directory.

### Plugin vs Skill Architecture

The custodian **plugin** is the active operational code loaded by the Hermes gateway (provides hooks, tools, slash commands). The custodian **skill** is a reference copy for backward compatibility and documentation. Do not recreate this as a standalone skill — it is superseded by the plugin.

- **Plugin location:** `~/.hermes/plugins/custodian/` (git repo, editable install via `pip install -e`)
- **Skill location:** `~/.hermes/profiles/<profile>/skills/ocas-custodian/` (reference copy)
- **Active version:** Check plugin `__init__.py` `__version__` or `git log -1 --oneline` in plugin dir
- **Reference version:** Check skill SKILL.md frontmatter `version`

**Actual update procedure** (run in plugin directory):
```bash
cd ~/.hermes/plugins/custodian && git pull
```
The cron job `custodian:update` (schedule `0 7 * * *`) runs the skill's update command which returns a JSON note — the real update is the git pull above. The plugin's `/custodian update` slash command does the same.

See `references/plugin-vs-skill-architecture.md` for details on the editable install, version tracking, and recovery if the skill directory is deleted.

## OKRs

See `references/okrs.md`.

## Disk Compaction

See `references/disk-compaction.md` for cleanup when disk >80%.

## Gotchas

- **Never modify files inside skill package directories** — Custodian repairs operational failures but must not touch skill SKILL.md or reference files.
- **Pipe-to-interpreter blocked in cron** — Write script files to `/tmp/` instead of piping curl to python.
- **Confidence model auto-promotes/demotes tiers** — Track this in `confidence_model.json`.
- **Log compaction preserves escalation records** — Evidence logs older than 30 days (no-op) or 90 days (error/gap) are compacted to daily summaries, but escalation records are never auto-deleted.
- **Skill library hygiene requires user confirmation** — Never auto-remove skill directories or files.
- **`Path.home() / ".hermes"` breaks in cron** — Never use this pattern in scripts that run in cron/scheduled contexts. Hardcode `<hermes-root>`.
- **Script paths must match HERMES_HOME** — Cron job `script` fields must point to scripts under `$HERMES_HOME/scripts/`. When `HERMES_HOME=<hermes-home>` (set by systemd), use `<hermes-home>/scripts/`. Do NOT use `<hermes-root>/scripts/` — it will be blocked. See `references/cron-script-path-security-model.md` for the full diagnosis.
- **`issues.jsonl` field name inconsistency** — Entries use `issue_id` OR `id`. Normalize: check both.
- **Duplicate issues for same root cause** — Search existing open issues before creating new ones.
- **state.db VACUUM disk space requirement** — VACUUM on a large state.db temporarily requires ~2x the DB size in free disk space.
- **Stale error detection via script path mismatch** — When a cron job's `last_error` traceback shows a different script path than the current `script` field, the error is stale.
- **workspace-mcp entry point missing** — See `references/workspace-mcp-binary-fix.md`.
- **System Python files can shadow profile-local fixes** — See `references/known-code-fixes-and-cascade.md`.
- **`known_issues.json` nested structure issue** — Some entries are nested as sub-keys. Flatten before matching.
- **Stale failure counter vs stale error** — A job can have `consecutive_failures > 0` with `last_status=ok`. If `last_error` is null, the job is healthy. The failure counter is stale from a previous transient failure that has since resolved. No fix needed if the job is running on schedule and producing expected output.
- **Plugin directory missing `__init__.py`** — A plugin directory at `~/.hermes/plugins/<name>/` may lack an `__init__.py` at the top level even when the plugin code exists in a subdirectory (e.g., `hermes_<name>_plugin/__init__.py`). The plugin loads via `pyproject.toml` editable install, but the missing `__init__.py` generates "Failed to load plugin" warnings on every cron scheduler tick (every 60s). This is cosmetic noise — the plugin functions. Do NOT escalate as `oc_plugin_load_failed`. Classify as `oc_plugin_init_missing_noise` (Tier 2, surface only with count).
- **Escalation runner concurrent execution gap** — Check the latest esc-run journal before classifying an issue as open.
- **`custodian_issues` tool returns stale/merged view** — The `custodian_issues` tool can return issues with `escalation_needed: true` that have already been resolved by a prior escalation runner run. The tool's view is a merged/cached representation, NOT the live state of `issues.jsonl`. Always verify against the raw `issues.jsonl` file (via `terminal(command="cat ...")`) AND check the latest esc-run journal before concluding action is needed. If the latest esc-run journal shows all issues resolved or addressed, skip the full scan and return `[SILENT]`. This is the single most efficient optimization for the escalation runner: a 5-second journal check can replace a 60-second full scan.
- **Custom provider 401s are NOT transient** — Check the `base_url` before concluding "transient."
- **Null provider jobs can route to broken fallback providers** — When a cron job has `provider: null` and `model: null`, it uses the default provider from config.yaml. But if config.yaml's fallback_providers list includes a provider with an expired API key (e.g., ovhcloud with `api_key: ''`), some jobs may hit the fallback instead of the default. **This also affects jobs with explicit `provider` and `model` values** — `genie:update` and `soul:sync` both have `provider=openrouter` and `model=openrouter/owl-alpha` yet hit 403 errors referencing `kepler.ai.cloud.ovh.net`. The fallback routing can intercept auxiliary/fallback LLM calls even when the primary provider is explicitly set. Diagnosis: check `last_error` for 403 messages mentioning unexpected providers, then inspect config.yaml's `fallback_providers` list and the `ovhcloud` provider's `api_key`. Fix: either renew the broken fallback provider's credentials, remove it from `fallback_providers`, or set affected jobs' `provider`/`model` explicitly and disable auxiliary fallbacks. This pattern produced 3+ simultaneous job failures that looked like a systemic outage but were actually a single config issue. (2026-06-18) See `references/null-provider-fallback-routing-2026-06-18.md` for the full diagnosis.
- **`dispatch-email-15min` and similar null-provider jobs are vulnerable to broken fallback providers** — Jobs with `provider: null` + `model: null` (like `dispatch-email-15min`) route through the default provider and can be intercepted by broken fallback providers in config.yaml. When diagnosing 403/401 errors on null-provider jobs, always check `fallback_providers` list for entries with empty `api_key`. Removing the broken fallback entries fixes these jobs without needing to set explicit provider/model. (2026-06-18)
- **Journal gap detection** — If no observation or action journal has been written for 3+ consecutive days, flag as `oc_journal_gap` (Tier 2). Check the latest journal in `{agent_root}/commons/journals/ocas-custodian/YYYY-MM-DD/` — if the most recent file's date is >3 days ago, the cron may not be firing or the scan is completing without writing output. This is a silent failure mode: the job runs but produces no evidence record. During deep scan, always verify today's journal exists; if not, write it even if the scan finds nothing actionable. (2026-06-18)
- **Fix-loop detection** — When the same fix has been applied >= 3 times to the same fingerprint and `schedule_adjusted_stickiness < 0.5` for all attempts (fix doesn't survive one full schedule cycle per recurrence), do NOT apply the fix again. Auto-demote to Tier 3, create an RCA record with the full occurrence chain, and escalate with the root cause hypothesis. This is the single most important gotcha: Custodian must stop repeating fixes that don't hold.
- **Stale error state: status=error + consecutive_failures=None (or 0)** — A job can have `status=error` with `consecutive_failures=None` (literal null, not zero) and a `last_error` that references a provider/path that no longer exists. This indicates the scheduler never updated its internal state after the underlying issue was resolved externally (e.g., broken fallback provider removed from config.yaml by a prior escalation run). **Diagnosis**: (1) check `last_error` for provider references (e.g., `kepler.ai.cloud.ovh.net`), (2) verify current config.yaml no longer contains that provider, (3) check if the job's `next_run_at` is on schedule (proving the scheduler is running it but stuck on the old error). **Fix**: `hermes cron pause <id>` then `hermes cron resume <id>` to reset scheduler state. This is distinct from "Stale failure counter vs stale error" below — here the status itself is stale, not just the counter. **Verification before fix**: Always verify the underlying cause is already gone before resetting — if `last_error` references a provider still in config.yaml, the error is active, not stale. (2026-06-18)
- **`hermes cron edit` requires relative script paths** — The `--script` flag accepts only paths relative to `~/.hermes/scripts/`, not absolute paths. If the script only exists under the profile directory (`<hermes-root>/profiles/<profile>/scripts/`), create a symlink or copy to `~/.hermes/scripts/` first, then use just the filename.
- **`hermes cron create` syntax** — The command to register new cron jobs is `hermes cron create` (NOT `hermes cron add`). Positional args: `schedule` then `prompt`. Flags: `--name`, `--skill`, `--deliver`, `--no-agent`, `--repeat`, `--script`, `--workdir`. There is NO `--prompt`, `--schedule`, or `--model` flag — schedule and prompt are positional. Example: `hermes cron create --name "job:name" --skill ocas-skill "*/10 * * * *" "Prompt text here"`.
- **Plugin hook signatures must accept `**kwargs`** — The Hermes plugin framework may pass additional keyword arguments to hook callbacks (e.g., `task_id`). All hook functions must include `**kwargs` in their signature to remain compatible with evolving framework contracts. A hook without `**kwargs` will crash on every invocation when the framework adds new kwargs. This applies to ALL hooks: `post_tool_call`, `on_session_start`, `on_session_end`, `on_session_reset`, and any future hooks.
- **Plugin hook params before `**kwargs` must have defaults** — Even with `**kwargs`, if a parameter before `**kwargs` lacks a default value (e.g., `ctx` instead of `ctx=None`), the hook will crash when the framework doesn't pass that positional arg. All pre-`**kwargs` params must use `param=default` syntax.
- **Editable install path may differ from plugin directory** — When Hermes uses `pip install -e`, the active plugin code is at the path in the editable finder's `MAPPING` dict, NOT at `~/.hermes/plugins/<name>/`. Always verify via `importlib.util.find_spec('hermes_<name>_plugin').origin` before editing. See `references/editable-install-path-discovery.md`.
- **Elephas pipeline bridge dependency** — The `elephas_cron_pipeline.py` script connects to LadybugDB via `ladybug_client` on port 9192 (chronicle). It does NOT manage bridge lifecycle. Before running the pipeline: (1) check bridge with `curl -sf http://localhost:9192/health`, (2) if down, the most common cause is the C API library mismatch — bridge exits immediately without `LBUG_C_API_LIB_PATH`. See `references/elephas-bridge-recovery-2026-06-18.md` for the exact fix (env var, pkill, restart). The script is at `<hermes-home>/commons/db/ocas-elephas/elephas_cron_pipeline.py` (also accessible via `<hermes-root>/commons/db/ocas-elephas/elephas_cron_pipeline.py` — same inode). **Do NOT use `fuser` + `kill -9`** — use `pkill -f "ladybug_bridge"` then verify with `fuser`.
- **Elephas pipeline double-run prevention** — The `elephas_cron_pipeline.py` script has `if __name__ == "__main__": run_pipeline()` at the bottom. Never invoke it via Python `exec()` — the exec runs the entire file including the `__main__` block, causing the pipeline to execute twice in one invocation. Use `python3 elephas_cron_pipeline.py` as a subprocess, or import `run_pipeline` from the module without using `exec()`.
- **Elephas Ladybug DB corruption** — The Ladybug DB file (`chronicle.lbug`) can be silently overwritten with binary data (thermal camera raw data, sensor output). Symptoms: `real_ladybug` segfaults (exit 139), SQLite reports "file is not a database", file magic bytes match FLIR/Lepton format instead of Ladybug. **Detection**: check file size (normal ~15MB, corrupted ~13MB) and `file` command output. **Recovery**: restore from `/root/backups/chronicle.lbug`. **Workaround**: copy backup to `/tmp/`, run deep scan against copy with DB_PATH override. See `references/elephas-db-corruption-2026-06-18.md`.
- **jobs.json path under profiles** — When running under a profile, the authoritative jobs.json is at `<hermes-root>/profiles/<profile>/cron/jobs.json`, NOT `<hermes-root>/cron/jobs.json`. Both exist; the profile-scoped one is correct for profile sessions.
- **state.db >10GB** — Flag as `oc_state_db_oversized` (Tier 2). VACUUM requires ~2x the DB size in temp space. If disk >80%, recommend message pruning instead of VACUUM.
- **read_file blocked on data files in restricted sessions** — In cron/sandboxed sessions, `read_file` may be denied for JSONL files, skill SKILL.md files, and other data files with "Background review denied non-whitelisted tool". Always use `skill_view(name=...)` to read skill files and `terminal(command="cat ...")` for JSONL/data files in cron context. NOTE: config files like `jobs.json` and plain text logs are NOT affected — `read_file` works fine on those paths. The blocking is file-type-specific, not universal.
- **Python source files can be deleted while .pyc remains** — When diagnosing `ModuleNotFoundError` for a module that previously worked, check if the `.py` source was deleted while the `.pyc` in `__pycache__` still exists. This happens when cleanup scripts or manual deletion targets `.py` files but not `__pycache__`. Symlinks from profile scripts dirs to the shared scripts dir break silently. Reconstruct from bytecode (see `util-hermes-ops/references/python-source-recovery-from-pyc.md`) or restore from git.
- **hermes cron pause/resume as general reset** — The pause/resume pattern (`hermes cron pause <id>` → `hermes cron resume <id>`) is a reliable fix for ANY stale scheduler state: stuck next_run_at, stale error status, stuck failure counters, and no_agent mismatches. It forces the scheduler to recalculate internal state from jobs.json. Prefer this over direct jobs.json edits for state-related issues.
- **Cron jobs can run successfully but produce no meaningful output** — `last_status=ok` only means the job executed without errors. It does NOT verify that the job produced its expected output files or side effects. See `references/cron-output-verification-gap.md`. When diagnosing a pipeline that "should be working," check the output file's mtime — not just the job's status.
- **`cannot schedule new futures after interpreter shutdown`** — A cron job that uses `concurrent.futures` can hit this error when the executor is reused across runs and the interpreter shuts down between runs. The job shows `status=error` with this message but `consecutive_failures=0`. Fix: `hermes cron pause <id>` then `hermes cron resume <id>`. The job will succeed on next run if the underlying issue was transient interpreter state.
- **Config empty sections are a Tier 1 auto-fix**: When `config.yaml` has null-valued top-level keys (e.g., `max_concurrent_sessions: null`, `mcp: null`), the `oc_config_empty_section` pattern is a **Tier 1 auto-fix** — remove the null keys from config.yaml during the repair pass. Do NOT just surface it; actually fix it. Verified 2026-06-16: deep scan found the warning but missed applying the fix. **Also check profile-specific config** at `<hermes-root>/profiles/<profile>/config.yaml` — null keys there (e.g., `max_concurrent_sessions: null`, `context_file_max_chars: null`) generate the same TUI warnings and should be fixed with the same priority. Use `sed` or direct file edit; the `patch` tool may block config file edits. **⚠ sed multiline pitfall:** `sed -i '/pattern/{N;/other/d}'` deletes the matched line AND the next line if it matches `other`. When targeting a specific entry in a YAML list, verify what else was deleted — the sed may also remove adjacent indented entries (e.g., provider definitions) that share the matched pattern. Always run `grep` after sed to confirm the resulting file state. (2026-06-18)
- **Corvus skill directory missing**: The `ocas-corvus` cron job (`corvus:deep`, `corvus:update`) references skill `ocas-corvus` but the skill directory may not exist in the skills path. The data directories (`commons/data/ocas-corvus/`, `commons/journals/ocas-corvus/`) exist and are populated, but without a SKILL.md the skill_view tool returns "not found". This is a Tier 2 issue — the scan still runs (it's prompt-based, not skill-based) but the skill definition is missing. Fix: create the skill at `skills/infrastructure/ocas-corvus/SKILL.md` with the deep scan workflow. (2026-06-18)
- **Memory-system-design and skilllab skill directories missing**: Same pattern as ocas-corvus. `memory-system-design` is referenced by `elephas:deep`, `elephas:update`, `elephas:ingest`. `skilllab` is referenced by `10khr-grind`. All run fine prompt-based. Tier 2, requires user confirmation to create skill directories. (2026-06-18)
- **Escalation runner must deduplicate issues.jsonl before writing** — When updating `issues.jsonl`, always read all entries, deduplicate by `issue_id` (or `id`), keep the best status per entry, then write back. Without dedup, multiple escalation runner runs accumulate duplicate entries (3+ per issue) that inflate counts and cause repeated fix attempts. See `references/escalation-runner-2026-06-08-1915.md` for the correct Python dedup pattern.
- **`fix_effectiveness.jsonl` schema contamination** — Raw fix log entries (fields: `fingerprint`, `fix_id`, `target`, `outcome`, `timestamp`) can get appended to `fix_effectiveness.jsonl`, which expects confidence records (fields: `fingerprint`, `attempts`, `successes`, `failures`, …). When `ConfidenceModel._load()` reads malformed entries without the `attempts` key, `should_escalate()` crashes with `KeyError: 'attempts'`, which causes `custodian_status` to fail, which gets logged as an error — a self-inflicted crash loop. Fix: (1) validate `"attempts" in r` before storing in `_load()`, (2) use `rec.get("attempts", 0)` in `should_escalate()`, (3) clean malformed entries from the file. See `references/fix-effectiveness-schema-contamination.md`.
- **`git checkout --theirs` during stash pop conflict resolution silently drops local changes** — When `git stash pop` produces conflicts and you resolve with `git checkout --theirs`, stashed changes in that file are lost without warning. For `__init__.py`, this reverts `__version__` to the pre-pull value. Always verify version strings after conflict resolution, or resolve conflicts manually.
- **`git stash pop` does NOT auto-drop on conflicts** — After resolving conflicts from a stash pop, the stash entry persists. You must manually `git stash drop` after committing the resolved merge. — The `custodian:update` cron job can accidentally delete or overwrite the skill directory at `profiles/<profile>/skills/ocas-custodian/`. If the directory is missing entirely: (1) find the `source:` URL from cron output logs — `grep -r "source:" <profile>/cron/output/*/*.md | grep custodian`, (2) `git clone <source_url> /tmp/custodian-src`, (3) `mkdir -p <skill_dir>/references <skill_dir>/scripts && cp /tmp/custodian-src/SKILL.md <skill_dir>/ && cp /tmp/custodian-src/references/* <skill_dir>/references/ && cp /tmp/custodian-src/scripts/* <skill_dir>/scripts/`, (4) verify with `head -5 <skill_dir>/SKILL.md`. The canonical source is always in the SKILL.md frontmatter `source:` field. Apply this same recovery pattern to ANY missing OCAS skill with a `source:` URL. **Prevention:** self-update should never `rm -rf` the skill directory — only `git fetch`/`git merge` within it. Add a pre-update backup: `cp -r <skill_dir> /tmp/custodian-backup-$(date +%s)` before any git operations.
- **Plugin discovery: check BOTH profile path AND system path** — Hermes loads plugins from BOTH `<hermes-root>/profiles/<profile>/plugins/` (profile-scoped) AND `/usr/local/lib/hermes-agent/plugins/` (system-wide). When scanning for "empty plugin directories" or "plugin not loaded" warnings, ALWAYS check the profile path first. The "Context engine 'chronicle' not found" warnings (203+ occurrences) were FALSE POSITIVES — the Chronicle plugin was loading from `<hermes-home>/plugins/chronicle/` (complete with all `.py files`), but the scan only checked `/usr/local/lib/hermes-agent/plugins/memory/chronicle/` and `/usr/local/lib/hermes-agent/plugins/context_engine/chronicle/` (which were empty). The "already registered by a plugin" warnings CONFIRM the plugin IS loading from the profile path. Fix: update scan logic to check profile plugin path before concluding a plugin is missing.
- **Config changes can resolve issues without updating issues.jsonl** — A user or another process can change config.yaml (e.g., setting `enabled: false` for an MCP server) or install a plugin to the profile path, resolving the underlying problem without updating issues.jsonl. The escalation runner then finds stale open issues that are already resolved. Before acting on any open issue about a specific server/plugin/config setting: (1) check the current config.yaml for the relevant `enabled` flag, (2) check BOTH profile and system plugin paths for file existence, (3) check `context.engine` setting before classifying context-engine-missing as a failure. Verify current state independent of the issue description. See `references/escalation-runner-2026-06-15-1108.md` for examples.
- **Checkpoint store git corruption (missing refs/heads + objects/)** — When `checkpoints/store/.git` exists but is missing standard git directories (`refs/heads/`, `objects/`), checkpoint_manager logs errors on every write. The corruption can happen if the store directory is moved, if git is killed mid-init, or if a cleanup script removes git internals. **Fix:** Back up `.git` to `.git.bak`, `rm -rf .git`, `git init` in the store directory. Preserve `HEAD`, `config`, `indexes/`, `packed-refs`, and `projects/` contents — the reinit creates fresh git structure. The checkpoint store will rebuild its refs on next write. (2026-06-18)
- **Escalation runner must check ALL issues.jsonl paths** — Issues accumulate in 5+ different `issues.jsonl` files across the filesystem. The same root cause often appears in multiple files with different `issue_id` values. Before any escalation run, use `find <hermes-root> -name "issues.jsonl"` to discover all paths, then deduplicate by description/summary. See `references/escalation-runner-multi-path-issues.md`.
- **issues.jsonl can contain multiple JSON objects per line** — Entries are sometimes concatenated on a single line (not newline-separated). Naive `json.loads(line)` fails with `JSONDecodeError: Extra data`. Use a brace-depth parser that walks the line character by character. See `references/escalation-runner-multi-path-issues.md` for the parser code.
- **Always clear `escalation_needed` when resolving issues** — When closing stale issues, set `escalation_needed: false` in the same pass. A systematic bug leaves `escalation_needed: true` on resolved entries, causing false-positive escalation on every subsequent run. After any batch close, sweep all files for `status: resolved` + `escalation_needed: true` and clear the flag.
- **Corvus proposals can be stale** — Corvus writes InsightProposals to `<hermes-root>/proposals/` and `<hermes-home>/commons/data/ocas-corvus/proposals/`. These can flag issues that custodian has already resolved (e.g., MCP servers already disabled in config.yaml). Before acting on any Corvus proposal: (1) verify current live state independently, (2) check the latest custodian scan journal, (3) proposals older than 24h with no matching open issue in `issues.jsonl` are likely stale. See `references/escalation-runner-multi-path-issues.md`.
- **System agent files: patch BOTH editable source AND installed copy** — When hermes-agent is installed in editable mode, Python imports resolve to the source checkout (`<hermes-root>-agent/agent/`), but a separate installed copy exists at `/usr/local/lib/hermes-agent/agent/`. Both may be loaded depending on the import path. When patching system agent files (e.g., `subdirectory_hints.py`), always patch BOTH locations and clear stale `.pyc` caches. Verify which path is actually loaded via `importlib.util.find_spec('agent.<module>').origin`. See `references/subdirectory-hints-home-dir-pattern.md` for a worked example.
- **no_agent script field is a literal path, not a command line** — When `no_agent: true`, the `script` field is treated as a literal file path. Arguments like `foo.py --flag` will fail with "Script not found" because the entire string is resolved as a path. Fix: create a wrapper script that bakes in the arguments, symlink to `~/.hermes/scripts/`, and point the cron job at the wrapper. See `references/no-agent-script-argument-pattern.md`. (2026-06-20)
- **no_agent scripts exiting 1 for no-op are false-positive errors** — Monitor scripts (`monitor:journals`, `monitor:list`, `dispatch:briefing-deliver`) that exit 1 when there's no work to do will be flagged as errors by cron. This is by design in the scripts but conflicts with cron's error detection. Classify as `oc_cron_no_agent_exit_1_noop` (Tier 2, surface only). Do NOT escalate. Fix the scripts to exit 0 for no-op cases if the noise becomes problematic. (2026-06-20)
- **`fallback_model` with broken credentials affects ALL null-provider jobs** — The `fallback_model` top-level key in profile config (`<hermes-root>/profiles/<profile>/config.yaml`) can contain a custom provider with expired/invalid API key. When a job with `provider: null` falls through to the fallback model, it hits the broken provider. This is especially dangerous because `custodian:light` (the primary detection mechanism) has `provider: null` — if it can't run due to fallback_model 401, issues go undetected. Symptoms: `custodian:light` fails with `RuntimeError: HTTP 401: Authentication failed with upstream provider` mentioning `provider=custom` and a custom base_url. Diagnosis: check `config.yaml` for `fallback_model` section with a custom provider. Fix: update the API key, change to a working provider, or remove the `fallback_model` entry. See `references/null-provider-fallback-routing-2026-06-18.md` for the full pattern and the 2026-06-20 manifest.build variant. (2026-06-20)
## Support File Map

| File | When to read |
|------|-------------|
| `references/cron-output-verification-gap.md` | When a cron job shows `last_status=ok` but the expected output file hasn't been updated. Or when diagnosing any pipeline where "the job runs but nothing changes." |
| `references/script-path-security-block-pattern.md` | During cron script scanning — when a job's script field points outside `$HERMES_HOME/scripts/`. Fix direction depends on `HERMES_HOME` value. |
| `references/google-oauth-client-deleted-pattern.md` | When diagnosing Google OAuth errors — if `deleted_client` error appears, the OAuth client was deleted from GCS |
| `references/cron-mass-fastforward-after-gateway-downtime.md` | During cron scanning — when MANY jobs show overdue next_run_at simultaneously |
| `references/cron-timeout-first-occurrence-pattern.md` | When a cron job shows `status=error` with `consecutive_failures=null/0` and `last_error` present — first occurrence, likely transient |
| `references/cron-script-path-home-pattern.md` | During cron script scanning — detecting and fixing `Path.home() / ".hermes"` in scripts |
| `references/cron-script-environment-pitfalls.md` | When cron job scripts fail with import errors or path blocks |
| `references/spec-ocas-recovery.md` | When implementing or auditing the recovery contract |
| `references/okrs.md` | When reviewing skill performance against targets |
| `references/transient-provider-errors.md` | When a cron job fails with provider errors |
| `references/browser-cdp-502-loop-pattern.md` | During log scanning — CDP supervisor 502 loop classification |
| `references/runtime-error-triage.md` | When a cron job fails with generic RuntimeError |
| `references/divergent-branch-handling.md` | During self-update — handling topic branches |
| `references/system-maintenance.md` | During disk cleanup — storage monitoring patterns |
| `references/api_endpoints.md` | During API key audits — service test endpoints |
| `references/script-library-organization.md` | When organizing or relocating scripts |
| `references/self-improvement.md` | When reviewing fix effectiveness |
| `references/elephas-ingest-timeout-diagnostic.md` | When elephas:ingest times out |
| `references/elephas-bridge-recovery-2026-06-18.md` | When the Chronicle bridge (port 9192) fails to start due to C API library mismatch — env var fix, pkill, restart procedure |
| `references/elephas-db-corruption-2026-06-18.md` | When the Ladybug DB file is corrupted (overwritten with binary sensor data) — detection, recovery, workaround |
| `references/elephas-pipeline-architecture-2026-06-18.md` | When running or diagnosing the elephas pipeline — two implementations, which to use, malformed journal patterns |
| `references/escalation-runner-2026-06-01.md` | Escalation runner 2026-06-01 reference |
| `references/escalation-runner-2026-06-01-2113.md` | Escalation runner 2026-06-01 21:13 reference |
| `references/escalation-runner-2026-06-01-2206.md` | Escalation runner 2026-06-01 22:06 reference |
| `references/escalation-runner-2026-06-03.md` | Escalation runner 2026-06-03 reference |
| `references/escalation-runner-2026-06-03-2040.md` | Escalation runner 2026-06-03 20:40 reference |
| `references/escalation-runner-2026-06-03-1604.md` | Escalation runner 2026-06-03 16:04 reference |
| `references/terminal-cwd-not-found-pattern.md` | During log scanning — `os.getcwd()` on deleted CWD |
| `references/known-script-auth-issues.md` | When a cron job script fails with import errors, path blocks, or auth failures |
| `references/known-code-fixes-and-cascade.md` | During escalation runs — applying known code patches or triaging MCP cascade failures |
| `references/critical-pitfalls.md` | Before any scan — commonly-hit traps |
| `references/known_issues.json` | At start of every scan — check for known unresolved issues |
| `references/provider-401-diagnosis.md` | When diagnosing HTTP 401 errors |
| `references/null-provider-fallback-routing-2026-06-18.md` | When diagnosing 403 errors from unexpected providers, or when jobs with explicit provider settings still route to broken fallback providers. Contains diagnosis steps, fix attempts, and verified sed fix with pitfall warning. |
| `references/confidence-model.md` | When classifying or escalating issues |
| `references/fix-safety.md` | Before applying any fix — check safety envelope and tier definitions |
| `references/root-cause-analysis.md` | During Step 3b — before applying any fix to a recurring fingerprint |
| `references/rca-schema.md` | When creating or updating RCA records |
| `references/rca-backfill-2026-06-05.md` | Historical analysis of recurring issues from the last 3 weeks |
| `references/deep-scan.md` | Before running `custodian.scan.deep` |
| `references/conformance.md` | During skill conformance checks |
| `references/non-fatal-error-patterns.md` | During issue classification — identifying Tier 2 patterns |
| `references/schedule-optimization.md` | During schedule optimization |
| `references/web-search-protocol.md` | During web search pass |
| `references/background-tasks.md` | When setting up storage or registering jobs |
| `references/rally-health-check.md` | Rally-specific health checks: cash drift, stale pending actions, research staleness |
| `references/platform-compatibility.md` | Before running scans on a new platform |
| `references/using-script.md` | When running scripts |
| `references/plugin-self-update-2026-06-18.md` | During `custodian.update` — plugin directory update procedure, stash pop conflict resolution, known local patches |
| `references/self-update.md` | Before running `custodian.update` |
| `references/config-recovery.md` | When config corruption is detected |
| `references/light-scan-2026-05-20.md` | During light scan review — diagnostic pattern for stale `last_status: error` |
| `references/light-scan-2026-06-04-2114.md` | Light scan 2026-06-04 21:14 reference |
| `references/light-scan-2026-06-04-1609.md` | Light scan 2026-06-04 16:09 reference |
| `references/deep-scan-2026-05-30-2100.md` | Deep scan 2026-05-30 reference |
| `references/deep-scan-2026-06-04.md` | Deep scan 2026-06-04 reference |
| `references/deep-scan-2026-06-14-0215.md` | Deep scan 2026-06-14 02:15 — email:draft fix, plugin __init__.py noise pattern, elephas stale counter |
| `references/deep-scan-clean-verdict-pattern.md` | When every error job has cf=None, skip to silent verdict in ~30s — transient error shortcut |
| `references/light-scan-2026-06-14-1500.md` | Light scan 2026-06-14 15:00 — bones:paper-trade upstream timeout (first occurrence, transient), stale counters on elephas:ingest + weave-enrichment-health-check |
| `references/jobs-not-running-diagnostic.md` | During cron scanning — when MANY jobs show overdue `next_run_at` simultaneously |
| `references/escalation-runner-2026-06-08-1915.md` | Escalation runner 2026-06-08 19:15 — JSONL deduplication pattern, state.db batch pruning results, spot:update git stash fix |
| `references/escalation-runner-2026-06-04-1807.md` | Escalation runner 2026-06-04 18:07 reference |
| `references/escalation-runner-concurrent-execution-gap.md` | Before classifying any issue as open/unresolved |
| `references/workspace-mcp-binary-fix.md` | When workspace-mcp-fixed fails with "No such file or directory" |
| `references/checkpoint-store-git-corruption-pattern.md` | When checkpoint_manager logs git errors — missing refs/heads/ and objects/ in checkpoints/store/.git. Fix: backup .git, rm -rf, git init. |
| `references/chronicle-plugin-dirs-empty-pattern.md` | During plugin directory scanning — when `plugins/memory/chronicle/` or `plugins/context_engine/chronicle/` have no `.py` files (only `__pycache__`). Distinct from `oc_chronicle_kwargs_get_duplicate` (code bug in existing files). |
| `references/interactive-menu.md` | When invoked interactively via `/` command — two-level menu layout, response parsing, platform adaptation |
| `references/empty-plugin-dir-detection.md` | During cron scanning — detecting empty plugin directories that silently break discovery |
| `references/fix-effectiveness-schema-contamination.md` | When `custodian_status` crashes with `KeyError: 'attempts'` — fix_effectiveness.jsonl has mixed-in raw fix log entries lacking the confidence record schema |
| `references/escalation-runner-2026-06-08-1915.md` | Escalation runner 2026-06-08 19:15 — JSONL deduplication pattern, state.db batch pruning results, spot:update git stash fix |
| `references/escalation-runner-2026-06-08-1915.md` | Escalation runner 2026-06-08 19:15 — custodian_issues tool stale data lesson, workflow optimization: check esc-run journal first |
| `references/light-scan-2026-06-08-1805.md` | Light scan 2026-06-08 18:05 — first occurrence of `oc_hook_post_tool_call_task_id` (1363 hits), `oc_cron_429_transient` (6 jobs), stale error state on soul:sync, stale failure counters on elephas:ingest + weave-enrichment-health-check |
| `references/escalation-runner-2026-06-08-2315.md` | Escalation runner 2026-06-08 23:15 — state.db VACUUM success (reclaimed 6.96 GB), FTS theory corrected, CWD/read_file quirks |
| `references/credential-leak-backup-commit-pattern.md` | When a backup cron job commits credential files (.env, auth.json, nous-auth.json) to git — detection via GitGuardian, fix via history rewrite + .gitignore |
| `references/oc-hook-post-tool-call-task-id-pattern.md` | During log scanning — post_tool_call hook task_id kwarg mismatch (plugin code bug, non-fatal but noisy) |
| `references/escalation-runner-2026-06-15-1123.md` | Escalation runner 2026-06-15 11:23 — multi-path issue discovery, 6 issues resolved, 2 demoted |
| `references/escalation-runner-multi-path-issues.md` | Before any escalation run — discover ALL issues.jsonl paths, JSONL multi-object parsing, deduplicate across files, stale issue heuristics, and escalation_needed flag cleanup |
| `references/escalation-runner-journal-path-and-schema.md` | Journal directory structure (date-based subdirectories), all_clear silent-run schema, "check journal first" optimization, issues.jsonl path hierarchy |
| `references/escalation-runner-2026-06-15-1108.md` | Escalation runner 2026-06-15 11:08 — stale issues from config changes, verify current state before acting |
| `references/escalation-runner-2026-06-15-2100.md` | Escalation runner 2026-06-15 21:00 — Corvus proposal staleness, email:check resolved, transient broken pipe pattern |
| `references/chronicle-session-lookup-noise-pattern.md` | Chronicle context engine session lookup noise pattern — plugin loads at gateway but agent-session lookup falls back to compressor |
| `references/editable-install-path-discovery.md` | When editing plugin files that don't seem to take effect — finding the actual loaded path via importlib |
| `references/plugin-vs-skill-architecture.md` | When confused about plugin vs skill versions, or when skill directory is missing — update procedure, editable install, recovery |
| `references/skill-reference-path-mismatch-pattern.md` | When a skill's agent code reads reference files from `<hermes-root>/commons/data/<skill>/references/` but they exist at `<hermes-root>/profiles/<profile>/skills/<skill>/references/` — Tier 2, requires skill code fix |
| `references/transient-401-self-resolution-pattern.md` | When a null-provider job hits a first-occurrence 401 from the default upstream, then re-runs successfully without intervention. Non-fatal, monitor only. |
| `references/subdirectory-hints-home-dir-pattern.md` | When `subdirectory_hints.py` `_add_path_candidate` fails with `RuntimeError: Could not determine home directory` because `$HOME` is unset in cron execution environment — Tier 2, framework bug. **Fix applied 2026-06-17**: added `RuntimeError` to except clause. |
| `references/escalation-runner-2026-06-17.md` | Escalation runner run 2026-06-17: subdirectory_hints fix, 3 resolved issues, verify-before-acting lesson |
| `references/no-agent-script-argument-pattern.md` | When `no_agent: true`, the `script` field is treated as a literal path — arguments embedded in the filename cause "Script not found". Fix: wrapper script pattern. |
| `references/stale-error-state-pause-resume-fix.md` | When a job shows `status=error` with `consecutive_failures=None/0` and `last_error` referencing a provider/path that no longer exists. Diagnosis steps, verify-before-acting procedure, and pause/resume fix pattern. |
| `references/post-fix-stale-error-pattern.md` | After applying a Tier 1 fix — when affected jobs still show `last_error` from their pre-fix run but `consecutive_failures=0`. How to classify as stale vs active. |
| `references/escalation-runner-2026-06-18-0925.md` | Escalation runner 2026-06-18 09:25 — OVH/LLM7 provider 403 fix (removed broken providers + fallback entries), checkpoint store git reinit (missing refs/heads + objects), dispatch-email-15min newly identified as OVH-affected |
| `references/fix-applied-pending-restart-sweep.md` | During deep scan Step 9b — verify and close issues stuck in `fix_applied_pending_restart` for >7 days. MCP ModuleNotFoundError verification pattern. |
| `references/deep-scan-2026-06-17-1415.md` | Deep scan 2026-06-17 14:15 — fix_applied_pending_restart limbo pattern, nodriver verification, 0 orphans |
| `references/light-scan-2026-06-18-1130.md` | Light scan 2026-06-18 11:30 — stale OVH error pause/resume fix for genie:update + soul:sync, stale counters confirmed non-fatal, memory-system-design skill path mismatch |
| `references/light-scan-2026-06-17-2000.md` | Light scan 2026-06-17 20:00 — checkpoint_store git self-resolution pattern (0 errors, .git recreated but empty, known non-fatal), stale counters confirmed non-fatal |