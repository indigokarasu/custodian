# Escalation Runner 2026-06-15 11:08 UTC

## Session Summary

Escalation runner cron job ran at 11:08 UTC. Found 3 open `escalation_needed: true` issues in issues.jsonl. Resolved 2, downgraded 1. Zero remaining open escalations.

## Issues Reviewed

### 1. `oc_mcp_server_files_missing_20260614` — RESOLVED (stale issue)

**Claim:** 4 MCP servers (instagram, pdsx, spotify, threads) enabled but server files missing.
**Finding:** All 4 servers are `enabled: false` in config.yaml. Zero connection errors in logs. The issue was created when they were enabled; config was changed after.
**Action:** Set `status: resolved`, `escalation_needed: false`.

### 2. `oc_chronicle_plugins_empty_20260614` — RESOLVED (false positive from incomplete scan)

**Claim:** Chronicle plugin dirs at `/usr/local/lib/hermes-agent/plugins/memory/chronicle/` and `.../context_engine/chronicle/` are empty.
**Finding:** The Chronicle plugin loads from the profile path `<hermes-home>/profiles/indigo/plugins/chronicle/` (all .py files present). The system-path dirs are empty but the profile path takes precedence. The "already registered by a plugin" warnings in logs confirm successful loading.
**Action:** Set `status: resolved`, `escalation_needed: false`.

### 3. `oc_context_engine_chronicle_not_loaded_20260613` — DOWNGRADED (config choice, not failure)

**Claim:** Context engine 'chronicle' not found — falling back to built-in compressor (349 occurrences).
**Finding:** The Chronicle plugin loads fine. The warnings occur because config.yaml sets `context.engine: compressor`, not `context.engine: chronicle`. This is a user configuration choice — the plugin is installed but not selected as the active context engine.
**Action:** Set `escalation_needed: false`. Added resolution note explaining the config option.

## Key Lesson: Verify Current Config State Before Acting on Old Issues

The MCP servers issue (1) demonstrates a class of stale issue: **config.yaml changes made after an issue was filed can resolve the issue without updating issues.jsonl**. The escalation runner should:

1. For MCP-related issues: check current `enabled` status in config.yaml before concluding action is needed
2. For plugin-related issues: check BOTH profile path AND system-path before concluding files are missing
3. For context engine issues: check `context.engine` setting in config.yaml before classifying as a failure

## Pattern: Issues Can Become Stale via Config Changes

When the escalation runner finds an open issue about a specific server/plugin/context-engine:
- **Don't trust the issue description alone** — verify the current state of config.yaml and filesystem
- Config changes are the most common cause of issue staleness (someone fixed it manually, or a different cron/session resolved it)
- This is distinct from the `custodian_issues` tool stale data problem (gotcha in `escalation-runner-2026-06-08-1915.md`) — here the *underlying reality* changed, not just the tool's cache

## System State

- Gateway: running (PID 369402, uptime since 04:16)
- Cron jobs: 112/113 ok, 1 transient error (bones:paper-trade upstream timeout)
- Disk: 80% (threshold, not critical)
- state.db: 4.8GB (known Tier 2)
- Failing MCP servers: google-workspace, stealth-browser (324/323 occurrences, known Tier 2 TaskGroup errors)
