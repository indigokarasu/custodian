# Fix Safety Envelope

Every autonomous fix must satisfy all four:

1. **Non-destructive** -- no delete, overwrite, or permanent alteration
2. **Reversible** -- pre-fix state restorable without backup
3. **Minimal scope** -- smallest surface to address symptom
4. **Functionality-preserving** -- cannot reduce capability

Hard constraints: never modify skill package files, never delete files, never modify another skill's data dir, never restart gateway, never change user settings without acknowledgment.

**Exception for code-level fixes:** Known code bugs (Tier 4 → resolved) may be patched in gateway source files (`gateway/*.py`) during escalation runs. These are logged as `outcome: code_fix_applied_pending_restart` and require user to restart the gateway. The patch must be minimal, reversible, and documented in the fix record. **Scope boundary:** this exception covers `gateway/*.py` SOURCE only. Plugin code (e.g. `plugins/chronicle/engine/store.py` — the live memory engine behind `state.db`) is NOT a gateway-source file; it is OUTSIDE the autonomous envelope. Do NOT autonomously patch plugin memory-engine files, and never restart the gateway to load such a patch (hard constraint). For these, annotate the issue `user_gated=true` with `user_gated_reason`, embed the proposed minimal patch in the issue/journal for the user to apply + `hermes gateway restart`, and surface it in the report. See `references/escalation-loop-tier4-code-defect-verify.md`.

# Tier Classification

| Tier | Label | Action |
|---|---|---|
| 1 | Auto-fix | Apply immediately, register verify job, log fix record |
| 2 | Plan | Surface with proposed change, do not apply |
| 3 | Escalate | Write escalation journal with `briefing` payload, invoke Mentor plan if available |
| 4 | Alert only | Cannot fix -- surface with diagnostics (code-level fixes may apply) |

**Confidence override:** The confidence model can reclassify tiers. A fingerprint with `confidence_score >= 0.6` and `recommended_tier == 1` is treated as Tier 1 regardless of its original classification. A fingerprint with `confidence_score < 0.2` and `attempts >= 3` is auto-promoted to Tier 3.

High-recurrence override: if `recurrence_after_fix / successes > 0.5`, auto-promote next occurrence from Tier 1 to Tier 3.

# Tier 1 Auto-Fix Registry

All Tier 1 fixes defined in `references/known_issues.json`. Read at start of every scan. Pre-seeded fingerprints:

| Fingerprint | Fix |
|---|---|
| `oc_cron_disabled_transient` | Re-enable cron job |
| `oc_cron_stuck_missed` | Force-run missed job |
| `oc_cron_no_agent_mismatch` | Remove and re-create the cron job (resets scheduler internal state; see `references/cron-no-agent-mismatch.md`) |
| `oc_journal_dir_missing` | Create directory |
| `oc_skill_data_dir_missing` | Create directory + default config.json |
| `oc_jsonl_oversized` | Rotate with date suffix |
| `oc_jsonl_malformed_lines` | Quarantine to `.error` file |
| `oc_gateway_token_missing` | `platform diagnostics --generate-gateway-token` |
| `oc_oauth_token_expiring` | OAuth refresh (token still valid, expiry <= 12h) |
| `oc_background_task_missing` | Register cron or heartbeat entry per SKILL.md |
| `oc_cron_dead_skill_ref` | Remove dead skill from job's `skills` array, or delete job |
| `oc_cron_dead_script_ref` | Update script path or delete job |
| `oc_cron_duplicate_function` | Delete duplicate job (keep canonical name/earliest ID) |
| `oc_cron_orphaned_job` | Remove cron job not declared in any SKILL.md and never ran (last_status=None, last_run_at=None) |
| `oc_skill_uninitialized` | Create storage dirs, default config, empty JSONL |
| `oc_platform_missing_webhook` | Disable platform in config.yaml (`platforms.{name}.enabled: false`) |
| `oc_model_metadata_context_length` | Set `model.context_length` / `fallback_model.context_length` in config.yaml |
| `oc_cron_orphaned_job` | Remove orphaned job (not declared in any SKILL.md, never ran) |
| `oc_git_branch_no_tracking` | `git branch --set-upstream-to=origin/{branch} {branch}` in skill repo |
| `oc_mcp_alphavantage_403` | Set `mcp_servers.alphavantage.enabled: false` in config.yaml (safe, reversible — 1000+/day 403 errors) |
| `oc_http_429_concurrent` | Stagger cron schedules: offset each job's start minute so they fire sequentially instead of simultaneously. See Cron Schedule Staggering procedure in Escalation Runner section. |
| `oc_vision_model_incompatible` | Set `auxiliary.vision.provider` from `auto` to the explicit provider that hosts the vision model (e.g., `openrouter`) |
| `oc_http_401_nous_api_key` | Set `auxiliary.{task}.provider: openrouter` in config.yaml (bypass expired Nous credential) |
<<<<<<< Updated upstream
| `oc_google_oauth_refresh_400` | Google OAuth tokens are managed by the MCP server and the central `google_auth.py` helper. If tokens fail, re-authorize via `python3 <hermes-home>/skills/infrastructure/google-workspace-auth/scripts/google_oauth_init.py` — do NOT look for a `refresh_google_tokens.py` script (it does not exist). |
=======
| `oc_google_oauth_refresh_400` | Google OAuth tokens are managed by the MCP server and the central `google_auth.py` helper. If tokens fail, re-authorize via `python3 ~/.hermes/skills/infrastructure/google-workspace-auth/scripts/google_oauth_init.py` — do NOT look for a `refresh_google_tokens.py` script (it does not exist). |
>>>>>>> Stashed changes
| `oc_cron_next_run_at_none` | Pause and resume the job via `hermes cron pause <id>` then `hermes cron resume <id>` to force scheduler recalculation |
| `oc_cron_stale_empty_error` | Pause and resume the job via `hermes cron pause <id>` then `hermes cron resume <id>`. Triggered when `status=error` but `last_error` is empty/null and `consecutive_failures=0` — indicates a stale error state from a previous transient failure. |
| `oc_kanban_dispatcher_stuck` | Kanban dispatcher reports "ready queue non-empty for N consecutive ticks but 0 workers spawned". Correlated with gateway mass-restarts. Tier 2 — monitor, investigate if >=15 ticks. See `references/kanban-dispatcher-stuck-pattern.md`. |

**Confidence-promoted fixes:** Additional fingerprints may be auto-fixed based on the confidence model. These are tracked in `fix_effectiveness.jsonl` with `recommended_tier: 1` and applied during the repair pass.

# Non-Fatal Error Patterns (Tier 2 — Surface Only)

These patterns are detected during scans but are NOT auto-fixed. They are logged for awareness.

See `references/non-fatal-error-patterns.md` for the full table.

# Fix Verification

Every Tier 1 fix registers a one-shot cron job `custodian:verify:{fix_id}` with delay per fix type (2-15 min). On verification failure: set `outcome: fix_attempted_failed`. Two consecutive failures: promote to Tier 3. Fix records appended to `fixes.jsonl` with `fix_id`, `issue_id`, `command`, `reversibility`, `pre_fix_state`, `post_fix_state`, `outcome`.

**Confidence update:** After verification, update `fix_effectiveness.jsonl`:
- Success: increment `successes`, recompute `confidence_score`
- Failure: increment `failures`, recompute `confidence_score`. If `success_rate < 0.5` over 2+ attempts, set `recommended_tier: 3`.

# Post-Fix Cleanup

After successful verification, run fix-specific cleanup (check backoff, confirm next run, validate permissions). Record in `cleanup_events.jsonl`.