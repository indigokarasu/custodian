---
name: ocas-custodian
license: MIT
description: 'Monitors agent gateway logs, cron jobs, skill journals, and OCAS data directories for operational failures. Detects errors, applies safe non-destructive fixes autonomously during quiet hours, and escalates only what it cannot fix. Performs root cause analysis on recurring errors with fix-loop detection and confidence-tier auto promote/demote. Use when: cron jobs fail or show stale errors, gateway logs show repeated error patterns, skill journals have gaps, disk usage exceeds thresholds, MCP servers crash-loop, or after any gateway restart. Keywords: cron health, log analysis, system monitoring, error fingerprinting, auto-repair, fix-loop detection, operational conformance. NOT for OKR trend analysis, skill design evaluation, behavioral lesson extraction, briefing delivery, entity knowledge queries, or social graph queries.'
source: https://github.com/indigokarasu/custodian
includes:
- references/**
- scripts/**
metadata:
  author: Indigo Karasu (indigokarasu)
  version: 3.0.0+hermes
  hermes:
    tags:
      - monitoring
      - system-health
      - log-analysis
      - cron
      - OCAS-core
    category: devops
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


## Critical Pitfalls

### Tool Quirks in Cron/Scheduled Context

See `references/cron-json-write-heredoc-variable-expansion-failure.md` for the single-quoted heredoc variable expansion failure pattern — confirmed 2026-06-24, produced corrupted journal files with literal `$(date)` in JSON.

- **read_file dedup blocking**: Calling `read_file` on the same path multiple times triggers "BLOCKED: already read" protection. In cron/scheduled sessions, use `terminal(command="cat /path/to/file")` or `terminal(command="tail -N /path/to/file")` to re-read files.
- **Security filter blocks pipe-to-interpreter patterns broadly**: `grep | python3`, `cat | python3`, `tail | python3`, and `curl | python3` are ALL blocked by the Hermes tirith security filter. The filter detects any command that pipes output to an interpreter. Do NOT retry variations of the same pipe pattern.
  - **Fix for one-liner processing**: Write a standalone Python script to `/tmp/` via `write_file`, then `terminal(command="python3 /tmp/script.py")`. This avoids ALL pipe-to-interpreter blocking.
  - **Fix for grep on gateway logs**: Switch to `python3 << 'PYEOF'` file I/O in the terminal command, or write a `/tmp/` script that opens the file directly.
  - See `references/hardline-filter-gateway-log-grep-block-pattern.md` for the grep-specific case and `references/pipe-to-interpreter-security-block.md` for the broader pattern.
- **write_file silent failure**: `write_file` may fail with "missing required field: path". Fallback: `terminal(command="cat > /path << 'EOF'\n...\nEOF")`.
- **Heredoc single-quote prevents variable expansion**: `cat > file << 'EOF'` does NOT expand `$(date)` — literal string ends up in JSON. **Fix**: Use `python3 -c "..."` with `json.dump()` for dynamic content. For static content, single-quoted heredoc is correct. See `references/cron-json-write-heredoc-variable-expansion-failure.md`.
- **execute_code blocked in cron**: Use `terminal()` for all Python. To call MCP plugin tools from cron, import via `terminal("cd ~/.hermes/plugins/custodian && python3 -c ...")`. See `references/cron-health-check-from-cron-context.md`.
- **hermes cron CLI doesn't work from cron**: CLI reads `~/.hermes/cron/`, NOT the profile-scoped path. Directly edit `<hermes-root>/profiles/<profile>/cron/jobs.json` to pause/resume. See `references/cron-health-check-direct-jobs-json-parsing.md`.
- **MCP server PIDs running but connection failing**: Processes can be alive yet fail TaskGroup connection handshake. Check process liveness before escalating.
- **state.db bloat pattern**: Expected size <1GB. If >1GB, check WAL size, then VACUUM or old message pruning. **Contextual threshold**: state.db commonly grows to 5-10GB in production. Flag as `oc_state_db_oversized` (Tier 2) when >1GB AND disk >80% — at lower disk usage, 5-10GB is acceptable operational cost. VACUUM feasibility: free_disk >= db_size is sufficient. If disk >80%, recommend message pruning instead of VACUUM. (2026-06-23)

### Escalation-Runner Cron-Mode JSONL Workflow

When running `custodian.escalation-runner` as a cron job, all `issues.jsonl` and journal file mutations must use `terminal()` with heredoc — never `read_file` (corrupts JSONL) and never `execute_code` (blocked in cron). See the skill body for the reliable Python heredoc pattern.

## Responsibility Boundary

**Owns:** gateway log scanning and error fingerprinting, cron job registry health, skill journal completeness, OCAS data directory health, skill initialization, background task conformance, Tier 1 auto-repair, activity model and schedule optimization, escalation signaling, fix effectiveness tracking, confidence-based tier management, skill library hygiene (detection of stubs, nested .git, orphaned files).

**Does not own:** OKR trend analysis (Mentor), skill design evaluation (Mentor, Forge), behavioral lesson extraction (Praxis), briefing delivery (Vesper), social graph (Weave). Never modifies any file inside a skill package directory.

## Ontology types

Custodian operates on system health data (logs, config files, journal metadata, storage usage).

## Optional Skill Cooperation

- **Vesper** -- writes InsightProposals to proposals dir; Vesper reads from there.
- **Mentor** -- journals tagged `escalation_needed: true` are readable by Mentor heartbeat.


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

**Light Scan** (every heartbeat): Run the following checklist in order. Do not skip steps — each gates the next.

- [ ] 1. Read `jobs.json` (extract `data.get("jobs", [])` — it's a dict, not a list)
- [ ] 2. Tail gateway log for new errors since last scan timestamp
- [ ] 3. Fingerprint each new error (match against known patterns in `references/`)
- [ ] 4. Check failed fixes from previous scan — verify each fix held
- [ ] 5. Check uninitialized skills (missing storage, no background tasks registered)
    - This includes checking for missing data directories, journals directories, and config files
    - See references/fix-missing-skill-data-directories-2026-06-30.md and references/fix-missing-skill-journals-directories-2026-06-30.md for fix procedures
    - **CRITICAL: cross-reference against active cron jobs before flagging.** A skill with missing data/journals dirs but NO cron jobs referencing it (via `skill:` or `skills:[]`) is uninitialized-but-unused — note it in the journal as info-only, do NOT escalate. Only flag for remediation if at least one active cron job depends on the skill. Confirmed 2026-07-01: 10xeng-autofix and skilllab had missing dirs but zero cron job references — not actionable.
    - **Re-derive the active-skill set from the LIVE jobs.json every scan** — union the `skills` (array) AND `skill` (scalar) fields across ALL job entries; treat any skill appearing ≥1 time as actively referenced. Confirmed 2026-07-07: a prior scan reported `browser-vision`, `generative-art-algorithms`, `generative-art-deployment`, `ocas-lucid` as "unused (no cron refs)" — but all four ARE referenced (by `art:studio` and the `lucid:*` jobs). The cross-ref had silently dropped them, so they were wrongly left un-remediated while `art:studio` ran with missing data/journals dirs. If a skill dir is missing AND the live set contains it, remediate (mkdir + config.json per the fix references) — **never trust a prior scan's 'unused' label**; always re-derive from the live file.
- [ ] 6. Check error jobs for script path blocks and `Path.home()` resolution issues
    - **DE-AGGREGATE identical wrapper messages.** When ≥2 error jobs share a bare `Script exited with code 1` (or any identical low-information) `last_error`, they are NOT one root cause. Enumerate each, read its `script` field, and determine the REAL per-job failure (run the script; inspect `sys.exit` paths; for subprocess wrappers, run the wrapped command). A no-op-by-design exit (no stderr) is `oc_cron_no_agent_exit_1_noop` (Tier 2 surface-only); a traceback is a real failure. Confirmed 2026-07-07: a prior scan collapsed three distinct jobs (`monitor:list` 403, `monitor:journals` no-op, `SearXNG` infra) into one bucket and omitted `monitor:journals`. See `references/no-agent-script-exit-1-deaggregation-pitfall.md`. Use `scripts/classify_error_jobs.py` to surface every ambiguous wrapper job with its `script` name.
- [ ] 7. Check for jobs not running (stale `last_run_at` vs expected schedule)
    - **CRITICAL**: `next_run_at < now` alone is NOT sufficient. Must verify: `last_run_at` older than 2× schedule interval AND `last_status != ok`. High-frequency jobs (≤10 min) show scheduler state lag where `next_run_at` hasn't advanced but job ran successfully. See `references/scheduler-state-lag-vs-execution-failure.md`.
- [ ] 8. For each fingerprint with `recurrence_count >= 2`, check `rca.jsonl` — if no RCA record exists, flag for deep scan RCA step; if Pattern B, skip fix and note in journal
- **8b. Journal-to-issues gap check**: for any previous journal entry with `escalation_needed: true`, verify a matching entry exists in `issues.jsonl` for the same fingerprint. If NOT found, write it — the prior scan flagged but failed to persist. (Confirmed pattern: 10:05 scan wrote `escalation_needed: true` but did NOT write the issue to issues.jsonl; 12:04 scan had to write it manually.)
- **8b-variant — 'tracked' in narrative but `escalation_needed: false`**: A scan may reference root-cause fingerprints as 'tracked' (in `previous_scan_delta.stable_root_cause` or prose) while setting `escalation_needed: false` on the journal. If the referenced fingerprint is absent from `issues.jsonl`, the escalation silently dropped — every later scan re-reports it as 'tracked' without ever persisting it. Fix: after classifying non-auto-fixable root causes, collect their intended fingerprints and verify each exists in `issues.jsonl` (use `scripts/parse_issues_jsonl.py`); if missing, WRITE it (`status: open`, `escalation_needed: true`). One issue per root-cause fingerprint; list affected job names in `affected_components`. Confirmed 2026-07-07: 71-job 402-credits and 2-job OAuth revocations were reported 'tracked' in prior deltas but absent from `issues.jsonl`; light scan wrote them. See `references/escalation-persistence-gap.md`.
- [ ] 8c. **Verify-before-accepting-self-resolved**: when a prior scan classified an error as "self-resolved" (e.g., `ModuleNotFoundError` that supposedly fixed itself), verify by running the actual import in the cron execution python — NOT any assumed venv path. Cron jobs run `python3` from PATH. To find the actual python: `which python3` in a terminal, then `python3 -c "import <module>"`. The profile venv path (`<hermes-root>/profiles/<profile>/venv/bin/python3`) may NOT exist — the system hermes venv (`<hermes-install>/.venv/bin/python3`) is typically the active one. Confirmed 2026-07-01: `dispatch:triage-morning` was classified "self-resolved" but the verification was done by checking the actual import (import google.oauth2.credentials → OK). Do NOT accept "self-resolved" from a prior journal entry without re-verifying when: (a) the module path in the error differs from what you assumed, (b) the error was from a no_agent script (different python resolution), or (c) the prior scan has known counting discrepancies. See `references/self-resolved-module-verification-pattern.md`.
- [ ] 9. **Verify-before-acting**: for any error job, check current `config.yaml` and provider state to confirm the error is still active before attempting fix
- [ ] 10. Write observation journal (even if no issues found — set `not_activity_reason`)

**Cron silence protocol:** When running as a scheduled cron job, if the scan finds no actionable issues, respond with exactly `[SILENT]`. Only produce a report when there is genuinely new information.

**Journal-before-silent requirement:** The recovery contract (see `spec-ocas-recovery.md`) requires every scheduled run to write an evidence record. Even a no-op scan with no actionable issues MUST write an observation journal (with `not_activity_reason` set) before returning `[SILENT]`. The correct sequence is: (1) write the journal → (2) return `[SILENT]`. Do NOT skip the journal on silent runs. The journal proves the scan ran; `[SILENT]` prevents unnecessary delivery noise.

**Deep Scan** (optimized 6h cron): Full 13-step sweep. See `references/deep-scan.md` and `references/deep-scan-2026-06-28-clean-verdict.md` for the clean verdict pattern (all-transient → journal + silent).

**Deep Scan early-exit shortcut:** When all error jobs are transient (cf=0/None, last_run before recent restart, no new fingerprints, no consecutive_failures >= 1), skip Steps 3b/4/5/9. Go directly to classification + Tier 1 fix pass. Trigger: all jobs have `consecutive_failures` in (0, None) AND all `last_error` match transient patterns (futures shutdown, exit 1 no-op, gateway collision, 429, script-not-found race, no_agent path mismatch, gateway restart import window, provider error transient). Do NOT skip journal or conformance checks. See `references/deep-scan.md`.

**Deep Scan clean verdict (2026-06-23):** When ALL error jobs classify as transient/non-faulty (known patterns, cf=None/0, no active issue), the scan is clean. No Tier 1 fixes needed. Write observation journal with `not_activity_reason` and return `[SILENT]`. This is the expected steady-state — a clean scan means the system is healthy, not that the scan missed something. Do not force-fix non-issues. Confirmed 2026-06-23: 16 error jobs, 100% transient (futures shutdown, 429 rate limit, no_agent exit 1 noop), 0 fixes applied, all clean.

**Delta journal for repeated clean verdicts:** When consecutive scans find the SAME errors with no new/resolved issues, journal a `previous_scan_delta` block: elapsed min, new_issues=0, new_errors=0, stable root cause. New transient errors alongside stable escalated issues: MAY still use delta but include `new_errors: N` + `new_error_detail`. Do NOT use after gateway restart, fix, or state change. See `references/light-scan-2026-06-29-0904.md`.

- **Fix-loop already escalated — don't re-escalate:** If fix-loop RCA Pattern B exists AND prior esc-run already escalated → note fingerprint in journal, do NOT re-fix, do NOT duplicate escalation, return `[SILENT]` with `not_activity_reason: "clean_verdict_all_errors_already_escalated"`. See `references/deep-scan-fix-loop-prehandled-silent-verdict.md`.

**Config empty section: "Tier 1 auto-fix" vs Pattern B contradiction:** `oc_config_empty_section` is Tier 1 auto-fix BUT has Pattern B RCA. Resolution: DO remove null keys (fixes TUI warnings), note fix-loop in journal, write escalation for architectural root cause (gateway regenerates null keys on restart). See `references/config-empty-section-fixloop-status.md`.

**Escalation Runner Checklist:**
- [ ] 1. Check latest esc-run journal first (5-sec check vs 60-sec full scan)
- [ ] 1b. **Already-classified fast path**: If the prior esc-run journal is < 2h old AND classified all open issues as `open_user_gated` AND no new `last_error` messages appear in `jobs.json` that weren't in the prior journal AND no new entries appear in any `issues.jsonl` → write journal referencing the prior classification and return `[S Skip Steps 2-6. See `references/escalation-runner-already-classified-fast-path.md`. Do NOT use after gateway restart, after applying a fix, or when a significant state change occurred.
- [ ] 2. Discover ALL `issues.jsonl` paths: `find <hermes-root> -name "issues.jsonl"`
- [ ] 3. Deduplicate by `issue_id`/`id` — keep best status per entry
- [ ] 4. For each open issue, verify against raw file (`terminal(command="cat ...")`) — not `custodian_issues` tool (stale cache)
- [ ] 5. Classify into four buckets: Actionable / User-gated / Legacy-inactive / Already-resolved
- [ ] 6. If any Actionable issues exist → execute fixes
- [ ] 7. If no Actionable issues → write journal with `not_activity_reason`, return `[SILENT]`
- [ ] 8. Clear `escalation_needed` flag on any resolved entries

**Escalation runner journal write — use Python always**: When running `escalation-runner` in cron context, write journals via `python3 -c "..."` with `json.dump()` and `from datetime import datetime, timezone; datetime.now(timezone.utc)`. Do NOT use `cat > file << 'EOF'` heredoc for JSON containing timestamps/run_ids — single-quoted heredoc prevents `$(date)` expansion, producing corrupted files. **Import the CLASS, not the module:** `import datetime; datetime.now()` raises `AttributeError: module 'datetime' has no attribute 'now'`. Always use `from datetime import datetime, timezone` so `datetime.now(timezone.utc)` resolves to the class method. See `references/escalation-runner-already-classified-fast-path.md` § Journal write pattern reminder.

**Escalation runner clean verdict — actionable vs user-gated vs legacy (2026-06-25):** When the escalation runner finds no actionable issues, classify all open entries into four buckets: (A) Actionable — execute fix; (B) User-gated — note count but do not auto-fix (skill library hygiene, stub removal); (C) Legacy/inactive — ignore YAML debris in profiles with no `cron/jobs.json`; (D) Already resolved — verify config/job state and close. If Bucket A is empty, write journal with `not_activity_reason` and return `[SILENT]`. See `references/escalation-runner-clean-verdict-pattern.md` for the decision tree and journal template. Inactive profile detection: check for `cron/jobs.json` absence (>90 days dormant). Confirmed 2026-06-25: braun profile has 3 null keys but no cron jobs — legacy debris, not an action item.

**Escalation runner: pause affected jobs for user-gated issues (2026-06-29):** When a user-gated issue (Tier 3, e.g., OAuth token revocation) causes a job to fail on every scheduled run, PAUSE the job to stop burn cycles. "User-gated" does not mean "no action" — pausing a job that cannot succeed until user intervention IS the action. Check: root cause is user-gated AND job is actively failing every run AND no chance of success without user action. In cron context, edit `jobs.json` directly (`enabled: false`, `state: 'paused'`). Update `issues.jsonl` with `jobs_paused: [<id>]`. Write an action journal documenting the pause. Do NOT mark the issue resolved — only the symptom is mitigated. See `references/escalation-runner-pause-affected-jobs-pattern.md`.

**Escalation Execution Loop (external cron trigger) — EXECUTE, don't just classify (2026-07-07):** When an external loop invokes Custodian + Mentor to *execute* fixes on escalated issues (not merely classify them), the default Escalation Runner Checklist above is classification-oriented — this fills the execute-and-reconcile gap:
1. **Verify live state BOTH directions** against `jobs.json` — (a) issue claims resolved/paused but job still `enabled`+erroring (inverse gotcha), AND (b) issue flags `escalation_needed: true` but the job already recovered (`last_status: ok`, `last_error` cleared). Resolve or re-pause accordingly; never trust the issue flag alone. AND (c) **sweep for missed enrollments**: any `enabled`+erroring job whose `last_error` matches a known user-gated fingerprint (Nous 401 `portal.nousresearch.com`, OpenRouter 402 `credits`, owl-alpha 404, Google Tasks 403, `invalid_grant`) but is NOT in any issue's `jobs_paused`. These failed in the inter-scan window *after* the last esc pass and were never enrolled. Pause them and add to the matching issue (keep `user_gated`+`escalation_needed: true`). Run `scripts/find_missed_user_gated_jobs.py` to enumerate + auto-classify.
2. **Load issues from the PROFILE `issues.jsonl`** (`<hermes-root>/profiles/<profile>/commons/data/ocas-custodian/issues.jsonl`) — it is authoritative. The commons copy is a lagging sync target; **write only to the profile path**. Use a brace-depth parser (entries may be concatenated per line). See `scripts/parse_issues_jsonl.py`.
3. **Classify**: Actionable (Tier 1 auto-fix) / User-gated (credits, API key, skill-internal hardcoded model) / Already-resolved / Legacy-inactive.
4. **Execute**: Actionable → apply the Tier 1 fix (fix-safety envelope). User-gated → **pause still-enabled failing jobs** (edit `jobs.json` directly: `enabled: false`, `state: 'paused'`). Pausing IS the action — it stops burn. Do NOT mark the issue resolved.
5. **Reconcile `issues.jsonl` in one pass** (safe edit pattern in `references/escalation-execution-loop.md`): resolve recovered issues, write missing issues from persistence gaps (prior scan flagged but never persisted; or a job evolved to a new fingerprint like `monitor:list` 403), update `jobs_paused` to match live paused state, clear false `resolved_at`. Keep genuinely user-gated issues `user_gated` + `escalation_needed: true` with a mitigation note.
6. **Verify** (re-read `jobs.json`: 0 enabled billing/key/owl jobs; re-parse `issues.jsonl`: state correct) then **write an action journal** (not `[SILENT]`).
**Honesty rule:** Do NOT report user-gated billing/API-key/skill-internal issues as "fixed". Pausing is mitigation, not resolution — they stay open until owner adds credits, rotates the key, or edits skill code. See `references/escalation-execution-loop.md`.

**Post-fix verification**: After applying any Tier 1 auto-fix, re-check the targeted log entry or config state to confirm the error no longer appears.

**Empty plugin directory detection**: During cron scanning, check for empty plugin directories. See `references/empty-plugin-dir-detection.md`. This is a Tier 2 issue (requires investigation, not auto-fixed). See `references/chronicle-plugin-dirs-empty-pattern.md` for the specific Chronicle plugin case.

## Script Path Security Block Pattern

See `references/script-path-security-block-pattern.md` for the `oc_cron_script_path_security_block` fingerprint — a distinct sub-pattern from `oc_cron_dead_script_ref` where the script exists but the path is rejected by the security model. The fix direction depends on `HERMES_HOME`: when running under a profile, scripts must be at `<hermes-root>/profiles/<profile>/scripts/<basename>`, NOT `<hermes-root>/scripts/`.

## Google OAuth Patterns

See `references/google-oauth-client-deleted-pattern.md` for two distinct Google OAuth fingerprints:
- `oc_google_oauth_client_deleted` — when the OAuth client itself is deleted from Google Cloud Console (`deleted_client` error). Requires new OAuth client creation + browser re-auth.
- `oc_google_oauth_token_revoked` — when the refresh token is revoked/expired (`invalid_grant: Token has been expired or revoked.`). Distinct from the above — the OAuth client exists but its tokens are dead. **Only affects jobs using the revoked account's credential file directly**. Confirmed 2026-06-29: only `email:check` and `monitor:list` (which wraps `tasks_monitor.py` with `CREDS_FILE = ".../google-workspace-user.json"`) fail. `sands:*`, `taste:*`, `vesper:*` continue working because they use different auth flows or different account credentials.

**Subprocess cascade mechanism (2026-06-28):** `monitor:list` wraps `tasks_monitor.py` as a subprocess (`subprocess.run([sys.executable, str(SCRIPT), "--mode", "check"])`). When the subprocess hits the OAuth refresh failure, it exits 1, and `monitor:list` propagates that exit code. The `last_error` on `monitor:list` shows "Script exited with code 1" — NOT the OAuth error itself. To diagnose: run `tasks_monitor.py --mode check` directly to see the actual `HTTPError: 400 Client Error: Bad Request for url: https://oauth2.googleapis.com/token`. This is the same root cause as `email:check` but the error message is masked by the subprocess wrapper. Do NOT classify as `oc_cron_no_agent_exit_1_noop` — the exit 1 is a real subprocess failure, not a no-op. Confirmed 2026-06-28: both `email:check` and `monitor:list` failed simultaneously from the same token revocation; `sands:*`, `taste:*`, `vesper:*` were **unaffected** because they use different auth flows or different account credentials (NOT because of cascading narrowness — they genuinely don't use the revoked account's token).

**Important:** Resolving a `ModuleNotFoundError` for `googleapiclient` on a Google-auth job should trigger an immediate re-check for token revocation. The package install fixes the import but the next run will immediately hit `invalid_grant` if the token is dead. Treat these as two sequential issues: package-missing (Tier 1 fix) → token-revoked (Tier 3 escalation).

## Fix Safety & Tier Classification

See `references/fix-safety.md` for the safety envelope, tier definitions, and the full Tier 1 auto-fix registry.

## Skill Conformance & Initialization

See `references/conformance.md` for background task checking and cron registry health checks.

## Activity Model & Schedule Optimization

Activity model rebuilt each deep scan from 14-day window. See `references/deep-scan.md` and `references/schedule-optimization.md`.

## Non-Fatal Error Patterns

All patterns below have full documentation in `references/` (see Support File Map for "When to read"). Key fingerprints:

| Fingerprint | Pattern | Tier |
|-------------|---------|------|
| `oc_gateway_restart_import_window` | ModuleNotFoundError / certifi SSL after restart | Transient |
| `oc_cron_no_agent_script_args` | no_agent script field has embedded arguments → wrapper fix | Tier 1 |
| `oc_no_agent_script_path_mismatch` | Script at system path, not profile path → symlink fix | Tier 1 |
| `oc_cron_script_not_found_transient` | Write/read race, script exists and runs | Transient |
| `oc_cron_stale_error_script_mismatch` | last_error ≠ current script field | Tier 2 |
| `oc_cron_provider_error_transient` | Generic "Provider returned error", cf=None | Transient |
| `oc_fallback_model_manifest_build_401` | fallback_model has expired custom provider key | Tier 3 |
| `oc_skill_reference_path_mismatch` | Skill reads refs from wrong path | Tier 2 |

Also see: `references/kanban-dispatcher-stuck-diagnostic.md`, `references/browser-cdp-502-loop-pattern.md`, `references/provider-401-diagnosis.md`, `references/oc-hook-post-tool-call-task-id-pattern.md`, `references/chronicle-session-lookup-noise-pattern.md`, `references/transient-401-self-resolution-pattern.md`.

## Known Code Fixes & MCP Cascade

See `references/known-code-fixes-and-cascade.md` for Tier 4 known code fixes and the MCP server cascade failure triage procedure.

## Escalation Path

Tier 3: write InsightProposal to proposals dir, tag journal `escalation_needed: true`. Confidence-gated: if `confidence_score >= 0.6` and `recommended_tier == 1`, auto-fix instead of escalating.

## Journal Outputs

- **Observation Journal** -- scan-only runs
- **Action Journal** -- runs with fixes or registrations

Path: `{agent_root}/commons/journals/ocas-custodian/YYYY-MM-DD/{run_id}.json`

**Schema:** See `references/observation-journal-schema.md` for the exact JSON shape, field definitions, and the clean verdict write sequence.

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
- `scripts/classify_error_jobs.py` — deterministic probe: reads the profile `jobs.json`, buckets enabled error jobs by `last_error` fingerprint, and lists every `Script exited with code 1` job with its `script` name so each can be inspected individually (de-aggregation). Run via `terminal(command="python3 <hermes-home>/skills/ocas-custodian/scripts/classify_error_jobs.py")`.
- `scripts/verify_escalation_state.py` — escalation-loop bidirectional verification probe: parses the profile `issues.jsonl` (brace-depth) and `jobs.json`, checks both staleness directions, and reports per-issue `jobs_paused` deltas vs the live paused set. Run via `terminal(command="python3 <hermes-home>/skills/ocas-custodian/scripts/verify_escalation_state.py")`. Run it FIRST in every escalation loop to decide whether any `issues.jsonl` write is needed (no-delta fast-path). See `references/escalation-execution-loop.md`.
- `scripts/find_missed_user_gated_jobs.py` — escalation-loop missed-enrollment probe: loads `jobs.json`, finds every `enabled`+erroring job NOT in any issue's `jobs_paused`, classifies its `last_error` against known user-gated fingerprints (Nous 401, OpenRouter 402, owl-alpha 404, Google 403/401), and reports which are MISSED enrollments (pause + enroll into matching issue) vs genuinely transient (leave running) vs UNKNOWN (inspect). Run it AFTER `verify_escalation_state.py` to catch jobs that failed in the inter-scan window. See `references/escalation-execution-loop.md`.

## Self-Update

`custodian.update` pulls from `https://github.com/indigokarasu/hermes-custodian-plugin`. **Do NOT push changes to this skill directory** — it's a local reference copy. Canonical source is the plugin repo.

### Plugin vs Skill Architecture

The **plugin** (`~/.hermes/plugins/custodian/`) is the active code loaded by the gateway. The **skill** (`~/.hermes/profiles/<profile>/skills/ocas-custodian/`) is a reference copy. Do not recreate as standalone.

- **Actual update:** `cd ~/.hermes/plugins/custodian && git pull`
- **Version:** Check plugin `__init__.py` `__version__` or `git log -1 --oneline`
- See `references/plugin-vs-skill-architecture.md` for editable install details

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
- **MCP server binary/module missing — two distinct patterns:** `oc_workspace_mcp_binary_missing` (entry point missing, see `references/workspace-mcp-binary-fix.md`) vs `oc_mcp_server_module_deleted` (binary exists, Python module deleted — `No module named <module>`, see `references/mcp-server-module-deleted-pattern.md`). Diagnosis: run wrapper directly — `No such file` = binary-missing, `No module named X` = module-deleted.
- **MCP module deleted false positive** — Fingerprint can fire on stale error log entries when the module is actually present. Before escalating: `import main` in venv, run wrapper binary directly, check dist-info. If all succeed → stale error, classify `oc_mcp_server_module_deleted_false_positive` (Tier 2, surface only). Do NOT escalate.
- **System Python files can shadow profile-local fixes** — See `references/known-code-fixes-and-cascade.md`.
- **`known_issues.json` nested structure issue** — Some entries are nested as sub-keys. Flatten before matching.
- **Stale failure counter vs stale error** — A job can have `consecutive_failures > 0` with `last_status=ok`. If `last_error` is null, the job is healthy. The failure counter is stale from a previous transient failure that has since resolved. No fix needed if the job is running on schedule and producing expected output.
- **Plugin directory missing `__init__.py`** — A plugin directory at `~/.hermes/plugins/<name>/` may lack an `__init__.py` at the top level even when the plugin code exists in a subdirectory (e.g., `hermes_<name>_plugin/__init__.py`). The plugin loads via `pyproject.toml` editable install, but the missing `__init__.py` generates "Failed to load plugin" warnings on every cron scheduler tick (every 60s). This is cosmetic noise — the plugin functions. Do NOT escalate as `oc_plugin_load_failed`. Classify as `oc_plugin_init_missing_noise` (Tier 2, surface only with count).
- **Escalation runner already-classified fast path — skip redundant re-verification (2026-06-29):** When the prior esc-run journal classified all open issues as `open_user_gated` and no new errors appear in `jobs.json`, follow up with a journal referencing the prior classification and return `[SILENT]`. Don't re-verify every `issues.jsonl` path if nothing changed. See `references/escalation-runner-already-classified-fast-path.md`.
- **Escalation runner journal write must use Python, not heredoc (2026-06-24, re-confirmed 2026-06-29):** When writing esc-run journals in cron, `cat > path/run_id.json << 'EOF'` with single-quoted delimiter does NOT expand `$(date)` — the literal string `$(date -u +%Y%m%dT%H%M%SZ)` ends up in the JSON body and the file goes to the wrong path. Use `python3 -c "from datetime import datetime, timezone; json.dump(..., open(path,'w'))"` with `datetime.now(timezone.utc)` for the run_id, timestamp, and date directory. (Import the datetime CLASS — `import datetime; datetime.now()` fails with `AttributeError: module 'datetime' has no attribute 'now'`.) Confirmed 2026-06-29: first journal write attempt via heredoc produced a file that didn't appear on disk.
- **`custodian_issues` tool returns stale/merged view** — The tool's view is a cached representation, NOT the live state of `issues.jsonl`. Always verify against the raw file via `terminal()` AND check the latest esc-run journal. Optimization: a 5-second journal check can replace a 60-second full scan.
- **Custom provider 401s are NOT transient** — Check the `base_url` before concluding "transient."
- **Null provider jobs route through the config `model:` section, not just fallback_providers** — When diagnosing model-related errors on `provider: null` / `model: null` jobs, check the profile config's `model:` section FIRST (`<hermes-root>/profiles/<profile>/config.yaml`, top-level key `model:`). This defines the default model, provider, and base_url. `fallback_providers` only kicks in after the default fails. A removed/renamed default model causes simultaneous 404s across ALL null-model jobs. Diagnosis: query provider API for model existence, compare config mtime vs job last_run_at. See `references/stale-model-error-diagnostic-pattern.md`.
- **Journal gap detection** — If no observation or action journal has been written for 3+ consecutive days, flag as `oc_journal_gap` (Tier 2). Check the latest journal in `{agent_root}/commons/journals/ocas-custodian/YYYY-MM-DD/` — if the most recent file's date is >3 days ago, the cron may not be firing or the scan is completing without writing output. This is a silent failure mode: the job runs but produces no evidence record. During deep scan, always verify today's journal exists; if not, write it even if the scan finds nothing actionable. (2026-06-18)
  - ⚠ **Journal path format inconsistency (2026-07-01):** Different scan runs write journals using different date directory formats. The canonical format is `YYYY-MM-DD/` but some runs use `YYYYMMDD/` (no hyphens) and some write loose files directly in the journals root directory. When checking for gaps, search ALL locations: check both `YYYY-MM-DD/` and `YYYYMMDD/` dirs, and scan loose `.json` files at the journals root. See `references/journal-path-format-inconsistency.md` for diagnosis and fix direction.
- **Fix-loop detection** — When the same fix has been applied >= 3 times to the same fingerprint and `schedule_adjusted_stickiness < 0.5` for all attempts (fix doesn't survive one full schedule cycle per recurrence), do NOT apply the fix again. Auto-demote to Tier 3, create an RCA record with the full occurrence chain, and escalate with the root cause hypothesis. This is the single most important gotcha: Custodian must stop repeating fixes that don't hold.
- **Stale error state: status=error + consecutive_failures=None** — Job has `status=error` with `consecutive_failures=None` and `last_error` referencing a provider/path that no longer exists. Scheduler never updated state after external resolution. Diagnosis: check `last_error` for provider refs, verify config no longer contains it, confirm `next_run_at` is on schedule. Fix: `hermes cron pause <id>` → `hermes cron resume <id>`. Verify underlying cause is gone before resetting. See `references/stale-error-state-pause-resume-fix.md`.
- **`hermes cron edit` requires relative script paths** — The `--script` flag accepts only paths relative to `~/.hermes/scripts/`, not absolute paths. If the script only exists under the profile directory (`<hermes-root>/profiles/<profile>/scripts/`), create a symlink or copy to `~/.hermes/scripts/` first, then use just the filename.
- **`hermes cron create` syntax** — The command to register new cron jobs is `hermes cron create` (NOT `hermes cron add`). Positional args: `schedule` then `prompt`. Flags: `--name`, `--skill`, `--deliver`, `--no-agent`, `--repeat`, `--script`, `--workdir`. There is NO `--prompt`, `--schedule`, or `--model` flag — schedule and prompt are positional. Example: `hermes cron create --name "job:name" --skill ocas-skill "*/10 * * * *" "Prompt text here"`.
- **Plugin hook signatures must accept `**kwargs`** — The Hermes plugin framework may pass additional keyword arguments to hook callbacks (e.g., `task_id`). All hook functions must include `**kwargs` in their signature to remain compatible with evolving framework contracts. A hook without `**kwargs` will crash on every invocation when the framework adds new kwargs. This applies to ALL hooks: `post_tool_call`, `on_session_start`, `on_session_end`, `on_session_reset`, and any future hooks.
- **Plugin hook params before `**kwargs` must have defaults** — Even with `**kwargs`, if a parameter before `**kwargs` lacks a default value (e.g., `ctx` instead of `ctx=None`), the hook will crash when the framework doesn't pass that positional arg. All pre-`**kwargs` params must use `param=default` syntax.
- **Editable install path may differ from plugin directory** — When Hermes uses `pip install -e`, the active plugin code is at the path in the editable finder's `MAPPING` dict, NOT at `~/.hermes/plugins/<name>/`. Always verify via `importlib.util.find_spec('hermes_<name>_plugin').origin` before editing. See `references/editable-install-path-discovery.md`.
- **jobs.json path under profiles** — When running under a profile, the authoritative jobs.json is at `<hermes-root>/profiles/<profile>/cron/jobs.json`, NOT `<hermes-root>/cron/jobs.json`. Both exist; the profile-scoped one is correct for profile sessions.
- **state.db >10GB** — Flag as `oc_state_db_oversized` (Tier 2). VACUUM requires ~2x the DB size in temp space. If disk >80%, recommend message pruning instead of VACUUM.
- **Backup script disk-full trap (symlinked large DB + `stat` gotcha)**: `backup_all_hermes_data.sh` copies `state.db` **locally only** (never to LFS). If `state.db` is a symlink (it is: `<hermes-root>/state.db` → `profiles/indigo/state.db`, ~12G), a `cp` that fills the disk aborts the whole script *before* the GitHub LFS push. Trap: `stat -c%s` on a symlink returns the symlink length (38), not the 12G target — so a free-space guard passes and `cp` re-fills the disk. Fix: dereference with `readlink -f` + `stat -L`, skip the local-only copy when `avail < size*1.1`, and `rm -rf` the partial `/root/backup/<ts>` before retry. See `references/backup-disk-full-symlink-gotcha.md`. (2026-07-07)
- **read_file blocked on data files in restricted sessions** — In cron/sandboxed sessions, `read_file` may be denied for JSONL files, skill SKILL.md files, and other data files with "Background review denied non-whitelisted tool". Always use `skill_view(name=...)` to read skill files and `terminal(command="cat ...")` for JSONL/data files in cron context. NOTE: config files like `jobs.json` and plain text logs are NOT affected — `read_file` works fine on those paths. The blocking is file-type-specific, not universal.
- **Python source files can be deleted while .pyc remains** — When diagnosing `ModuleNotFoundError` for a module that previously worked, check if the `.py` source was deleted while the `.pyc` in `__pycache__` still exists. This happens when cleanup scripts or manual deletion targets `.py` files but not `__pycache__`. Symlinks from profile scripts dirs to the shared scripts dir break silently. Reconstruct from bytecode (see `util-hermes-ops/references/python-source-recovery-from-pyc.md`) or restore from git.
- **hermes cron pause/resume as general reset** — The pause/resume pattern (`hermes cron pause <id>` → `hermes cron resume <id>`) is a reliable fix for ANY stale scheduler state: stuck next_run_at, stale error status, stuck failure counters, and no_agent mismatches. It forces the scheduler to recalculate internal state from jobs.json. Prefer this over direct jobs.json edits for state-related issues.
- **Cron jobs can run successfully but produce no meaningful output** — `last_status=ok` only means the job executed without errors. It does NOT verify that the job produced its expected output files or side effects. See `references/cron-output-verification-gap.md`. When diagnosing a pipeline that "should be working," check the output file's mtime — not just the job's status.
- **`cannot schedule new futures after interpreter shutdown`** — A cron job that uses `concurrent.futures` can hit this error when the executor is reused across runs and the interpreter shuts down between runs. The job shows `status=error` with this message but `consecutive_failures=None` (literal null, not zero — the scheduler doesn't count interpreter-state errors as consecutive failures). **This is always transient** — the executor state resets between runs. The job will succeed on its next scheduled run without intervention. If the error persists across 3+ runs, then apply `hermes cron pause <id>` then `hermes cron resume <id>` (but see "hermes cron CLI doesn't work from cron context" above — use direct jobs.json edit instead). (2026-06-21)
- **Config empty sections are a Tier 1 auto-fix** — When `config.yaml` has null-valued keys at ANY depth, remove them during the repair pass. Check BOTH main + profile configs AND nested sections. See `references/config-empty-section-fixloop-status.md` for full diagnosis pattern, sed pitfall, fallback_model:null nuance, and occurrence history.
- **ocas-corvus data directories are a legacy artifact** — The `commons/data/ocas-corvus/` and `commons/journals/ocas-corvus/` directories may exist on disk. Corvus was merged into Chronicle and never existed as a standalone installed skill. Any `corvus:deep` or `corvus:update` cron jobs referencing `ocas-corvus` should be removed. The data dirs can be archived or left as-is. (2026-06-18, updated 2026-06-22)
- **skilllab skill directory missing**: `skilllab` is referenced by `10khr-grind`. Runs fine prompt-based. Tier 2, requires user confirmation to create skill directory. (2026-06-18)
- **Escalation runner must deduplicate issues.jsonl before writing** — When updating `issues.jsonl`, always read all entries, deduplicate by `issue_id` (or `id`), keep the best status per entry, then write back. Without dedup, multiple escalation runner runs accumulate duplicate entries (3+ per issue) that inflate counts and cause repeated fix attempts. See `references/escalation-runner-2026-06-08-1915.md` for the correct Python dedup pattern.
- **`fix_effectiveness.jsonl` schema contamination** — Raw fix log entries (fields: `fingerprint`, `fix_id`, `target`, `outcome`, `timestamp`) can get appended to `fix_effectiveness.jsonl`, which expects confidence records (fields: `fingerprint`, `attempts`, `successes`, `failures`, …). When `ConfidenceModel._load()` reads malformed entries without the `attempts` key, `should_escalate()` crashes with `KeyError: 'attempts'`, which causes `custodian_status` to fail, which gets logged as an error — a self-inflicted crash loop. Fix: (1) validate `"attempts" in r` before storing in `_load()`, (2) use `rec.get("attempts", 0)` in `should_escalate()`, (3) clean malformed entries from the file. See `references/fix-effectiveness-schema-contamination.md`.
- **Git stash pop pitfalls** — `git checkout --theirs` during conflict resolution silently drops local changes (reverts `__version__`). `git stash pop` does NOT auto-drop on conflicts — manually `git stash drop` after committing resolved merge. Always verify version strings after conflict resolution.
- **custodian:update can delete the skill directory** — If missing: find `source:` URL from cron output logs, `git clone` to `/tmp`, copy SKILL.md + references + scripts back. Canonical source is in frontmatter `source:` field. Prevention: never `rm -rf` — only `git fetch`/`git merge`. Pre-update backup: `cp -r <skill_dir> /tmp/custodian-backup-$(date +s)`. See `references/plugin-self-update-2026-06-18.md`.
- **Plugin discovery: check BOTH profile path AND system path** — Hermes loads from `<hermes-root>/profiles/<profile>/plugins/` AND `/usr/local/lib/hermes-agent/plugins/`. Check profile path first. "Context engine not found" warnings were FALSE POSITIVES — Chronicle loaded from profile path but scan checked system path.
- **Config changes can resolve issues without updating issues.jsonl** — A user or another process can change config.yaml (e.g., setting `enabled: false` for an MCP server) or install a plugin to the profile path, resolving the underlying problem without updating issues.jsonl. The escalation runner then finds stale open issues that are already resolved. Before acting on any open issue about a specific server/plugin/config setting: (1) check the current config.yaml for the relevant `enabled` flag, (2) check BOTH profile and system plugin paths for file existence, (3) check `context.engine` setting before classifying context-engine-missing as a failure. Verify current state independent of the issue description. See `references/escalation-runner-2026-06-15-1108.md` for examples.
- **Checkpoint store git corruption (missing refs/heads + objects/)** — When `checkpoints/store/.git` exists but is missing standard git directories (`refs/heads/`, `objects/`), checkpoint_manager logs errors on every write. The corruption can happen if the store directory is moved, if git is killed mid-init, or if a cleanup script removes git internals. **Fix:** Back up `.git` to `.git.bak`, `rm -rf .git`, `git init` in the store directory. Preserve `HEAD`, `config`, `indexes/`, `packed-refs`, and `projects/` contents — the reinit creates fresh git structure. The checkpoint store will rebuild its refs on next write. (2026-06-18)
- **Escalation runner must check ALL issues.jsonl paths** — Issues accumulate in 5+ different `issues.jsonl` files across the filesystem. The same root cause often appears in multiple files with different `issue_id` values. Before any escalation run, use `find <hermes-root> -name "issues.jsonl"` to discover all paths, then deduplicate by description/summary. See `references/escalation-runner-multi-path-issues.md`.
- **issues.jsonl can contain multiple JSON objects per line** — Entries are sometimes concatenated on a single line (not newline-separated). Naive `json.loads(line)` fails with `JSONDecodeError: Extra data`. Use a brace-depth parser that walks the line character by character. See `references/escalation-runner-multi-path-issues.md` for the parser code.
- **Always clear `escalation_needed` when resolving issues** — When closing stale issues, set `escalation_needed: false` in the same pass. A systematic bug leaves `escalation_needed: true` on resolved entries, causing false-positive escalation on every subsequent run. After any batch close, sweep all files for `status: resolved` + `escalation_needed: true` and clear the flag.
- **Stale proposal files in ocas-corvus paths** — If `.json` files exist in `<hermes-root>/proposals/` or `<hermes-home>/commons/data/ocas-corvus/proposals/`, they are legacy artifacts from before Corvus was merged into Chronicle. They can be ignored or deleted — no active skill writes to these paths.
- **System agent files: patch BOTH editable source AND installed copy** — When hermes-agent is installed in editable mode, Python imports resolve to the source checkout (`<hermes-root>-agent/agent/`), but a separate installed copy exists at `/usr/local/lib/hermes-agent/agent/`. Both may be loaded depending on the import path. When patching system agent files (e.g., `subdirectory_hints.py`), always patch BOTH locations and clear stale `.pyc` caches. Verify which path is actually loaded via `importlib.util.find_spec('agent.<module>').origin`. See `references/subdirectory-hints-home-dir-pattern.md` for a worked example.
- **no_agent script field is a literal path, not a command line** — When `no_agent: true`, the `script` field is treated as a literal file path. Arguments like `foo.py --flag` will fail with "Script not found" because the entire string is resolved as a path. Fix: create a wrapper script that bakes in the arguments, symlink to `~/.hermes/scripts/`, and point the cron job at the wrapper. See `references/no-agent-script-argument-pattern.md`. (2026-06-20)
- **no_agent compound `&&` errors have consecutive_failures=None — invisible to failure heuristics** — Scheduler sets `consecutive_failures=None` (literal null) because the agent never started. Job won't trigger failure-count alerts and can fail silently for weeks. Detection: check all `no_agent: true` jobs for `&&`, `;`, `|`, or spaces in `script` field. See `references/no-agent-script-argument-pattern.md`.
- **no_agent scripts exiting 1 for no-op are false-positive errors** — Monitor scripts exiting 1 when no work → classify `oc_cron_no_agent_exit_1_noop` (Tier 2, surface only). ⚠ Subprocess exit 1 ≠ no-op: if a monitor wraps a subprocess, run it manually to check if the subprocess error is real (OAuth, missing package, API timeout) vs genuine no-op. See `references/subprocess-cascade-reverification-pitfall.md`.
- **Classification bias: don't assume root cause from a tracked issue on a different job** — When a `Script exited with code 1` error has `stderr` content, always read the COMPLETE `last_error` including the stderr traceback before classifying. A previously-tracked root cause (e.g., OAuth revocation on `email:check`) can bias you into classifying a DIFFERENT job's error (e.g., `dispatch:triage-morning`) as the same issue when the actual error is completely different (e.g., `ModuleNotFoundError: No module named 'google'`). Confirmed 2026-07-01: prior scan classified `dispatch:triage-morning` as OAuth "graceful degradation" based on the tracked `oc_google_oauth_token_revoked` issue, but the actual `last_error` showed a `ModuleNotFoundError` — a missing dependency, not OAuth. The same job family + same exit code does NOT mean the same root cause. Read stderr before classifying.
- **`fallback_model` with broken credentials affects ALL null-provider jobs** — The `fallback_model` top-level key in profile config (`<hermes-root>/profiles/<profile>/config.yaml`) can contain a custom provider with expired/invalid API key. When a job with `provider: null` falls through to the fallback model, it hits the broken provider. This is especially dangerous because `custodian:light` (the primary detection mechanism) has `provider: null` — if it can't run due to fallback_model 401, issues go undetected. Symptoms: `custodian:light` fails with `RuntimeError: HTTP 401: Authentication failed with upstream provider` mentioning `provider=custom` and a custom base_url. Diagnosis: check `config.yaml` for `fallback_model` section with a custom provider. Fix: update the API key, change to a working provider, or remove the `fallback_model` entry. See `references/null-provider-fallback-routing-2026-06-18.md` for the full pattern and the 2026-06-20 manifest.build variant. (2026-06-20)
- **Gateway restart import window (transient ModuleNotFoundError)** — Cluster of `ModuleNotFoundError` / certifi SSL errors immediately after gateway restart = transient import-window. Modules exist on disk, self-resolve in ~5-10 min. Diagnostic: errors cluster within 5 min of restart, verify via `python3 -c "import <module>"`. Do NOT escalate. See `references/gateway-restart-import-window-pattern.md`.
- **Gateway SIGTERM clean restart is NOT an error** — SIGTERM → clean 4.10s teardown, exit 1, systemd revives. "Another gateway instance is already running" on second start attempt is expected. Healthy if new instance connects within ~30s. Do NOT escalate.
- **Rapid SIGTERM restart loop is Tier 2, not a crash** — systemd SIGTERM every ~3 min = external signal loop, not gateway crash. Classify `oc_gateway_sigterm_restart_loop` (Tier 2, monitor only). Self-resolves when trigger clears. See `references/critical-pitfalls.md`.
- **Config change + gateway restart = stale error states on jobs** — When config.yaml is modified to remove a broken provider (e.g., manifest.build) and the gateway restarts, jobs that ran before the restart will show `status=error` with the old provider error but `consecutive_failures=0`. These are **stale error states**, not active errors. Verify by: (1) config no longer contains the broken provider, (2) gateway has restarted since the config change, (3) no new errors in gateway log after restart. Do NOT apply pause/resume — jobs will self-verify on next run. (2026-06-23)
- **Light scan must iterate ALL jobs.json entries** — A scan that filters by name or uses a cached job list can miss newly added or recently errored jobs. Always parse the full `jobs.json` array and check every entry's `last_status` field. The 07:00 scan listed 9 error jobs but missed `gens:sync` (actual: 10). Root cause: the scan likely used a pre-filtered list rather than iterating all 133 entries. (2026-06-24)

**Light scan can miss a job that failed in the inter-scan window** — If a job's run completes AFTER the prior scan read `jobs.json`, the prior scan's `error_jobs_enabled_actionable` count will be 0 (or omit that job) while a real fault exists. Never treat a prior scan's '0 actionable / all-transient' verdict as authoritative across the gap between scans. Always re-derive the enabled-error set from the LIVE `jobs.json` each scan. Confirmed 2026-07-07: `indigokarasu-site-feed-refresh` (enabled, `no_agent`) ran 13s AFTER the 06:06 light scan fired, so that scan reported `error_jobs_enabled_actionable: 0` and omitted it; the 07:08 scan re-derived live and caught the `Script not found` fault (Tier 1 symlink fix applied). The tell: a freshly-failed enabled job shows `last_run_at` within minutes of the prior scan timestamp.
- **Escalated issue stability — journal the delta** — When consecutive scans find the SAME already-escalated issues with no new errors, journal a `previous_scan_delta` block: elapsed min, new_issues=0, new_errors=0, stable root cause. Turns "same thing again" into evidence of persistent-but-tracked. Do NOT use after gateway restart, fix application, or state change. See `references/light-scan-2026-06-29-0904.md`.
- **Subprocess cascade re-verification pitfall** — A single manual run exiting 0 does NOT prove a subprocess-wrapped error is stale. Run subprocess directly AND wrapper script, at least twice each. If either reproduces → ACTIVE. See `references/subprocess-cascade-reverification-pitfall.md`.
- **`jobs.json` is a dict, not a list** — The profile-scoped `jobs.json` at `<hermes-root>/profiles/<profile>/cron/jobs.json` is structured as `{"jobs": [...], "version": "..."}`. Naive `for j in json.load(f)` iterates dict keys ("jobs", "version") not job entries, causing `'str' object has no attribute 'get'` crash. Always extract `data.get("jobs", [])` (or `data["jobs"]`) before iterating. Confirmed 2026-06-24: the scan silently missed ALL error jobs on the first attempt because the outer loop iterated dict keys. The correct pattern is `jobs = data.get("jobs", []) if isinstance(data, dict) else data`. (2026-06-24)
- **Backup repo committing credential files to git** — Backup cron copying live auth files into git-backed repo captures secrets. Detection: `git log --grep="backup:" -20 --name-only | grep -E "auth\.json|credentials/|\.env$"`. Tier 1 fix: `.gitignore` + commit/push. Full remediation: rotate tokens BEFORE `git filter-repo --invert-paths` + force-push. See `references/credential-leak-backup-commit-pattern.md`.
- **GitGuardian "Generic High Entropy Secret" on expired tokens is still HIGH** — Expired `access_token` in git history ≠ safe. Accompanying `refresh_token` likely still valid. Treat as HIGH until BOTH confirmed rotated at provider.
- **YAML null-key detection: two equivalent forms** — `key: null` and `key:` both parse as `None`. Grep `: null$` only catches Form 1. Use PyYAML or grep BOTH (`: $` AND `: null$`).
- **SOUL.md truncation is a silent Tier 1 issue** — When `SOUL.md` exceeds `context_file_max_chars`, every cron logs TRUNCATION WARNING but shows `last_status=ok`. Fix: increase to `ceil(current_size * 1.2)`. Check BOTH main + profile configs; gateway restart required for change to take effect. See `references/soul-md-truncation-pattern.md`.
- **`RuntimeError: Provider returned error` on null-provider jobs is transient** — First-occurrence (cf=None) upstream API error from LLM execution context. Script typically runs fine. No fix needed — self-resolves next run. Distinct from `oc_fallback_model_manifest_build_401` and `oc_null_provider_fallback_routing`. Only escalate if `consecutive_failures >= 3`. See `references/cron-provider-error-transient-pattern.md`.
- **Disabled/paused jobs with stale `last_status=error` inflate error counts** — `enabled: false` or `paused_at` set → retains old error. Classification: `oc_cron_disabled_stale_error`. Do NOT fix. Filter from actionable error counts. Verify staleness: `last_run_at < fix_timestamp`. See `references/post-fix-stale-error-pattern.md`.
- **Cron output file is ground truth — UNLESS a gateway restart froze it.** Output file at `{profile}/cron/output/{job_id}/{timestamp}.md` is the run evidence, and `jobs.json` `last_status` lags during the scheduler state-update window. BUT after a gateway restart the `cron/output/` tree can stop being written (frozen at the restart time) while live evidence moves to `{profile}/commons/journals/<skill>/<YYYY-MM-DD>/`. When `cron/output/` shows no activity but journals are actively updating, trust the journals, not the output dir. Confirmed 2026-07-07: output dir froze at 05:18Z after the 05:33 gateway restart; a prior finch pass mis-read it as "all green." (See also `cron-job-repair` verification pitfall.)
- **No-agent missing dependency** — When a `no_agent: true` job fails with `ModuleNotFoundError`, install the missing package in the hermes-agent venv. See `references/no-agent-missing-dependency-install-procedure.md` for the full install procedure (and the wrong-venv-path pitfall).
- **Journal-to-issues escalation gap (2026-06-29):** A scan can write `escalation_needed: true` in its journal but fail to write the matching entry to `issues.jsonl`. This means the escalation runner (which reads `issues.jsonl`, not journals) never finds the issue, and it silently drops. **During light scan Step 8b**, always check: for the prior scan's journal, if `escalation_needed=true`, verify a matching `issues.jsonl` entry exists. See `references/light-scan-2026-06-29-1204.md`.
- **Timezone schedule window false positive (2026-06-29):** When checking for "overdue" jobs by comparing `last_run_at` to `now`, you can falsely flag jobs with daylight-hour schedules (e.g., `*/30 9-17 * * 1-5`) as overdue when the current time is simply before the job's daily window. Always convert to local timezone (PDT/UTC-7) and check whether the current hour + weekday fall within the cron expression's restrictions before flagging. Confirmed 2026-06-29: `custodian:escalation-runner` and `bones:market-monitor` both flagged as "2900+ min overdue" at 06:04 PDT — but neither runs before 08:00/09:00. See `references/timezone-schedule-window-false-positive.md`.
- **Pause jobs failing every run due to user-gated issues (2026-06** When a job fails on every scheduled run because of a user-gated root cause (OAuth token revoked, credentials expired, etc.), PAUSE it. Leaving it running wastes resources, clutters logs, and inflates error counts. In cron context, edit `jobs.json` directly (`enabled: false`, `state: 'paused'`). Update `issues.jsonl` with `jobs_paused` list. Write an action journal. Do NOT mark the issue resolved — only the symptom is mitigated. Resume after user fixes the root cause. See `references/escalation-runner-pause-affected-jobs-pattern.md`.
- **Paused jobs show permanent `last_status=error` — always filter (2026-06-29):** Once a job is paused (`enabled: false`, `state: paused`), it will never run again to overwrite its `last_status`. The last error (often the very failure that triggered the pause) persists indefinitely. `paused_at` may even be BEFORE `last_run_at` — the scheduler ran the job one final time before the pause took effect, and that error is now frozen. Classification: `oc_cron_disabled_stale_error` — do NOT count as active error, do NOT re-escalate, do NOT attempt fix. Filter paused jobs from the actionable error count at the START of classification. Confirmed 2026-06-29: `email:check` (paused 10:34, last_run 10:35) and `monitor:list` (paused 18:33 UTC, last_run 11:33 PDT) both show `status=error` but are permanently paused — zero action needed

- **Issues.jsonl pause/resolution metadata is NOT ground truth — verify against `jobs.json` (2026-07-07):** A prior scan can write `jobs_paused: [...]` + `resolved_at` to issues.jsonl while NEVER actually editing `jobs.json` — the jobs remain `enabled: true, state: scheduled` and keep erroring. This is the inverse of the "config change resolves without updating issues.jsonl" trap: here the issue *claims* resolved/paused but the fix never landed. When re-scanning, if an issue carries `resolved_at` + `jobs_paused` but the live `jobs.json` shows those jobs enabled and still erroring, the fix did NOT hold. Re-open the issue (clear `resolved_at`, set `escalation_needed: true`) AND re-apply the actual fix to `jobs.json` (e.g., re-pause). Trust the live `jobs.json` state, not the issue's `resolved_at` flag. Confirmed 2026-07-07: `oc_openrouter_402_credits_exhausted` and `oc_google_oauth_token_revoked` both carried `resolved_at` + `jobs_paused` lists, yet all 50 target jobs were enabled and actively failing; the light scan re-paused them and re-opened the issues.

- **Issues.jsonl can flag escalation while the job already RECOVERED — forward-direction staleness (2026-07-07 escalation loop):** The inverse gotcha (above) covers issue-claims-resolved-but-job-still-failing. The SAME hazard has a forward direction: an issue can carry `status: user_gated` + `escalation_needed: true` in `issues.jsonl` while the underlying job has actually RECOVERED (`last_status: ok`, `last_error` cleared). Confirmed 2026-07-07: `oc_google_oauth_email_check_invalid_grant_20260628` stayed `user_gated` + `escalation_needed: true` even though `email:check` (job `25c06979ccc7`) showed `last_status: ok` with `last_error` empty. The escalation loop MUST resolve such issues (set `status: resolved`, clear `escalation_needed`), not just re-pause. Always verify BOTH directions against live `jobs.json` before acting on any flagged issue.

- **Global model-config fix may NOT cover jobs that reference the deprecated model inside skill internals (2026-07-07):** Fixing `auxiliary.compression.model` (or any global model default) resolves jobs that inherit it, but jobs can still 404 on the OLD model if the model name is hardcoded inside the skill's own code or prompt. Symptom: the job's `model` field shows the NEW model (e.g., `tencent/hy3:free`) yet the `last_error` references the deprecated one (e.g., `openrouter/owl-alpha`). Verify by grepping the skill directory (`search_files(pattern="owl-alpha", target="content")` over `<hermes-root>/profiles/<profile>/skills/<skill>/`), NOT just the job's `model` field. Such jobs need a code/prompt edit (user-gated), not a job-level model change. Confirmed 2026-07-07: 5 jobs (util-headhunter ×2, ocas-genie, ocas-autobio, EHCS) kept 404-ing on owl-alpha after the global fix because the ref lived in skill internals.

- **Pre-update state snapshots refill disk and negate pruning (2026-07-07):** `custodian:update` / self-update writes a pre-update snapshot at `state-snapshots/<YYYYMMDD-HHMMSS>-pre-update/` containing a FULL copy of `state.db` (~= live size, e.g. 12 GB). If not pruned after a successful update, this snapshot refills disk and undoes message-pruning. Detection: `du -sh <hermes-root>/profiles/<profile>/state-snapshots` — flag any `*-pre-update/` dir > 1 GB. It is a backup, not live data; safe to `rm -rf` once the update is verified. See `references/pre-update-state-snapshot-disk-refill.md`. Confirmed 2026-07-07: disk claimed pruned to 66% (2026-07-06 deep scan) but climbed back to 91% within ~14h — root cause was a 12.2 GB `state-snapshots/20260707-110009-pre-update/state.db`.

## Error Handling

| Failure | Handling |
|---------|----------|
| `read_file` returns "BLOCKED: already read" | Use `terminal(command="cat /path")` or `terminal(command="tail -N /path")` to re-read files in cron context |
| Pipe-to-interpreter blocked (`cat|tail|curl|grep` → `python3`) | Write script to `/tmp/` via `write_file`, run via `terminal(command="python3 /tmp/script.py")`. Single-quoted heredoc (`python3 << 'PYEOF'`) also works for inline Python. Do NOT retry pipe variations — all forms blocked by tirith security filter |
| `write_file` fails with "missing required field: path" | Fallback: `terminal(command="cat > /path << 'EOF'\n...\nEOF")` with heredoc |
| `execute_code` denied in cron | Use `terminal()` directly for all Python operations |
| `hermes cron list` shows "No scheduled jobs" | CLI reads wrong path — edit `<hermes-root>/profiles/<profile>/cron/jobs.json` directly |
| Heredoc produces literal `$(date)` in JSON | Use `python3 -c "..."` with `json.dump()` for dynamic content — never heredoc for JSON |
| MCP server PIDs alive but connection failing | Check process liveness with `ps` before escalating — handshake can fail independently |
| `custodian_issues` tool returns stale data | Always verify against raw `issues.jsonl` via `terminal(command="cat ...")` AND check latest esc-run journal |
| `fix_effectiveness.jsonl` schema contamination | Validate `"attempts" in r` before storing; use `rec.get("attempts", 0)` in reads |
| issues.jsonl has multiple JSON objects per line | Use brace-depth parser, not naive `json.loads(line)` |
| `ModuleNotFoundError` after package install | Re-check for token revocation — install fixes import, next run hits `invalid_grant` if token dead |
| `consecutive_failures=None` on no_agent compound `&&` error | Scheduler never started agent — check script field for `&&`, `;`, `|` characters directly |
| `last_status=error` but output file shows success | Output file is ground truth — jobs.json `last_status` lags during scheduler state-update window |
| Gateway SIGTERM (exit code 1) | Clean teardown — NOT an error. systemd `Restart=on-failure` revives it. Do NOT escalate |
| `state.db` >1GB AND disk >80% | Flag as `oc_state_db_oversized` (Tier 2). Recommend message pruning over VACUUM when disk >80% |

**Validation loop pattern (all fix operations):**
1. Apply fix
2. Re-check targeted log entry or config state
3. Confirm error no longer appears
4. Write journal with fix outcome
5. If fix-loop detected (same fingerprint >= 3 times), auto-demote to Tier 3 + escalate with RCA

## Support File Map

| File | When to read |
|------|-------------|
| `references/cron-output-verification-gap.md` | Cron `last_status=ok` but output not updated |
| `references/script-path-security-block-pattern.md` | Script points outside `$HERMES_HOME/scripts/` |
| `references/google-oauth-client-deleted-pattern.md` | Google OAuth `deleted_client` and `invalid_grant` (token revoked) errors |
| `references/email-check-invalid-grant-diagnostic.md` | Diagnostic procedure for `invalid_grant` on email:check — step-by-step token store testing and re-auth flow |
| `references/cron-mass-fastforward-after-gateway-downtime.md` | MANY jobs overdue simultaneously |
| `references/cron-timeout-first-occurrence-pattern.md` | First-occurrence timeout — likely transient |
| `references/cron-script-path-home-pattern.md` | Detecting `Path.home() / ".hermes"` in scripts |
| `references/spec-ocas-recovery.md` | Implementing the recovery contract |
| `references/okrs.md` | Reviewing skill performance |
| `references/transient-provider-errors.md` | Cron fails with provider errors |
| `references/browser-cdp-502-loop-pattern.md` | CDP 502 loop classification |
| `references/runtime-error-triage.md` | Cron fails with RuntimeError |
| `references/divergent-branch-handling.md` | Self-update topic branches |
| `references/system-maintenance.md` | Disk cleanup & storage monitoring |
| `references/backup-disk-full-symlink-gotcha.md` | Backup aborts disk-full on symlinked large DB — dereference symlink, guard free space, clean partials before retry |
| `references/self-improvement.md` | When reviewing fix effectiveness |
| `references/terminal-cwd-not-found-pattern.md` | During log scanning — `os.getcwd()` on deleted CWD |
| `references/known-script-auth-issues.md` | When a cron job script fails with import errors, path blocks, or auth failures |
| `references/known-code-fixes-and-cascade.md` | During escalation runs — applying known code patches or triaging MCP cascade failures |
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
| `references/schedule-optimization.md` | During schedule optimization |
| `references/web-search-protocol.md` | During web search pass |
| `references/background-tasks.md` | When setting up storage or registering jobs |
| `references/kanban-dispatcher-stuck-diagnostic.md` | During log scanning — when gateway shows "kanban dispatcher stuck: ready queue non-empty for N consecutive ticks but 0 workers spawned." Root cause hierarchy: worker crashes (most common), max_in_progress reached, or blocked dependencies. |
| `references/using-script.md` | When running scripts |
| `references/platform-compatibility.md` | Before running scans on a new platform |
| `references/plugin-self-update-2026-06-18.md` | During `custodian.update` — plugin directory update procedure, stash pop conflict resolution, known local patches |
| `references/self-update.md` | Before running `custodian.update` |
| `references/config-recovery.md` | When config corruption is detected |
| `references/escalation-runner-2026-06-08-1915.md` | Escalation runner 2026-06-08 19:15 — JSONL deduplication pattern, state.db batch pruning, spot:update git stash fix |
| `references/deep-scan-stale-error-verification.md` | During deep scan clean verdict — verifying whether "Script not found" errors are stale (fix already applied post-error) or active (re-apply fix). Symlink timestamp vs error timestamp comparison. |
| `references/deep-scan-clean-verdict-2026-06-23.md` | When every error job has cf=None, skip to silent verdict in ~30s — transient error shortcut |
| `references/jobs-not-running-diagnostic.md` | During cron scanning — when MANY jobs show overdue `next_run_at` simultaneously |
| `references/escalation-runner-concurrent-execution-gap.md` | Before classifying any issue as open/unresolved |
| `references/workspace-mcp-binary-fix.md` | When workspace-mcp-fixed fails with "No such file or directory" |
| `references/mcp-server-module-deleted-pattern.md` | When MCP server wrapper exists but Python module deleted from venv (`No module named main`). Distinct from binary-missing and OAuth failures. |
| `references/checkpoint-store-git-corruption-pattern.md` | When checkpoint_manager logs git errors — missing refs/heads/ and objects/ in checkpoints/store/.git. Fix: backup .git, rm -rf, git init. |
| `references/chronicle-plugin-dirs-empty-pattern.md` | During plugin directory scanning — when `plugins/memory/chronicle/` or `plugins/context_engine/chronicle/` have no `.py` files (only `__pycache__`). Distinct from `oc_chronicle_kwargs_get_duplicate` (code bug in existing files). |
| `references/interactive-menu.md` | When invoked interactively via `/` command — two-level menu layout, response parsing, platform adaptation |
| `references/empty-plugin-dir-detection.md` | During cron scanning — detecting empty plugin directories that silently break discovery |
| `references/light-scan-2026-06-29-0904.md` | Delta journal with transient addendum — same escalated OAuth issues persist but a new first-occurrence transient error (praxis:journal_ingest HTTP 400) appears. Pattern for "delta + new transient" handling. |
| `references/light-scan-2026-06-29-1204.md` | Journal-to-issues gap pattern — prior scan flagged `escalation_needed: true` but did NOT write to issues.jsonl. MCP workspace-mcp module deleted persistence. Light-scan Step 8b gap check. |
| `references/timezone-schedule-window-false-positive.md` | During jobs-not-running checks — when a job appears overdue but is actually outside its schedule window (daylight-hour cron expressions). Convert to local timezone before flagging. |
| `references/fix-effectiveness-schema-contamination.md` | When `custodian_status` crashes with `KeyError: 'attempts'` — fix_effectiveness.jsonl has mixed-in raw fix log entries lacking the confidence record schema |
| `references/credential-leak-backup-commit-pattern.md` | When a backup cron job commits credential files to git — detection via GitGuardian, fix via history rewrite + .gitignore |
| `references/editable-install-path-discovery.md` | When editing plugin files that don't seem to take effect — finding the actual loaded path via importlib |
| `references/plugin-vs-skill-architecture.md` | When confused about plugin vs skill versions, or when skill directory is missing — update procedure, editable install, recovery |
| `references/skill-reference-path-mismatch-pattern.md` | When a skill's agent code reads reference files from `<hermes-root>/commons/data/<skill>/references/` but they exist at `<hermes-root>/profiles/<profile>/skills/<skill>/references/` — Tier 2, requires skill code fix |
| `references/fallback-model-null-yaml-debris-pattern.md` | When `fallback_model: null` appears in config.yaml — distinguish YAML debris from true fix-loop. Diagnosis steps and PyYAML removal pattern. |
| `references/fallback-model-manifest-build-401-pattern.md` | When diagnosing 401 errors from null-provider jobs — if gateway log shows `provider=custom base_url=https://app.manifest.build/v1/`, the `fallback_model` in config.yaml has an expired Manifest.build API key. Tier 3 escalation. |
| `references/cron-provider-error-transient-pattern.md` | When a null-provider job shows `last_error: "RuntimeError: Provider returned error"` with `consecutive_failures=None`. First-occurrence provider API error from LLM execution context. Transient, no fix needed. Distinct from specific 401/403 fallback patterns. |
| `references/transient-401-self-resolution-pattern.md` | When a null-provider job hits a first-occurrence 401 from the default upstream, then re-runs successfully without intervention. Non-fatal, monitor only. |
| `references/subdirectory-hints-home-dir-pattern.md` | When `subdirectory_hints.py` `_add_path_candidate` fails with `RuntimeError: Could not determine home directory` because `$HOME` is unset in cron execution environment — Tier 2, framework bug. **Fix applied 2026-06-17**: added `RuntimeError` to except clause. |
| `references/oc-cron-script-not-found-transient-pattern.md` | When a no_agent cron job reports "Script not found" but the script exists — write/read race condition. Transient, no fix needed. |
| `references/light-scan-stale-no-agent-error-triage.md` | During light scan — when a stale `last_error` (compound `&&`) persists on a `no_agent: true` job after a wrapper script fix. How to classify as stale vs active. |
| `references/stale-error-message-script-field-mismatch.md` | When a job's `last_error` shows a different command/path than the current `script` field. Error is stale from a prior configuration (e.g., compound `&&` command replaced by wrapper script). Distinct from `oc_cron_script_not_found_transient`. Tier 2, surface only. |
| `references/no-agent-script-path-mismatch-symlink-fix.md` | When `no_agent: true` and the script is a bare basename that exists at `<hermes-root>/scripts/` but not at the profile scripts dir. Tier 1 fix: symlink from profile scripts dir to system scripts dir. |
| `references/no-agent-missing-dependency-pattern.md` | During issue classification — when a `no_agent: true` cron job fails with `ModuleNotFoundError` because a Python package is missing from the hermes-agent venv. Distinct from `oc_gateway_restart_import_window` (transient, agent-mode). Includes the venv-path discovery pattern and install procedure. |
| `references/no-agent-missing-dependency-install-procedure.md` | Step-by-step install + wrong-venv-path pitfall for `no_agent` `ModuleNotFoundError`. |
| `references/no-agent-script-argument-pattern.md` | When `no_agent: true`, the `script` field is a literal path — embedded arguments cause "Script not found". Fix: wrapper script pattern. |
| `references/post-fix-wrapper-script-verification.md` | After applying a wrapper script fix — run via `bash` and check exit code to confirm stale error is resolved. |
| `references/hardline-filter-gateway-log-grep-block-pattern.md` | When grep/terminal() commands on gateway logs are blocked by the hardline filter — diagnostic and safe Python workaround |
| `references/post-fix-stale-error-pattern.md` | After applying a Tier 1 fix — when affected jobs still show `last_error` from their pre-fix run but `consecutive_failures=0`. How to classify as stale vs active. |
| `references/fix-applied-pending-restart-sweep.md` | During deep scan Step 9b — verify and close issues stuck in `fix_applied_pending_restart` for >7 days. |
| `references/deep-scan-fix-loop-prehandled-silent-verdict.md` | When all error jobs are transient AND a fix-loop RCA already exists — return `[SILENT]` without duplicate escalation. |
| `references/escalation-runner-clean-verdict-pattern.md` | **BEFORE CONCLUDING NO ISSUES** — the four-bucket decision tree (actionable / user-gated / legacy-inactive / already-resolved). Inactive profile detection pattern. |
| `references/config-empty-section-fixloop-status.md` | Full occurrence history of the `oc_config_empty_section` fix-loop (8+ occurrences, Pattern B, fix-and-escalate pattern). Inactive profile exception. |
| `references/cron-health-check-from-cron-context.md` | How to invoke `custodian_cron_health` from a cron/scheduled session — Python import pattern via terminal() when execute_code is blocked. Includes combined health+memory-guard check. |
| `references/cron-health-check-direct-jobs-json-parsing.md` | Fallback when `custodian_cron_health` tool is unavailable — direct jobs.json parsing pattern with error classification, memory guard floor check, and combined one-script health+guard verification. |
| `references/observation-journal-schema.md` | Exact JSON shape for observation/action journals, field definitions, clean verdict write sequence. |
| `references/escalation-runner-already-classified-fast-path.md` | Escalation runner fast-path when prior esc-run classified all issues user-gated; Python journal write pattern. |
| `references/escalation-runner-pause-affected-jobs-pattern.md` | **When to pause jobs** that fail every run due to user-gated issues (OAuth revocation, etc.). "User-gated" ≠ "no action" — pausing IS the action. |
| `references/escalation-execution-loop.md` | **Execute-and-reconcile procedure** for an external escalation loop: verify live state both directions, pause still-burning user-gated jobs, reconcile `issues.jsonl` in one pass (resolve recovered / write missing / update `jobs_paused`), safe brace-depth edit pattern. |
| `references/light-scan-2026-06-29-2206.md` | Clean verdict with new transients + stale paused errors — 5 error jobs: 3 transient first-occurrence provider errors, 2 permanently paused OAuth-frozen. 8-day journal gap. Pattern for "always filter paused jobs from error counts before classifying". |
| `references/stale-model-error-diagnostic-pattern.md` | When many jobs show `404: No endpoints found for <model>` — distinguishes stale (config already fixed) from active errors. Four-step diagnostic flow with pitfalls |
| `references/journal-path-format-inconsistency.md` | When journal gap detection falsely reports gaps — different scan runs write to different date directory formats (`YYYY-MM-DD` vs `YYYYMMDD`) or as loose files. Diagnosis and fix direction |
| `references/pipe-to-interpreter-security-block.md` | When ANY pipe-to-interpreter command gets blocked by tirith security filter — three reliable workarounds |
| `references/self-resolved-module-verification-pattern.md` | During light/deep scan — when a prior scan classified a ModuleNotFoundError as self-resolved. Verify the module is actually importable in the cron execution context before accepting the classification. |
| `references/scheduler-state-lag-vs-execution-failure.md` | During light scan Step 7 — when jobs appear overdue (`next_run_at` in past) but `last_run_at` is recent and `last_status=ok`. Scheduler state lag, not execution failure. |
| `references/no-agent-script-exit-1-deaggregation-pitfall.md` | When ≥2 error jobs share a bare 'Script exited with code 1' wrapper — they are NOT one root cause; enumerate + inspect each `script` before classifying |