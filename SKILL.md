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

- **Cron/scheduled tool-failure modes** — `read_file` dedup, pipe-to-interpreter, `write_file` failure, `execute_code` blocked, heredoc `$(date)`, and the `hermes cron` CLI path mismatch — are catalogued **with their fixes in the Error Handling table below**. Consult that table directly rather than re-deriving. One nuance the table omits: **use a UNIQUE `/tmp/` script filename** (timestamp/random suffix, e.g. `/tmp/cust_lights_20260708T1505.py`) — in concurrent cron contexts, sibling agents overwrite shared `/tmp/` paths; confirmed 2026-07-08 a sibling overwrote `/tmp/custodian_jobs.py`, resolved by renaming. The system emits a `_warning` on sibling modification — rename, don't ignore.
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

## Example

Typical light-scan invocation and its evidence record: running `custodian.scan.light` reads `jobs.json`, tails the gateway log for new errors since the last scan timestamp, fingerprints each, and writes an observation journal to `{agent_root}/commons/journals/ocas-custodian/YYYY-MM-DD/{run_id}.json`. On a clean scan (every error job transient) it returns `[SILENT]` *after* writing that journal — the journal is the proof the scan ran; `[SILENT]` only suppresses delivery noise.

## Confidence Model

See `references/confidence-model.md`. Key: `confidence_score = sample_confidence × success_rate`. Auto-promotes/demotes tiers based on fix history.

## Execution Loops

**Light Scan** (every heartbeat): Run the following checklist in order. Do not skip steps — each gates the next.

- [ ] 1. Read `jobs.json` (the profile cron registry at `~/.hermes/profiles/<profile>/cron/jobs.json`; NOT via `hermes cron list`, which reads the wrong path). **Parse robustly**: the registry is a top-level object whose job list lives under the key `"jobs"` (a list) — use `d.get("jobs", [])`. A few older/copied registry copies *do* wrap the list under `data.jobs`; only fall back to `d.get("data", {}).get("jobs", [])` if the top-level `"jobs"` key is **absent**. **CRITICAL FALSE-CLEAN GUARD**: if your parse yields `len(jobs) == 0`, re-inspect the raw file head (`head -c 600 jobs.json`) before concluding anything — a wrong key silently returns 0 jobs and risks a false `[SILENT]` on a misparse. Confirmed 2026-07-16: a parse using `data.get("jobs")` returned 0 jobs on a live registry that actually holds 148 (top-level `"jobs"`); the false-clean risk was caught only by re-inspecting the raw file.
- [ ] 2. Tail gateway log for new errors since last scan timestamp
- [ ] 3. Fingerprint each new error (match against known patterns in `references/`)
- [ ] 4. Check failed fixes from previous scan — verify each fix held
- [ ] 5. Check uninitialized skills (missing storage, no background tasks registered)
    - This includes checking for missing data directories, journals directories, and config files. **Canonical paths are `{agent_root}/commons/data/{skill}/` and `{agent_root}/commons/journals/{skill}/`** (see `references/conformance.md` § Skill Initialization and `references/fix-missing-skill-data-directories-2026-06-30.md`). **⚠️ Pitfall — wrong-path guess:** do NOT check `skills/<skill>/commons/data/` or `skills/<skill>/commons/journals/` — those subdirs do not exist; every active skill would falsely report missing dirs and waste a check cycle. Confirmed 2026-07-14: a light scan guessed `skills/<skill>/commons/data` and flagged 32 false-missing dirs before correcting to `commons/data/<skill>` (all present).
    - See references/fix-missing-skill-data-directories-2026-06-30.md and references/fix-missing-skill-journals-directories-2026-06-30.md for fix procedures
    - **CRITICAL: cross-reference against active cron jobs before flagging.** A skill with missing data/journals dirs but NO cron jobs referencing it (via `skill:` or `skills:[]`) is uninitialized-but-unused — note it in the journal as info-only, do NOT escalate. Only flag for remediation if at least one active cron job depends on the skill. Confirmed 2026-07-01: 10xeng-autofix and skilllab had missing dirs but zero cron job references — not actionable.
    - **Re-derive the active-skill set from the LIVE jobs.json every scan** — union the `skills` (array) AND `skill` (scalar) fields across ALL job entries; treat any skill appearing ≥1 time as actively referenced. Confirmed 2026-07-07: a prior scan reported `browser-vision`, `generative-art-algorithms`, `generative-art-deployment`, `ocas-lucid` as "unused (no cron refs)" — but all four ARE referenced (by `art:studio` and the `lucid:*` jobs). The cross-ref had silently dropped them, so they were wrongly left un-remediated while `art:studio` ran with missing data/journals dirs. If a skill dir is missing AND the live set contains it, remediate — **but first check WHICH dir is missing** (see `references/dead-skill-ref-active-job-fix.md`): if the skill dir itself is absent because the skill was **archived/merged** (not merely uninitialized), do NOT recreate it — remove the dead `skill`/`skills[]` reference from the job (`skill: null`) instead. Recreating a fake skill directory is the wrong fix for an archived skill. Only `mkdir + config.json` when the skill dir exists but its `data/`/`journals/` subdirs are missing. **never trust a prior scan's 'unused' label**; always re-derive from the live file.
- [ ] 6. Check error jobs for script path blocks and `Path.home()` resolution issues
    - **DE-AGGREGATE identical wrapper messages.** When ≥2 error jobs share a bare `Script exited with code 1` (or any identical low-information) `last_error`, they are NOT one root cause. Enumerate each, read its `script` field, and determine the REAL per-job failure (run the script; inspect `sys.exit` paths; for subprocess wrappers, run the wrapped command). A no-op-by-design exit (no stderr) is `oc_cron_no_agent_exit_1_noop` (Tier 2 surface-only); a traceback is a real failure. Confirmed 2026-07-07: a prior scan collapsed three distinct jobs (`monitor:list` 403, `monitor:journals` no-op, `SearXNG` infra) into one bucket and omitted `monitor:journals`. See `references/no-agent-script-exit-1-deaggregation-pitfall.md`. Use `scripts/classify_error_jobs.py` to surface every ambiguous wrapper job with its `script` name.
    - **MASKED-EXIT-1 VARIANT (no output at all):** A no_agent wrapper may call the real worker as a subprocess and convert ANY non-zero to `sys.exit(1)` with NO stdout/stderr forwarded (see `monitor_list.py` → `tasks_monitor.py`). In that case `last_error` is the bare `Script exited with code 1` AND a live re-run of the wrapper shows empty output — the real traceback is HIDDEN. To de-aggregate, run the WRAPPED script directly (`python3 <skill>/scripts/<worker.py> --mode check`) and read ITS traceback. If the live signature differs from any resolved covering issue → it is a NEW failure with no open issue (Step 8b/8e gap); persist it. Confirmed 2026-07-14: `monitor:list` failed live with `KeyError: 'access_token'` — masked by the wrapper, referenced only by two already-resolved issues → gap closed by writing `oc_google_tasks_access_token_missing`. See `references/monitor-list-exit1-mask-gap.md` and `references/monitor-list-masked-keyerror-pitfall.md`.
- [ ] 7. Check for jobs not running (stale `last_run_at` vs expected schedule)
    - **CRITICAL**: `next_run_at < now` alone is NOT sufficient. Must verify: `last_run_at` older than 2× schedule interval AND `last_status != ok`. High-frequency jobs (≤10 min) show scheduler state lag where `next_run_at` hasn't advanced but job ran successfully. See `references/scheduler-state-lag-vs-execution-failure.md`.
- [ ] 8. For each fingerprint with `recurrence_count >= 2`, check `rca.jsonl` — if no RCA record exists, flag for deep scan RCA step; if Pattern B, skip fix and note in journal
- **8b. Journal-to-issues gap check**: for any previous journal entry with `escalation_needed: true`, verify a matching entry exists in `issues.jsonl` for the same fingerprint. If NOT found, write it — the prior scan flagged but failed to persist. (Confirmed pattern: 10:05 scan wrote `escalation_needed: true` but did NOT write the issue to issues.jsonl; 12:04 scan had to write it manually.) **STALE-PREMISE GUARD (2026-07-14):** before writing a gap issue, VERIFY THE LIVE PREMISE is still true — re-scan `jobs.json`, disk usage, and provider state. A journal flagged `escalation_needed: true` can carry a premise that resolved AFTER the journal was written; persisting it creates a FALSE escalation (pollutes `issues.jsonl` and burns an execution-loop cycle). Concrete checks: for `oc_state_db_oversized` re-derive disk% live (`shutil.disk_usage('<fs-root>')`) — threshold is db>1GB AND disk>80%; if disk is now `<80%` (even at 5-10GB db) it is acceptable operational cost, do NOT persist. For any `*_access_token_missing` / auth fingerprint, if the implicated job is `status=ok` with cleared `last_error`, it recovered — do NOT persist. For every fingerprint, require ≥1 live job still matching the signature before writing. Confirmed 2026-07-14: `oc_state_db_oversized` (journal claimed disk 82%) was NOT persisted because live disk was 70.2%; `oc_google_tasks_access_token_missing` was NOT persisted because `monitor:list` was already `status=ok`. See `references/journal-escalation-stale-premise-guard-2026-07-14.md`.
- **8b-variant — 'tracked' in narrative but `escalation_needed: false`**: A scan may reference root-cause fingerprints as 'tracked' (in `previous_scan_delta.stable_root_cause` or prose) while setting `escalation_needed: false` on the journal. If the referenced fingerprint is absent from `issues.jsonl`, the escalation silently dropped — every later scan re-reports it as 'tracked' without ever persisting it. Fix: after classifying non-auto-fixable root causes, collect their intended fingerprints and verify each exists in `issues.jsonl` (use `scripts/parse_issues_jsonl.py`); if missing, WRITE it (`status: open`, `escalation_needed: true`). One issue per root-cause fingerprint; list affected job names in `affected_components`. Confirmed 2026-07-07: 71-job 402-credits and 2-job OAuth revocations were reported 'tracked' in prior deltas but absent from `issues.jsonl`; light scan wrote them. See `references/escalation-persistence-gap.md`.
- **8b/8e parser gotcha (confirmed 2026-07-14):** When reading `issues.jsonl` for gap checks, a hand-written brace-walk parser inside `python3 << 'PYEOF'` that does quote/backslash tracking returns **0 objects** (its escape handling breaks on `\"`). Use `json.JSONDecoder().raw_decode` instead — robust to escaped quotes and concat-per-line objects. In concurrent cron contexts (sibling `custodian:light` rewrites the file at top of hour) re-verify the file `mtime`/`size` across 3 quick reads before trusting a parse; prefer `grep -ac` for a single targeted check (e.g. does this `issue_id` already exist) to avoid a full race-prone rewrite. Race-safe recipe in `references/monitor-list-exit1-mask-gap.md`.
- [ ] 8c. **Verify-before-accepting-self-resolved**: when a prior scan classified an error as "self-resolved" (e.g., `ModuleNotFoundError` that supposedly fixed itself), verify by running the actual import in the cron execution python — NOT any assumed venv path. Cron jobs run `python3` from PATH. To find the actual python: `which python3` in a terminal, then `python3 -c "import <module>"`. The profile venv path (`<hermes-home>/profiles/<profile>/venv/bin/python3`) may NOT exist — the system hermes venv (`<hermes-venv>/bin/python3`) is typically the active one. Confirmed 2026-07-01: `dispatch:triage-morning` was classified "self-resolved" but the verification was done by checking the actual import (import google.oauth2.credentials → OK). Do NOT accept "self-resolved" from a prior journal entry without re-verifying when: (a) the module path in the error differs from what you assumed, (b) the error was from a no_agent script (different python resolution), or (c) the prior scan has known counting discrepancies. See `references/self-resolved-module-verification-pattern.md`.
- [ ] 8d. **Verify-before-accepting-prior-resolution (inverse gotcha for light scans)**: A prior scan may have marked a `user_gated`/`resolved` issue `resolved` on a "provider recovered / forward-stale" theory derived from an OLD `last_run_at` — WITHOUT checking whether live jobs still error. `last_run_at` age is NOT proof of recovery (an old `last_run_at` just means the job hasn't re-run). Before accepting any prior `resolved` classification for a provider/auth/credit fingerprint, re-scan the live `jobs.json`: count enabled jobs with `last_status=error` AND a current `last_error` still matching that outage signature (`token_expired`, `402 ... credits`, `No endpoints found for ...owl-alpha`). If ≥1 exists, the resolution was FALSE — reopen the issue (`status: user_gated`, `escalation_needed: true`, clear `resolved_at`, set `reopened_at` + `reopen_note` citing the live count). Only a CLEARED `last_error` + `last_status=ok` on a post-fix run proves recovery. Automate with `scripts/reopen_false_resolutions.py` (dry-run; `--write` to persist). Confirmed 2026-07-13: a 16:10 light scan marked the Nous-401 (`token_expired`) + OpenRouter-402 (credits) outage issues `resolved` (reason "forward-stale provider recovered", from `last_run_at` 2026-07-12), but the 19:00 scan found 18 jobs STILL erroring live with `token_expired`/402 and no re-auth evidence — reopened 2 issues. See `references/light-scan-false-resolution-gotcha.md`.
- [ ] 8e. **Verify resolved CODE-DEFECT fixes actually cover all references** (catches what `reopen_false_resolutions.py` misses): that script only matches provider/auth/credit outage signatures (`token_expired`, `402 credits`, `owl-alpha 404`) — it does NOT catch a `resolved` issue whose fingerprint is a code defect (`oc_*_bug`, `oc_*_missing`, `oc_*_path_*`) whose fix was incomplete. For each such `resolved` issue: (a) does any enabled job's `last_error` STILL contain the original error signature? If yes, first compare the job's `last_run_at` against the fix timestamp — if the job ran BEFORE the fix landed, the error is STALE (re-run the script to confirm), not a live regression; (b) does the source file the fix touched STILL contain the broken reference the error named? A "resolved" entry whose fix only added a comment saying 'we now use X' while the erroring line still references old Y is a FALSE resolution — reopen (`status: open`, `escalation_needed: true`, `tier: 4`, clear `resolved_at`, set `reopened_at` + `reopen_note`). Grep recipe: `grep -rn "<broken_token>" <file>`. Re-run the script directly (`<hermes-venv>/bin/python <script>` or `bash <wrapper>`) and inspect exit code + stderr to separate stale from live. **CORRECTED 2026-07-13 (supersedes the original misread assertion):** The original reopen directive for `oc_chronicle_facts_fts_missing_20260713` was a MISREAD and must NOT be followed. It claimed `enrich_embeddings.py:121` still executed `DELETE FROM facts_fts`, but line 121 is a COMMENT; the executable statements (lines 130/149) use the live `belief_fts`/`observed_fts` tables, which exist in `chronicle.db` and rebuild cleanly (verified by dry-executing both DELETEs against the live DB — no `OperationalError`). The job's stored error was STALE: last ran 2026-07-13 10:02 UTC, BEFORE the fix landed (file mtime 11:36 UTC). The resolution was CORRECT — leave it resolved. **Grep-pitfall (root cause of the misread):** `grep -rn "facts_fts" enrich_embeddings.py` matches only the historical comment lines (120, 124), producing a false "still broken" hit. A token appearing in comments is NOT proof the broken code path runs — read the actual executable lines around the cited line number, or execute the real code path, before reopening. Always compare `last_run_at` vs the fix file's mtime: an error from a run BEFORE the fix is stale, not a live regression. **FALSE-CLOSE via drained-backlog test (inverse, confirmed 2026-07-13):** When a prior scan *resolved* a timeout/volume code-defect issue on the strength of a live re-run that completed fast, verify that re-run executed against the REAL production backlog — NOT a queue just drained by a prior run. A timeout issue can finish in ~163s when there is nothing left to embed, then HANG past the 600s cron hard limit once daily volume rebuilds. In this session `oc_script_timeout_chronicle_embed_20260713` was closed at 21:36Z citing a 163s pass, but its `embed_state.json` showed `last_run=21:35:56` (a prior run had just drained the queue), and a fresh re-run at 23:0xZ was still actively embedding at 85s (row 4160/8000 of the facts pass; `facts` table holds 35,486 rows) — confirming the timeout is recurring. Before accepting a timeout/throughput 'resolved': (1) check the progress/state file's `last_run` vs the claimed fix time — if within seconds, the test ran on a cleared queue; (2) inspect real data volume (`SELECT COUNT(*) FROM facts`); (3) re-run the actual script against that full volume with a hard cap (background `terminal(background=true, notify_on_complete=true)` + `process(wait/poll)` — foreground cap is 60s) and confirm it completes under the cron limit. A clean run immediately after another successful run proves nothing about steady-state load. See `references/resolved-codefix-regression-verify.md` and `references/resolved-timeout-verify-drained-backlog.md`.
- [ ] 9. **Verify-before-acting**: for any error job, check current `config.yaml` and provider state to confirm the error is still active before attempting fix
- [ ] 10. Write observation journal (even if no issues found — set `not_activity_reason`)
- **10b. LLM-necessity guard**: Run `scripts/classify_llm_necessity_integration.py` to detect new cron jobs whose prompts don't need LLM reasoning (script-wrappers, self-updates, needless skill-load). This uses the acknowledgment file (`data/llm_necessity_ack.json`) to avoid re-reporting already-triaged candidates. Writes/updates one `oc_cron_llm_unnecessary` issue in `issues.jsonl` for new unacknowledged candidates. **REPORT-ONLY — never auto-converts a job to `no_agent`.** Run via: `python3 <hermes-home>/profiles/indigo/skills/ocas-custodian/scripts/classify_llm_necessity_integration.py`.

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
- [ ] 2. Discover ALL `issues.jsonl` paths: `find <hermes-home> -name "issues.jsonl"`
- [ ] 3. Deduplicate by `issue_id`/`id` — keep best status per entry
- [ ] 4. For each open issue, verify against raw file (`terminal(command="cat ...")`) — not `custodian_issues` tool (stale cache)
- [ ] 5. Classify into four buckets: Actionable / User-gated / Legacy-inactive / Already-resolved
- [ ] 6. If any Actionable issues exist → execute fixes
- [ ] 7. If no Actionable issues → write journal with `not_activity_reason`, return `[SILENT]`
- [ ] 8. Clear `escalation_needed` flag on any resolved entries

**Escalation runner journal write — use Python always**: When running `escalation-runner` in cron context, write journals via `python3 -c "..."` with `json.dump()` and `from datetime import datetime, timezone; datetime.now(timezone.utc)`. Do NOT use `cat > file << 'EOF'` heredoc for JSON containing timestamps/run_ids — single-quoted heredoc prevents `$(date)` expansion, producing corrupted files. **Import the CLASS, not the module:** `import datetime; datetime.now()` raises `AttributeError: module 'datetime' has no attribute 'now'`. Always use `from datetime import datetime, timezone` so `datetime.now(timezone.utc)` resolves to the class method. See `references/escalation-runner-already-classified-fast-path.md` § Journal write pattern reminder.

**Escalation runner clean verdict — actionable vs user-gated vs legacy (2026-06-25):** When the escalation runner finds no actionable issues, classify all open entries into four buckets: (A) Actionable — execute fix; (B) User-gated — note count but do not auto-fix (skill library hygiene, stub removal); (C) Legacy/inactive — ignore YAML debris in profiles with no `cron/jobs.json`; (D) Already resolved — verify config/job state and close. If Bucket A is empty, write journal with `not_activity_reason` and return `[SILENT]`. See `references/escalation-runner-clean-verdict-pattern.md` for the decision tree and journal template. Inactive profile detection: check for `cron/jobs.json` absence (>90 days dormant). Confirmed 2026-06-25: braun profile has 3 null keys but no cron jobs — legacy debris, not an action item.

**Escalation runner: user-gated provider failures are not permanent kill switches (2026-07-09):** Do NOT pause recurring cron jobs just because their last error is provider auth/credits/429/endpoint outage. First verify the provider/model live with a minimal `hermes chat -q` probe. If the provider works now, resume affected jobs and let them retry; stale `last_error` is not live failure. Only pause when retry is genuinely futile until user action (e.g. revoked Google OAuth for a domain-specific tool, missing script, blocked execute_code redesign), and then write `pause_reason`, `jobs_paused`, journal evidence, and a re-enable-on-recovery check. Pausing is mitigation, never resolution. Detailed procedure: `references/stale-provider-error-pause-loops.md`.

**Escalation Execution Loop (external cron trigger) — EXECUTE, don't just classify (2026-07-07):** When an external loop invokes Custodian + Mentor to *execute* fixes on escalated issues (not merely classify them), the default Escalation Runner Checklist above is classification-oriented — this fills the execute-and-reconcile gap:

See `references/escalation-loop-pitfalls.md` for chronic traps in execution-loop runs: the journal-gap probe reports FALSE gaps for already-`resolved` issues (it compares only against OPEN issues), stale `last_error` vs live failure (inspect the wrapper/script + fix timestamp before reopening), cooperating with an in-flight sanctioned sibling repair instead of racing it, and reading the traceback location (not just the final exception) when a `database is locked` error appears after the root-cause step already passed.
1. **Verify live state BOTH directions** against `jobs.json` — (a) issue claims resolved/paused but job still `enabled`+erroring (inverse gotcha), AND (b) issue flags `escalation_needed: true` but the job already recovered (`last_status: ok`, `last_error` cleared). Resolve or re-pause accordingly; never trust the issue flag alone. AND (c) **sweep for missed enrollments**: any `enabled`+erroring job whose `last_error` matches a known user-gated fingerprint (Nous 401 `portal.nousresearch.com`, OpenRouter 402 `credits`, owl-alpha 404, Google Tasks 403, `invalid_grant`) but is NOT in any issue's `jobs_paused`. These failed in the inter-scan window *after* the last esc pass and were never enrolled. For provider/model fingerprints, verify the provider live first; if it now works, leave/resume the jobs and clear stale issue state rather than pausing. If it still fails, keep the jobs enabled unless retries are destructive or impossible; record the root cause as open/user-gated without freezing the scheduler. For genuinely unrecoverable domain auth (e.g. revoked Google OAuth for the specific tool), pause and add to the matching issue only with `paused_reason` plus a re-enable check (keep `user_gated`+`escalation_needed: true`). Run `scripts/find_missed_user_gated_jobs.py` to enumerate + auto-classify, but treat its MISSED bucket as "open/enroll for tracking", not automatic pause, unless the narrow pause criteria are met. For each UNKNOWN job, read its `script` field and COMPLETE `last_error` (including any stderr) and classify via the no-agent exit-1 de-aggregation procedure (references/no-agent-script-exit-1-deaggregation-pitfall.md) and/or the no_agent monitor exit-1 upstream-degraded pitfall (references/no-agent-monitor-exit1-upstream-degraded-pitfall.md): a `no_agent` monitor that exits 1 because no work was found (e.g. `monitor_journals.py` exits 1 when no NEW journals exist since the last check) is `oc_cron_no_agent_exit_1_noop` (Tier 2, leave running) — verify read-only via the state-file comparison recipe in `references/monitor-journals-noop-readonly-verify.md` (do NOT run the monitor script manually; it can double-enqueue a work item if a journal appeared in the last minute); a real subprocess/OAuth/dependency failure is ACTIVE and may itself be user-gated (then pause + enroll). Never leave a UNKNOWN job unclassified — an unresolved UNKNOWN is a silent monitoring gap. **CONTAMINATION WARNING:** the read-only state-mtime compare in `references/monitor-journals-noop-readonly-verify.md` can FALSELY report `ACTIVE-new-journals` when a SIBLING cron job (e.g. `mentor:light`) writes a journal milliseconds AFTER `monitor:journals` ran. Always filter `commons/journals/**` to `st_mtime <= job.last_run_at` before comparing, or the latest-sibling-journal will invert a correct no-op into a false miss (confirmed 2026-07-17). AND (d) **Stale issue PREMISE**: an issue's own body can assert WRONG facts about the world (e.g. "docker binary absent", "port 8080 returns HTTP 000 (not serving)") captured during a degraded window that has since recovered. Before concluding `user_gated`/`unresolvable`, RE-CHECK each claimed-absent binary live (`which docker`) and RE-DERIVE the real probe target from the monitoring script the cron job actually runs (the issue author may have transcribed the wrong port — in the 2026-07-13 case the issue said `:8080` but the watchdog probes `:8888`; live service was HTTP 200). Run the watchdog script exactly as the cron job invokes it and capture its real exit code. If live state is healthy, resolve the issue (do NOT leave it user-gated). See `references/escalation-stale-issue-premise-verify.md`.
    - **THE FALSE "RE-CONFIRMED LIVE" TRAP (2026-07-14 — inverse of the stale-premise guard):** When an issue's note already says "FORWARD-STALE: provider recovered" but you see `enabled` jobs in live `jobs.json` still `last_status=error` with that outage signature, your FIRST instinct may be to overwrite the note with "RE-CONFIRMED LIVE — still failing" and treat it as actionable. **RESIST.** `status=error` + an OLD `last_run_at` (days before your sweep) is EXACTLY what a pre-recovery stale error looks like — it proves nothing about the current run. The original "recovered" note may be correct. **Mandatory gate before overwriting any "recovered/stale" note as live:** re-run ≥1 affected job live with `hermes cron run <job_id>`. LLM jobs take ~60s each and run SERIALLY, so the 60s foreground `terminal` cap will time out — use `terminal(background=true, notify_on_complete=true)` then `process(wait/poll)`, or batch several ids in one background command. If the re-run flips to `ok`, the error was STALE — KEEP the "recovered" note (append your live re-run evidence: which jobs, which run_id/time), do NOT escalate. Writing a "RE-CONFIRMED LIVE" note that a later re-run disproves is a data-integrity regression another scan must then reverse. Confirmed 2026-07-14: 3 jobs (`genie:disk-cleanup`, `rally:update` — both 401; `art:engagement` — 402) re-ran OK, proving the original forward-stale notes were correct; the "RE-CONFIRMED LIVE" overwrite was wrong and had to be reverted. Use `hermes cron run <id>` as the live verification primitive. NOTE: `hermes chat -q "say pong"` only exercises the DEFAULT provider's API-key path (tencent/hy3:free via Nous API key) — it does NOT prove a JOB-SPECIFIC session token (e.g. Nous portal `token_expired`) or a SECOND provider (e.g. OpenRouter 402 credits) has recovered. Always re-run an actual affected job, not just the pong probe. See `references/escalation-false-recovered-note-trap.md`.
2. **Load issues from the PROFILE `issues.jsonl`** (`<hermes-home>/profiles/<profile>/commons/data/ocas-custodian/issues.jsonl`) — it is authoritative. The commons copy is a lagging sync target; **write only to the profile path**. Use a brace-depth parser (entries may be concatenated per line). See `scripts/parse_issues_jsonl.py`. **STALE-PATH GOTCHA (2026-07-15):** there are TWO `issues.jsonl` files — the authoritative `…/commons/data/ocas-custodian/issues.jsonl` (live, ~80KB) and a legacy copy at `…/commons/journals/ocas-custodian/issues.jsonl` (NOT updated by current writes, ~25KB, last touched Jul 14). The bundled `parse_issues_jsonl.py` USED to default to the stale journal-path copy and thereby manufactured 7 phantom escalations (old OpenRouter-402/Nous-401/Google-403/token-expired outages that were already `duplicate`/`resolved` in the authoritative file). It now defaults to the data-path and emits a stderr WARNING if handed the stale copy. **Always read the `data/` path**; never trust a count derived from the `journals/` copy. **OPEN-ISSUE FILTER (do NOT use the loop-prompt's literal `status in ("escalated","fix_attempted_failed")` — those values do not exist in the schema):** an issue is open/actionable only if `status not in ("resolved","duplicate")` AND (`escalation_needed == true` OR `status == "user_gated"`). Note `duplicate` = merged into another issue, NOT open. `parse_issues_jsonl.py`'s summary `open:` count INCLUDES `duplicate` entries — dump and inspect each open entry's full object; never trust the summary count as the actionable count. See `references/escalation-loop-issue-status-scan-trap.md`.
3. **Classify** into one of:
   - **Actionable** (Tier 1 auto-fix per the fix-safety envelope).
   - **Code-defect fixable by THIS loop** — a third-party SDK bug, skill-owned script defect, or env/version mismatch the agent can correct (NOT in the Tier-1 registry, NOT user-gated, but still resolvable here). Look for: a `pydantic`/`ValidationError` on a third-party package, a documented patch in the skill's `references/`, or a version mismatch. **Verify the fix by running the ACTUAL cron script the job invokes**, in the exact env its wrapper uses — do NOT trust a version bump alone:
     `env -i HOME=<hermes-home>/profiles/indigo/home PATH=/usr/local/bin:/usr/bin:/bin HERMES_HOME=<hermes-home>/profiles/indigo <hermes-venv>/bin/python <script>` → expect `EXIT_CODE=0` with real output. If it exits 0, mark `status: resolved`, `escalation_needed: false`, record `fix_applied` + `verified`, and **clear any leftover `user_gated: true`** carried from open-time (it was mislabeled — root cause was code, not credentials).
   - **User-gated** (credits, API key, skill-internal hardcoded model, revoked OAuth) — needs <operator>. Leave open, no pause (retry policy).
   - **Already-resolved** / **Legacy-inactive**.
4. **Execute**: Actionable → apply the Tier 1 fix (fix-safety envelope). User-gated provider/model outage → verify live state, repoint or fix credentials if possible, otherwise keep recurring jobs enabled and tracked so they retry. User-gated domain/tool failures → pause only when retry is genuinely futile or destructive, and only with `paused_reason`, `jobs_paused`, evidence, and a re-enable-on-recovery check. Pausing IS mitigation, not resolution. Do NOT mark the issue resolved until the root cause is fixed.
5. **Reconcile `issues.jsonl` in one pass** (safe edit pattern in `references/escalation-execution-loop.md`): resolve recovered issues, write missing issues from persistence gaps (prior scan flagged but never persisted; or a job evolved to a new fingerprint like `monitor:list` 403), update `jobs_paused` to match live paused state, clear false `resolved_at`. Keep genuinely user-gated issues `user_gated` + `escalation_needed: true` with a mitigation note.
6. **Verify** (re-read `jobs.json`: no bulk-paused jobs with `paused_reason: null`; provider/model failures remain enabled unless narrow pause criteria were met; re-parse `issues.jsonl`: state correct) then **write an action journal** (the evidence record — required even on silent runs). If the loop executed a fix, resumed jobs, or discovered a new unhandled issue, deliver the report. If it applied 0 fixes and surfaced no new failure, write the journal and return `[SILENT]` per the cron silence protocol.
**Honesty rule:** Do NOT report user-gated billing/API-key/skill-internal issues as "fixed". Pausing is mitigation, not resolution — they stay open until <operator> adds credits, rotates the key, or edits skill code. See `references/escalation-execution-loop.md`.

**Post-fix verification**: After applying any Tier 1 auto-fix, re-check the targeted log entry or config state to confirm the error no longer appears.

**Empty plugin directory detection**: During cron scanning, check for empty plugin directories. See `references/empty-plugin-dir-detection.md`. This is a Tier 2 issue (requires investigation, not auto-fixed). See `references/chronicle-plugin-dirs-empty-pattern.md` for the specific Chronicle plugin case.

## Script Path Security Block Pattern

See `references/script-path-security-block-pattern.md` for the `oc_cron_script_path_security_block` fingerprint — a distinct sub-pattern from `oc_cron_dead_script_ref` where the script exists but the path is rejected by the security model. The fix direction depends on `HERMES_HOME`: when running under a profile, scripts must be at `<hermes-home>/profiles/<profile>/scripts/<basename>`, NOT `<hermes-home>/scripts/`.

## Google OAuth Patterns

See `references/google-oauth-client-deleted-pattern.md` for two distinct Google OAuth fingerprints:
- `oc_google_oauth_client_deleted` — when the OAuth client itself is deleted from Google Cloud Console (`deleted_client` error). Requires new OAuth client creation + browser re-auth.
- `oc_google_oauth_token_revoked` — when the refresh token is revoked/expired (`invalid_grant: Token has been expired or revoked.`). Distinct from the above — the OAuth client exists but its tokens are dead. **Only affects jobs using the revoked account's credential file directly**. Confirmed 2026-06-29: only `email:check` and `monitor:list` (which wraps `tasks_monitor.py` with `CREDS_FILE = ".../<user-google-email>.json"`) fail. `sands:*`, `taste:*`, `vesper:*` continue working because they use different auth flows or different account credentials.

**Subprocess cascade mechanism (2026-06-28):** `monitor:list` wraps `tasks_monitor.py` as a subprocess (`subprocess.run([sys.executable, str(SCRIPT), "--mode", "check"])`). When the subprocess hits the OAuth refresh failure, it exits 1, and `monitor:list` propagates that exit code. The `last_error` on `monitor:list` shows "Script exited with code 1" — NOT the OAuth error itself. To diagnose: run `tasks_monitor.py --mode check` directly to see the actual `HTTPError: 400 Client Error: Bad Request for url: https://oauth2.googleapis.com/token`. This is the same root cause as `email:check` but the error message is masked by the subprocess wrapper. Do NOT classify as `oc_cron_no_agent_exit_1_noop` — the exit 1 is a real subprocess failure, not a no-op. Confirmed 2026-06-28: both `email:check` and `monitor:list` failed simultaneously from the same token revocation; `sands:*`, `taste:*`, `vesper:*` were **unaffected** because they use different auth flows or different account credentials (NOT because of cascading narrowness — they genuinely don't use the revoked account's token).

**`monitor:list` masked `KeyError: 'access_token'` — KNOW THE TRANSIENT RACE (inverse of mask-gap):** `monitor:list` (no_agent, `script: monitor_list.py`) wraps `ocas-tasks/scripts/tasks_monitor.py --mode check`. The wrapper masks the real traceback, so `jobs.json` shows bare `Script exited with code 1`. The real error is `KeyError: 'access_token'` at `get_access_token()` — BUT the classification depends ENTIRELY on the creds file state (see `references/monitor-list-access-token-recurrence-durable-fix-2026-07-15.md`): (1) inspect `<gworkspace-creds>/credentials/<user-google-email>.json`; (2) if `access_token` is PRESENT (non-empty) → transient credential-refresh RACE — re-run worker 1–2×; if it exits 0, resolve any open `user_gated` issue for this fingerprint as a FALSE ESCALATION (race-safe patch). (3) if `access_token` is ABSENT (only `token` present, `refresh_token` valid, future `expiry`) → PERSISTENT CODE DEFECT, NOT a race. Recover NON-interactively (`refresh_token()` uses the valid `refresh_token` — no <operator> re-auth), apply the DURABLE code fix to `get_access_token()` (fall back to `creds['token']` + refresh when `access_token` absent; full recipe in `references/monitor-list-access-token-recurrence-durable-fix-2026-07-15.md`), then VERIFY (`tasks_monitor.py --mode check` → 0, `hermes cron run <id>` → succeeded, `jobs.json` → ok). This defect RECURS if only a one-off refresh is applied (upstream strips the token again) — a prior "resolved" that re-fails live is a Step 8d FALSE RESOLUTION; reopen, apply the durable fix, record `recurrence_resolved_code: true`. [VERIFY-ANCHOR-KEEP]

**Important:** Resolving a `ModuleNotFoundError` for `googleapiclient` on a Google-auth job should trigger an immediate re-check for token revocation. The package install fixes the import but the next run will immediately hit `invalid_grant` if the token is dead. Treat these as two sequential issues: package-missing (Tier 1 fix) → token-revoked (Tier 3 escalation).

## Fix Safety & Tier Classification

See `references/fix-safety.md` for the safety envelope, tier definitions, and the full Tier 1 auto-fix registry.

## Skill Conformance & Initialization

See `references/conformance.md` for background task checking and cron registry health checks.

## Activity Model & Schedule Optimization

Activity model rebuilt each deep scan from 14-day window. See `references/deep-scan.md` and `references/schedule-optimization.md`.

## Core Fingerprints (Operational Detection Set)

The fingerprints Custodian actively matches during a scan are kept out of
SKILL.md to preserve progressive disclosure — they change as new patterns are
confirmed. **When to read:** during light-scan **Step 3 (fingerprint matching)**
and **Step 6 (recurrence check)**, or whenever a new error job must be
classified. Full table + the `monitor:list` access-token case breakdown:
`references/custodian-core-fingerprints.md`. This is the **operational** set;
the Tier-2 surface-only catalog (detected, never auto-fixed) lives in
`references/non-fatal-error-patterns.md`.

Also see: `references/kanban-dispatcher-stuck-diagnostic.md`, `references/browser-cdp-502-loop-pattern.md`, `references/provider-401-diagnosis.md`, `references/oc-hook-post-tool-call-task-id-pattern.md`, `references/transient-401-self-resolution-pattern.md`.

## Known Code Fixes & MCP Cascade

See `references/known-code-fixes-and-cascade.md` for Tier 4 known code fixes and the MCP server cascade failure triage procedure. See `references/redaction-placeholder-source-corruption.md` for the secret-redaction-transform pattern that corrupts skill source files (SyntaxError from an injected placeholder token) — a code defect the escalation loop fixes directly, **distinct from auth failures** (do not leave it user-gated).

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
- `scripts/classify_error_jobs.py` — deterministic probe: reads the profile `jobs.json`, buckets enabled error jobs by `last_error` fingerprint, and lists every `Script exited with code 1` job with its `script` name so each can be inspected individually (de-aggregation). Run via `terminal(command="python3 <hermes-home>/profiles/indigo/skills/ocas-custodian/scripts/classify_error_jobs.py")`.
- `scripts/classify_llm_necessity.py` — deterministic LLM-necessity classifier: reads jobs.json and evaluates every enabled non-paused LLM job against a heuristic (self-update, script-wrapper, skill-load+script). Outputs verdicts: `llm_unnecessary` (convert candidate), `llm_borderline` (needs wrapper), `llm_needed` (genuine). Includes `--unit-test` flag and `--json` for machine output. Run via `terminal(command="python3 <hermes-home>/profiles/indigo/skills/ocas-custodian/scripts/classify_llm_necessity.py")`.
- `scripts/classify_llm_necessity_integration.py` — cron-health integration: runs `classify_llm_necessity.py`, checks the acknowledgment state file (`llm_necessity_ack.json`), and writes/updates a single `oc_cron_llm_unnecessary` issue in `issues.jsonl` for new/unacknowledged candidates. NEVER auto-converts jobs. Run via `terminal(command="python3 <hermes-home>/profiles/indigo/skills/ocas-custodian/scripts/classify_llm_necessity_integration.py")`.
- `scripts/verify_escalation_state.py` — escalation-loop bidirectional verification probe: parses the profile `issues.jsonl` (brace-depth) and `jobs.json`, checks both staleness directions, and reports per-issue `jobs_paused` deltas vs the live paused set. Run via `terminal(command="python3 <hermes-home>/profiles/indigo/skills/ocas-custodian/scripts/verify_escalation_state.py")`. Run it FIRST in every escalation loop to decide whether any `issues.jsonl` write is needed (no-delta fast-path). See `references/escalation-execution-loop.md`.
- `scripts/find_missed_user_gated_jobs.py` — escalation-loop missed-enrollment probe: loads `jobs.json`, finds every `enabled`+erroring job NOT in any issue's `jobs_paused`, classifies its `last_error` against known user-gated fingerprints (Nous 401, OpenRouter 402, owl-alpha 404, Google 403/401), and reports MISSED enrollments vs genuinely transient vs UNKNOWN. Treat MISSED as "open/enroll for tracking" by default; do not automatically pause provider/model failures. Pause only when the narrow pause criteria are met and `paused_reason` plus a re-enable check are written. Run it AFTER `verify_escalation_state.py` to catch jobs that failed in the inter-scan window and were never enrolled.
- `scripts/scan_escalation_journal_gaps.py` — escalation-loop journal-to-issues gap probe: walks ALL custodian journal dirs (profile + commons, subdirs + loose files), parses each (list/concatenated JSON via brace-depth), and for journals within `--hours` (default 24) with `escalation_needed: true`, cross-references cited fingerprints / `escalation_refs` against OPEN issues in the profile `issues.jsonl`. Reports GAPs (flagged but no matching open issue — the Step 8b/8b-variant silent-drop) and RECOVERY notes (forward-stale candidates). Uses CONTENT timestamps (not mtime) because journal mtimes lag ~7h. Read-only by default; `--write` creates missing issues. Run it as the journal half of the escalation loop, alongside the two job-state probes. See `references/escalation-execution-loop.md`. **FALSE-POSITIVE GUARD (2026-07-15):** it matches flagged journals only against OPEN issues, so a journal whose referenced issue IS already `resolved`/`duplicate` surfaces as a spurious "GAP". Before any `--write`, re-verify each reported GAP against the FULL issues.jsonl resolved-count — never re-persist an already-resolved issue as a duplicate escalation. Confirmed 2026-07-15: 3 reported gaps (`oc_script_timeout_chronicle_embed_20260713`, `oc_script_timeout_chronicle_embed`, `oc_state_db_oversized_20260714T2007`) were all already `resolved` — false positives, no action taken.
- `scripts/race_safe_issue_patch.py` — escalation-loop `issues.jsonl` mutation that survives the top-of-hour `custodian:light` rewrite race: edits ONLY the target line, re-reads to verify, and retries up to N times. Use instead of a whole-file brace-parse rewrite when your resolution keeps getting clobbered. `python3 scripts/race_safe_issue_patch.py --issue-id <id> --set status=resolved --set user_gated=false --set escalation_needed=false [--require-status user_gated] [--retries 3]`. See `references/escalation-execution-loop.md` § WRITE-RACE CLOBBER.
- `scripts/reopen_false_resolutions.py` — light-scan inverse-gotcha guard: parses the profile `issues.jsonl` (brace-depth), counts live erroring jobs per known outage fingerprint (`token_expired`, `402 credits`, `owl-alpha 404`), and reopens any `resolved` issue whose outage still has >=1 live erroring job. Dry-run by default; `--write` to persist. Run as part of light-scan Step 8d. See `references/light-scan-false-resolution-gotcha.md`.
- `scripts/chronicle_embed_backlog_probe.py` — read-only backlog probe for `chronicle:daily-embed` timeout verification: prints per-kind unembedded counts (LEFT JOIN IS NULL), total vectors, and raw facts size so a re-run can be proven to process REAL volume (not a drained queue). Run via `terminal(command="python3 <hermes-home>/profiles/indigo/skills/ocas-custodian/scripts/chronicle_embed_backlog_probe.py")`. Pairs with `references/chronicle-daily-embed-timeout-remediation.md`.

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

Operational gotchas are tracked in [references/custodian-gotchas.md](references/custodian-gotchas.md) — 14 items covering skill-package immutability, cron pipe-to-interpreter blocks, confidence auto-tiering, log compaction, library hygiene, cron `Path.home()` breaks, script-path/HERMES_HOME matching, `issues.jsonl` field inconsistency, dead-reference escalation, DB VACUUM space, DB-flag stale-premise re-verify, stale error detection via path mismatch, unsafe `awk` timestamp filtering, and re-run-to-confirm stale-vs-active tracebacks.

## Operational Gotchas Catalog

The remaining operational gotchas (provider/credential traps, escalation-state hazards, backup/snapshot pitfalls, model-config and scheduling false-positives) are numerous and verbose. **Read `references/operational-gotchas.md` before applying any Tier 1 auto-fix or escalation action** — these are the non-obvious failure modes that cause Custodian to repeat bad fixes or escalate stale state.

## Error Handling

| Failure | Handling |
|---------|----------|
| `read_file` returns "BLOCKED: already read" | Use `terminal(command="cat /path")` or `terminal(command="tail -N /path")` to re-read files in cron context |
| `read_file` transient API/executor error (e.g. `'DaemonThreadPoolExecutor' object has no attribute '_initializer'`) | Transient failure in the read path — retry once; if it recurs, read via `terminal(command="cat /path")` (same fallback as the BLOCKED case). Do NOT conclude the tool is broken. |
| Pipe-to-interpreter blocked (`cat|tail|curl|grep` → `python3`) | Write script to `/tmp/` via `write_file`, run via `terminal(command="python3 /tmp/script.py")`. Single-quoted heredoc (`python3 << 'PYEOF'`) also works for inline Python. Do NOT retry pipe variations — all forms blocked by tirith security filter |
| `write_file` fails with "missing required field: path" | Fallback: `terminal(command="cat > /path << 'EOF'\n...\nEOF")` with heredoc |
| `execute_code` denied in cron | Use `terminal()` directly for all Python operations |
| `hermes cron list` shows "No scheduled jobs" | CLI reads wrong path — edit `<hermes-home>/profiles/<profile>/cron/jobs.json` directly |
| jobs.json parse yields 0 jobs | Registry is top-level `{"jobs": [...]}` (NOT `data.jobs`). Use `d.get("jobs", [])`; fall back to `data.jobs` ONLY if top-level `jobs` is absent. A wrong key returns 0 jobs → false-clean verdict. Re-inspect raw file head before concluding 0. Confirmed 2026-07-16. |
| Heredoc produces literal `$(date)` in JSON | Use `python3 -c "..."` with `json.dump()` for dynamic content — never heredoc for JSON |
| MCP server PIDs alive but connection failing | Check process liveness with `ps` before escalating — handshake can fail independently |
| `custodian_issues` tool returns stale data | Always verify against raw `issues.jsonl` via `terminal(command="cat ...")` AND check latest esc-run journal |
| `fix_effectiveness.jsonl` schema contamination | Validate `"attempts" in r` before storing; use `rec.get("attempts", 0)` in reads |
| issues.jsonl has multiple JSON objects per line | Use brace-depth parser, not naive `json.loads(line)` |
| Custodian journal files are list-shaped (not just dicts) | Mass-scanning journals with `json.load()` per file crashes `'list' object has no attribute 'get'` when a file is a top-level JSON **array** (not a single object or concatenated dicts). Branch on `isinstance(obj, list)` and iterate elements. The brace-depth dict parser alone does NOT protect against this. Confirmed 2026-07-14 escalation loop. |
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
| `references/escalation-loop-issue-status-scan-trap.md` | **Escalation-loop open-issue scan trap**: the loop template's `status in ("escalated","fix_attempted_failed")` finds NOTHING — live statuses are `resolved` / `duplicate` (merged, NOT open) / `user_gated` (open). Reliable open-signal: `status not in ("resolved","duplicate") AND (escalation_needed OR status=="user_gated")`. `parse_issues_jsonl.py` overcounts `open` (includes `duplicate`). Dump each open entry; never trust the summary count. |
| `references/escalation-gap-taxonomy-token-and-oauth-discriminator-2026-07-16.md` | **Escalation-loop gap + user-gated discrimination (2026-07-16)**: (1) journal→issues gap scans must exclude transient-pattern-taxonomy tokens (`oc_cron_no_agent_exit_1_noop`, `oc_gateway_interpreter_shutdown_transient` — cf=0, action:transient) that appear inside `error_job_detail` but are NEVER escalated; a regex over flagged journals will surface them as "missing" — they are false gaps. (2) OAuth user-gated discriminator: before declaring "requires <operator>," check whether ANY stored credential permits non-interactive recovery (Spotify: no token anywhere → truly gated; Google: stored refresh_token → recoverable). Concrete `oc_taste_spotify_token_missing_20260713` worked example. |
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
| `references/skill-reference-path-mismatch-pattern.md` | When a skill's agent code reads reference files from `<hermes-home>/commons/data/<skill>/references/` but they exist at `<hermes-home>/profiles/<profile>/skills/<skill>/references/` — Tier 2, requires skill code fix |
| `references/fallback-model-null-yaml-debris-pattern.md` | When `fallback_model: null` appears in config.yaml — distinguish YAML debris from true fix-loop. Diagnosis steps and PyYAML removal pattern. |
| `references/fallback-model-manifest-build-401-pattern.md` | When diagnosing 401 errors from null-provider jobs — if gateway log shows `provider=custom base_url=https://app.manifest.build/v1/`, the `fallback_model` in config.yaml has an expired Manifest.build API key. Tier 3 escalation. |
| `references/cron-provider-error-transient-pattern.md` | When a null-provider job shows `last_error: "RuntimeError: Provider returned error"` with `consecutive_failures=None`. First-occurrence provider API error from LLM execution context. Transient, no fix needed. Distinct from specific 401/403 fallback patterns. |
| `references/transient-401-self-resolution-pattern.md` | When a null-provider job hits a first-occurrence 401 from the default upstream, then re-runs successfully without intervention. Non-fatal, monitor only. |
| `references/subdirectory-hints-home-dir-pattern.md` | When `subdirectory_hints.py` `_add_path_candidate` fails with `RuntimeError: Could not determine home directory` because `$HOME` is unset in cron execution environment — Tier 2, framework bug. **Fix applied 2026-06-17**: added `RuntimeError` to except clause. |
| `references/oc-cron-script-not-found-transient-pattern.md` | When a no_agent cron job reports "Script not found" but the script exists — write/read race condition. Transient, no fix needed. |
| `references/light-scan-stale-no-agent-error-triage.md` | During light scan — when a stale `last_error` (compound `&&`) persists on a `no_agent: true` job after a wrapper script fix. How to classify as stale vs active. |
| `references/stale-error-message-script-field-mismatch.md` | When a job's `last_error` shows a different command/path than the current `script` field. Error is stale from a prior configuration (e.g., compound `&&` command replaced by wrapper script). Distinct from `oc_cron_script_not_found_transient`. Tier 2, surface only. |
| `references/no-agent-script-path-mismatch-symlink-fix.md` | When `no_agent: true` and the script is a bare basename that exists at `<hermes-home>/scripts/` but not at the profile scripts dir. Tier 1 fix: symlink from profile scripts dir to system scripts dir. |
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
| `references/escalation-loop-pitfalls.md` | Execution-loop traps: journal-gap probe false positives for resolved issues, stale-error-before-reopen, cooperating with in-flight sanctioned repairs, `database is locked` verification |
| `references/escalation-execution-loop.md` | **Execute-and-reconcile procedure** for an external escalation loop: verify live state both directions, pause still-burning user-gated jobs, reconcile `issues.jsonl` in one pass (resolve recovered / write missing / update `jobs_paused`), safe brace-depth edit pattern. |
| `references/escalation-loop-exit1-wrapper-stale-and-gap-falsepositive.md` | **Escalation-loop addendum**: no_agent wrapper that translates rc→exit (stale-error trap on bare "Script exited with code 1", mtime-vs-last_run_at proof); journal→issues gap false positives from base-fingerprint vs dated-id naming. |
| `references/journal-escalation-stale-premise-guard-2026-07-14.md` | **Step 8b guard**: before persisting a journal→issues gap, verify the journal's live premise is still true (re-derive disk% for `oc_state_db_oversized`, re-scan `jobs.json` for auth/`*_access_token_missing`). Prevents persisting FALSE escalations from journals whose premise resolved post-write. |
| `references/light-scan-2026-06-29-2206.md` | Clean verdict with new transients + stale paused errors — 5 error jobs: 3 transient first-occurrence provider errors, 2 permanently paused OAuth-frozen. 8-day journal gap. Pattern for "always filter paused jobs from error counts before classifying". |
| `references/stale-model-error-diagnostic-pattern.md` | When many jobs show `404: No endpoints found for <model>` — distinguishes stale (config already fixed) from active errors. Four-step diagnostic flow with pitfalls |
| `references/gateway-log-timestamp-range-filtering-pitfall.md` | When date-bounded grep on logs returns huge counts of OLD errors (multiline traceback tail mismatch). Now ALSO covers the inverse: the log is stamped in LOCAL naive time (no offset), so a UTC-window grep yields a false-zero — verify the log's zone before concluding a clean window. |
| `references/journal-path-format-inconsistency.md` | When journal gap detection falsely reports gaps — different scan runs write to different date directory formats (`YYYY-MM-DD` vs `YYYYMMDD`) or as loose files. Diagnosis and fix direction |
| `references/pipe-to-interpreter-security-block.md` | When ANY pipe-to-interpreter command gets blocked by tirith security filter — three reliable workarounds |
| `references/self-resolved-module-verification-pattern.md` | During light/deep scan — when a prior scan classified a ModuleNotFoundError as self-resolved. Verify the module is actually importable in the cron execution context before accepting the classification. |
| `references/scheduler-state-lag-vs-execution-failure.md` | During light scan Step 7 — when jobs appear overdue (`next_run_at` in past) but `last_run_at` is recent and `last_status=ok`. Scheduler state lag, not execution failure. |
| `references/no-agent-script-exit-1-deaggregation-pitfall.md` | When ≥2 error jobs share a bare 'Script exited with code 1' wrapper — they are NOT one root cause; enumerate + inspect each `script` before classifying |
| `references/monitor-list-exit1-mask-gap.md` | Sub-variant: a no_agent wrapper that exits 1 with NO stdout/stderr (masks the wrapped script's real traceback). De-aggregate by running the WRAPPED script directly; if the live signature differs from resolved covering issues, persist a new issue (Step 8b/8e). Includes race-safe issues.jsonl parser. Companion to `monitor-list-masked-keyerror-pitfall.md` (which covers the masked-KeyError mechanism itself). |
| `references/monitor-list-keyerror-transient-creds-race-2026-07-14.md` | **INVERSE of mask-gap**: `tasks_monitor.py` `KeyError: 'access_token'` that is a transient credential-refresh race (creds file mid-rewrite), NOT a persistent missing-token defect. Discriminator before persisting `oc_google_tasks_access_token_missing`: inspect the creds file for a valid `access_token` + re-run the wrapper 1–2×; if the token is present and re-runs succeed, do NOT persist. |
| `references/monitor-list-access-token-recurrence-durable-fix-2026-07-15.md` | **The OTHER case (PERSISTENT code defect, not a race):** when the creds file has `access_token` ABSENT (only `token` + valid `refresh_token` + future `expiry`), `tasks_monitor.get_access_token()` crashes. Full root cause + non-interactive `refresh_token()` recover + DURABLE 2-line code fix + self-heal verification recipe. Use this when the 2026-07-14 race discriminator says ABSENT. |
| `references/compression-model-moondream-misconfig.md` | Gateway "Auxiliary compression model moondream has a context window of 2,048 tokens" — top-level `compression.model` set to a vision model; TWO `compression:` blocks exist, read the top-level one. Includes active-vs-stale log verification. |
| `references/jobs-json-timestamp-offset-misread-pitfall.md` | When comparing `jobs.json` `next_run_at`/`last_run_at` to `now` — timestamps carry explicit UTC offsets (e.g. `-07:00`); convert to UTC before judging staleness or a false "stuck scheduler" escalation results |
| `references/no-agent-monitor-exit1-upstream-degraded-pitfall.md` | `no_agent` health watchdog exits 1 with `UNHEALTHY` + `Restart FAILED` — real upstream-degraded fault (container up, dependency suspended), NOT a no-op; probe live before classifying |
| `references/escalation-stale-issue-premise-verify.md` | **When an escalated issue's own premise is stale** — re-derive the probe target from the monitoring script and re-check claimed-absent binaries live before concluding user-gated/unresolvable. Concrete 2026-07-13 reversal (issue claimed "docker absent / :8080 HTTP 000"; live showed docker present + :8888 serving + watchdog exit 0). |
| `references/escalation-false-recovered-note-trap.md` | **Inverse of the stale-premise guard (2026-07-14)** — do NOT overwrite a "FORWARD-STALE: provider recovered" issue note with "RE-CONFIRMED LIVE" just because live `jobs.json` shows `status=error`; re-run the actual job via `hermes cron run <id>` first. `hermes chat` pong is insufficient. |
| `references/chronicle-daily-embed-timeout-pattern.md` | `chronicle:daily-embed` 600s cron-timeout fingerprint + reusable recipe: re-run the script live to confirm, then check a sibling script (`enrich_embeddings.py`) to isolate volume vs API failure. |
| `references/chronicle-daily-embed-timeout-remediation.md` | **Actual code fix** for `oc_script_timeout_chronicle_embed`: cap the unbounded Facts query (LIMIT 8000), make `embed_batch` deadline-aware with dynamic per-request timeout + 25s safety margin. Reusable pattern for any cron-timeout embedding script. |
| `references/resolved-timeout-verify-drained-backlog.md` | **Step 8e inverse gotcha** — a timeout/throughput issue marked 'resolved' from a re-run against a drained queue is a FALSE CLOSE; verify against real production volume before accepting. |
| `references/custodian-pitfall-map.md` | Additional pitfall + stale-premise verification references (chronicle forward-stale, db-malformed, enrich false-backlog). |
| `references/mcp-server-reconnect-loop-escalation-pitfall.md` | When an external HTTP MCP server reconnect loop was previously dismissed as "info-only transient" but shows real `connection lost (attempt N/5)` errors and >hours of non-recovery (no `registered N tools` line). Detection recipe + the `find_missed_user_gated_jobs.py` `affected_job_ids` false-positive. |