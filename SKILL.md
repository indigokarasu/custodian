---
name: ocas-custodian
description: >
  Monitors agent gateway logs, cron jobs, skill journals, and OCAS data
  directories for operational failures. Detects errors, applies safe
  non-destructive fixes autonomously during quiet hours, initializes
  uninitialized skills, registers missing background tasks, and escalates
  issues it cannot fix. Use when asking about system health, log errors, cron
  failures, skill initialization, or overnight maintenance. Triggers on:
  'check system health', 'fix log errors', 'why is X failing', 'initialize
  skills', 'clean up errors'.
metadata:
  author: Indigo Karasu
  email: mx.indigo.karasu@gmail.com
  version: "1.4.0+hermes"
  hermes:
    tags: [monitoring, maintenance, health]
    category: interface
    cron:
      - name: "custodian:deep"
        schedule: "0 1,7,13,19 * * *"
        command: "custodian.scan.deep"
      - name: "custodian:update"
        schedule: "0 0 * * *"
        command: "custodian.update"
  openclaw:
    skill_type: system
    visibility: public
    filesystem:
      read:
        - "{agent_root}/commons/data/ocas-custodian/"
        - "{agent_root}/commons/journals/ocas-custodian/"
        - "{agent_root}/commons/journals/*/"
        - "{agent_root}/commons/data/*/"
      write:
        - "{agent_root}/commons/data/ocas-custodian/"
        - "{agent_root}/commons/journals/ocas-custodian/"
    self_update:
      source: "https://github.com/indigokarasu/custodian"
      mechanism: "version-checked tarball from GitHub via gh CLI"
      command: "custodian.update"
      requires_binaries: [gh, tar, python3]
    cron:
      - name: "custodian:deep"
        schedule: "0 1,7,13,19 * * *"
        command: "custodian.scan.deep"
      - name: "custodian:update"
        schedule: "0 0 * * *"
        command: "custodian.update"
    heartbeat:
      - name: "custodian:light"
        command: "custodian.scan.light"
---

# Custodian

Custodian detects, classifies, and repairs agent platform operational failures autonomously during quiet hours so the user wakes to clean logs, initialized skills, and registered background tasks -- surfacing only what it could not fix.

## When to use

- Asked to check system health, fix log errors, review cron failures
- Asked to initialize skills or register missing background tasks
- Asked why the agent platform or a specific skill is failing
- Running overnight maintenance or a system audit
- Invoked automatically via heartbeat or cron

## Responsibility Boundary

**Owns:** gateway log scanning and error fingerprinting, cron job registry health, skill journal completeness, OCAS data directory health, skill initialization, background task conformance, Tier 1 auto-repair, activity model and schedule optimization, escalation signaling, fix effectiveness tracking.

**Does not own:** OKR trend analysis (Corvus, Mentor), skill design evaluation or rebuilding (Mentor, Forge), behavioral lesson extraction (Praxis), briefing delivery (Vesper), entity knowledge (Elephas), social graph (Weave). Never modifies any file inside a skill package directory.

## Ontology types

Custodian operates on system health data (logs, config files, journal metadata, storage usage). Entities encountered during scans and repairs are recorded as entity observations in journals for downstream consumption.

Custodian may read skill config files and journal metadata.

## Optional Skill Cooperation

- **Vesper** -- writes InsightProposals to `{agent_root}/commons/data/ocas-custodian/proposals/`; Vesper reads from there (cooperative read; Custodian owns). Without Vesper, issues stay in `issues.jsonl`.
- **Mentor** -- journals tagged `escalation_needed: true` are readable by Mentor heartbeat. Without Mentor, escalated issues await manual review.
- **Corvus** -- if installed, reads Corvus observation journals for `routine_prediction` InsightProposals. Blended 70% Corvus / 30% own model. Functions normally without Corvus.
- **Elephas** -- journal entity observations consumed during Chronicle ingestion

## Commands

- `custodian.init` -- create storage, register background tasks idempotently, copy bundled plan to Mentor plans dir, build initial activity model
- `custodian.scan.light` -- tail gateway log (last 100 lines), check cron registry, retry open `fix_attempted_failed` items, check for uninitialized skills, write Observation Journal
- `custodian.scan.deep` -- full sweep: all light steps + full JSONL scan, doctor diagnostic, journal health, skill conformance, skill init pass, activity model rebuild, schedule optimization, repair pass, web search pass, escalation pass, report, Vesper signal
- `custodian.verify {fix_id}` -- verify fix outcome, update record, escalate if failed twice
- `custodian.repair.auto` -- apply all pending Tier 1 fixes from last scan
- `custodian.repair.plan` -- generate structured repair plan for Tier 2/3 issues
- `custodian.issues.list` -- list open issues with tier, status, age, recurrence
- `custodian.issues.resolve {issue_id}` -- mark issue resolved manually
- `custodian.status` -- emit SkillStatus JSON
- `custodian.schedule.show` -- display current and target scan schedule with optimization confidence
- `custodian.update` -- pull latest from GitHub source (preserves journals and data)

## OKRs

Universal OKRs from spec-ocas-journal.md apply to all runs.

```yaml
skill_okrs:
  - name: fix_success_rate
    metric: fraction of Tier 1 fixes that resolve the underlying issue without recurrence within 7 days
    direction: maximize
    target: 0.85
    evaluation_window: 30_runs
  - name: skill_init_coverage
    metric: fraction of installed skills properly initialized and registered at any time
    direction: maximize
    target: 1.0
    evaluation_window: 7_runs
  - name: scan_detection_accuracy
    metric: fraction of real errors detected in light/deep scans within expected latency
    direction: maximize
    target: 0.90
    evaluation_window: 30_runs
```

## Initialization

This skill initializes on first use via:

```bash
custodian.init
```

This creates required data directories, registers background task cron jobs and heartbeat entries, builds the initial activity model from gateway logs, and prepares bundled workflow plans for Mentor.

---

## Execution Loop -- Light Scan

Runs every heartbeat. Must be fast (seconds). No web search, doctor, or report.

1. Read `{agent_root}/cron/jobs.json` -- find jobs with `enabled: false` that were previously enabled in `skill_conformance.jsonl`. Re-enable (Tier 1).
2. Tail `{agent_root}/logs/agent-YYYY-MM-DD.log` -- last 100 lines. Fingerprint ERROR entries against `references/known_issues.json` then `learned_issues.jsonl`. Apply Tier 1 fixes. Open issues for Tier 3/4.
3. Check `issues.jsonl` for `status: fix_attempted_failed`. Retry Tier 1 up to 3 times before escalating.
4. Check for uninitialized skills (data dir or config.json missing). Initialize immediately.
5. Write Observation Journal.

## Execution Loop -- Deep Scan

Runs on optimized 6-hour cron schedule. Isolated session, lightContext.

1. **Load context** -- own journals (7 days), `fix_effectiveness.jsonl`. Identify recurring fingerprints, known-failed fixes, already-searched queries.
2. **Collect** -- full day gateway log, cron run logs, skill journals from scan window, all OCAS data dirs, the platform diagnostic tool.
3. **Fingerprint + classify** -- match against `references/known_issues.json` then `learned_issues.jsonl`. Unknowns default Tier 3.
4. **Rebuild activity model** -- parse gateway log `message.processed` events (`source: user` vs `source: cron|heartbeat`). Blend Corvus if present (70/30). Update `activity_model.json`. Determine `current_state`.
5. **Optimize schedule** -- score current schedule against activity model. If score < 6, compute better schedule. Shift max 30 min per slot. Update cron if changed.
6. **Skill conformance** -- scan installed skills, parse `## Background tasks`, cross-reference against the platform scheduling registry and `HEARTBEAT.md`. Register missing (Tier 1). Surface mismatches (Tier 2).
7. **Skill init pass** -- initialize any skill missing data dir, config.json, or journal dir.
8. **Repair pass** -- all Tier 1 fixes. Activity-aware: if active, only urgent fixes (failure in last 5 min); defer rest. Register verify jobs. Execute prior deferred fixes if now quiet.
9. **Web search pass** -- for unknown fingerprints with `recurrence_count >= 1`, run next mutation query (see Web Search Protocol).
10. **Escalation pass** -- Tier 3/4 open issues: include `briefing` payload in journal. Tag journal `escalation_needed: true`.
11. **Report** -- `{agent_root}/commons/data/ocas-custodian/reports/YYYY-MM-DD-HHMM.md`. If all clean and previous cycle also clean: suppress Vesper signal.
12. **Write journal** -- Action (if fixes applied) or Observation (scan-only).

## Fix Safety Envelope

Every autonomous fix must satisfy all four:

1. **Non-destructive** -- no delete, overwrite, or permanent alteration
2. **Reversible** -- pre-fix state restorable without backup
3. **Minimal scope** -- smallest surface to address symptom
4. **Functionality-preserving** -- cannot reduce capability

Hard constraints: never modify skill package files, never delete files, never modify another skill's data dir, never restart gateway, never change user settings without acknowledgment.

## Tier Classification

| Tier | Label | Action |
|---|---|---|
| 1 | Auto-fix | Apply immediately, register verify job, log fix record |
| 2 | Plan | Surface with proposed change, do not apply |
| 3 | Escalate | Write escalation journal with `briefing` payload, invoke Mentor plan if available |
| 4 | Alert only | Cannot fix -- surface with diagnostics |

High-recurrence override: if `recurrence_after_fix / successes > 0.5`, auto-promote next occurrence from Tier 1 to Tier 3.

## Tier 1 Auto-Fix Registry

All Tier 1 fixes defined in `references/known_issues.json`. Read at start of every scan. Pre-seeded fingerprints:

| Fingerprint | Fix |
|---|---|
| `oc_cron_disabled_transient` | Re-enable cron job |
| `oc_cron_stuck_missed` | Force-run missed job |

| `oc_journal_dir_missing` | Create directory |
| `oc_skill_data_dir_missing` | Create directory + default config.json |
| `oc_jsonl_oversized` | Rotate with date suffix |
| `oc_jsonl_malformed_lines` | Quarantine to `.error` file |
| `oc_gateway_token_missing` | `platform diagnostics --generate-gateway-token` |
| `oc_oauth_token_expiring` | OAuth refresh (token still valid, expiry <= 12h) |
| `oc_background_task_missing` | Register cron or heartbeat entry per SKILL.md |
| `oc_skill_uninitialized` | Create storage dirs, default config, empty JSONL |

## Fix Verification

Every Tier 1 fix registers a one-shot cron job `custodian:verify:{fix_id}` with delay per fix type (2-15 min). On verification failure: set `outcome: fix_attempted_failed`. Two consecutive failures: promote to Tier 3. Fix records appended to `fixes.jsonl` with `fix_id`, `issue_id`, `command`, `reversibility`, `pre_fix_state`, `post_fix_state`, `outcome`.

## Post-Fix Cleanup

After successful verification, run fix-specific cleanup (check backoff, confirm next run, validate permissions). Record in `cleanup_events.jsonl`.

## Skill Conformance Checking

On every deep scan: scan `{agent_root}/skills/`, parse each SKILL.md `## Background tasks`, cross-reference against the platform scheduling registry and `HEARTBEAT.md`. Missing tasks: Tier 1 fix. Schedule mismatches: Tier 2. Orphaned `custodian:*` jobs: Tier 2. Write `skill_conformance.jsonl` per skill.

## Skill Initialization

Uninitialized when: data dir missing, config.json missing, or journal dir missing. Sequence (additive only, never overwrite):

1. Create `{agent_root}/commons/data/{skill-name}/` if missing
2. Write default config.json with ConfigBase fields -- only if absent
3. Create `{agent_root}/commons/journals/{skill-name}/` if missing
4. Verify commons/ directory structure exists
5. Run conformance check for background tasks
6. Register missing tasks (Tier 1, subject to parameter availability)
7. Register verify job (15 min delay)
8. Append DecisionRecord

Do not run the skill's own `{skill}.init` command.

## Activity Model

Maintained in `activity_model.json`, rebuilt every deep scan from 14-day gateway log window. Confidence per hour: `active_days / total_days` (>= 0.75 high, >= 0.40 med, < 0.40 low). `current_state: active` if hour confidence >= med and in active window, else `quiet`.

Repair rules: quiet = all Tier 1; active = urgent only (failure in last 5 min), defer rest; low confidence = execute but suppress noisy effects. Cold start: `01:00, 07:00, 13:00, 19:00 PT`. Optimization begins after 7 days.

## Schedule Optimization

Exactly 4 runs/day, min 2h gap, max 30 min shift per cycle. Score each run time against activity model (-2 to +2 per slot, max +8). If score >= 6: no change. If < 6: find candidate maximizing score, shift toward target. Update cron registration.

Confidence gate: high = optimize freely, med = only if score <= 2, low = hold.

## Web Search Protocol

Fire when: fingerprint unknown, recurrence increased since last search, last search not actionable, not escalated/suppressed, < 5 attempts.

Query mutation sequence: (1) `{error} agent skill`, (2) `{error} {tech_context}`, (3) `{error_pattern} fix`, (4) `{component} {failure_mode}`, (5) `{failure_mode} root cause diagnosis`.

On actionable result: attempt fix, append to `learned_issues.jsonl` if successful. On no result: record and continue mutation on next recurrence.

## Self-Improvement

`fix_effectiveness.jsonl`: per-fingerprint tracking of attempts, successes, failures, recurrence. High-recurrence escalation: `recurrence_after_fix / successes > 0.5` promotes to Tier 3.

Custodian OKRs (every journal): `success_rate`, `issues_detected`, `issues_auto_fixed`, `fix_success_rate`, `mean_time_to_fix_ms`, `open_residuals`, `escalations`, `high_recurrence_fingerprints`, `skills_initialized`, `background_tasks_registered`, `schedule_score`, `journal_completeness`.

## Escalation Path

Tier 3: append `status: escalated` to `issues.jsonl`, tag journal `escalation_needed: true`, write InsightProposal (`anomaly_alert`) to `{agent_root}/commons/data/ocas-custodian/proposals/{proposal_id}.json`. Vesper reads from this directory. If Mentor present, note `mentor.plan.run custodian-repair --arg issue_id={id}` available.

Clean state: zero open issues + previous cycle clean = suppress Vesper signal. First run of day or issues now resolved = emit clean bill of health.

## Journal Outputs

- **Observation Journal** -- scan-only runs, no fixes applied
- **Action Journal** -- any run with Tier 1 fixes or cron registrations

Both include full Custodian OKR block. Path: `{agent_root}/commons/journals/ocas-custodian/YYYY-MM-DD/{run_id}.json`

When entities are encountered during a run, include the following fields in `decision.payload`:

- `entities_observed` — entities encountered (e.g. Entity/AI for systems and services being monitored, Concept/Event for failures and incidents, Thing/DigitalArtifact for log files and config files)
- `relationships_observed` — relationships between observed entities
- `preferences_observed` — any preferences inferred from observations

Each entity observation must include a `user_relevance` field: `user` if the entity is directly related to the user's world, `agent_only` if encountered incidentally during internal operations, `unknown` if unclear. Almost all Custodian entities are `agent_only` — they are infrastructure, not the user's personal world.

## Background tasks

| Job | Mechanism | Schedule | Command |
|---|---|---|---|
| `custodian:light` | heartbeat | every heartbeat cycle | `custodian.scan.light` |
| `custodian:deep` | cron | optimized 6h (initial: `0 1,7,13,19 * * *` PT) | `custodian.scan.deep` |
| `custodian:update` | cron | `0 0 * * *` (midnight daily) | Self-update from GitHub source |

Registration during `custodian.init` (idempotent -- check the platform scheduling registry first).

## Storage Layout

```
{agent_root}/commons/data/ocas-custodian/
  config.json                  -- ConfigBase + scan_window_minutes, optimization settings
  issues.jsonl                 -- issue lifecycle records
  fixes.jsonl                  -- fix attempt records with pre/post state
  cleanup_events.jsonl         -- post-fix cleanup records
  fix_effectiveness.jsonl      -- per-fingerprint outcome tracking
  learned_issues.jsonl         -- runtime-learned fingerprints from web search
  skill_conformance.jsonl      -- per-skill background task conformance
  activity_model.json          -- rolling 14-day activity pattern (rebuilt each deep scan)
  deferred_fixes.jsonl         -- fixes queued for next quiet window
  schedule_state.json          -- current/target schedule, optimization history
  decisions.jsonl              -- DecisionRecord entries
  proposals/                   -- InsightProposal files for Vesper (cooperative read)
    {proposal_id}.json
  reports/
    YYYY-MM-DD-HHMM.md         -- deep scan summaries (7-day retention)
{agent_root}/commons/journals/ocas-custodian/
  YYYY-MM-DD/{run_id}.json
```

## Platform Compatibility

The `scripts/custodian.py` script was originally written for the `openclaw` CLI. On Hermes, the CLI is `hermes` (not `openclaw`), and some commands differ:

| OpenClaw command | Hermes equivalent | Status |
|---|---|---|
| `openclaw cron add --name X --cron S --message M` | `hermes cron add --name X --skill SKILL S 'PROMPT'` | Different syntax and flags |
| `openclaw cron edit ID --enabled true` | `hermes cron resume ID` | Different subcommand |
| `openclaw cron run ID` | `hermes cron run ID` | Same |
| `openclaw doctor` | `hermes doctor` | May differ |

**The script's `CronRegistry.add_cron_job()` calls `openclaw` which does not exist on Hermes.** The `init` command will fail with `FileNotFoundError: 'openclaw'`. Path references also use `~/openclaw/` instead of `{agent_root}/commons/`.

**Workaround:** Execute deep scans manually by reasoning directly. For cron registration, use `hermes cron add` with `--skill` and `--name` flags. For data operations, manipulate JSONL files directly using `read_file`/`write_file`/terminal tools. The data directory is `{agent_root}/commons/data/ocas-custodian/` (not `~/openclaw/data/`).

### Hermes-Specific Execution Patterns (Deep Scan)

On Hermes, the deep scan must be executed manually by reasoning through each step. Here are the exact patterns:

**Agent root path:** `~/.hermes` (not `~/openclaw/`). Commons dirs: `~/.hermes/commons/data/` and `~/.hermes/commons/journals/`.

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

**Duplicate cron job detection:** `hermes cron list` output includes job IDs. When multiple entries share the same name/schedule, the earliest-registered ID is canonical — remove the later ones. This happens when `init` commands are run multiple times.

**HEARTBEAT.md creation:** Located at `{agent_root}/HEARTBEAT.md`. Format:
```markdown
# Heartbeat Tasks
| Task | Skill | Command | Description |
|------|-------|---------|-------------|
| taskname | ocas-skillname | skill.command | Description |
```
Skills that declare `mechanism: heartbeat` in their Background tasks table go here, not in the cron registry.

**InsightProposal format** (for Vesper escalation):
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

**Background task scan:** Read each `~/.hermes/skills/ocas-*/SKILL.md`, find `## Background tasks` section, parse the table rows for Job name/Mechanism/Schedule. Cross-reference against `hermes cron list` output and HEARTBEAT.md. Skills with `mechanism: heartbeat` → add to HEARTBEAT.md. Skills with `mechanism: cron` → register via `hermes cron add`.

**Activity model:** On Hermes, `message.processed` events may not be labeled in gateway.log. Use log line counts per timestamp as a proxy for activity volume. Build hourly confidence from the proportion of active hours vs. total observed hours across the 7-14 day window.

**Schedule scoring:** Score each slot -2 to +2 based on quietness (lower activity = higher score). Quiet slots score +2, moderate +1, high activity -2. Total max = 8. If score < 6 and confidence >= med, shift each slot max 30 minutes toward the target.

## Using the script

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

## Support File Map

| File | Purpose | When to read |
|---|---|---|
| `references/known_issues.json` | Pre-seeded fingerprint registry with tier, fix, reversibility | Start of every scan before classifying errors |
| `references/plans/custodian-repair.plan.md` | Mentor Workflow Plan for Tier 3 multi-step repair | Copied to Mentor plans dir during init; referenced in escalation |
| `scripts/custodian.py` | Deterministic CLI helper for all scan, repair, and data operations | Called by the agent for every custodian command |

## Update command

This skill self-updates every 24 hours via:

```bash
custodian.update
```

This pulls the latest version from GitHub and restarts the skill's background tasks if applicable.

### Common patterns
- **Skill initialized but never ran**: Data directory exists with config.json, journal directory empty, cron jobs scheduled but last_run_at is null. Likely missing: dependencies, credentials, or MCP server configuration.
- **Skill running but failing**: Journal entries exist with errors, last status is "error". Check recent journal entries for error messages, check logs for stack traces or failure reasons.
- **Skill not scheduled**: Data directory exists, no cron jobs in registry. Need to run skill's init command or register cron jobs manually.
- **MCP server missing**: Skill requires MCP server (check SKILL.md), config.yaml has no mcp_servers entry for that service. Need to add MCP server configuration to config.yaml.
- **Credentials missing**: Skill requires authentication (check SKILL.md), no credential files found. Need to set up OAuth or service account credentials.

### Credential audit (.env health)

Triggered by: "audit/verify/test credentials or API keys", post-rotation onboarding, or recurring `auth failed`/`unauthorized` errors. Tests every active key in `.env` against its live API, annotates results, removes dead keys, documents alternate auth.

1. **Parse sources** — `.env` (active non-comment key=value), `~/.hermes/config.yaml` `model.api_key`, session history (`session_search` for rotated keys), memory. Skip `***`/`(empty)` placeholders.
2. **Test each key** with a minimal read-only call. For per-service endpoints and auth methods, see `references/api_endpoints.md`.
3. **Classify** — valid (annotate `# Valid as of YYYY-MM-DD`), broken (remove), alternate auth (annotate `# ACCESS METHOD: ...`; OAuth/CLI/local services — do NOT flag as broken).
4. **Recover broken keys** — search session history and memory for user-provided replacements; test and substitute.
5. **Write `.env`** — annotated valid keys, alternate-auth comments, broken lines removed (never leave `***` or `(empty)`).
6. **Report** — X valid, Y broken (removed), Z alternate auth. Record as `credential_audit` observation journal entry.

### Integration with other skills
This skill is diagnostic only. It does not fix issues but provides the information needed for:
- **ocas-custodian** - Can use diagnostic results for health monitoring
- **ocas-forge** - Can use diagnostic results when building or fixing skills
- **google-cloud-api-setup** - For setting up missing Google Cloud credentials
- **mcp/native-mcp** - For configuring missing MCP servers
- Individual skill init commands - For initializing or reinitializing skills

