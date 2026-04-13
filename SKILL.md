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
  version: "1.3.4"
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
      requires_binaries: [gh, tar]
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

**Search tool selection:** Prefer SearXNG (via the N2 MCP or a self-hosted instance) for all queries. If SearXNG is unavailable, fall back to the agent's default search tool silently.

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

## Execution notes

All scan, init, verify, repair, status, and issues operations are performed directly by the agent — no helper script. Read and write JSONL files, parse logs, fingerprint errors, rebuild the activity model, and update the schedule using the data structures described above and in `references/known_issues.json`.

**Cron registration:** Always use Hermes-native platform scheduling commands. Never use any binary or script for cron management.

**Web search handoff:** During `scan.deep` Step 9, if `{agent_root}/commons/data/ocas-custodian/search_candidates.json` exists, read it and execute the web search pass directly using the query mutation sequence in Web Search Protocol. Write actionable results to `learned_issues.jsonl`.

**Escalation handoff:** After Step 9, check open Tier 3/4 issues and write InsightProposals to `{agent_root}/commons/data/ocas-custodian/proposals/` if Vesper is installed. Vesper reads from this directory.

## Support File Map

| File | Purpose | When to read |
|---|---|---|
| `references/known_issues.json` | Pre-seeded fingerprint registry with tier, fix, reversibility | Start of every scan before classifying errors |
| `references/plans/custodian-repair.plan.md` | Mentor Workflow Plan for Tier 3 multi-step repair | Copied to Mentor plans dir during init; referenced in escalation |

## Update command

This skill self-updates every 24 hours via:

```bash
custodian.update
```

This pulls the latest version from GitHub and restarts the skill's background tasks if applicable.
