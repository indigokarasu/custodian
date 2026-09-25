---
name: ocas-custodian
license: MIT
description: 'Monitors agent gateway logs, cron jobs, skill journals, and OCAS data directories for operational failures. Detects errors, applies safe non-destructive fixes autonomously during quiet hours, escalates the rest. RCA on recurring errors with fix-loop detection and confidence-tier auto promote/demote. Use when: cron jobs fail or show stale errors, gateway logs show repeated error patterns, skill journals have gaps, disk usage exceeds thresholds, MCP servers crash-loop, or after any gateway restart. Keywords: cron health, log analysis, system monitoring, error fingerprinting, auto-repair, fix-loop detection, operational conformance. NOT for OKR trend analysis, skill design evaluation, behavioral lesson extraction, briefing delivery, or social graph queries.'
source: https://github.com/<agent-handle>/custodian
includes:
- references/**
- scripts/**
metadata:
  author: Indigo Karasu (indigokarasu)
  version: "3.1.0"
  hermes:
    tags:
      - monitoring
      - system-health
      - log-analysis
      - cron
      - OCAS-core
    category: devops
triggers:
- system health
- log errors
- cron failures
- skill journal errors
- operational monitoring
---

# Custodian

Enforces the `spec-ocas-recovery.md` contract: every scheduled run writes evidence; schedule gaps trigger remedial passes; degraded mode is explicit, never a silent skip; self-repair re-validates.

## Interactive Menu

Interactive invocation: two-level menu in `references/interactive-menu.md`.

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

## Known Code Fixes

- **`send_email.py` template mismatch** (`oc_vesper_template_missing`): only `job_search` valid — `references/send-email-template-mismatch.md`.
- **Wrong path prefix**: use `$AGENT_ROOT/profiles/<profile>/commons/...` — `references/wrong-path-prefix-in-skill-scripts.md`.

## Critical Pitfalls

- **Tool quirks in cron context** — `read_file` dedup, pipe-to-interpreter, `write_file` failures, `execute_code` denial, heredoc `$(date)` expansion (single-quoted heredocs still produced a literal `$(date)`, 2026-06-24 — `references/cron-json-write-heredoc-variable-expansion-failure.md`), and the `hermes cron` CLI path mismatch: fixes in `references/cron-tool-failure-handling-table.md`. Nuance: use a **unique `/tmp/` script name** (cron siblings overwrite shared paths, 2026-07-08); on the sibling `_warning`, rename.
- **MCP PIDs alive but connection failing**: processes can live yet fail the TaskGroup handshake — check liveness before escalating.
- **state.db bloat**: expected <1GB; flag `oc_state_db_oversized` (Tier 2) at >1GB AND disk >80% (5-10GB fine below that). **Why dual threshold**: avoids false flags; >80% makes it actionable — VACUUM needs free space ≈ DB size; pruning is safer.
- **Stale 503 (2026-07-27):** Nous HTTP 503 on 7 jobs — stale; keep `oc_provider_503_upstream_capacity` open until `hermes cron run <id>` succeeds.
- **Skill update-wrapper failures** (rebase-stuck / merge-conflict batches): abort, reset to origin/main, verify HEAD==origin/main, rerun, force-flip via `hermes cron run` — `references/skill-update-rebase-conflict-batch-pattern.md`.

Cron-run `issues.jsonl`/journal mutations: `terminal()` + heredoc only — never `read_file` (corrupts JSONL) or `execute_code` (blocked in cron).

## Responsibility Boundary

**Owns:** gateway log scanning + fingerprinting, cron registry health, skill journal completeness, data-dir health, skill initialization, background-task conformance, Tier 1 auto-repair, activity/schedule optimization, escalation signaling, fix-effectiveness tracking, tier management, library hygiene.

**Does not own:** OKR trends (Mentor), skill design (Mentor, Forge), lesson extraction (Praxis), briefings (Vesper), social graph (Weave). Never modifies files inside a skill package.

## Ontology types

Custodian operates on system health data (logs, config, journals, storage).

## Optional Skill Cooperation

- **Vesper** reads InsightProposals written to the proposals dir.
- **Mentor** reads journals tagged `escalation_needed: true`.

## Commands

`custodian.init` — create storage, register background tasks, build activity model. `custodian.scan.light` — tail gateway log, check cron registry, retry failed fixes, check uninitialized skills. `custodian.scan.deep` — full sweep (`references/deep-scan.md`). `custodian.verify {fix_id}` · `repair.auto` · `repair.plan` · `issues.list` · `issues.resolve {issue_id}` · `status` · `schedule.show` · `escalation-runner` · `update`.

`custodian.secrets.audit` — read-only inline-secret scan, dedupes by value (`references/secret-audit.md`). `custodian.secrets.remediate` — plan; `--apply` migrates the safe subset (MCP `headers` to `${ENV}`; never overwrite `.env` keys; back up each file; credential blobs stay MANUAL). Re-run the audit until 0 inline hits.

## Example

A light scan reads `jobs.json`, tails the gateway log for new errors, fingerprints each, and journals to `{agent_root}/commons/journals/ocas-custodian/YYYY-MM-DD/{run_id}.json`. A clean scan (all errors transient) returns `[SILENT]` *after* that journal — the journal proves the scan ran; `[SILENT]` suppresses delivery noise only.

## Confidence Model

`confidence_score = sample_confidence × success_rate`; tiers auto-promote/demote on fix history — `references/confidence-model.md`.

## Workflow: Scan & Escalation Loops

Full checklists in `references/execution-loops.md` — read before any scan or escalation run.

- **Light Scan** — 10-step checklist: jobs.json parse, gateway-log tail, fingerprint matching, uninitialized skills, exit-1 de-aggregation, jobs-not-running, journal→issues persistence, stale-premise verification, LLM-necessity integration.
- **Deep Scan** — 13-step sweep (`references/deep-scan.md`); early-exit when all errors are transient (cf=None/0).
- **Escalation Runner** — 8-step checklist + verification; already-classified fast path (<2h old, unchanged).
- **Escalation Execution Loop** — bidirectional state verification, missed-enrollment sweep, four-bucket classification, one-pass `issues.jsonl` reconcile.

Run gates (all apply in cron context):

- [ ] Registry parsed from `<profile>/cron/jobs.json` (never `hermes cron list`); raw-file re-check before any false-clean verdict
- [ ] New gateway errors since the last scan tailed and fingerprinted; error jobs re-run/live-probed before stale-vs-active classification
- [ ] Journal written BEFORE `[SILENT]`; fix outcomes re-validated before any issue is marked resolved

**Cron silence protocol:** runs with no actionable issues respond with exactly `[SILENT]` — report only genuinely new information. **Journal-before-silent:** a no-op scan MUST write its observation journal (`not_activity_reason` set) first — that journal is the evidence the contract requires.

**Two-stage self-healing contract** (`spec-ocas-recovery.md`): 1) log the decision (what, why, fingerprint, Tier) to `issues.jsonl`/journal BEFORE any fix; 2) attempt the repair; 3) re-validate BEFORE logging success — re-run the affected job/script, confirm the original signature is gone, then mark resolved. A fix without re-validation is a claim, not a resolution.

**Recovery log compaction:** past 1,000 entries, logs compact into per-day rollups — never drop evidence a re-validation depended on.

**Post-fix verification:** after any Tier 1 fix, re-check the targeted log/config, then close the registry loop with `hermes cron run <id>` (batch: `scripts/verify_fixes_cron_run.py`).

## Operational Hints (2026-07)

- **Model-pin drift can't run via CLI (2026-07-22):** `hermes cron` has no `update` subcommand; `edit` lacks `--provider`/`--model`. If <operator> chose a pin target, edit `jobs.json` directly; else leave user-gated (`references/escalation-execution-loop.md`).
- **False-escalation resolution:** an open `user_gated` issue claiming "Job still erroring live" with `last_run_at` days before your sweep is the stale-error signature — re-run the job; success = FALSE ESCALATION, resolve it (`scripts/race_safe_issue_patch.py` patches race-safely).

## Script Path Security Block Pattern

Script exists but its path is rejected; under a profile, scripts must be at `<hermes-home>/profiles/<profile>/scripts/<basename>` — `references/script-path-security-block-pattern.md`.

## Google OAuth Patterns

Two fingerprints (`references/google-oauth-client-deleted-pattern.md`): `oc_google_oauth_client_deleted` — client deleted in the Cloud Console; `oc_google_oauth_token_revoked` — refresh token expired/revoked (`invalid_grant`), affecting only direct-credential jobs (`email:check`, `monitor:list`).

Wrapper-cascade masking + `KeyError: 'access_token'` variant: `references/subprocess-cascade-oauth-masking.md`, `references/monitor-list-access-token-recurrence-durable-fix-2026-07-15.md`. **Sequential rule:** after fixing a `googleapiclient` `ModuleNotFoundError` on a Google-auth job, immediately re-check for token revocation (Tier 1 → Tier 3).

## Safety, Conformance & Activity

Safety envelope + Tier 1 auto-fix registry: `references/fix-safety.md`. Background-task conformance + registry health: `references/conformance.md`. Activity model (rebuilt each deep scan from a 14-day window) + schedule optimization: `references/schedule-optimization.md`. Storage / platform: `references/background-tasks.md`, `references/platform-compatibility.md`.

## Core Fingerprints (Operational Detection Set)

**When to read:** during light-scan Step 3 (fingerprint matching) and Step 6 (recurrence check). Full table: `references/custodian-core-fingerprints.md`; Tier-2 surface-only catalog: `references/non-fatal-error-patterns.md`.

## Known Code Fixes & MCP Cascade

See `references/known-code-fixes-and-cascade.md` (Tier 4 code fixes, MCP cascade triage) and `references/redaction-placeholder-source-corruption.md` (secret-redaction pattern that corrupts skill source — fix directly, do NOT leave user-gated).

## Escalation Path

Tier 3: InsightProposal to the proposals dir + journal tag `escalation_needed: true`. Confidence-gated: `confidence_score >= 0.6` with `recommended_tier == 1` → auto-fix instead.

## Journal Outputs

- **Observation Journal** (scan-only) / **Action Journal** (with fixes): `{agent_root}/commons/journals/ocas-custodian/YYYY-MM-DD/{run_id}.json` — `references/observation-journal-schema.md`.

## Background tasks

| Job | Mechanism | Schedule | Command |
|---|---|---|---|
| `custodian:light` | cron | `0 * * * *` | `custodian.scan.light` |
| `custodian:deep` | cron | `0 8,14,20,2 * * *` | `custodian.scan.deep` |
| `custodian:escalation-runner` | cron | `*/30 9-17 * * 1-5` | Process escalated issues |
| `custodian:cron-health` | cron (no_agent) | `0 8,14,20,2 * * *` | Health line + alert gate |

## Scripts

Scripts with CLI flags accept `--help` (lazy imports — works without optional deps); usage + cron staggering: `references/using-script.md`.

- `classify_error_jobs.py` — bucket error jobs by fingerprint; surface bare `Script exited with code 1` for de-aggregation
- `classify_llm_necessity.py` / `_integration.py` — LLM-necessity classifier; report-only (`--json`, `--unit-test`)
- `verify_escalation_state.py` — bidirectional staleness check (`issues.jsonl` + `jobs.json`)
- `find_missed_user_gated_jobs.py` — enabled+erroring jobs missing from every issue
- `scan_escalation_journal_gaps.py` — journal→issues gap probe (`--write` creates)
- `race_safe_issue_patch.py` / `reopen_false_resolutions.py` — race-safe patch / reopen false resolutions
- `verify_fixes_cron_run.py` — batch post-fix verification (`hermes cron run`)
- `chronicle_embed_backlog_probe.py` — read-only backlog probe

## Self-Update

Skill updates run centrally via the fleet updater — a shared `update_skill.sh` scheduled under the Hermes home; it never discards uncommitted or unpushed work. `custodian.update` self-updates the plugin from GitHub. Do NOT push here (local reference copy) — `references/plugin-vs-skill-architecture.md`.

## OKRs

See `references/okrs.md`.

## Disk Compaction

See `references/disk-compaction.md` for cleanup when disk >80%.

## Gotchas / Operational Pitfalls

Operational gotchas (14 items: skill-package immutability, pipe-to-interpreter, confidence auto-tiering, log compaction, library hygiene, ...): `references/custodian-gotchas.md`. Provider/credential/escalation-state traps: **read `references/operational-gotchas.md` before any Tier 1 fix**.

## Error Handling

| Failure | Handling |
|---------|----------|
| `read_file` "BLOCKED: already read" / transient error | re-read via `terminal(command="cat /path")` / `tail`; retry once first |
| Pipe-to-interpreter blocked | script to unique `/tmp/` name via `write_file` → `terminal()`; never retry pipe variants |
| `write_file` "missing required field: path" | fallback `cat > /path << 'EOF' ... EOF` |
| jobs.json parses to 0 jobs | `d.get("jobs", [])`; re-inspect raw head before concluding 0 |
| `last_status=error` after a fix | registry lags — `hermes cron run <id>` flips it; stale ≠ failure |
| literal `gateway restart` in command | interlock blocks — reword (e.g. "gateway reload") |

Full table (all 20 cron failure modes + tool variants): `references/cron-tool-failure-handling-table.md`.

**Validation loop (all fixes):** apply → re-check log/config → confirm gone → journal → fix-loop (≥3× same fingerprint) auto-demotes to Tier 3 + RCA.

## Support File Map

Full file→**When to read** index (100+ entries, keyed by failure signature): `references/support-file-map.md` — consult before any scan/fix.
