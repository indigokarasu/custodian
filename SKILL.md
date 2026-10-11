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
  version: "3.3.0"
  hermes:
    tags:
      - monitoring
      - system-health
      - log-analysis
      - cron
      - OCAS-core
    category: devops
    config:
      - key: HERMES_HOME
        description: "Hermes home; roots the profile / skills / scripts paths every scan reads"
        default: "~/.hermes"
      - key: HERMES_PROFILE
        description: "Active profile name; selects <HERMES_HOME>/profiles/<profile>/cron/jobs.json"
        default: "indigo"
      - key: HERMES_PROFILE_HOME
        description: "Profile home (<HERMES_HOME>/profiles/<HERMES_PROFILE>) for journals and commons"
        default: "~/.hermes/profiles/indigo"
      - key: AGENT_ROOT
        description: "Agent root for commons/ paths (e.g. $AGENT_ROOT/profiles/<profile>/commons)"
        default: "~/.hermes"
triggers:
- system health
- log errors
- cron failures
- skill journal errors
- operational monitoring
---

# Custodian

Enforces the `spec-ocas-recovery.md` contract: every scheduled run writes evidence; schedule gaps trigger remedial passes; degraded mode is explicit, never a silent skip; self-repair re-validates.

## When to Use

Interactive invocation offers a two-level menu: `references/interactive-menu.md`.

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

One-line index; **read `references/known-code-fixes.md` before applying any fix whose signature matches** — it carries the file, the mechanism, and the correct handling for each.

- `send_email.py` template mismatch (`oc_vesper_template_missing`) — only `job_search` valid.
- Wrong commons path prefix — use `$AGENT_ROOT/profiles/<profile>/commons/...`.
- Plugin manifest parse spam — stray `...` YAML document-end markers; check upstream first.
- Tier 4 code fixes + MCP cascade triage; secret-redaction source corruption (fix directly, never user-gate).
- Google OAuth `deleted_client` / `invalid_grant`; after a `googleapiclient` `ModuleNotFoundError`, immediately re-check token revocation.
- Env-sync gate recurrence; `hello_operator` `Router` naming trap (`ps | grep router` misses it).
- Safety envelope, background-task conformance, activity model + schedule optimization, storage/platform, script-path rejection.

## Critical Pitfalls

Full catalogue — 40+ measurement-integrity rules, each written because a real number came from a defect rather than from the system: `references/measurement-pitfalls.md`. **Read it before publishing any zero, rate, or "clean" verdict.** The six rules that cause the most findings, the three runner traps, and the cron tool-quirk table live in `references/measurement-rules-six.md`; the four most critical narratives (entry-module probe, ImportError during update, shared store interpreter, named driver verification) live in `references/inline-pitfall-narratives.md`.

Load-bearing, one line each: a sweep that reads 0 files is indistinguishable from a clean sweep (print the per-set file count and the exit code); the denominator must physically host the numerator (assert capacity per hour); key every hourly bucket by DAY *AND* HOUR, in LOCAL time; every appended `issues.jsonl` row must set `issue_id` AND `escalation_needed`; cron is MINUTE HOUR DOM MONTH DOW; before publishing any zero or rate, grep the literal signature once and READ the line.

## Responsibility Boundary

**Owns:** gateway log scanning + fingerprinting, cron registry health, skill journal completeness, data-dir health, skill initialization, background-task conformance, Tier 1 auto-repair, activity/schedule optimization, escalation signaling, fix-effectiveness tracking, tier management, library hygiene.

**Does not own:** OKR trends (Mentor), skill design (Mentor, Forge), lesson extraction (Praxis), briefings (Vesper), social graph (Weave). Never modifies files inside a skill package.

**Ontology:** operates on system health data (logs, config, journals, storage). **Cooperation:** Vesper reads InsightProposals written to the proposals dir; Mentor reads journals tagged `escalation_needed: true`.

## Commands

**These are deferred tools, not shell commands.** Invoke them through `tool_describe` / `tool_call` by exact name. There is no `custodian` binary on PATH and `hermes custodian ...` is not a hermes subcommand — running either as a terminal command fails and wastes 3-6 tool calls every session.

| Tool name | Args | Does |
|---|---|---|
| `custodian_scan` | `mode: light \| deep` | light = tail gateway log, cron registry, retry failed fixes; deep = full sweep (`references/deep-scan.md`). **⚠️ Verified broken 2026-10-04 — do not use its counts as a census; read `references/custodian-scan-tool-broken.md` first.** Treat output as unverified until `returned_mode == requested_mode` and no two entries share identical evidence across distinct fingerprint ids; prefer `scripts/custodian_sweep.py` (self-test first) + direct registry probes. |
| `custodian_issues` | `action: list \| summary \| resolve` (+`issue_id`) | issue triage |
| `custodian_cron_health` | none | cron health report + alert gate |
| `custodian_status` | none | plugin status (**currently broken**: raises `KeyError: 'attempts'` — do not retry, fall back to `custodian_cron_health`) |

`custodian.init` — create storage, register background tasks, build activity model. `custodian.verify {fix_id}` · `repair.auto` · `repair.plan` · `schedule.show` · `escalation-runner` · `update`.

`custodian.secrets.audit` — read-only inline-secret scan, deduped by value (`references/secret-audit.md`). `custodian.secrets.remediate` — plan; `--apply` migrates the safe subset (MCP `headers` to `${ENV}`; never overwrite `.env` keys; back up each file; credential blobs stay MANUAL). Re-run the audit until 0 inline hits.

## Example

A light scan reads `jobs.json`, tails the gateway log for new errors, fingerprints each, and journals the result. A clean scan (all errors transient) returns `[SILENT]` *after* that journal — the journal proves the scan ran; `[SILENT]` suppresses delivery noise only.

## Confidence Model

`confidence_score = sample_confidence × success_rate`; tiers auto-promote/demote on fix history — `references/confidence-model.md`. Tier 3 escalation is confidence-gated at `>= 0.6` with `recommended_tier == 1` → auto-fix instead.

## Workflow: Scan & Escalation Loops

**Full checklists in `references/execution-loops.md` — read before any scan or escalation run.** Four loops: Light Scan (10 steps), Deep Scan (13 steps, `references/deep-scan.md`, early-exit when all errors are transient cf=None/0), Escalation Runner (8 steps + verification, already-classified fast path under 2h old and unchanged), and Escalation Execution Loop (bidirectional state verification, missed-enrollment sweep, four-bucket classification, one-pass `issues.jsonl` reconcile).

Run gates (all apply in cron context):

- [ ] Registry parsed from `<profile>/cron/jobs.json` (never `hermes cron list`); read the raw file head BEFORE parsing (a wrong key path yields a false-clean); raw-file re-check before any false-clean verdict
- [ ] New gateway errors since the last scan tailed and fingerprinted; error jobs re-run/live-probed before stale-vs-active classification
- [ ] Journal written BEFORE `[SILENT]`; fix outcomes re-validated before any issue is marked resolved

**Cron silence protocol:** runs with no actionable issues respond with exactly `[SILENT]`. **Journal-before-silent:** a no-op scan MUST write its observation journal (`not_activity_reason` set) first — that journal is the evidence the contract requires.

**Two-stage self-healing contract** (`spec-ocas-recovery.md`): 1) log the decision (what, why, fingerprint, Tier) to `issues.jsonl`/journal BEFORE any fix; 2) attempt the repair; 3) re-validate BEFORE logging success — re-run the affected job/script, confirm the original signature is gone, then mark resolved. A fix without re-validation is a claim, not a resolution.

**Post-fix verification:** after any Tier 1 fix, re-check the targeted log/config, then close the registry loop with `hermes cron run <id>` (batch: `scripts/verify_fixes_cron_run.py`).

**Operational hints.** *Model-pin drift can't run via CLI (2026-07-22):* `hermes cron` has no `update` subcommand and `edit` lacks `--provider`/`--model`. If <operator> chose a pin target, edit `jobs.json` directly; else leave user-gated (`references/escalation-execution-loop.md`). `hermes cron edit` DOES exist for prompt/schedule/name — check `--help` before concluding anything is uneditable. *False-escalation resolution:* an open `user_gated` issue claiming "Job still erroring live" with `last_run_at` days before your sweep is the stale-error signature — re-run the job; success = FALSE ESCALATION, resolve it (`scripts/race_safe_issue_patch.py` patches race-safely).

## Core Fingerprints (Operational Detection Set)

**When to read:** during light-scan Step 3 (fingerprint matching) and Step 6 (recurrence check). Full table: `references/custodian-core-fingerprints.md`; Tier-2 surface-only catalog: `references/non-fatal-error-patterns.md`.

## Escalation Path

Tier 3: InsightProposal to the proposals dir + journal tag `escalation_needed: true`. Full execution lessons — read before applying a Tier 1/2 fix or filing anything `user_gated`: `references/escalation-execution-lessons.md`.

## Background tasks

| Job | Mechanism | Schedule | Command |
|---|---|---|---|
| `custodian:light` | cron | `0 * * * *` | `custodian_scan` mode=light |
| `custodian:deep` | cron | `0 8,14,20,2 * * *` | `custodian_scan` mode=deep |
| `custodian:escalation-runner` | cron | `*/30 9-17 * * 1-5` | `custodian_issues` action=resolve |
| `custodian:cron-health` | cron (no_agent) | `0 8,14,20,2 * * *` | `custodian_cron_health` |

## Scripts

25 scripts, all with `--help` (lazy imports — works without optional deps). Per-script usage, cron staggering, and the call-script-vs-reason-directly table: `references/using-script.md`. The one that matters most:

`append_issue_row.py` — **the sanctioned append path to `issues.jsonl`.** Asserts `issue_id` + `escalation_needed` on every row and verifies the open-count delta against the filter as written, so the silent-visibility defect fails loudly at write time. `--dry-run` prints the row and the expected delta and writes nothing; the append path is `--expect-delta N`, which re-prints the open-count delta and verifies it. Read the flags off the script's own `--help`, not off this file. `references/append_issue_row_control.py` (24 arms, throwaway store — run after any change).

## Self-Update

Skill updates run centrally via the fleet updater — a shared `update_skill.sh` scheduled under the Hermes home; it never discards uncommitted or unpushed work. `custodian.update` self-updates the plugin from GitHub. Do NOT push here (local reference copy) — `references/plugin-vs-skill-architecture.md`.

## Reference Collections

- **OKRs** — `references/okrs.md`. **Disk compaction** (cleanup when disk >80%) — `references/disk-compaction.md`.
- **Operational gotchas** (14 items: skill-package immutability, pipe-to-interpreter, confidence auto-tiering, log compaction, library hygiene): `references/custodian-gotchas.md`. Provider/credential/escalation-state traps: **read `references/operational-gotchas.md` before any Tier 1 fix**.

## Error Handling

| Failure | Handling |
|---------|----------|
| `read_file` "BLOCKED: already read" / transient error | re-read via `terminal(command="cat /path")` / `tail`; retry once first |
| Pipe-to-interpreter blocked | script to a unique `/tmp/` name via `write_file` → `terminal()`; never retry pipe variants |
| `write_file` "missing required field: path" | fallback `cat > /path << 'EOF' ... EOF` |
| jobs.json parses to 0 jobs | `d.get("jobs", [])`; re-inspect raw head before concluding 0 |
| `last_status=error` after a fix | registry lags — `hermes cron run <id>` flips it; stale ≠ failure |
| literal `gateway restart` in command | interlock blocks — reword (e.g. "gateway reload") |
| `append_issue_row.py` exits 2 | the row was appended and its open-count delta disagreed — inspect the row before trusting the store |
| a sweep returns 0 for every fingerprint | almost always a filter/opener bug, not a clean system — see `references/measurement-pitfalls.md` |

Full table (all 20 cron failure modes + tool variants): `references/cron-tool-failure-handling-table.md`.

**Validation loop (all fixes):** apply → re-check log/config → confirm gone → journal → fix-loop (≥3× same fingerprint) auto-demotes to Tier 3 + RCA.

## Support File Map

Full file→**When to read** index, keyed by failure signature: `references/support-file-map.md` — consult before any scan/fix.
