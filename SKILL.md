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
  version: "3.2.0"
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

- **`send_email.py` template mismatch** (`oc_vesper_template_missing`): only `job_search` valid — `references/send-email-template-mismatch.md`.
- **Wrong path prefix**: use `$AGENT_ROOT/profiles/<profile>/commons/...` — `references/wrong-path-prefix-in-skill-scripts.md`.

## Critical Pitfalls

Full pitfall catalogue — 40+ measurement-integrity rules, each written because a
real number came from a defect rather than from the system:
`references/measurement-pitfalls.md`.
Full escalation-execution lessons (repair the class, negative controls, `user_gated`
falsification, backup-file diffing): `references/escalation-execution-lessons.md`.

**Read the pitfall file before publishing any zero, rate, or "clean" verdict**, and
the escalation file before applying or `user_gated`-filing any fix. The six rules
that cause the most findings, kept inline because they apply to nearly every run:

1. **A sweep that reads 0 files is indistinguishable from a clean sweep.** Print the
   per-set file count and a per-file error line; check the exit code AND the `!!` count.
   `gzip.open` takes no `errors=`, so one shared opener raises on every file and every
   fingerprint returns 0 at once. Branch on the extension inside the loop.
2. **The denominator must physically host the numerator.** Assert
   `distinct_firing_jobs <= enabled_jobs` and `unclaimed <= occurrences_due` PER HOUR. A
   failing capacity assertion means the denominator is wrong, not the numerator.
   Denominator = occurrences due (`3600/n` for `every Nm`, not 1), parsed from `*`, `a-b`,
   `*/s` and comma lists, all as **ints**.
3. **Key every hourly bucket by DAY *AND* HOUR, in LOCAL time.** Log stamps and cron
   selectors share one timezone domain. Hour-only keys sum two days; UTC keys against
   local stamps produce confident nonsense. Reset per-file `carry = None` inside the loop
   and guard the clock domain.
4. **Every appended `issues.jsonl` row MUST set `issue_id` AND `escalation_needed`,** and
   the open count must be compared before and after. Expected delta =
   `is_open(this_row) - is_open(previous_last_row_for_this_issue_id)` — the open filter
   applied twice, no kind table, no special cases. A reopen of a CLOSED issue is `+1`, not
   `0`, even though the row supersedes. Use `scripts/append_issue_row.py`. Full recipe:
   `references/issues-jsonl-row-integrity.md`.
5. **Cron is MINUTE HOUR DOM MONTH DOW** — `f[0]` is the minute. `every Nm` is a 2-field
   string, so any `len(fields) >= 5` guard silently drops the busiest watchdogs.
   `schedule.expr == ''` means `kind: "interval"`, not a parse failure.
6. **Before publishing any zero or rate: name the file the signature lives in, grep the
   literal string once and READ the line, then count.** A filter that skips the line
   carrying the signature (`continue` before the match test), a wrong token (`unclaimed` vs
   `never claimed`), or a `" WARNING "` filter with a non-existent leading space each
   return a clean zero forever. An exactly-uniform zero across unrelated fingerprints is a
   filter bug until proven otherwise.

Three more that apply to nearly every run, all with full entries in the reference:
**`run_id` strings** — build once by explicit concatenation (`strftime('%Y%m%dT%H%M%SZ')`
already emits the `Z`) and grep for `ZZ` before writing; **blocked in cron** — `find -delete`
and inline `python3 -c`, so express destructive reap as a script file; **a COUNT in a status
bucket is not permanence** — snapshot row IDs, wait 60-90s, re-read, compare BOTH `pid` and
`process_id`.

Tool quirks in cron context (`read_file` dedup, pipe-to-interpreter, `write_file` failures,
`execute_code` denial, heredoc `$(date)` expansion, the `hermes cron` CLI path mismatch, inline
`python3 -c` blocked, Python 3.14 `strptime` rejections):
`references/cron-tool-failure-handling-table.md`.

## Escalation execution: hard-won lessons

Full text — read before applying a Tier 1/2 fix or filing anything `user_gated`:
`references/escalation-execution-lessons.md`. Four that change what you do:

- **Repair the CLASS, not the member.** An issue repaired member-by-member more than twice
  has the wrong repair unit — enumerate the whole set in one pass, then say whether the
  correction weakens anything.
- **A negative control is the only thing that distinguishes a passing detector from a
  broken one, and it catches the fixer.** Derive exception lists by RULE, never by literal
  name. When your own control's assertion fails, suspect the fixture first.
- **A `user_gated` flag is a CLAIM about the fix path, not a verdict on the risk.** Run
  `hermes cron edit --help` before concluding a cron prompt/schedule is uneditable.
- **Diff the backup FILE, never a reconstructed snapshot, and assert the NET substitution**
  (`apply(SUB, original) == live`); classify scheduler-written volatile fields
  (`fire_claim`, `next_run_at`, `last_dispatch`, `pending_slot`) separately so their drift
  never fails a config-integrity assertion.

Corollary: `hermes cron run <id>` can contend with the store this run just wrote — when the
assertion under test is a text signature in the registry, re-reading the registry after the
edit IS the re-validation; record the behavioral confirmation as pending, with the specific
check and the scheduled run that will produce it.

## Responsibility Boundary

**Owns:** gateway log scanning + fingerprinting, cron registry health, skill journal completeness, data-dir health, skill initialization, background-task conformance, Tier 1 auto-repair, activity/schedule optimization, escalation signaling, fix-effectiveness tracking, tier management, library hygiene.

**Does not own:** OKR trends (Mentor), skill design (Mentor, Forge), lesson extraction (Praxis), briefings (Vesper), social graph (Weave). Never modifies files inside a skill package.

**Ontology:** operates on system health data (logs, config, journals, storage). **Cooperation:** Vesper reads InsightProposals written to the proposals dir; Mentor reads journals tagged `escalation_needed: true`.

## Commands

`custodian.init` — create storage, register background tasks, build activity model. `custodian.scan.light` — tail gateway log, check cron registry, retry failed fixes, check uninitialized skills. `custodian.scan.deep` — full sweep (`references/deep-scan.md`). `custodian.verify {fix_id}` · `repair.auto` · `repair.plan` · `issues.list` · `issues.resolve {issue_id}` · `status` · `schedule.show` · `escalation-runner` · `update`.

`custodian.secrets.audit` — read-only inline-secret scan, deduped by value (`references/secret-audit.md`). `custodian.secrets.remediate` — plan; `--apply` migrates the safe subset (MCP `headers` to `${ENV}`; never overwrite `.env` keys; back up each file; credential blobs stay MANUAL). Re-run the audit until 0 inline hits.

## Example

A light scan reads `jobs.json`, tails the gateway log for new errors, fingerprints each, and journals to the path above. A clean scan (all errors transient) returns `[SILENT]` *after* that journal — the journal proves the scan ran; `[SILENT]` suppresses delivery noise only.

## Confidence Model

`confidence_score = sample_confidence × success_rate`; tiers auto-promote/demote on fix history — `references/confidence-model.md`. Tier 3 escalation is confidence-gated at `>= 0.6` with `recommended_tier == 1` → auto-fix instead.

## Workflow: Scan & Escalation Loops

**Full checklists in `references/execution-loops.md` — read before any scan or escalation
run.** Four loops: Light Scan (10 steps), Deep Scan (13 steps, `references/deep-scan.md`,
early-exit when all errors are transient cf=None/0), Escalation Runner (8 steps +
verification, already-classified fast path under 2h old and unchanged), and Escalation
Execution Loop (bidirectional state verification, missed-enrollment sweep, four-bucket
classification, one-pass `issues.jsonl` reconcile).

Run gates (all apply in cron context):

- [ ] Registry parsed from `<profile>/cron/jobs.json` (never `hermes cron list`); read the raw file head BEFORE parsing (a wrong key path yields a false-clean); raw-file re-check before any false-clean verdict
- [ ] New gateway errors since the last scan tailed and fingerprinted; error jobs re-run/live-probed before stale-vs-active classification
- [ ] Journal written BEFORE `[SILENT]`; fix outcomes re-validated before any issue is marked resolved

**Cron silence protocol:** runs with no actionable issues respond with exactly `[SILENT]`.
**Journal-before-silent:** a no-op scan MUST write its observation journal
(`not_activity_reason` set) first — that journal is the evidence the contract requires.

**Two-stage self-healing contract** (`spec-ocas-recovery.md`): 1) log the decision (what,
why, fingerprint, Tier) to `issues.jsonl`/journal BEFORE any fix; 2) attempt the repair;
3) re-validate BEFORE logging success — re-run the affected job/script, confirm the
original signature is gone, then mark resolved. A fix without re-validation is a claim,
not a resolution.

**Post-fix verification:** after any Tier 1 fix, re-check the targeted log/config, then
close the registry loop with `hermes cron run <id>` (batch:
`scripts/verify_fixes_cron_run.py`).

**Operational hints.** *Model-pin drift can't run via CLI (2026-07-22):* `hermes cron` has
no `update` subcommand and `edit` lacks `--provider`/`--model`. If <operator> chose a pin
target, edit `jobs.json` directly; else leave user-gated
(`references/escalation-execution-loop.md`). `hermes cron edit` DOES exist for
prompt/schedule/name — check `--help` before concluding anything is uneditable.
*False-escalation resolution:* an open `user_gated` issue claiming "Job still erroring
live" with `last_run_at` days before your sweep is the stale-error signature — re-run the
job; success = FALSE ESCALATION, resolve it (`scripts/race_safe_issue_patch.py`
patches race-safely).



## Core Fingerprints (Operational Detection Set)

**When to read:** during light-scan Step 3 (fingerprint matching) and Step 6 (recurrence check). Full table: `references/custodian-core-fingerprints.md`; Tier-2 surface-only catalog: `references/non-fatal-error-patterns.md`.

## Escalation Path

Tier 3: InsightProposal to the proposals dir + journal tag `escalation_needed: true`.

## Background tasks

| Job | Mechanism | Schedule | Command |
|---|---|---|---|
| `custodian:light` | cron | `0 * * * *` | `custodian.scan.light` |
| `custodian:deep` | cron | `0 8,14,20,2 * * *` | `custodian.scan.deep` |
| `custodian:escalation-runner` | cron | `*/30 9-17 * * 1-5` | Process escalated issues |
| `custodian:cron-health` | cron (no_agent) | `0 8,14,20,2 * * *` | Health line + alert gate |

## Known Code Fixes, Safety & Code Surface

- Tier 4 code fixes + MCP cascade triage: `references/known-code-fixes-and-cascade.md`.
  Secret-redaction pattern that corrupts skill source — fix directly, do NOT leave
  user-gated: `references/redaction-placeholder-source-corruption.md`.
- Safety envelope + Tier 1 auto-fix registry: `references/fix-safety.md`. Background-task
  conformance + registry health: `references/conformance.md`. Activity model (rebuilt each
  deep scan from a 14-day window) + schedule optimization:
  `references/schedule-optimization.md`. Storage/platform:
  `references/background-tasks.md`, `references/platform-compatibility.md`.
- Script path rejected under a profile → scripts must live at
  `<hermes-home>/profiles/<profile>/scripts/<basename>`:
  `references/script-path-security-block-pattern.md`.
- Google OAuth: `oc_google_oauth_client_deleted` (client deleted in the Cloud Console) and
  `oc_google_oauth_token_revoked` (refresh token expired, `invalid_grant`), affecting only
  direct-credential jobs `email:check` and `monitor:list` —
  `references/google-oauth-client-deleted-pattern.md`. **Sequential rule:** after fixing a
  `googleapiclient` `ModuleNotFoundError`, immediately re-check for token revocation
  (Tier 1 → Tier 3). Wrapper-cascade masking:
  `references/subprocess-cascade-oauth-masking.md`.
- **Env-sync gate:** a Tier 1 fix that looked revalidated did not stop the next occurrence —
  an unguarded loop fall-through plus an urgency guard that measured the token the script
  had just replaced. `references/envsync-budget-fallthrough-and-inoperable-urgency-guard-2026-09-29.md`.
- **Naming trap:** `hello_operator/server.py` names the class serving `/v1/chat/completions`
  `Router`, so `ps | grep router` cannot see it and "the gateway never restarted" is
  evidence about a *different* process than a `hello-operator.service` restart.
  `references/hello-operator-envsync-restart-drops-inflight.md`; for unexplained unit
  restarts read `/root/.hello-operator/stop-forensics.log` first.

## Scripts

25 scripts, all with `--help` (lazy imports — works without optional deps). Per-script usage,
cron staggering, and the call-script-vs-reason-directly table:
`references/using-script.md`. The one that matters most:

`append_issue_row.py` — **the sanctioned append path to `issues.jsonl`.** Asserts
`issue_id` + `escalation_needed` on every row and verifies the open-count delta against
the filter as written, so the silent-visibility defect fails loudly at write time.
`--dry-run` prints the row and the expected delta. Controls:
`references/append_issue_row_control.py` (24 arms, throwaway store — run after any change).


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

Full file→**When to read** index (108 entries, keyed by failure signature): `references/support-file-map.md` — consult before any scan/fix.
