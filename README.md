# ⚙️ Custodian

  <img src="./assets/readme/hero.jpg" width="100%" alt="Custodian">

Monitors agent gateway logs, cron jobs, skill journals, and OCAS data directories for operational failures. Detects errors, applies safe non-destructive fixes autonomously during quiet hours, and escalates only what it cannot fix. Performs root cause analysis on recurring errors with fix-loop detection and confidence-tier auto promote/demote. Use when: cron jobs fail or show stale errors, gateway logs show repeated error patterns, skill journals have gaps, disk usage exceeds thresholds, MCP servers crash-loop, or after any gateway restart. Keywords: cron health, log analysis, system monitoring, error fingerprinting, auto-repair, fix-loop detection, operational conformance. NOT for OKR trend analysis, skill design evaluation, behavioral lesson extraction, briefing delivery, entity knowledge queries, or social graph queries.

**Skill name:** `ocas-custodian`
**Version:** 3.0.0+hermes
**Type:** 
**Layer:** devops
**Author:** Indigo Karasu

---

## 📖 Overview

Monitors agent gateway logs, cron jobs, skill journals, and OCAS data directories for operational failures. Detects errors, applies safe non-destructive fixes autonomously during quiet hours, and escalates only what it cannot fix. Performs root cause analysis on recurring errors with fix-loop detection and confidence-tier auto promote/demote. Use when: cron jobs fail or show stale errors, gateway logs show repeated error patterns, skill journals have gaps, disk usage exceeds thresholds, MCP servers crash-loop, or after any gateway restart. Keywords: cron health, log analysis, system monitoring, error fingerprinting, auto-repair, fix-loop detection, operational conformance. NOT for OKR trend analysis, skill design evaluation, behavioral lesson extraction, briefing delivery, entity knowledge queries, or social graph queries.

---

## 🔧 Capabilities

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
- `oc_google_oauth_client_deleted` — when the OAuth client itself is deleted from Google Cloud Console (`deleted_client` error). Requires new OAuth client creation + browser re-auth.
- `oc_google_oauth_token_revoked` — when the refresh token is revoked/expired (`invalid_grant: Token has been expired or revoked.`). Distinct from the above — the OAuth client exists but its tokens are dead. **Only affects jobs using the revoked account's credential file directly**. Confirmed 2026-06-29: only `email:check` and `monitor:list` (which wraps `tasks_monitor.py` with `CREDS_FILE = ".../<user-google-email>.json"`) fail. `sands:*`, `taste:*`, `vesper:*` continue working because they use different auth flows or different account credentials.
- `scripts/classify_error_jobs.py` — deterministic probe: reads the profile `jobs.json`, buckets enabled error jobs by `last_error` fingerprint, and lists every `Script exited with code 1` job with its `script` name so each can be inspected individually (de-aggregation). Run via `terminal(command="python3 <hermes-home>/profiles/indigo/skills/ocas-custodian/scripts/classify_error_jobs.py")`.
- `scripts/classify_llm_necessity.py` — deterministic LLM-necessity classifier: reads jobs.json and evaluates every enabled non-paused LLM job against a heuristic (self-update, script-wrapper, skill-load+script). Outputs verdicts: `llm_unnecessary` (convert candidate), `llm_borderline` (needs wrapper), `llm_needed` (genuine). Includes `--unit-test` flag and `--json` for machine output. Run via `terminal(command="python3 <hermes-home>/profiles/indigo/skills/ocas-custodian/scripts/classify_llm_necessity.py")`.
- `scripts/classify_llm_necessity_integration.py` — cron-health integration: runs `classify_llm_necessity.py`, checks the acknowledgment state file (`llm_necessity_ack.json`), and writes/updates a single `oc_cron_llm_unnecessary` issue in `issues.jsonl` for new/unacknowledged candidates. NEVER auto-converts jobs. Run via `terminal(command="python3 <hermes-home>/profiles/indigo/skills/ocas-custodian/scripts/classify_llm_necessity_integration.py")`.
- `scripts/verify_escalation_state.py` — escalation-loop bidirectional verification probe: parses the profile `issues.jsonl` (brace-depth) and `jobs.json`, checks both staleness directions, and reports per-issue `jobs_paused` deltas vs the live paused set. Run via `terminal(command="python3 <hermes-home>/profiles/indigo/skills/ocas-custodian/scripts/verify_escalation_state.py")`. Run it FIRST in every escalation loop to decide whether any `issues.jsonl` write is needed (no-delta fast-path). See `references/escalation-execution-loop.md`.
- `scripts/find_missed_user_gated_jobs.py` — escalation-loop missed-enrollment probe: loads `jobs.json`, finds every `enabled`+erroring job NOT in any issue's `jobs_paused`, classifies its `last_error` against known user-gated fingerprints (Nous 401, OpenRouter 402, owl-alpha 404, Google 403/401), and reports MISSED enrollments vs genuinely transient vs UNKNOWN. Treat MISSED as "open/enroll for tracking" by default; do not automatically pause provider/model failures. Pause only when the narrow pause criteria are met and `paused_reason` plus a re-enable check are written. Run it AFTER `verify_escalation_state.py` to catch jobs that failed in the inter-scan window and were never enrolled.
- `scripts/scan_escalation_journal_gaps.py` — escalation-loop journal-to-issues gap probe: walks ALL custodian journal dirs (profile + commons, subdirs + loose files), parses each (list/concatenated JSON via brace-depth), and for journals within `--hours` (default 24) with `escalation_needed: true`, cross-references cited fingerprints / `escalation_refs` against OPEN issues in the profile `issues.jsonl`. Reports GAPs (flagged but no matching open issue — the Step 8b/8b-variant silent-drop) and RECOVERY notes (forward-stale candidates). Uses CONTENT timestamps (not mtime) because journal mtimes lag ~7h. Read-only by default; `--write` creates missing issues. Run it as the journal half of the escalation loop, alongside the two job-state probes. See `references/escalation-execution-loop.md`. **FALSE-POSITIVE GUARD (2026-07-15):** it matches flagged journals only against OPEN issues, so a journal whose referenced issue IS already `resolved`/`duplicate` surfaces as a spurious "GAP". Before any `--write`, re-verify each reported GAP against the FULL issues.jsonl resolved-count — never re-persist an already-resolved issue as a duplicate escalation. Confirmed 2026-07-15: 3 reported gaps (`oc_script_timeout_chronicle_embed_20260713`, `oc_script_timeout_chronicle_embed`, `oc_state_db_oversized_20260714T2007`) were all already `resolved` — false positives, no action taken.

---

## 📊 Outputs

See `SKILL.md` for outputs, journals, and persistence rules.

---

## 📄 Files

| File | Purpose |
|---|---|
| `SKILL.md` | Skill definition |
| `references/` | Supporting documentation |
| `scripts/` | Helper scripts |


## 📚 Documentation

Read `SKILL.md` for operational details, schemas, and validation rules.

Read `references/` for detailed specifications and examples.


---

## 📄 License

MIT License — see `LICENSE` for details.