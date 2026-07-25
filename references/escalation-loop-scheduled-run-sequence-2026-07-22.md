---
name: escalation-loop-scheduled-run-sequence-2026-07-22
license: MIT
description: Worked probe sequence for a scheduled-cron escalation execution loop that finds only user-gated issues plus one untracked live code defect. Concrete order, classification, and the gap-persist decision.
---

# Scheduled Escalation-Loop Run Sequence (worked example 2026-07-22)

Use when a cron/dispatch prompt invokes the escalation execution loop. This is the
verified probe order from a real run where NO issue was auto-fixable and one live
failure was untracked.

## Trigger-premise guard (first)
The invoking prompt may name a status filter that **does not exist** in the schema
(e.g. `status in ("escalated","fix_attempted_failed")`). Always override with the
real open-signal (see `references/escalation-loop-issue-status-scan-trap.md`):
`status not in ("resolved","duplicate") AND (escalation_needed == true OR status == "user_gated")`.
Use a brace-depth parser (`scripts/parse_issues_jsonl.py`, or inline
`json.JSONDecoder().raw_decode`) — never `json.loads(line)` per line (multiple objects
per line).

## Probe order
1. **`scripts/verify_escalation_state.py`** FIRST. Reports per-issue `jobs_paused` delta
   vs live, inverse-gotcha (issue says resolved but job still erroring), and
   forward-staleness candidates. If `Reconcile write needed: False`, no issues.jsonl
   write is needed for pause-state sync.
2. **Dump + classify every enabled+erroring job individually** from
   `~/.hermes/profiles/<profile>/cron/jobs.json` (top-level `"jobs"` key — NOT
   `data.jobs`). Do NOT trust `issues.jsonl` alone: the 4 erroring jobs in this run were
   2 transient (429, interpreter-shutdown → leave running), 1 spend-guard (matches an
   open issue), and 1 untracked live code defect.
3. **`scripts/reopen_false_resolutions.py`** (dry-run; `--write` to persist) — confirms
   no prior `resolved` issue is still live.
4. **`scripts/find_missed_user_gated_jobs.py`** — finds enabled+erroring jobs not in any
   `jobs_paused`. Treat MISSED as "open/enroll for tracking", not automatic pause for
   provider/model failures.
5. **`scripts/scan_escalation_journal_gaps.py --hours 24`** (read-only) — finds journals
   flagged `escalation_needed: true` with no matching OPEN issue. **FALSE-POSITIVE GUARD:**
   cross-reference each reported gap fingerprint against the FULL issues.jsonl resolved
   count. A fingerprint whose issue is already `resolved`/`duplicate` is a spurious gap —
   do NOT re-persist it. (In this run `oc_enrich_embeddings_sqlite_readonly_monkeypatch`
   surfaced as a gap but was already resolved → ignored.)

## Classification decision
- **User-gated / no CLI path** → leave `escalation_needed: true`, do NOT mark resolved.
  Confirmed non-fixable classes this run:
  - Interactive OAuth (Spotify) — job already `enabled:false` (paused), no burn.
  - **Config-drift model-pin**: `hermes cron update` / `--provider` / `--model` DO NOT
    EXIST (verify via `hermes cron --help`). Spend-guard aborts before any inference
    (zero cost). Repin needs a <operator>-chosen target written directly to `jobs.json`.
    Do NOT fabricate a failing CLI call.
  - **DB oversized at >80% disk**: skill rule = prune (not VACUUM); destructive + needs
    <operator> policy decision → leave user_gated.
- **Untracked live code defect** (job errors with a real traceback, not in any open
  issue) → persist a new Tier-4 issue for Forge. **Do NOT blind-edit skill-package
  source** (immutability guard). Capture the root cause so Forge's one-line fix is
  obvious. Example persisted this run: `oc_rally_daily_performance_tuple_get_20260722`
  — `fetch_daily_closes()` returns `(out, truncated)` tuple but caller does `.get()`;
  fix is `[0]` unpack.

## Honesty rule
Never report a user-gated billing/OAuth/model issue as "fixed". Pausing / spend-guard
abort is mitigation, not resolution. All genuinely user-gated issues stay open pending
<operator>.

## Close-out
- Persist any new issue via a race-safe append (a sibling `custodian:light` may rewrite
  `issues.jsonl` at the top of the hour) — write ONE line, then `grep -c` to verify.
- Write an **action journal** to
  `commons/journals/ocas-custodian/YYYY-MM-DD/<run_id>.json` with `json.dump()` (never
  heredoc) even when no fix was applied. Set `not_activity_reason: null` and
  `action_taken: true`.
