# Recurrence of a Fingerprint *Class* on a Distinct Job (2026-07-23)

## Problem

Custodian's existing journal→issues gap checks (Light Scan Step 8b and 8b-variant)
only fire when a **journal** flagged `escalation_needed: true` but no open issue
exists for the cited fingerprint. They do **not** catch this case:

- A live error job errors with an error whose **fingerprint class** has been seen before.
- The prior same-class issue(s) were correctly **resolved per-job** (not per
  fingerprint-family) — e.g. `oc_bones_content_policy_blocked_20260720` resolved
  for `bones:research`, `oc_sands_evening_brief_content_policy_20260722` resolved
  for `sands:evening-brief`.
- The *current* errored job (`ocas-autobio-observe`) is **not** in any of those
  resolved issues' `affected_job_ids`.
- No journal flagged it (it was classified as "known pattern / pre-classified").

Result: the same class of failure silently recurs on a new job, with no open
issue and no flag. Every scan re-classifies it as non-actionable and persists nothing.

This is distinct from:
- Step 8b/8b-variant gap (needs a *flagged journal* with no open issue — not present here).
- Step 8d false-resolution (that's about a *resolved* issue whose fault is *still live*;
  here the old issues were genuinely resolved for their own jobs).

## Detection recipe (Light Scan Step 8f)

For each live error job (after de-aggregation in Steps 6–8):

1. Map the error to a reusable fingerprint **class** token, e.g.:
   `content_policy_blocked`, `token_expired`, `402 credits`,
   `interpreter shutdown`, `Script exited with code 1`.
2. Grep the **full** `issues.jsonl` (resolved + open — NOT just open) for that class token.
3. If matches exist but **all** are `status: resolved`/`duplicate` AND the current
   job id is **not** in any matching issue's `affected_job_ids`:
   → NEW occurrence of a recurring class on a distinct job.
4. Persist a **fresh** dated issue:
   `status: open`, `escalation_needed: true`, `user_gated` per class nature,
   `affected_job_ids: [current_job_id]`, `affected_components: [job_name]`.
   Do NOT re-open the old resolved issue (it was correct for its own job).
5. **STALE-PREMISE GUARD:** require ≥1 live enabled job currently matching the
   signature before writing (re-derive from `jobs.json`, not the stored error string).

## Worked example (2026-07-23 light scan)

- Error job: `ocas-autobio-observe` (id `6ca08a339814`), `last_status=error`,
  `last_error: RuntimeError: content_policy_blocked: 你好，我无法给到相关内容。`,
  last ran 2026-07-22 23:04 PDT, enabled + scheduled.
- Class token: `content_policy_blocked`.
- `issues.jsonl` matches:
  - `oc_bones_content_policy_blocked_20260720` → `status: resolved`, `affected_job_ids: [33c4e5964e37]` (bones:research)
  - `oc_sands_evening_brief_content_policy_20260722` → `status: resolved`, `affected_components: [sands:evening-brief]`
- Current job `6ca08a339814` is in NEITHER → distinct-job recurrence.
- Written: `oc_autobio_content_policy_blocked_20260723T0505Z`
  (`status: open`, `escalation_needed: true`, `user_gated: true`, `tier: 3`).

## Why not just keep issues open per-class?

Resolving per-job is correct behavior — it lets a fixed job clear its issue
without being re-tripped by a sibling. The gap is purely in *detection*: a
recurrence on a new job must be recognized as a recurrence and tracked, even
though the original issue legitimately closed. Step 8f closes that detection gap
without breaking the per-job resolution model.
