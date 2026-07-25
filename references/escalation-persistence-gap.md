# Escalation Persistence Gap — "tracked but not written" variant

## The gap
A scan's journal can reference root-cause fingerprints as **"tracked"** (e.g. in
`previous_scan_delta.stable_root_cause` or prose) while setting
`escalation_needed: false` on the journal itself. The issue was **never written
to `issues.jsonl`**. Net effect: the escalation silently drops — Mentor and the
escalation runner never see it, and every subsequent scan re-reports it as
"tracked" without ever persisting it.

This is distinct from the classic Step 8b gap (journal says
`escalation_needed: true` but no `issues.jsonl` entry exists). Here the journal
never even claimed to escalate — it just *narrated* the root cause as known.

## Why it happens
- The scan classified the root cause as known / non-novel and set
  `escalation_needed: false` — often because the affected jobs are already
  **paused**, so the scan treats them as "filtered stale" rather than
  "actionable."
- But "paused + stale" at the **job** level ≠ "root cause is tracked in the
  **issues store**." The underlying user-gated root cause (credits exhausted,
  OAuth revoked, model removed) still needs an `issues.jsonl` entry so Mentor
  can act on it.

## Detection (during light scan)
1. After classifying non-auto-fixable root causes, collect their intended
   fingerprint names (e.g. `oc_openrouter_402_credits_exhausted`,
   `oc_google_oauth_*_invalid_grant`, `oc_openrouter_404_*_model_removed`).
2. Parse `issues.jsonl` (use `scripts/parse_issues_jsonl.py`) and build the set
   of existing `fingerprint` values.
3. For any intended fingerprint **absent** from that set, the escalation was
   never persisted. Write it now (`status: open`, `escalation_needed: true`).

## Fix
Append the missing entries to `issues.jsonl` as JSON lines. Set
`escalation_needed: true` so Mentor picks them up. Do **not** re-count the same
root cause across many paused jobs as many issues — **one issue per root-cause
fingerprint**, with `affected_components` listing the affected job names.

## Confirmed instance (2026-07-07)
Prior light scans reported `oc_openrouter_402_credits_exhausted_20260706` and
the OAuth revocation as "tracked" in `previous_scan_delta`, but `issues.jsonl`
contained neither (only stale skill-hygiene entries). 71 jobs failing with 402
(credits) and 2 with OAuth `invalid_grant` had no persisted escalation. The
07:08 light scan re-derived the enabled-error set live, found the gap, and wrote
3 missing escalations:
- `oc_openrouter_402_credits_exhausted` (71 jobs)
- `oc_google_oauth_email_check_invalid_grant` (2 jobs)
- `oc_openrouter_404_owl_alpha_model_removed` (7 jobs)