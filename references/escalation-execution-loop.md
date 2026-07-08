# Escalation Execution Loop (external cron trigger)

Use when an external loop invokes Custodian (+ Mentor for planning) to **execute** fixes on
issues Custodian flagged but could not auto-fix — not merely to classify them. The default
`custodian.escalation-runner` checklist in SKILL.md is classification-oriented; this file
fills the execute-and-reconcile gap.

Confirmed working procedure (2026-07-07 escalation loop run):

## Procedure

1. **Verify live state BOTH directions** against `jobs.json` (profile path) before acting on any
   flagged issue:
   - (a) Inverse staleness: issue claims `resolved`/`jobs_paused` but the job is still
     `enabled: true, state: scheduled` and erroring → re-pause + re-open.
   - (b) Forward staleness: issue carries `escalation_needed: true` (e.g. `user_gated`) but the
     underlying job has **recovered** (`last_status: ok`, `last_error` cleared) → resolve it,
     don't just re-pause. Both directions are real; never trust the issue flag alone.

2. **Load issues from the PROFILE `issues.jsonl`** —
   `<hermes-root>/profiles/<profile>/commons/data/ocas-custodian/issues.jsonl`. It is
   authoritative. The commons copy (`<hermes-root>/commons/data/ocas-custodian/issues.jsonl`)
   is a lagging sync target — **write only to the profile path**. Entries may be concatenated on
   a single line; use a brace-depth parser (see `scripts/parse_issues_jsonl.py` or the template
   below).

3. **Classify each open issue**: Actionable (Tier 1 auto-fix) / User-gated (credits exhausted,
   invalid/out-of-funds API key, skill-internal hardcoded model) / Already-resolved / Legacy-inactive.

4. **Execute**:
   - Actionable → apply the Tier 1 fix (within the fix-safety envelope).
   - User-gated → **pause still-enabled failing jobs** by editing `jobs.json` directly
     (`enabled: false`, `state: 'paused'`). Pausing IS the action — it stops resource burn.
     Do NOT mark the issue resolved (the root cause still needs owner).

5. **Reconcile `issues.jsonl` in ONE pass** (safe edit pattern below):
   - Resolve issues whose underlying jobs actually recovered (set `status: resolved`,
     `escalation_needed: false`, add `resolved_at` + `resolution` note).
   - Write missing issues from persistence gaps: a prior scan flagged `escalation_needed: true`
     but never persisted the entry, OR a job evolved to a new fingerprint (e.g. `monitor:list`
     moved `invalid_grant` → `403 Forbidden (Google Tasks API)`). One issue per root-cause
     fingerprint; list affected job names in `affected_components`/`jobs_paused`.
   - For user-gated issues: update `jobs_paused` to match the live paused state, clear any false
     `resolved_at`, keep `status: user_gated` + `escalation_needed: true`, add a `mitigation`
     note.

6. **Verify** (re-read `jobs.json`: 0 enabled billing/key/owl jobs; re-parse `issues.jsonl`: state
   correct) then **write an action journal** (not `[SILENT]`).

## Fast-path: mitigation already complete (no-delta)

When you reach Steps 4-5 and verification shows **all user-gated failing jobs are already
paused** AND each open issue's `jobs_paused` already matches the live paused set (no
`missing`/`extra` deltas), the mitigation is complete. Do NOT rewrite `issues.jsonl` — a
no-delta rewrite is wasted work and, under concurrency, creates a needless race. Instead:
- Keep the issues `user_gated` + `escalation_needed: true` (root cause still unfixed).
- Write the **action journal** documenting the verification (counts, classification,
  recommendations). This is the deliverable — the loop's value here is *confirmation*,
  not a forced mutation.
- Report to the user with remediation asks (add credits / rotate key / fix skill-internal
  model / re-auth or enable API).

This is the expected steady-state outcome when a prior loop/scan already paused the jobs.
Confirmed 2026-07-07: all 4 open issues (OpenRouter 402, Nous 401, owl-alpha 404, Google
Tasks 403) were already fully paused with accurate metadata; the loop verified, documented,
and left them open.

## Concurrency pitfall

Multiple cron invocations of the escalation loop (and `custodian:light` scans) can fire
**simultaneously** against the same `issues.jsonl` and `jobs.json`. Observed 2026-07-07:
sibling-subagent write warnings plus concurrent `run_20260707_*.json` light journals in the
same date directory. Implications:
- Minimize writes. Never rewrite `issues.jsonl` when there is no delta (see fast-path above).
- When a write IS required, use the atomic one-per-line rewrite pattern (brace-depth parse
  -> mutate -> `os.replace` temp file). Last writer wins with correct data; the operation is
  idempotent in effect, so a concurrent writer cannot corrupt state.
- Pausing is idempotent (`enabled:false` twice is harmless); prefer it over reads-then-acts
  that assume exclusive access.

## Deterministic verification probe

`scripts/verify_escalation_state.py` performs the full bidirectional check (both directions
+ metadata-delta per open issue) and prints a summary. Run it FIRST in every loop execution
to decide whether any write is even needed:
```
python3 scripts/verify_escalation_state.py
```
It reports: live paused counts by fingerprint, INVERSE-gotcha count, FORWARD-staleness
candidates (issues whose jobs all recovered), whether a reconcile write is needed, and which
enabled+erroring jobs are intentionally left running (transient).

**Run `scripts/find_missed_user_gated_jobs.py` AFTER `verify_escalation_state.py`.**
`verify_escalation_state.py` lists `Enabled+erroring NOT in any jobs_paused` and heuristically
labels them `expected=transient, leave running` — but that label is only a default. The missed-
enrollment probe classifies each such job's `last_error` against known user-gated fingerprints
(Nous 401, OpenRouter 402, owl-alpha 404, Google 403/401) and reports which are MISSED
enrollments (pause + enroll into the matching issue) vs genuinely transient vs UNKNOWN (inspect).
This catches jobs that failed in the inter-scan window *after* the last esc pass and were never
added to an issue's `jobs_paused`.

## Honesty rule

Do NOT report user-gated billing / API-key / skill-internal issues as "fixed". Pausing is
mitigation, not resolution. They stay `user_gated` + `escalation_needed: true` until owner adds
credits, rotates the key, or edits skill code (Custodian must not edit skill-internal files).

## Inter-scan missed-enrollment pitfall (2026-07-07)

`verify_escalation_state.py` reports `Enabled+erroring NOT in any jobs_paused` and labels them
`expected=transient, leave running`. That label is ONLY a default — each must be inspected
individually. In the 2026-07-07T23:34Z loop, 3 of 6 such jobs (`bones:research`,
`bones:position-tracker`, `finch:work`) were emitting the Nous 401 message (`portal.nousresearch.com`)
but had never been added to `oc_nous_401_key_invalid_20260707`'s `jobs_paused` — they failed in
the inter-scan window *after* the 21:04 esc pass. They were real missed escalations, not
transient. Rule: if a job in that list matches a user-gated fingerprint, it is a MISSED
enrollment, not transient — pause it and enroll it into the matching issue (keep
`user_gated` + `escalation_needed: true`). Use `scripts/find_missed_user_gated_jobs.py` to
auto-classify; for each MISSED job: set `enabled: false`, `state: 'paused'` in `jobs.json` and
append its id to the recommended issue's `jobs_paused`.

This is the escalation-loop instance of the `custodian:light` gotcha "a job can fail in the
inter-scan window between scans" — it applies identically to the loop's own passes.

## Safe `issues.jsonl` edit pattern (brace-depth parser + one-per-line rewrite)

Write to `/tmp/` and run via `terminal(python3 /tmp/edit.py)` (cron context: no `execute_code`,
no pipe-to-interpreter).

```python
#!/usr/bin/env python3
import json, os
from datetime import datetime, timezone

P = "<hermes-home>/commons/data/ocas-custodian/issues.jsonl"
NOW = datetime.now(timezone.utc).isoformat()

def parse_line(line):
    objs = []; depth = 0; buf = ""; in_str = False; esc = False
    for ch in line:
        if in_str:
            buf += ch
            if esc: esc = False
            elif ch == '\\': esc = True
            elif ch == '"': in_str = False
            continue
        if ch == '"':
            in_str = True; buf += ch; continue
        if ch == '{':
            depth += 1; buf += ch; continue
        if ch == '}':
            depth -= 1; buf += ch
            if depth == 0:
                if buf.strip():
                    try: objs.append(json.loads(buf))
                    except Exception: pass
                buf = ""
            continue
        if depth > 0: buf += ch
    return objs

with open(P) as f:
    raw = f.read()
entries = []
for line in raw.splitlines():
    if line.strip():
        entries.extend(parse_line(line))

by = {}
for e in entries:
    by[e.get("issue_id") or e.get("id")] = e   # last wins (dedupe)

# --- mutate `by` here: resolve / write / update jobs_paused ---
# e.g. by["oc_x"]["status"] = "resolved"; by["oc_x"]["escalation_needed"] = False

# preserve first-appearance order, append any new
out, seen = [], set()
for e in entries:
    iid = e.get("issue_id") or e.get("id")
    if iid in by and iid not in seen:
        out.append(by[iid]); seen.add(iid)
for iid, e in by.items():
    if iid not in seen:
        out.append(e); seen.add(iid)

tmp = P + ".tmp"
with open(tmp, "w") as f:
    for e in out:
        f.write(json.dumps(e) + "\n")
os.replace(tmp, P)
print("wrote", len(out), "entries")
```

The rewrite normalizes to one JSON object per line — this also repairs the multi-object-per-line
inconsistency. All other entry data is preserved verbatim.
