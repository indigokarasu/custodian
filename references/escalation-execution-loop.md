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
<<<<<<< Updated upstream
   `<hermes-home>/profiles/<profile>/commons/data/ocas-custodian/issues.jsonl`. It is
   authoritative. The commons copy (`<hermes-home>/commons/data/ocas-custodian/issues.jsonl`)
=======
   `~/.hermes/profiles/<profile>/commons/data/ocas-custodian/issues.jsonl`. It is
   authoritative. The commons copy (`~/.hermes/commons/data/ocas-custodian/issues.jsonl`)
>>>>>>> Stashed changes
   is a lagging sync target — **write only to the profile path**. Entries may be concatenated on
   a single line; use a brace-depth parser (see `scripts/parse_issues_jsonl.py` or the template
   below).

3. **Classify each open issue**: Actionable (Tier 1 auto-fix) / User-gated (credits exhausted,
   invalid/out-of-funds API key, skill-internal hardcoded model) / Already-resolved / Legacy-inactive.

4. **Execute**:
   - Actionable → apply the Tier 1 fix (within the fix-safety envelope).
   - User-gated → **pause still-enabled failing jobs** by editing `jobs.json` directly
     (`enabled: false`, `state: 'paused'`). Pausing IS the action — it stops resource burn.
     Do NOT mark the issue resolved (the root cause still needs <operator>).

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
- **The issue-set ITSELF can change between reads (not just content).** A target you
  discovered in pass 1 (e.g. a duplicate `oc_nous_api_key_invalid_20260707` surfaced by a
  grep/brace-parse probe) may be **absent** from the file by the time you mutate it — a
  concurrent `custodian:light` scan or a sibling loop can rewrite `issues.jsonl`, recombine,
  or drop/merge the entry between your discovery read and your write. Confirmed 2026-07-13: a
  probe reported an issue ID that had 0 occurrences in BOTH the pre-edit backup and the
  post-edit file — it had never existed in the authoritative file (or a concurrent scan had
  already collapsed it). **Mitigation:** immediately before mutating, re-grep/re-parse the LIVE
  file for the exact target ID; if it is gone, treat the target as already-handled (no-op)
  rather than fabricating a write for it. Never assume an ID present in an earlier probe still
  exists at write time — re-verify at the last moment.

## WRITE-RACE CLOBBER (your mutation wiped AFTER you wrote it — 2026-07-14)

The "Concurrency pitfall" above covers the issue-set changing between your discovery read
and your write. A **second, distinct** failure mode confirmed 2026-07-14: you mutate correctly,
the file write succeeds, but a **sibling `custodian:light` (top-of-hour) scan rewrites
`issues.jsonl` moments later** with its own whole-file version — which does NOT contain your
change, because it read the file *before* your write landed. Result: your resolution silently
disappears. The existing "os.replace is idempotent" claim guards data *integrity*, not the
*retention of your own mutation* against a later writer.

**Mitigations (apply when a write IS required):**
1. **Rewrite ONLY the target line** (targeted single-line edit), not the whole file. A
   whole-file rewrite maximizes the surface a concurrent writer can clobber. Editing just the
   one line means a sibling rewriting a *different* issue is far less likely to wipe your change.
2. **Immediately re-read and verify** after writing — if the mutation isn't present, a sibling
   clobbered it.
3. **Retry the read-modify-verify up to 3x** (small sleep between). The window is short; one of
   your writes will "win" the race and stick.
4. **Gate on current status** where possible (`--require-status user_gated`) so a retry that
   races a genuine concurrent resolution doesn't double-apply.

Reusable helper: **`scripts/race_safe_issue_patch.py`** —
`python3 scripts/race_safe_issue_patch.py --issue-id <id> --set status=resolved
--set user_gated=false --set escalation_needed=false [--require-status user_gated] [--retries 3]`.
It does all four steps (targeted line edit + immediate verify + retry loop + status gate).

## SUBSTRING FINGERPRINT MATCHING FALSE-POSITIVE (the (b) forward-stale check — 2026-07-14)

When verifying whether an open user_gated issue still has LIVE matching jobs (the (b)
forward-stale check), a naive substring match against `last_error` **massively over-counts**
and masks true recovery. Confirmed 2026-07-14: matching the pattern `"token"` against job
`last_error` text returned 11 "matching" jobs for a Spotify issue and 9 for a Nous issue —
because `token_expired`, `tokens`, and `authentication token is expired` all contain the
substring `token`. Those jobs were 401/402 provider failures, NOT the Spotify/Spotify-token
issue. The false matches made it look like every issue was "still failing" when in fact the
Spotify issue had **zero** precise matches (its one real job, `taste:sync-spotify`, was
`enabled=False` with "Missing Spotify credentials" — correctly tracked).

**Rule:** Match **exact phrases**, not substrings. Examples:
- Spotify token missing → `"spotify" in t and ("missing" in t or "refresh_token" in t)`
  (NOT bare `"token"`)
- owl-alpha 404 → `"owl-alpha" in t and "404" in t`
- OpenRouter 402 → `"402" in t and ("credits" in t or "max_tokens" in t)`
- token_expired → exact `"token_expired" in t` (NOT `"token"`)
- Nous 401 → `"nous" in t and "401" in t`

A precise match will return 0 for issues whose jobs have actually recovered — that 0 is the
signal to resolve, not a bug. Always sanity-check a "still failing" count against the actual
job list before concluding an issue is live.

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

**Default-provider `token_expired` is also user-gated.** Agent-mode jobs can fail with
`RuntimeError: Error code: 401 ... Provided authentication token is expired ... code: token_expired`
without matching the Nous/OpenRouter/owl-alpha buckets. Treat this as
`oc_default_provider_token_expired`: create/update a profile `issues.jsonl` record, pause the
affected jobs, keep the issue `user_gated` + `escalation_needed: true`, and journal the mitigation.
`verify_escalation_state.py` may show `extra` paused jobs for this fingerprint until its mapping is
updated; verify with live `jobs.json` plus `find_missed_user_gated_jobs.py` (`missed=0`, `unknown=0`).
Session detail: `references/escalation-exec-default-token-expired-2026-07-09.md`.

## Journal-to-issues gap probe — READ-ONLY FIRST, reconcile before trusting `--write`

`scripts/scan_escalation_journal_gaps.py` is the journal half of the loop (runs alongside the two
job-state probes). It walks ALL custodian journal dirs, parses each (brace-depth), and for journals
with `escalation_needed: true` cross-references cited fingerprints against OPEN issues in the
profile `issues.jsonl`. Read-only by default; `--write` creates missing issues.

**CRITICAL GOTCHA (confirmed 2026-07-08):** the scan matches each journal's cited `escalation_refs`
(issue-ID-style names, e.g. `oc_owl_alpha_model_404_20260701`) against the `fingerprint` FIELD of
open issues (error-fingerprint names, e.g. `oc_http_404_model_deprecated`). Real billing issue
records carry `issue_id` = issue-ID-style but `fingerprint` = error-fingerprint, so the two NEVER
match. The scan therefore reports a "gap" for every already-tracked billing issue and `--write`
fabricates a duplicate record (same root cause, different fingerprint key, `jobs_paused: []`). It
also fabricates records for stale false-positives — fingerprints a journal flagged
`escalation_needed` but whose fault was ALREADY remediated (e.g. `oc_skill_data_dir_missing`,
`oc_cron_dead_skill_ref`, `oc_config_compression_model_misconfigured`: all verified clean against
live `jobs.json`/`config.yaml` on 2026-07-08).

On 2026-07-08 a single `--write` added 12 spurious records in one batch (timestamp
`2026-07-08T18:01:22.143565+00:00`), inflating the working set with duplicate/stale noise.

**Safe procedure:**
1. Run READ-ONLY first: `python3 scripts/scan_escalation_journal_gaps.py --hours 24`.
2. For each reported GAP, reconcile BEFORE writing: does an open issue already track the same
   `jobs_paused` jobs or `affected_components`? If yes (same root cause, different fingerprint key),
   it is NOT a gap — do not write.
3. Only `--write` for a genuinely NEW fingerprint whose fault is still live and un-tracked (verify
   against `jobs.json`/`config.yaml` live state first).
4. If you already ran `--write` and suspect inflation: `issues.jsonl` is newline-delimited (one
   JSON per line, NOT concatenated — brace-depth "Extra data" parse errors on this file are a
   parser artifact, not corruption). The batch shares one exact `created_at` timestamp — surgically
   remove those lines (backup first), then re-run `verify_escalation_state.py` to confirm billing
   `jobs_paused` counts are unchanged. Prefer the line-filter over a hand-rolled brace-depth
   rewrite on this file.

## Honesty rule

Do NOT report user-gated billing / API-key / skill-internal issues as "fixed". Pausing is
mitigation, not resolution. They stay `user_gated` + `escalation_needed: true` until <operator> adds
credits, rotates the key, or edits skill code (Custodian must not edit skill-internal files).

## FALSE-RESOLUTION via weak probe — the decisive check is TIME-BASED (2026-07-13)

A prior escalation loop resolved `oc_provider_auth_token_expired_20260712T040120` citing
"provider recovered — `hermes chat -q` returned PONG on the free default model
`tencent/hy3:free`, and OpenRouter `/models` returned HTTP 200." That verification was
**insufficient and produced a false-resolution of 19+ still-failing jobs.** Reasons:

- Probing the **free default model** only proves the *free* model works. It does NOT validate
  the **session token** the agent-mode jobs use. The `token_expired` fingerprint is documented
  (`oc_provider_auth_token_expired`) as **non-self-healing** — it recurs until <operator> re-auths.
- Probing the OpenRouter `/models` endpoint (HTTP 200) only proves the *endpoint* is up. It
  does NOT validate the OpenRouter **API key / credits** the failing jobs present.
- The loop saw *other* jobs (using the free model) report `status=ok` and inferred "provider
  recovered," then declared the token-expired jobs would "self-clear on next run." But none of
  them had re-run successfully afterward — their `last_run_at` predated the claimed recovery.

**The ONLY valid recovery evidence is TIME-BASED.** For each job enrolled in the issue, check:
did it actually re-run with `last_status=ok` AND `last_error` cleared **after** the claimed
recovery timestamp? If a job's `last_run_at` **predates** the recovery and its `last_error` is
still intact, the issue is NOT recovered — keep it open / re-open it.

Deterministic verification (run via `terminal(python3 ...)`, never `execute_code`):
```bash
# Did the affected jobs actually re-run OK since the claimed recovery?
python3 scripts/verify_recovery_by_runtime.py --issue oc_provider_auth_token_expired_20260712T040120
# OR explicit set + recovery timestamp:
python3 scripts/verify_recovery_by_runtime.py --jobs <id1>,<id2> --recovery 2026-07-13T16:10:00Z
```
Exit 0 = all re-ran OK (resolution valid); exit 1 = any still failing → RE-OPEN.

**Rule:** Never accept a "resolved" provider-token/credits/key issue unless
`verify_recovery_by_runtime.py` exits 0. A generic model/endpoint probe is NOT recovery
evidence. This applies to forward-stale reconciliation too: before resolving a token-expired
issue as "all jobs recovered," confirm the jobs' `last_run_at` is *newer than* the failures,
not just that some unrelated job using the free model succeeded.

**Reconciliation pattern when a prior loop's resolution is found false (this session):**
1. Restore issues.jsonl from the latest backup if you already mutated it with a wrong key.
2. Re-open the issue (`status: user_gated`, `escalation_needed: true`), clear
   `resolved_at` / `resolved_by` / `resolution` / `recovery_signal`.
3. Re-enroll the LIVE failing job ids by reading them from `jobs.json` directly (the prior
   `jobs_paused` set may have drifted). Do NOT re-pause jobs left running for self-recovery
   (provider-auth outages stay enabled per the no-pause policy) — re-opening the issue is the
   correction.
4. Write an action journal documenting the audit (prior run id, verification defect, live
   contradiction counts).

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

### CRITICAL: the `find_missed` probe's "pause" output is a DEFAULT, not a directive — suppress it for provider/model outages (confirmed 2026-07-14)

`scripts/find_missed_user_gated_jobs.py` prints, for every MISSED job:
`ACTION: pause each MISSED job (enabled=false, state=paused) and append its id to the
recommended_issue's jobs_paused.` **Do NOT follow that line literally for provider/model
outage fingerprints.** It is a generic default emitted regardless of fingerprint.

The governing no-pause carve-out (Custodian SKILL.md § "Escalation runner: user-gated provider
failures are not permanent kill switches" + this file's Step 1c): **recurring cron jobs whose
last error is a provider outage — 402 credits, 401 invalid/out-of-funds key, owl-alpha 404,
`token_expired`, 429 — stay ENABLED and tracked, never paused.** Pausing them stops retry and
is pure mitigation that also freezes legitimate future runs. The issue stays
`user_gated` + `escalation_needed: true` with `jobs_paused: []`; the jobs keep running so they
self-clear when <operator> adds credits / rotates the key / re-auths.

**Worked example (2026-07-14 loop):** the probe reported 5 MISSED jobs
(`haiku:content-review`, `Job Search Feedback Monitor`, `weave:overnight-enrichment`,
`art:engagement`, `EHCS Monthly Refill Form`) all matching OpenRouter-402 / Nous-401 / owl-404
fingerprints. All were left **enabled** (retry policy) — none paused. Pausing them would have
violated the carve-out. Their root cause is already tracked by the open `user_gated` issues
(`oc_openrouter_402_credits_exhausted_*`, `oc_nous_api_key_invalid_*`); the jobs simply were
not yet reflected in `jobs_paused` (which is fine — `jobs_paused: []` is the correct state for
provider outages, since they are intentionally left running).

**Refined MISSED-handling rule:**
- MISSED job matches a **provider/model** fingerprint (402, 401 invalid-key, owl-404,
  `token_expired`, 429) → **leave ENABLED** (`state: scheduled`); do NOT append to `jobs_paused`;
  ensure the matching issue is `user_gated` + `escalation_needed: true` with accurate
  `affected_components`. No `jobs.json` mutation.
- MISSED job matches a **domain/tool** failure that is genuinely futile to retry (revoked
  Google OAuth for a specific tool, missing script, hard dead-end) → pause (`enabled: false`,
  `state: 'paused'`), append id to the issue's `jobs_paused`, with `paused_reason` +
  re-enable-on-recovery check.

This narrows the 2026-07-07 "pause it and enroll it" guidance: that earlier run's 3 MISSED jobs
were Nous-401 *domain* failures on `bones`/`finch` jobs with no self-recovery path; provider
outages are a distinct, leave-enabled class. When in doubt, class the fingerprint: outage =
leave running; dead-end = pause.

## UNKNOWN-bucket de-aggregation (no_agent exit-1 no-op monitors)

`find_missed_user_gated_jobs.py` buckets non-matching enabled+erroring jobs as `UNKNOWN (inspect
manually)`. Do NOT default UNKNOWN to transient — inspect each one. For `no_agent` jobs with bare
`Script exited with code 1` and no captured stderr:

1. Pull the job's `script` field from `jobs.json` (literal path / bare basename).
<<<<<<< Updated upstream
2. Locate it: `find <hermes-home> -name "<script>.py"`.
=======
2. Locate it: `find ~/.hermes -name "<script>.py"`.
>>>>>>> Stashed changes
3. **If the `script` is a `.sh` wrapper** (auto-generated no-agent wrapper), run the WRAPPER
   itself, not just the inner python — `timeout 120 bash <path-to-wrapper.sh>`. Confirmed
   2026-07-14: `rally:daily-activity-check` stored `"Script exited with code 1"` but running
   only the inner `rally_daily_activity_check.py` left the wrapper's `rc=1 → exit 0` translation
   invisible. The wrapper (with the `set -a; . .env` + rc-translation) exits **0** live; the
   stored error was stale (pre-wrapper-fix). Running the inner python alone would have
   mis-classified it as an active failure. **The cron scheduler executes the `script` field
   verbatim — reproduce THAT to get the true exit code.**
   - For a bare `.py` script: `timeout 60 python3 <path> 2>&1` (in cron, write a `/tmp` script
     and run via `terminal` — `cat|python3` is blocked by the tirith filter).
4. Read the script source. If it `sys.exit(1)` with **no stderr** when there is nothing to do
   (polling monitor exiting 1 when no new work since last check), it is a **healthy no-op** —
   `oc_cron_no_agent_exit_1_noop` (Tier 2 surface-only). The scheduler marks exit 1 as
   `status=error`, but the job works as designed.
5. A traceback / stderr = real failure — investigate the actual error.

**Safety rule: never pause a no-op monitor.** A polling monitor (e.g. `monitor:journals`) MUST stay
running to detect new work; pausing it breaks downstream ingestion. Leave UNKNOWN no-op monitors
`enabled` + `state: scheduled` and record them as Tier 2 surface-only in the journal.

**Worked example (2026-07-08 loop):** `monitor:journals` (`94510fb15ae2`) showed `Script exited with
code 1`, no stderr. `monitor_journals.py` confirmed `sys.exit(1)` on the `latest_mtime <=
last_mtime` branch (no new journals since last check) — healthy no-op, not a missed user-gated
enrollment. Left running. Matches the known 2026-07-07 `monitor:journals` no-op pattern.

## Safe `issues.jsonl` edit pattern (brace-depth parser + one-per-line rewrite)

Write to `/tmp/` and run via `terminal(python3 /tmp/edit.py)` (cron context: no `execute_code`,
no pipe-to-interpreter).

```python
#!/usr/bin/env python3
import json, os
from datetime import datetime, timezone

<<<<<<< Updated upstream
P = "<hermes-home>/profiles/indigo/commons/data/ocas-custodian/issues.jsonl"
=======
P = "~/.hermes/profiles/indigo/commons/data/ocas-custodian/issues.jsonl"
>>>>>>> Stashed changes
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