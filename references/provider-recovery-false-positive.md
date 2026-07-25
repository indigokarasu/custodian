# Provider Recovery — False-Positive Verification (2026-07-13)

## What this is
A failure mode in the escalation execution loop where an issue is closed/reopened as
"provider recovered" based on an **inadequate liveness probe**, while the affected
cron jobs are still actively erroring. The resolution note is written, but the
underlying fault never cleared — a *write-only* recovery.

## Anti-pattern (observed 2026-07-13)
`issues.jsonl` carried resolution notes like:
- `"resolution_note": "Provider recovered: verified via hermes chat -q ping returning 'pong' ..."`
- `"FORWARD-STALE: provider recovered ... same-skill jobs ran OK via default Nous provider"`

Meanwhile `scripts/verify_escalation_state.py` reported **33 enabled+erroring jobs**,
and the specific affected jobs had `last_status=error` with `last_run_at` within the
last day. The "recovered" claims were false.

Root cause of the false signal: the verification probe did not exercise the same
auth path the failing jobs use.
- `hermes chat -q "reply pong"` — when run, started an agent and did NOT return a
  clean pong; even when it does, it may authenticate with the **default** provider/key,
  not the expired session token the jobs need.
- `curl https://api.nousresearch.com/v1/models` returned **HTTP 000** (unreachable) in
  this session — a 200 here validates only that the models endpoint responds, NOT that a
  specific `token_expired` / `invalid_grant` credential is valid.
- `OpenRouter /models` 200 does not validate a Nous-expired session token.

## Correct verification (ground truth = live job state)
1. Run `scripts/verify_escalation_state.py`. If it reports **INVERSE-GOTCHA**
   (issue claims `jobs_paused`/resolved but the jobs are `enabled`+erroring), the
   provider did NOT recover — do not close the issue.
2. Read the actual `last_error` + `last_run_at` of the affected jobs directly from
   `jobs.json` (`data["jobs"]`). If `last_status=error` AND `last_run_at` is recent
   (within the job's schedule interval), the failure is **live** — keep the issue
   `open`/`user_gated`.
3. A probe ("pong", `/models` 200, free-default-model success) is only acceptable
   proof if it exercises the **same auth path** the failing job uses. For
   `token_expired` / `invalid_grant` / 401-key jobs, that usually requires the user's
   real credential — not cron-available. Treat probe-success as *supporting*, never as
   *dispositive*.
4. When in doubt: the `enabled`+`erroring` job list is authoritative. If the
   fingerprint's jobs appear there with recent runs, the issue stays open.

## Reconciliation when a false recovery is found
- Clear stale `jobs_paused` metadata (system auto-resumes paused jobs; no-pause policy
  keeps provider-outage jobs enabled+retrying) — set `jobs_paused: []`.
- Do NOT set `status: resolved`. Keep `user_gated` + `escalation_needed: true`.
- Write a `verification_note` recording that live job state contradicted the prior
  probe-based resolution.

## Session evidence
2026-07-13 esc-exec loop: 7 open issues all carried `jobs_paused` lists (claimed
19 / 5 / 1 paused) but `actual_paused=0` in jobs.json. Nous `/v1/models` returned
000; `hermes chat -q` did not return pong. Prior "recovered" entries were write-only.
Reconciled all 7 (cleared stale pause metadata, kept open/user_gated), wrote action
journal, verified weave+vibes git-credential fixes were already applied and
self-heal on next run.