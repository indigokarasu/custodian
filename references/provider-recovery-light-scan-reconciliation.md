# Provider-Recovery Light-Scan Reconciliation

## When this pattern appears

A prior escalation run (or `escalation-execution-loop`) paused a large batch of jobs for a
provider outage (Nous 401 "API key invalid/out of funds", OpenRouter 402 credits, etc.). A later
**light scan** finds:

- **ALL jobs enabled, 0 paused** in `jobs.json` (the system resumed them after the provider came back).
- A large count of `enabled` error jobs whose `last_error` is the provider outage fingerprint, but
  whose `last_run_at` is OLD (from before the resume) — i.e. **stale errors**, not live failures.
- `issues.jsonl` still has open `user_gated` issues whose `jobs_paused` lists reference those same jobs
  (now enabled). This is the inverse/stale-pause discrepancy: the issue claims paused, jobs are running.

Confirmed real case: 2026-07-11. 07-10 esc-run paused 92 jobs; 07-11 scan found 143 enabled, 94 stale
errors (63 Nous-401 + 23 OpenRouter-402 + 2 token-exp + 2 owl-alpha 404 + 1 Nvidia + 2 exit-1), 0 paused.

## Step 1 — Verify the recovery is REAL, not false

A prior pass can wrongly "resume on recovery" and fail again (observed 2026-07-10T04:34Z false recovery).
Discriminate with a **live provider probe**:

```
hermes chat -q "ping"
```

- Returns `pong` (OK) -> provider genuinely works now. The outage fingerprint errors are STALE.
- Returns the 401/402 -> outage still live; do NOT resolve; the jobs are burning and the issue stays open.

NOTE: `Gateway health monitor` (no_agent script) returning `ok` does NOT prove the LLM key works —
it never calls the model API. Only the `hermes chat -q` probe is authoritative.

## Step 2 — Reconcile issues.jsonl (profile path is authoritative)

`<hermes-home>/profiles/<profile>/commons/data/ocas-custodian/issues.jsonl` — newline-delimited
(one JSON per line) in this deployment; use line-by-line `json.loads`, NOT brace-depth, for this file.

For each open issue touched by the outage:

- **Provider-billing issues that recovered** (Nous 401, OpenRouter 402 when jobs are `provider: nous`
  and the model is served directly by Nous so the fallback never triggers): set `status: resolved`,
  `escalation_needed: false`, `resolved_at`, `jobs_paused: []`, and a `resolution_note` citing the pong
  probe timestamp + "stale errors self-heal on next run; re-open if next runs re-fail."
- **Persistent non-provider root causes** (Google OAuth 403, deprecated model hardcoded in skill internals,
  etc.): keep `status: user_gated` + `escalation_needed: true`, but set `jobs_paused: []` (jobs resumed,
  left running per the no-pause policy) and add a `note` that the job will retry and the root cause still
  needs the user's action (re-auth / code edit). Do NOT mark resolved.

Honesty rule: a resumed job is NOT a fixed root cause. Billing/credentials/skill-internal issues stay
open until <operator> adds credits, rotates the key, re-auths, or edits skill code.

## Step 3 — Add NEW issues only for genuinely new faults

Do NOT re-escalate the recovered billing outage. But surface real faults the scan found that are NOT
provider-outage-related, and write them to `issues.jsonl` so the journal→issues gap rule (Step 8b) holds:

- Infra faults (e.g. `SearXNG Health Watchdog` exit-1 with container Up but `curl localhost:8080` ->
  HTTP 000 = upstream-degraded). Leave the monitor RUNNING (auto-clears on upstream cooldown). Tier 3.
- Disk/resource faults (e.g. `DISK_CRITICAL` in sysadmin-health.log; a stale 12G pre-update state
  snapshot filling disk). Flag Tier 2 for deep-scan; do NOT auto-delete snapshots in a light scan.

## Step 4 — No pause, no fix on the billing jobs

Per the 2026-07-11 corrected guidance: do NOT pause recurring jobs for provider/auth/credits/429/outage.
The jobs are already enabled; leave them retrying. They self-heal on their next scheduled run.

## Step 5 — Write the observation journal with escalation_needed: true

The journal must carry `escalation_needed: true` + `escalation_refs` for the genuinely-open items
(persistent user-gated + new infra/disk), even though the bulk billing outage resolved. Required evidence
record even on a partly-actionable scan.