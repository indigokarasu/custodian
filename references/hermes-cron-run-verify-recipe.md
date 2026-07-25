# `hermes cron run <id>` live-verification recipe

**When to use:** escalation-loop Step 8d inverse gotcha / `references/escalation-false-recovered-note-trap.md` — re-running an actual cron job via `hermes cron run <id>` to prove a prior "resolved" issue was a **false resolution** (job still fails live) or to confirm recovery (job succeeds live). This is the authoritative live-verification primitive; `hermes chat -q "say pong"` only exercises the default provider and does NOT prove a job-specific session token or a second provider has recovered.

## The trap (confirmed 2026-07-22 escalation loop)

- `hermes cron run <id>` prints:
  ```
  Triggered job: <name> (<id>)
    Next run: <ts>
    Ran now: succeeded.
  ```
  or `Ran now: failed.` — **the job's true outcome lives ONLY in that printed line.**
- The CLI process itself exits `0` regardless of job success/failure. So `hermes cron run <id>; echo $?` → always `0`.
- Worse: `hermes cron run <id> | tail -5; RESULT=$?` → `$?` is `tail`'s exit (0), which **completely masks** the job result. In the 2026-07-22 loop this produced a false "stale/recovered" signal on `sands:evening-brief` and `10khr-grind` until the printed line was read directly.
- Also: `timeout 150 hermes cron run <id>` kills long LLM jobs at the cap → exit `124` with **no** `Ran now:` line. `sands:evening-brief` needed >150s; `10khr-grind` ran >800s.

## Correct recipe

```bash
# Redirect to a file (NO pipe, NO timeout cap)
hermes cron run <id> > /tmp/run_<id>.out 2>&1
# Then read the file — parse the printed line:
grep -E "Ran now: (succeeded|failed)" /tmp/run_<id>.out
# or: cat /tmp/run_<id>.out
```

For long LLM jobs, run in background and poll (foreground `terminal` cap is 180s; the pipe-to-interpreter filter blocks `| python3`):
```python
terminal(background=True, notify_on_complete=True,
         command="hermes cron run <id> > /tmp/run_<id>.out 2>&1")
process(wait/poll)   # then read /tmp/run_<id>.out
```

### Output is NOT streamed — the redirect file stays 0 bytes until completion (confirmed 2026-07-23)

`hermes cron run <id>` does not write incremental output to the redirected file as the job runs; the file stays **0 bytes for the entire job duration** and is only flushed on completion (the `Ran now:` line + any job stderr appear at the end). Consequences:
- Polling `/tmp/run_<id>.out` with `read_file` gives **no progress signal** — an empty file does not mean "stalled," it means "still running."
- `process(wait=...)` against the background run also shows the job "running" with no output preview for the full duration.
- Do NOT loop `wait` indefinitely watching an empty file. Bound your wait (e.g. two `wait` calls of ~60s each, capped because the foreground `terminal` cap is 180s), then **kill if still running** and fall back to the job's stored `last_status` / `last_run_at` + error class — which is usually sufficient evidence without the re-run (see next section). Confirmed 2026-07-23: `ocas-autobio-observe` (LLM job `6ca08a339814`) ran >300s with an empty output file before being killed; the live error was already known-deterministic.

### Skip the live re-run for `content_policy_blocked` (confirmed 2026-07-23)

A `content_policy_blocked` error is a **deterministic model refusal** keyed to the job's prompt — unlike `token_expired` / `402 credits` / `429`, it does **NOT** self-recover on re-run. Re-running an LLM job that last failed with `content_policy_blocked` will reproduce the same refusal and waste a 60s–300s+ run. To classify such a job as "live, needs <operator>" (no auto-fix exists — only a model/prompt/accept-failure decision by <operator> resolves it), you do NOT need a live re-run:
- require `last_status == error` AND a `last_run_at` that is recent (within the job's interval, not days-stale), AND the stored `last_error` containing `content_policy_blocked`;
- optionally confirm the fingerprint *class* recurs across distinct jobs (`oc_bones_content_policy_blocked_20260720`, `oc_sands_evening_brief_content_policy_20260722`, `oc_autobio_content_policy_blocked_20260723T0505Z`) — these are recurring-class, re-escalate-on-recurrence, NOT auto-fixable.
Only invoke `hermes cron run` for `content_policy_blocked` if you specifically need to confirm the refusal text is unchanged for a new variant — otherwise treat the stored error as live evidence and skip the run.

After a re-run, re-read `jobs.json` `last_run_at` / `last_status` to confirm the registry updated. It **lags** during the scheduler state-update window, so treat the printed `Ran now:` line as the authoritative signal, not just the registry — and a `last_run_at` timestamp *newer* than a prior "resolution" timestamp is the definitive proof of a false resolution (the job failed again after being marked resolved).

## Companion trap — `find_missed_user_gated_jobs.py` recommended_issue id can be stale

On 2026-07-22 it flagged job `afd52bb2f41d` as "MISSED" → `oc_owl_alpha_model_404_20260701`, but that target issue was already `resolved`; the live 404 job was correctly covered by the newer open issue `oc_http_404_job_search_feedback`. Always confirm a job is actually uncovered by checking the open-issues list directly (the `parse_issues_jsonl.py` open-discriminator: `status not in ("resolved","duplicate") AND (escalation_needed OR user_gated)`), not by trusting the script's `recommended_issue` id.
