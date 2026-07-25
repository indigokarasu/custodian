# False "RE-CONFIRMED LIVE" Note Trap (2026-07-14)

## The trap

In the escalation execution loop, when you find an issue whose note already says
"FORWARD-STALE: provider recovered" but live `jobs.json` shows `enabled` jobs
still `last_status=error` with that outage signature (401 `token_expired`, 402
`credits`, etc.), the instinct is to **overwrite the note with "RE-CONFIRMED
LIVE — still failing"** and treat it as actionable.

That overwrite is wrong whenever the error is *stale* (the job ran once before
recovery and never re-ran). `status=error` + an OLD `last_run_at` is the
signature of a pre-recovery stale error — it proves nothing about the current
run. You will then have written a false escalation that a later scan must
reverse.

## Confirmed incident (2026-07-14)

- 3 open issues (`oc_provider_auth_token_expired_20260712T040120`,
  `oc_nous_api_key_invalid_20260712T040120`,
  `oc_openrouter_402_credits_exhausted_20260712T040120`) carried notes
  "FORWARD-STALE: provider recovered".
- Live `jobs.json` showed 9 jobs `last_status=error` with 401 `token_expired`
  and 2 with 402 `credits` — all with `last_run_at` on 07-11/07-12.
- Loop overwrote all 3 notes to "RE-CONFIRMED LIVE 2026-07-14T17:25Z".
- Then re-ran jobs live:
  - `genie:disk-cleanup` → **ok**
  - `rally:update` → **ok**
  - `art:engagement` (a 402 job) → **ok**
- Conclusion: the original "recovered" notes were CORRECT. The overwrite was a
  data-integrity regression. Reverted all 3 notes to the accurate forward-stale
  state with live re-run evidence appended.

## The mandatory gate (before overwriting any "recovered" note as live)

1. Re-run ≥1 affected job live: `hermes cron run <job_id>`.
2. LLM jobs take ~60s each and run **serially** — the 60s foreground
   `terminal` cap will time out. Use
   `terminal(background=true, notify_on_complete=true)` then `process(wait/poll)`,
   or batch several ids in one background command:
   `for id in <id1> <id2> <id3>; do hermes cron run $id >/tmp/run_$id.log 2>&1; done`
3. If the re-run flips to `ok`: error was STALE → KEEP the "recovered" note
   (append evidence: which jobs + run_id/time). Do NOT escalate.
4. Only if the re-run STILL errors (or the job has a recent `last_run_at` post
   the recovery window) is the note genuinely wrong → then correct it, but label
   it "RE-CONFIRMED LIVE via re-run" with the re-run evidence, not a blind
   assertion from `status=error`.

## `hermes chat -q "say pong"` is NOT sufficient

`hermes chat` exercises only the DEFAULT provider's API-key path
(tencent/hy3:free served by the Nous API key). It does NOT prove:
- a job-specific **session token** (e.g. Nous portal `token_expired`) recovered, or
- a **second provider** (e.g. OpenRouter 402 credits) recovered.

Always re-run an *actual affected job*, not just the pong probe. The probe is a
useful first signal but never the closing proof.

## Why this matters for honesty

The escalation loop's honesty rule forbids reporting user-gated billing/key/OAuth
issues as "fixed". The inverse failure is just as bad: *inflating* a recovered
issue back to "live failing" creates noise, wastes a scan cycle reversing it, and
erodes trust in the issue store. When in doubt, re-run the job — never assert
live failure from a stale `status=error`.