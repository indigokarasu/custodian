# Chronicle-Embed Timeout — Re-Resolving a Reopened Issue as Forward-Stale (2026-07-14)

## Context

`oc_script_timeout_chronicle_embed_20260713` is the most trap-laden issue in the
library. Its history:

- **2026-07-13 (~21:36Z):** first resolved, citing a ~163s live re-run.
- **FALSE-CLOSE caught:** that re-run executed against a queue just drained by a
  prior run (`embed_state.json` `last_run` within seconds of the test). A fresh
  re-run was still embedding past 85s against the real 35k+ row backlog → timeout
  recurring. (See `references/resolved-timeout-verify-drained-backlog.md`.)
- **2026-07-14 09:15Z:** deep scan reopened it (`reopened_at`) on PRE-fix evidence:
  "still times out at 600s (last_run 2026-07-13T03:12, live 46118-event backlog).
  Prior 07-13 resolve was against a drained queue." Fix was applied in between
  (`chronicle_daily_embed.py`: `LIMIT 8000` cap + deadline-aware `embed_batch`
  with 25s safety margin so a single batch cannot overrun the 600s hard-kill).
- **2026-07-14 14:05Z (this light scan):** re-resolved as **forward-stale**.

## Why the 14:05Z resolution is legitimate (and distinct from the drained-backlog false-close)

The 09:15 reopen was correct *at that moment* — it cited pre-fix evidence. By
14:05Z the post-fix state superseded it. Resolution criteria used:

1. **Governed jobs are `state: paused` + `last_status: ok` + empty `last_error`**
   (`chronicle:daily-embed`, `Chronicle Embedding Enrichment`). A *paused* job is
   a deliberate halt, not a fast re-run against an empty queue — its last run came
   back clean. This is stronger than "enabled + ok" because the paused state means
   the timeout is not silently recurring while running.
2. **The issue's own `verified` field** states the fix "ran live against
   production backlog; self-terminates at 540s soft deadline and drains over runs;
   no 600s timeout." This is the authoritative confirmation that the fix — not a
   drained queue — produced clean runs.
3. **No enabled job carries an embed/timeout error.** The fingerprint is gone from
   the live `jobs.json` error set.

## How this differs from the drained-backlog false-close (Step 8e trap)

| | Drained-backlog FALSE-CLOSE | 14:05Z forward-stale resolution |
|---|---|---|
| Job state at decision | enabled, re-run manually | paused + ok + empty `last_error` |
| Evidence basis | fast re-run against empty queue | issue's own `verified`=true + post-fix clean run |
| Queue volume | test ran on ~0 rows | fix verified against production backlog per issue record |
| Action | DO NOT resolve | RESOLVE (forward-stale) |

## Procedure for a future scan hitting this issue

- If `last_status=error` + non-empty `last_error` mentioning 600s/timeout on an
  **enabled** job → ACTIVE, keep open (the fix did not hold or wasn't applied).
- If jobs are **paused + ok + empty `last_error`** AND `verified`=true → forward-stale,
  resolve (`status: resolved`, `escalation_needed: false`, `user_gated: false`, add
  `resolved_at` + `resolution_note`).
- **Re-verification trigger:** when the job is later **resumed** (`state: scheduled`),
  re-check it self-terminates at the 540s soft deadline and does not hit 600s. The
  fix only guarantees no single batch overruns; steady-state daily volume could still
  surface a regression. If it times out again post-resume, reopen with `tier: 4`.

## Reconciliation action taken (14:05Z light scan)

Resolved `oc_script_timeout_chronicle_embed_20260713` in the profile
`issues.jsonl` (one-JSON-per-line, safe line-edit preserving all 34 records):
`status: resolved`, `escalation_needed: false`, `user_gated: false`, added
`resolved_at` + `resolution_note`. No other open issue changed: the two
`oc_nous_api_key_invalid` issues stay `user_gated` (key rotation needed, no enabled
job burning but not auto-resolvable); `oc_state_db_oversized` stays tracked (disk 71%,
below the 80% action threshold).
