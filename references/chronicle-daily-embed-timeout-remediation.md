# Chronicle Daily Embed — Timeout Remediation (`oc_script_timeout_chronicle_embed`)

The `chronicle:daily-embed` cron job (script `scripts/chronicle_daily_embed.py`) kept hitting the
**600s cron hard-kill** with "Script timed out after 600s". Two root causes were confirmed and fixed
(2026-07-13). Pair this with `resolved-timeout-verify-drained-backlog.md` — that file covers
*detecting* the false close; this one covers the *fix*.

## Root cause 1 — unbounded Facts query
The Facts pass had NO `LIMIT`, while every other pass (episodes, notes = 8000; documents, events = 500)
was capped. With ~16k unembedded facts, the full backlog was dumped into a single run's item list,
guaranteeing the 600s timeout regardless of batching.

## Root cause 2 — fixed per-request timeout could overrun the deadline
`embed_batch()` used `urllib.request.urlopen(req, timeout=60)` with `retries=3`. A single batch that
started near the 540s soft deadline could block up to ~3×60s = 180s on retries/network, pushing past
the 600s hard-kill BEFORE the script's next `time.time() > deadline` check (which only runs *between*
batches). The deadline check was also at the hard limit (no margin), so a slow final batch overran.

## Fix (applied to scripts/chronicle_daily_embed.py)
1. **Cap the Facts query**: add `LIMIT 8000` (matches episodes/notes). Backlog drains across runs
   because the `LEFT JOIN ... IS NULL` queries exclude already-embedded items.
2. **Make `embed_batch` deadline-aware**: signature `embed_batch(texts, deadline=None, retries=2)`;
   compute a dynamic per-request timeout `req_timeout = max(12, min(40, (deadline - time.time())/2))`;
   abort retries if within 25s of the deadline (`return out` — commit what we have).
3. **Move the stop-check to a 25s safety margin**: `if time.time() > deadline - 25: break` in
   `embed_items`, so the next batch (incl. retries + commit) always finishes before the 600s kill.

## Verification (must be LIVE, not on a drained queue)
Run the actual script against the production backlog:
`<hermes-venv>/bin/python <hermes-home>/profiles/indigo/scripts/chronicle_daily_embed.py`
Check the log tail: it must print `Total: N vectors in Xs` with **X < 600**. Verified: 171.1s, 8,498
vectors (Facts 8,000 capped, Episodes 8,000, Events 498). A fast run (e.g. 163s) immediately AFTER
another successful run proves nothing — the queue was just drained. Re-run after daily volume rebuilds
or inspect `SELECT COUNT(*) FROM facts ...` (see `resolved-timeout-verify-drained-backlog.md`).

## Reusable pattern (any cron embedding / timeout script)
- Cap every unbounded source query with a `LIMIT` so one run can't pull the whole table.
- Never use a fixed per-request `timeout` in a deadline-bounded loop — derive it from the *remaining*
  budget: `min(floor, remaining / 2)`.
- Leave a safety margin (≥20–25s) before the hard kill so the last in-flight request + commit finishes.
- Verify with a LIVE run against real volume, not a freshly-drained queue.

## Verification pitfall — paused job ≠ unpause authorization (confirmed 2026-07-13)

When you re-run a PAUSED timeout job's script and it passes, that validates the timeout fix
ONLY. The job's `pause_reason` often encodes a SEPARATE, unresolved root cause that the timeout
verification does NOT address. Do NOT unpause on the timeout result alone.

**Concrete case (2026-07-13):** `chronicle:daily-embed` (id `f7fb5ff15067`) was paused with
`pause_reason = "oc_chronicle_facts_fts_missing: daily-embed times out 600s due to missing FTS
index"`. A live re-run under `timeout 600` completed in **10.0s, exit 0** — proving the 600s
timeout fix (Facts `LIMIT 8000` + deadline-aware batching) works. BUT the pause cited a *missing
facts_fts index*, a distinct skill-schema issue unrelated to the timeout. Unpausing was deferred
to the skill owner; the job stayed paused. **Separate the two decisions:**
1. Timeout fix verified? -> re-run script under `timeout 600`, expect exit 0 + wall < 600s.
2. `pause_reason` concern resolved? -> independently confirm THAT root cause is gone (e.g. FTS
   index exists / rebuilds cleanly) before clearing `paused_at`.

## Concrete verification recipe (deterministic)

Pre-check the live backlog so the run can't be dismissed as a drained-queue false-pass
(see `references/resolved-timeout-verify-drained-backlog.md`). Run the read-only probe:

    python3 <hermes-home>/profiles/indigo/skills/ocas-custodian/scripts/chronicle_embed_backlog_probe.py

Then re-run the REAL script under the exact cron hard limit, in a clean env, capturing exit + wall:

    cd <hermes-home>/profiles/indigo
    timeout 600 python3 scripts/chronicle_daily_embed.py 2>&1 | tee /tmp/daily_embed_verify.log
    echo "exit=${PIPESTATUS[0]}"

A clean run immediately after another successful run proves nothing (drained queue) — the backlog
probe showing >0 unembedded items in a capped pass confirms real volume was processed.
Note foreground `terminal()` caps at 60s; wrap in `background=true, notify_on_complete=true` and
`process(wait)` for the full 600s window.
