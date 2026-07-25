# chronicle:daily-embed 600s Timeout — `oc_script_timeout_chronicle_embed`

**Confirmed:** 2026-07-13 deep scan. `chronicle:daily-embed` (script `chronicle_daily_embed.py`)
<<<<<<< Updated upstream
`last_error`: `"Script timed out after 600s: <hermes-home>/profiles/indigo/scripts/chronicle_daily_embed.py"`.
=======
`last_error`: `"Script timed out after 600s: ~/.hermes/profiles/indigo/scripts/chronicle_daily_embed.py"`.
>>>>>>> Stashed changes
`consecutive_failures: null`. NOT user-gated — requires code/config tuning, not a credential.

## Symptom
Job `last_status=error` with `Script timed out after 600s`. The cron hard timeout is 600s; the
script sets `SOFT_TIMEOUT_SECS=540` but still blows past it on the daily document volume.

## Diagnosis recipe (reusable for ANY cron timeout)
1. **Re-run the actual script live** to confirm it's still hanging, not a stale error:
<<<<<<< Updated upstream
   `python3 <hermes-home>/profiles/indigo/scripts/chronicle_daily_embed.py` — if it runs >60s and
=======
   `python3 ~/.hermes/profiles/indigo/scripts/chronicle_daily_embed.py` — if it runs >60s and
>>>>>>> Stashed changes
   doesn't return, the timeout is active.
2. **Isolate volume vs embedding-API failure by checking a sibling script.** `enrich_embeddings.py`
   in the same skills dir performs a similar embedding pass but succeeded with `EXIT=0` (rebuilds
   FTS, 0 new items — fast). That proves the embedding API endpoint itself is reachable/working;
   the daily-embed failure is the **daily volume / loop runtime**, not an API outage.
3. **Read the script constants:** `EMBED_MODEL = "nvidia/llama-nemotron-embed-vl-1b-v2:free"`,
   `EMBED_BASE = "https://openrouter.ai/api/v1"`, `DAILY_DOC_LIMIT = 500`,
   `SOFT_TIMEOUT_SECS = 540`. A free-tier embedding endpoint at 500 docs/day is the likely
   throughput bottleneck.

## Fix directions (Tier 2 — plan, do not auto-apply without review)
- Lower `DAILY_DOC_LIMIT` (e.g. 200) so the daily batch fits under the soft/hard timeout.
- Raise the cron job `timeout` if the daily volume is legitimately large (the hard limit is 600s;
  upstream cron config controls it).
- Switch `EMBED_MODEL` to a faster/paid endpoint if the free nvidia model is degraded.
- Note: `SOFT_TIMEOUT_SECS=540` is a best-effort stop; it does not override the cron 600s hard kill.

## Classification rule
This fingerprint is **active and non-user-gated**. It must be persisted as a NEW open issue
(`status: open`, `escalation_needed: true`, `user_gated: false`) — do NOT fold it into the
provider/billing outage cluster, and do NOT mark it resolved just because other chronicle jobs
(`enrich_embeddings.py`, `weave:enrichability-recalc`) succeed. Those are different scripts.