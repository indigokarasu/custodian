# Chronicle Enrich — False Backlog via Skipped Records (2026-07-15)

**Distinct from the drained-backlog trap** (`resolved-timeout-verify-drained-backlog.md`): there the
queue held REAL production volume that a prior run drained; here the "backlog" is PHANTOM — records
that can NEVER be embedded — so it never drains and the probe's "live backlog present" verdict is
misleading for this script.

## The case (2026-07-15 deep scan)
Stored error: `Chronicle Embedding Enrichment` (script `enrich_embeddings.py`) `Script timed out
after 600s` @ 03:12, `cf=0`. A 12:05Z scan had **false-reopened `oc_script_timeout_chronicle_embed_20260713`**
on the premise that the unpatched sibling `enrich_embeddings.py` timed out. That premise was wrong.

## Trap A — phantom backlog from skipped records
`chronicle_embed_backlog_probe.py` counts events missing from `observed_vectors` (44,902 reported).
BUT `enrich_embeddings.py` embeds events inside a guard `if text and len(text.strip()) >= 3:` wrapped
in `try/except: pass` — events with 0-char text (e.g. `test_sig_debug`, `custodian_test_v2`,
`sig_ee0787749df8`) are **silently skipped** and will NEVER appear in `observed_vectors`. So the
"backlog" is permanent and is NOT evidence of embeddable volume. The probe cannot distinguish
embeddable from un-embeddable missing rows.

## Verification recipe (deterministic)
1. Live re-run the ACTUAL script under a hard cap:
   `cd <hermes-home>/profiles/indigo && timeout 100 <hermes-venv>/bin/python scripts/enrich_embeddings.py`
   If it prints `New embeddings: N (42.5s)` with **exit 0** (not killed at 600s), the stored timeout
   is STALE — `enrich_embeddings.py` is healthy.
2. Confirm the "backlog" is phantom: instrument a 3-row probe of `events` missing from
   `observed_vectors`; if the sample rows all have 0-char text, they are correctly skipped — not real
   volume.
3. Cross-check the 2026-07-13 fix on the SIBLING `chronicle_daily_embed.py` (LIMIT 8000 + deadline-aware
   batching) — its governed job `chronicle:daily-embed` should be `last_status=ok` with empty
   `last_error`. If so, the fix HELD; the sibling is a different script and was never the recurrence.

## Trap B — prior-scan reopen_note was itself a stale premise
The 12:05Z scan's `reopen_note` claimed the sibling "re-ran on schedule and timed out again" — but the
live re-run disproved it (42.5s, exit 0). This is the inverse of the false-recovered-note trap:
a prior scan's REOPEN on a false premise. Correct it:
`python3 scripts/race_safe_issue_patch.py --issue-id oc_script_timeout_chronicle_embed_20260713 \
  --set status=resolved --set escalation_needed=false --set user_gated=false --retries 3`
and record the corrected verdict in the observation journal (`not_activity_reason: clean_verdict_...`).

## Lesson
When a backlog probe reports a large count for a script that skips short/empty text, do not treat the
count as "real volume to be processed." Re-run the actual script; if it exits 0 fast, the stored error
is stale and the "backlog" is phantom. Always verify a sibling-script misattribution before reopening
a timeout issue.
