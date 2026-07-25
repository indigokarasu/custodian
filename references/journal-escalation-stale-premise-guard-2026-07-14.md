# Journal→Issues Gap: Stale-Premise Guard

**Rule:** Step 8b / 8b-variant say "journal flagged `escalation_needed: true` but no matching issue exists → write the issue." This is necessary but NOT sufficient. A flagged journal can carry a premise that **resolved after the journal was written**. Persisting it creates a FALSE escalation.

**Guard — verify the live premise before writing:**
1. For `oc_state_db_oversized`: re-derive disk% live (`shutil.disk_usage('/root')`). Threshold is `db>1GB AND disk>80%`. If disk is now `<80%` (even at 5–10GB db), it is acceptable operational cost → do NOT persist.
2. For any `*_access_token_missing` / provider / auth fingerprint: re-scan `jobs.json`. If the implicated job is `status=ok` with cleared `last_error`, it recovered → do NOT persist.
3. Generic: for every fingerprint, require **≥1 live job still matching the signature** before writing.

**Confirmed cases — 2026-07-14 escalation execution loop:**
- `oc_state_db_oversized`: journal claimed disk 82% (19G free). Live `shutil.disk_usage('/root')` = 70.2% used / 30.7G free. → NOT persisted (below threshold).
- `oc_google_tasks_access_token_missing_20260714`: journal flagged escalation. Live `monitor:list` job was `status=ok` (recovered, `last_error` cleared). → NOT persisted.

**Outcome:** Both correctly skipped. Loop wrote an evidence journal and returned `[SILENT]` (clean verdict — 6 open issues all genuinely `user_gated` billing/key/oauth, 0 actionable, 0 forward-stale, 0 inverse-gotcha, `reconcile_write_needed: False`).

**Related:** the Escalation Execution Loop item (d) "Stale issue PREMISE" covers the inverse (an existing issue's own body asserts wrong facts). This guard covers the gap-check direction (a journal flag whose premise is no longer live). Both must verify live state before acting.