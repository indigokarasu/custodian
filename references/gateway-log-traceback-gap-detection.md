# Gateway-Log Traceback Gap Detection

The standard light-scan flow keys off `jobs.json` error jobs. That view is **incomplete**: gateway-internal Python tracebacks from plugin code never become a `jobs.json` `last_error`. A scan can report "clean" (0 actionable error jobs) while the gateway log is throwing recurring plugin defects.

## Why this happens
Cron job failures are captured in `jobs.json` (script exit code / captured stderr). But gateway-level paths — context compression, memory/Chronicle event append, adapter code — run inside the gateway process and log tracebacks to `gateway.log` without a corresponding cron job error. Example (found 2026-07-22):
- `sqlite3.IntegrityError: CHECK constraint failed: actor IN ('user','agent','curator','system')` raised in `chronicle/engine/store.py:append_event`.
- `TypeError: ChronicleContextEngine.compress() got an unexpected keyword argument 'force'` raised from `agent/conversation_compression.py:624` calling the Chronicle engine override.

Both recurred 14–19× in the hours before a gateway restart, 0× after. Neither ever appeared in `jobs.json`.

## Detection recipe
1. Find the last restart boundary: `grep -nE "Starting Hermes Gateway|Received SIGTERM" <gateway.log>`.
2. Extract tracebacks since the last restart: `grep -nE "Traceback|IntegrityError|TypeError" <gateway.log>` (post-restart region).
3. Dedup by signature = exception class + in-plugin frame (e.g. `store.py:append_event`, `compress() got ... 'force'`).
4. Drop signatures already tracked by an OPEN issue in `issues.jsonl` (grep fingerprint/issue_id — don't double-track).
5. Drop single-shot / pre-restart-only signatures that did not recur post-restart (noise).
   **Reusable probe (packaged 2026-07-23):** `scripts/verify_plugin_defect_postrestart.py` does the full pre/post-restart split automatically — scans both gateway logs, locates the last restart marker, and emits per-signature `pre=` / `post=` counts plus a LIVE-vs-DORMANT verdict. Use it instead of hand-rolling a grep when deciding whether a gateway-internal plugin-defect issue is still live. Confirmed catch: `oc_chronicle_event_actor_check_constraint` (2026-07-23) was almost dismissed as dormant — the probe showed 11 post-restart hits (last 2026-07-23T11:59Z), proving it live.
6. For a NEW recurring signature with no open issue → persist as:
   `{"issue_id":"oc_<slug>_<date>","fingerprint":"oc_<slug>","status":"open","escalation_needed":true,"user_gated":false,"recommended_tier":4,"confidence":0.9,"severity":"degraded","description":"...","affected_components":[...],"recurrence_count":N,"first_seen_at":"...","last_seen_at":"...","note":"Code defect in <plugin>, NOT user-gated. <fix direction>."}`
   Append with `cat >> issues.jsonl << 'PYEOF'` (one JSON object per line) — race-safe vs the top-of-hour `custodian:light` rewrite. Never a whole-file rewrite.
7. Record `new_issues` in the observation journal.

## Known-signature catalog (persist once, then recognize)
- `oc_chronicle_event_actor_check_constraint` — actor CHECK constraint in `store.append_event`; fix = align event producer actor values to the CHECK set OR relax the schema. (added 2026-07-22)
- `oc_chronicle_contextengine_compress_force_kwarg` — `.compress()` missing `force=` kwarg; fix = add `force=...` + `**kwargs` to the Chronicle engine override. (added 2026-07-22)
- `oc_chronicle_event_seq_unique_constraint` — `UNIQUE constraint failed: events.seq` in `store.append_event`. (tracked by verifier as `seq_unique`)
- `oc_chronicle_db_locked_in_append_event` — `sqlite3.OperationalError: database is locked` raised in `chronicle/engine/store.py:append_event` (line 127, during `INSERT OR IGNORE INTO meta(key,value) VALUES('event_seq', (SELECT COALESCE(MAX(seq),0) FROM events))`). First observed 2026-07-25T00:34Z, single occurrence post-restart → NOT persisted (below ≥2 recurrence threshold). **Distinct from the other three** (those are schema/contract/kwargs defects; this is a SQLite write-concurrency defect under load). Catalogued so a future scan recognizes it instantly instead of treating it as novel. If it recurs (≥2 post-restart), persist as Tier-4 code defect (NOT user-gated), fix direction = serialize/retry the `meta` upsert or enable WAL + busy_timeout on the Chronicle DB connection.

The first three are Tier-4 plugin code defects — escalate to Mentor, do NOT mark user-gated.

**VERIFIER CATALOG IS NON-EXHAUSTIVE (2026-07-25):** `scripts/verify_plugin_defect_postrestart.py` only buckets THREE signatures (`actor_check`, `compress_force`, `seq_unique`). A 4th live signature in the log (e.g. the `database is locked` at `store.py:append_event` seen 2026-07-25) is NOT in its regex catalog, so the verifier reports `post=0 → DORMANT` for its 3 tracked signatures while the 4th keeps recurring — the **false-dormant blind spot** (see SKILL.md "VERIFIER FALSE-DORMANT" note). The raw-grep window in Step 2 is the AUTHORITATIVE live signal. Always run the Step-2 raw `grep` for `Traceback|IntegrityError|OperationalError|TypeError` in the post-restart window INDEPENDENTLY of the verifier; if the raw grep shows a recurring signature post-restart that the verifier did not bucket → treat as LIVE (persist if ≥2), do NOT trust the verifier's DORMANT verdict.
