# Gateway/errors Log Timestamp-Range Filtering Pitfall

## Symptom
When scanning gateway.log / errors.log for "errors since last scan at <TIMESTAMP>", a
naive `awk '$0 >= "2026-07-14 08:08:11"' file` (or `tail | awk` range) returns a huge
count of "new errors" — e.g. 251, 172, 115 — that on inspection are ALL old.

## Root cause
Python tracebacks and many error lines are MULTI-LINE. The first line of a traceback may
carry a timestamp, but every continuation line does NOT:

```
2026-07-13 22:31:34,011 INFO gateway.run: kanban dispatcher: reaped ...   <- has ts
Traceback (most recent call last):                                       <- NO ts
  File "...", line 18, in <module>                                       <- NO ts
sqlite3.OperationalError: disk I/O error                                <- NO ts
ValueError: Auxiliary compression model moondream has a context window... <- NO ts
```

`awk` compares the WHOLE line as a string against the timestamp literal. A line starting
with `T` (Traceback) or `s` (sqlite3) or `V` (ValueError) is lexicographically GREATER
than `"2026-07-14 08:08:11"` because `"T" > "2"` in ASCII. So every timestampless
continuation line after the cutoff matches the range — even if the actual traceback STARTED
days earlier (e.g. 2026-07-07). The match has nothing to do with when the error occurred.

## Confirmed incident
2026-07-14 light scan. Last scan cutoff `2026-07-14T08:08:11Z`. Naive range counts reported
251 (gateway.log), 115, 172 "post-scan errors". A per-line timestamp extractor
(`re.match(r"(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})", ln)`) filtering ONLY lines whose
extracted ts > cutoff, then counting ERROR/CRITICAL/Traceback/ValueError, returned **0**
new errors. The errors were all pre-cutoff stale traceback tails.

## Correct pattern (use this, never naive awk range)
```python
import re
LOG = "<hermes-home>/logs/gateway.log"
cut = ("2026-07-14", "08:08:11")   # (date, time) tuple, compared lexicographically
cnt = 0
for ln in open(LOG, errors="replace"):
    m = re.match(r"(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})", ln)
    if not m:
        continue                    # <-- SKIP timestampless lines, do not count them
    ts = (m.group(1), m.group(2))
    if ts <= cut:
        continue
    if re.search(r"ERROR|CRITICAL|ValueError|OperationalError|Traceback|Error code", ln):
        cnt += 1
print("post-cutoff error lines:", cnt)
```
Key rules:
1. Extract a timestamp from EACH line. If no timestamp, SKIP (do not match on the line body).
2. Compare `(date, time)` tuples — robust to both `T` and space separators.
3. Timestamps in `jobs.json` carry explicit UTC offsets (e.g. `-07:00`). The LOG's zone is
   a SEPARATE question — **do not assume the log is UTC**. The log stamp format and zone
   must be verified from the log's own lines (see "Zone verification" below), THEN the cutoff
   converted to match. See `references/jobs-json-timestamp-offset-misread-pitfall.md` for the
   `jobs.json` side.
4. For "since last scan", use the journal's content timestamp, not mtime (journal mtimes
   lag ~7h). See `references/journal-path-format-inconsistency.md`.

## Also applies to
- `errors.log`, `cleanup.log`, `gateway-exit-diag.log` — any multi-line Python traceback log.
- Grep range of the form `awk '$0 >= "TS"' | grep -i error` — same trap.
- Counting branches (e.g. `wc -l` after a range filter) — counts continuation lines, not
  real events.

## Zone verification — the log may NOT be UTC (confirmed 2026-07-17)

**The opposite trap from the multiline-awk one:** even a correct per-line timestamp
extractor (above) returns **0** new errors when the cutoff is expressed in UTC but the log
is stamped in **LOCAL naive time with no offset in the stamp**. A zero-result grep that
contradicts live system activity is a **false-clean**, not evidence of health.

### What happened
A light scan grepped `errors.log` for the UTC scan-window date `^2026-07-17 (0[2-9]|1[0-9]|2[0-3]):`
→ **0** ERROR/Traceback lines, suggesting a clean log window. But other cron jobs ran this
session, the registry held error jobs, and the gateway process was alive. Re-grepping with
the LOCAL window `^2026-07-16 (19:[0-9][0-9]|20:0[0-4])` (local zone `-07:00`) surfaced **62**
transient `agent.conversation_loop: Outer loop error` lines (root cause
`AttributeError: 'DaemonThreadPoolExecutor' object has no attribute '_initializer'`, a known
transient read-path error) across 7 cron jobs — all already self-recovered (`last_status=ok`,
`cf=None`). The UTC grep had produced a false-clean verdict and missed the error cluster.

### How to verify the log's zone before grepping by date
1. `head -3 errors.log` — the stamp format tells you the zone:
   - `2026-07-16 19:06:09,241 ERROR ...` (no `+HH:MM` suffix) → **LOCAL naive wall-clock**,
     NOT UTC. The date rolls over at local midnight, ~17:00Z here.
   - `2026-07-16T19:06:09-07:00` (explicit offset) → already zone-aware; convert accordingly.
   - `2026-07-16T02:06:09Z` (Z suffix) → UTC.
2. Cross-check: if a UTC-window grep returns 0 but the system is provably active (prior
   custodian journal has a later UTC `scan_started_at`, or `ps` shows the gateway alive),
   the mismatch is a zone error — re-derive the local window and re-grep.
3. Convert the `last_scan_utc` … `now_utc` window into the log's local zone, then grep that
   range. Concrete recipe used 2026-07-17 (local `-07:00`):
   ```bash
   cd <hermes-home>/logs
   # WRONG (UTC window, false 0):
   grep -E "^2026-07-17 (0[2-9]|1[0-9]|2[0-3]):" errors.log | grep -iE "ERROR|Traceback" | wc -l
   # RIGHT (local -07:00 window):
   grep -E "^2026-07-16 (19:[0-9][0-9]|20:0[0-4])" errors.log | grep -iE "ERROR|Traceback" | wc -l
   ```
4. Also note: a local-time log whose newest line mtime appears "hours behind" `now - UTC`
   is normal — it is not proof logging died. Confirm the gateway process is alive before
   concluding a silent log gap.

### Durable principle
Before concluding a clean log window from a date-bounded grep, **verify the log's own
timezone** and confirm the grep range covers the same instants the registry's UTC timestamps
describe. A zero-result grep that contradicts live activity is a prompt to re-check the zone,
not evidence of health. (Distinct from `references/jobs-json-timestamp-offset-misread-pitfall.md`,
which covers the explicit-offset case; here the stamp has NO offset, so the danger is silently
assuming it is UTC.)

## Why this matters for Custodian
Light-scan Step 2 ("tail gateway log for new errors since last scan timestamp") and the
deep-scan log pass both depend on accurate "new since cutoff" counts. A false count
triggers false escalations / false "system degraded" verdicts. Always use the per-line
extractor above; never trust `awk` string-range filtering on timestamped logs.
