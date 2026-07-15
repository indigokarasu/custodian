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
3. Timestamps in `jobs.json` carry explicit UTC offsets (e.g. `-07:00`). Convert the cutoff
   to the log's timezone (gateway.log is UTC) before comparing. See
   `references/jobs-json-timestamp-offset-misread-pitfall.md`.
4. For "since last scan", use the journal's content timestamp, not mtime (journal mtimes
   lag ~7h). See `references/journal-path-format-inconsistency.md`.

## Also applies to
- `errors.log`, `cleanup.log`, `gateway-exit-diag.log` — any multi-line Python traceback log.
- Grep range of the form `awk '$0 >= "TS"' | grep -i error` — same trap.
- Counting branches (e.g. `wc -l` after a range filter) — counts continuation lines, not
  real events.

## Why this matters for Custodian
Light-scan Step 2 ("tail gateway log for new errors since last scan timestamp") and the
deep-scan log pass both depend on accurate "new since cutoff" counts. A false count
triggers false escalations / false "system degraded" verdicts. Always use the per-line
extractor above; never trust `awk` string-range filtering on timestamped logs.
