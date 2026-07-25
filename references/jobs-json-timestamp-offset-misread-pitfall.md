# jobs.json Timestamp Offset Misread Pitfall (2026-07-08)

## The trap
Every `last_run_at` / `next_run_at` in the profile `jobs.json`
(`<hermes-home>/profiles/<profile>/cron/jobs.json`) carries an EXPLICIT
UTC offset, e.g.:

    "next_run_at": "2026-07-08T06:02:33.579453-07:00",
    "last_run_at": "2026-07-08T05:59:33.579453-07:00"

The scan environment's `now` is UTC (`2026-07-08T13:05:07Z`).

A naive comparison treats the `-07:00` wall-clock as if it were UTC and
concludes the job is ~7 hours overdue / "stuck" — a SPURIOUS
"jobs not running / frozen scheduler" escalation (Step 7 false positive).

## The correction
Convert the offset timestamp to UTC before comparing:

    "06:02:33 -07:00"  ==  13:02:33Z   (only ~2.5 min before now)

So the job was actually 2.5 min overdue and running fine on its 3-min
interval — NOT frozen.

## Reusable fix (Python)
```python
from datetime import datetime, timezone
def to_utc(s):
    # datetime.fromisoformat parses the -07:00 offset natively
    return datetime.fromisoformat(s).astimezone(timezone.utc)
now = datetime.now(timezone.utc)
overdue = to_utc(job["next_run_at"]) < now
```
Never compare the raw string or the offset-local wall-clock to a UTC
`now`. Always normalize BOTH sides to UTC first.

## Confirmed case (2026-07-08)
SearXNG Health Watchdog (`id=652c1df31466`, every 3m) showed
`next_run_at = 06:02:33-07:00` while now was 13:05Z. First reading:
"stuck for 8 hours, scheduler frozen after 05:33 gateway restart."
Correct reading: 2.5 min overdue, running every 3 min, failing because
SearXNG returned 0 results (upstream engines suspended), NOT because the
scheduler was frozen. The offset misread produced a false "not running"
hypothesis that had to be walked back.
