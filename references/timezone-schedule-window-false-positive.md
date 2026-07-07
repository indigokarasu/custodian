# Timezone Schedule Window False Positive

## The Trap

When scanning for "overdue" jobs by comparing `last_run_at` against the current time, you can falsely flag jobs that are on **daylight-hour schedules** (e.g., `*/30 9-17 * * 1-5`) as overdue — when in reality the current time is simply **before the job's scheduled window** for that day.

**Root cause:** The scan compares `last_run_at` (which may be from the previous day's last run) against `now` without checking whether the job's cron expression even permits running at the current hour.

## How It Happens

1. Job has schedule `*/30 9-17 * * 1-5` (weekdays 9 AM–5 PM only)
2. Last run was yesterday at 16:30 PDT
3. Current time is 06:04 PDT (Monday morning)
4. Naive scan: "last run was 13.5h ago, job runs every 30m → OVERDUE"
5. Reality: The job is not supposed to run between 17:00 and 09:00. It's on schedule for 09:00 today.

## Diagnostic Steps

When a job appears overdue:

1. **Check the cron expression** for hour restrictions:
   - `*/30 9-17 * * 1-5` → only runs 9 AM–5 PM on weekdays
   - `*/30 8-22 * * 1-5` → only runs 8 AM–10 PM on weekdays
   - `0 0 * * *` → only runs at midnight
   - `* * * * *` → runs every minute, no restrictions

2. **Convert current time to the schedule's timezone** (usually PDT/UTC-7 for this system):
   ```python
   from datetime import datetime, timezone, timedelta
   pdt = timezone(timedelta(hours=-7))
   now_pdt = datetime.now(timezone.utc).astimezone(pdt)
   ```

3. **Check if current hour is within the schedule window**:
   - If the cron hour field is `9-17` and current hour is 6 → NOT overdue
   - If the cron hour field is `*` → no restriction, proceed with overdue check

4. **Check weekday restrictions** (`1-5` = Mon-Fri):
   - If today is Saturday and schedule is `1-5` → NOT overdue

5. **Only flag as overdue if**:
   - Current time is within the schedule's hour AND weekday window
   - AND `last_run_at` is older than 2× the schedule interval
   - AND `next_run_at` is in the past

## Correct Overdue Detection Pattern

```python
from datetime import datetime, timezone, timedelta
import re

def is_job_actually_overdue(job, now_utc):
    """Returns True if job is genuinely overdue (not just outside its schedule window)."""
    sched = job.get('schedule', {})
    expr = sched.get('expr', '')
    parts = expr.split()
    if len(parts) < 5:
        return False
    
    hour_field = parts[1]
    dow_field = parts[4]
    
    # Convert to local timezone (PDT = UTC-7)
    pdt = timezone(timedelta(hours=-7))
    now_local = now_utc.astimezone(pdt)
    current_hour = now_local.hour
    current_dow = now_local.isoweekday()  # 1=Mon, 7=Sun
    
    # Check hour restriction
    if hour_field != '*':
        if '-' in hour_field:
            start, end = hour_field.split('-')
            if not (int(start) <= current_hour <= int(end)):
                return False  # Outside schedule window
        elif hour_field.isdigit():
            if current_hour != int(hour_field):
                return False
    
    # Check weekday restriction
    if dow_field != '*' and dow_field != '1-7':
        if '-' in dow_field:
            start, end = dow_field.split('-')
            if not (int(start) <= current_dow <= int(end)):
                return False  # Outside weekday window
    
    # Within schedule window — now check if actually overdue
    lra = job.get('last_run_at')
    if not lra:
        return True
    
    last_run = datetime.fromisoformat(lra)
    delta_min = (now_utc - last_run).total_seconds() / 60
    
    # Determine interval from minute field
    min_field = parts[0]
    if min_field.startswith('*/'):
        interval = int(min_field.replace('*/', ''))
    elif min_field == '*':
        interval = 1
    else:
        return False  # Specific minute, can't determine interval
    
    return delta_min > interval * 3  # 3x overdue threshold
```

## Worked Example — 2026-06-29 13:05 UTC Light Scan

Two jobs flagged as "overdue" by naive comparison:

| Job | Schedule | Last Run | Naive Delta | Local Time | In Window? | Actually Overdue? |
|-----|----------|----------|-------------|------------|------------|-------------------|
| custodian:escalation-runner | `*/30 9-17 * * 1-5` | 2026-06-27 21:19 UTC | 2984m | 06:04 PDT (Mon) | No (hour 6 < 9) | **No** |
| bones:market-monitor | `*/30 8-22 * * 1-5` | 2026-06-27 21:37 UTC | 2966m | 06:04 PDT (Mon) | No (hour 6 < 8) | **No** |

Both jobs had `next_run_at` set to 09:00 and 08:00 PDT respectively — confirming the scheduler was running them correctly, just not yet for today.

## Key Rule

> **Never flag a job as overdue without first checking whether the current time falls within its schedule window (hour + weekday restrictions).** Convert to the local timezone before comparing.
