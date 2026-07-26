# memory_guard failure pattern

## What to check

The `finch:memory-guard-floor` cron job (runs every 6h) enforces the hard char-cap on MEMORY.md.
Check its output for failures:

1. **Job not running**: If `finch:memory-guard-floor` has no `last_run_at` or gap > 8h, the guard floor is not active.
2. **STILL OVER CAP warning**: If the guard's DecisionRecord shows `over_cap_after=true`, directives alone exceed the cap — needs LLM consolidation or human review.
3. **INTEGRITY violation**: If the guard refuses to apply because directives would be lost, flag for immediate review.
4. **Lock contention**: If the guard reports "MEMORY.md locked by a live process" repeatedly, something is holding the lock.

## How to check

```bash
# Check job last_run from jobs.json (NOT hermes cron list — broken in cron context)
python3 -c "
import json
with open('<hermes-home>/profiles/indigo/cron/jobs.json') as f:
    data = json.load(f)
jobs = data.get('jobs', data) if isinstance(data, dict) else data
for j in jobs:
    if 'memory-guard-floor' in j.get('name',''):
        print(j.get('last_run_at'), j.get('last_status'))
"

# Check DecisionRecords for recent over_cap_after (last 24h)
grep -v "^$" <hermes-home>/profiles/indigo/commons/data/ocas-finch/decisions.jsonl | python3 -c "
import sys, json
from datetime import datetime, timezone, timedelta
cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
for line in sys.stdin:
    line = line.strip()
    if not line: continue
    d = json.loads(line)
    if d.get('over_cap_after'):
        ts = d.get('timestamp','')
        if ts >= cutoff.isoformat():
            print(json.dumps(d, indent=2))
"

# Check MEMORY.md current size (correct path: memories/ not profile root)
wc -c <hermes-home>/profiles/indigo/memories/MEMORY.md
```

## Severity

- **STILL OVER CAP**: Tier 2 — flag in next custodian report. Not urgent but needs attention within 24h.
- **INTEGRITY violation**: Tier 1 — immediate escalation. Directives are at risk.
- **Job not running**: Tier 2 — check if cron job is enabled; restart if needed.