# Rally Health Check Addendum (2026-06-17)

This addendum extends the custodian's cron health checks to catch Rally-specific failure modes that report `ok` but are actually broken.

## Cash Drift Detection

**What to check:** Read the latest `portfolio_state.jsonl` from Rally's active data directory. If `cash_pct > 0.15` (15%), the portfolio has a cash deployment problem.

**Why it matters:** Rally can report all jobs `ok` while cash sits at 71% because the sweep script's cash deployment path was blocked by a stale pending action. The cron jobs complete successfully but no trades are submitted.

**How to check:**
```bash
# Read latest portfolio state
tail -1 <hermes-home>/profiles/indigo/commons/data/ocas-rally/portfolio_state.jsonl | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'Cash: {d[\"cash_pct\"]:.1%}')"
```

**Alert threshold:** `cash_pct > 0.15` → flag as `warning`. `cash_pct > 0.50` → flag as `critical`.

## Stale Pending Actions

**What to check:** Read `pending_actions.jsonl` and look for actions in `staged` status with `attempts > 0` or `timestamp` older than 24h.

**Why it matters:** A staged action with a missing trade plan blocks cash deployment forever. The action can never execute because the referenced trade plan file doesn't exist.

**How to check:**
```bash
python3 -c "
import json
from datetime import datetime, timezone, timedelta
cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
with open('<hermes-home>/profiles/indigo/commons/data/ocas-rally/pending_actions.jsonl') as f:
    for line in f:
        a = json.loads(line.strip())
        if a.get('status') == 'staged':
            ts = datetime.fromisoformat(a['timestamp'])
            age = (datetime.now(timezone.utc) - ts).total_seconds() / 3600
            print(f'{a[\"action_id\"]:45}  attempts={a.get(\"attempts\",0)}  age={age:.1f}h')
"
```

**Alert threshold:** Any staged action older than 24h → flag as `warning`.

## Research Run Staleness

**What to check:** Compare the latest research journal timestamp to the current time. If no research run has completed in the last 24h (on a weekday), the research cron may be failing silently.

**How to check:**
```bash
ls -lt <hermes-home>/profiles/indigo/commons/journals/ocas-rally/2026-06-17/ 2>/dev/null | head -3
```

**Alert threshold:** No research journal from today (on a weekday) → flag as `warning`.