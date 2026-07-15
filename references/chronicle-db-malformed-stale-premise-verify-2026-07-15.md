# Chronicle/State DB "malformed" — Stale-Premise Verification (2026-07-15)

`oc_chronicle_db_malformed` and `oc_state_db_oversized` are written on live fault
evidence (a cron-side `integrity_check` timeout, a "database disk image is malformed"
log line, disk% measured at the peak of a pressure episode). Days later a deep scan
may find the system healthy again. **Before re-escalating or leaving these
`escalation_needed: true`, re-verify the live premise.** A prior scan's premise can
resolve after the issue was written (same stale-premise guard class as
`references/journal-escalation-stale-premise-guard-2026-07-14.md`).

## Symlink size-0 trap (applies to ANY hermes DB)
`chronicle.db` / `state.db` are often symlinks (`<hermes-root>/state.db -> <hermes-root>/profiles/<profile>/state.db`).
`ls -la` reports size `0` for the link -> any ratio math is wrong.
- Use `du -h <path>` (follows the link to the real file) for the true size.
- Use `find <hermes-root> -name chronicle.db` to locate the REAL file (prefer the
  `profiles/<profile>/commons/db/chronicle/chronicle.db` path over a sibling profile's copy).
Confirmed 2026-07-14: a 14GB live state.db showed `0` under `ls -la` but `14G` under `du -h`.

## integrity_check timeout trap (the core technique)
A naive `PRAGMA integrity_check` (UNBOUNDED) on a 3.3GB corrupt-or-large DB **times out at
60s** under the foreground `terminal()` cap and yields no result — which an automated
flow reads as "still corrupt." Do NOT rely on the unbounded check or `quick_check` that
silently returns nothing when the DB is busy/large.

**Use a BOUNDED check under a signal alarm instead.** Bounded `integrity_check(N)` scans
only N pages and returns fast; on a healthy DB it returns a single `('ok',)`.

```python
import sqlite3, signal
DB = "<hermes-home>/commons/db/chronicle/chronicle.db"
def _alarm(s, f):
    raise TimeoutError("integrity_check exceeded budget")
signal.signal(signal.SIGALRM, _alarm)
signal.alarm(50)                      # stay under the 60s foreground cap
try:
    c = sqlite3.connect(DB)
    rows = c.execute("PRAGMA integrity_check(150)").fetchall()
    print("integrity_check(150):", rows[:6], "rows=", len(rows))
finally:
    signal.alarm(0)
# Healthy DB -> [('ok',)] (len 1). Any ('..', '..') corruption row -> still corrupt.
```

For a fuller pass raise the bound (e.g. `integrity_check(800)`) but keep the alarm ~50s.
If it still returns `('ok',)`, the DB is NOT malformed NOW — resolve the issue as
stale-premise (do not leave it user_gated).

## Disk re-check
`oc_state_db_oversized` threshold is `db > 1GB AND disk > 80%`. Re-derive live disk%:
```python
import shutil
u = shutil.disk_usage('/root')
print("used%%=%.1f free=%dGB" % (100*u.used/u.total, u.free // 1e9))
```
Confirmed 2026-07-15: `oc_chronicle_db_malformed_20260714T1805Z` (claimed disk 93%, malformed)
was RESOLVED — live disk 70.3%, `integrity_check(150)=ok`. `oc_state_db_oversized_20260706`
resolved — disk 70.3% (14GB state.db acceptable at <80% disk). Both were stale-premise, not live faults.

## Reconcile pattern
Once verified healthy, close via the race-safe patch (survives the top-of-hour `custodian:light`
rewrite race):
```
python3 scripts/race_safe_issue_patch.py --issue-id <id> \
  --set status=resolved --set escalation_needed=false --set user_gated=false \
  --set resolution_note="Stale-premise verified <UTC>: <disk%> / integrity_check(150)=ok"
```
Add a `reopen_note` explaining the original premise so a future scan doesn't re-apply the
same ineffective escalation.
