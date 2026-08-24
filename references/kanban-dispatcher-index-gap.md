# Kanban Dispatcher Index Gap — `kanban_dispatcher_tick_failed`

## Fingerprint

`kanban_dispatcher_tick_failed`

## Symptom

```
ERROR gateway.run: kanban dispatcher: tick failed on board default
Traceback ...
  File ".../hermes_cli/kanban_db.py", line 1012, in connect
    conn.executescript(SCHEMA_SQL)
sqlite3.OperationalError: no such column: session_id
```

Recurring every 60 seconds (every dispatcher tick). 98+ errors per day.

## Root Cause

The `kanban_db.py` `connect()` function uses a module-level `_INITIALIZED_PATHS` set to cache which DB paths have been initialized. On first connection to a path, it runs:

1. `conn.executescript(SCHEMA_SQL)` — contains `CREATE TABLE IF NOT EXISTS` + `CREATE INDEX IF NOT EXISTS`
2. `_migrate_add_optional_columns(conn)` — adds missing columns via `ALTER TABLE`

**The gap**: `_migrate_add_optional_columns` handles adding missing **columns** but does NOT create missing **indexes**. If a legacy DB has the columns but is missing indexes (e.g., from a partial migration or manual schema fix), the `CREATE INDEX` in `executescript` fails because:

- `CREATE TABLE IF NOT EXISTS tasks (...)` is a no-op (table already exists without the column in the old schema)
- `CREATE INDEX idx_tasks_session_id ON tasks(session_id)` fails with "no such column" because the existing table doesn't have `session_id`

**However**, if the column WAS added (e.g., by a previous `_migrate_add_optional_columns` run in an older process), the index creation succeeds. The error only manifests when:
1. The gateway process restarts (clearing `_INITIALIZED_PATHS`)
2. The DB has the columns but NOT the indexes

## Fix (Tier 2 — Manual Index Creation)

Connect to the kanban DB and create missing indexes:

```python
import sqlite3
conn = sqlite3.connect('<hermes-home>/kanban.db')
conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_session_id ON tasks(session_id)")
conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_idempotency ON tasks(idempotency_key)")
conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)")
conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_assignee ON tasks(assignee)")
conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON tasks(created_at)")
conn.close()
```

After creating indexes, restart the gateway or wait for the next natural restart. The `_INITIALIZED_PATHS` cache will be empty, `executescript` will run, and all statements will succeed as no-ops.

## Verification

```bash
# Check indexes exist
sqlite3 <hermes-home>/kanban.db "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='tasks'"

# Check for new errors after gateway restart
strings <hermes-home>/logs/gateway.log | grep "kanban dispatcher: tick failed" | tail -5
```

## Long-Term Fix (Code Level)

The `_migrate_add_optional_columns` function in `kanban_db.py` should also create missing indexes, not just missing columns. This would make the fix automatic on first connection.

**File**: `$HERMES_INSTALL/hermes_cli/kanban_db.py`
**Function**: `_migrate_add_optional_columns()`

Add index creation at the end of the function:
```python
# Create missing indexes on legacy DBs
for idx_name, col_name in [
    ('idx_tasks_session_id', 'session_id'),
    ('idx_tasks_idempotency', 'idempotency_key'),
    ('idx_tasks_status', 'status'),
    ('idx_tasks_assignee', 'assignee'),
    ('idx_tasks_created_at', 'created_at'),
]:
    conn.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON tasks({col_name})")
```

## Recurrence Pattern

This issue recurs after every gateway restart if the indexes are not present. The `_INITIALIZED_PATHS` cache prevents re-initialization within the same process, so the error only appears on fresh process starts.

**First seen**: 2026-05-02
**Fixed**: 2026-05-19 (manual index creation + gateway restart)
**Status**: Resolved. Will recur if indexes are dropped or on fresh DB creation.