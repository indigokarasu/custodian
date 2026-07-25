# sqlite3 Connection Read-Only Attribute Monkeypatch Fix (CPython 3.12+)

## Symptom
A `no_agent` cron script that wraps a `sqlite3.Connection` to retry transient
`database is locked` errors crashes immediately on every run, at the
connection-setup line (before doing any work):

```
Traceback (most recent call last):
  File ".../scripts/enrich_embeddings.py", line 62, in <module>
    conn = _wrap_retry(conn)
  ...
AttributeError: 'sqlite3.Connection' object attribute 'execute' is read-only
```

The offending code assigns to read-only connection methods:

```python
conn.execute = lambda *a, **k: _guard(_exec, *a, **k)
conn.executemany = lambda *a, **k: _guard(_execmany, *a, **k)
conn.commit = lambda *a, **k: _guard(_commit, *a, **k)
```

## Root cause
CPython 3.12 changed the `sqlite3` module so `Connection.execute`,
`Connection.executemany`, and `Connection.commit` are **read-only attributes**
(they became regular methods, not assignable slots). Any code that monkeypatches
these at runtime raises `AttributeError`. Confirmed on Python 3.14.4:

```python
python3 -c "import sqlite3; c=sqlite3.connect(':memory:'); c.execute=1"
# AttributeError: 'sqlite3.Connection' object attribute 'execute' is read-only
```

This is a permanent **language-level** change, not an environment quirk. Any
legacy script using this monkeypatch pattern breaks on 3.12+.

## Why it matters for Custodian
This is a **deterministic code defect**, NOT a transient error. The job fails at
the connection-setup line before doing any work, so a `no_agent` job may show
`consecutive_failures: 0` on a fresh run yet be 100% broken. The `last_error`
carries a real traceback → classify as a **Tier 1 auto-fixable code defect**
(profile/user script, not a skill package, so the fix is within the safety
envelope — never modify skill-package files).

## Fix — proxy wrapper
Replace attribute assignment with a delegating proxy class:

```python
class _RetryConn:
    """Proxy over sqlite3.Connection; retries 'database is locked'."""
    def __init__(self, conn, retries=6, base=0.4):
        self._conn = conn
        self._retries = retries
        self._base = base
    def _guard(self, fn, *a, **k):
        last = None
        for i in range(self._retries):
            try:
                return fn(*a, **k)
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e).lower() and i < self._retries - 1:
                    last = e
                    time.sleep(self._base * (2 ** i))
                    continue
                raise
        raise last
    def execute(self, *a, **k):
        return self._guard(self._conn.execute, *a, **k)
    def executemany(self, *a, **k):
        return self._guard(self._conn.executemany, *a, **k)
    def commit(self, *a, **k):
        return self._guard(self._conn.commit, *a, **k)
    def close(self, *a, **k):
        return self._conn.close(*a, **k)
    def __getattr__(self, name):
        # Delegate everything else (row_factory, result rows, etc.)
        return getattr(self._conn, name)

def _wrap_retry(conn, retries=6, base=0.4):
    return _RetryConn(conn, retries, base)
```

The proxy preserves the original retry semantics (exponential backoff on
`database is locked`) without mutating read-only attributes. `__getattr__`
delegates any other access to the real connection.

## Detection
Any `last_error` containing `AttributeError` + `sqlite3.Connection` + `is read-only`
→ this pattern. Grep candidate scripts for `conn.execute =` / `conn.commit =` /
`conn.executemany =` to find others before they fire.

## Verification
1. **Isolated:** build the proxy over an in-memory connection; run
   `execute`/`executemany`/`commit`; confirm no `AttributeError` and rows persist.
2. **Real script:** run it (use `timeout` or background — embed step is
   network-bound); confirm it passes the previously-crashing line and exits 0.
3. **Flip registry:** `hermes cron run <id>` → `succeeded`; `jobs.json`
   `last_status` flips to `ok`.

## Fingerprint
`oc_sqlite3_readonly_monkeypatch` — Tier 1 auto-fix (code defect, in-profile script).

## Confirmed
2026-07-22: `Chronicle Embedding Enrichment` (`39d06c70d0a6`) failed every hour
with this error. Proxy fix applied to `~/.hermes/profiles/indigo/scripts/
enrich_embeddings.py` (backup `enrich_embeddings.py.bak_20260722T0505`); real run
rebuilt FTS (belief_fts=102805, observed_fts=74320) in 38.9s, exit 0; registry
flipped to ok via `hermes cron run`.
