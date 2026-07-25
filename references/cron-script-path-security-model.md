# Cron Script Path Security Model — Hermes Framework Constraint

## The Rule

Hermes's `cron/scheduler.py` validates all cron script paths. The script's resolved path MUST be a child of the designated scripts directory.

**Error message:**
```
Blocked: script path resolves outside the scripts directory
```

## How the Security Model Works

The validator in `_run_job_script()` (scheduler.py:943-961):
1. `scripts_dir = _get_hermes_home() / "scripts"`
2. `scripts_dir_resolved = scripts_dir.resolve()`
3. `path = Path(script_path).expanduser().resolve()` (for absolute paths)
4. `path.relative_to(scripts_dir_resolved)` — must succeed or the path is blocked

## How `_get_hermes_home()` Resolves

The function (scheduler.py:226-228):
```python
_hermes_home: Path | None = None  # module-level global

def _get_hermes_home() -> Path:
    return _hermes_home or get_hermes_home()
```

Resolution order:
1. `_hermes_home` global — set by `_job_profile_context()` when a job has a `profile` field, OR left as `None`
2. `get_hermes_home()` from `hermes_constants.py` — checks:
   a. `get_hermes_home_override()` (thread-local, set by `_job_profile_context`)
   b. `HERMES_HOME` environment variable
   c. `_get_platform_default_hermes_home()` → `Path.home() / ".hermes"` (fallback)

## Critical: Two Separate Module Globals

**IMPORTANT**: The `_hermes_home` global in `cron/scheduler.py` (line 223) is DIFFERENT from the `_hermes_home` in `gateway/run.py` (line 845). They are in different module namespaces. The gateway setting the global in its own module does NOT affect the scheduler's global.

For jobs WITHOUT a `profile` field, `_job_profile_context` yields `None` immediately (scheduler.py:256), leaving the scheduler's `_hermes_home` as `None`. It falls through to `get_hermes_home()` which checks the `HERMES_HOME` env var.

## When Running Under a Profile (e.g., "indigo")

<<<<<<< Updated upstream
The systemd service sets: `Environment="HERMES_HOME=<hermes-home>/profiles/indigo"`

For a job WITHOUT a profile:
- `_hermes_home` global = `None`
- Falls through to `get_hermes_home()` → returns `<hermes-home>/profiles/indigo` (from env var)
- `scripts_dir` = `<hermes-home>/profiles/indigo/scripts`
- Scripts at `<hermes-home>/profiles/indigo/scripts/<name>` PASS validation
- Scripts at `<hermes-home>/scripts/<name>` FAIL validation (wrong directory)

For a job WITH `profile: "indigo"`:
- `_job_profile_context` sets `_hermes_home = profile_home` = `<hermes-home>/profiles/indigo`
- Same result: `scripts_dir` = `<hermes-home>/profiles/indigo/scripts`
- Scripts at `<hermes-home>/profiles/indigo/scripts/<name>` PASS validation

## The Correct Fix

**Point script fields to `<hermes-home>/profiles/indigo/scripts/<basename>`** — the profile scripts directory. This is the directory that matches `HERMES_HOME/scripts/` when `HERMES_HOME=<hermes-home>/profiles/indigo`.

Do NOT point to `<hermes-home>/scripts/` — that directory is NOT under the allowed scripts dir when running under a profile.
=======
The systemd service sets: `Environment="HERMES_HOME=~/.hermes/profiles/indigo"`

For a job WITHOUT a profile:
- `_hermes_home` global = `None`
- Falls through to `get_hermes_home()` → returns `~/.hermes/profiles/indigo` (from env var)
- `scripts_dir` = `~/.hermes/profiles/indigo/scripts`
- Scripts at `~/.hermes/profiles/indigo/scripts/<name>` PASS validation
- Scripts at `~/.hermes/scripts/<name>` FAIL validation (wrong directory)

For a job WITH `profile: "indigo"`:
- `_job_profile_context` sets `_hermes_home = profile_home` = `~/.hermes/profiles/indigo`
- Same result: `scripts_dir` = `~/.hermes/profiles/indigo/scripts`
- Scripts at `~/.hermes/profiles/indigo/scripts/<name>` PASS validation

## The Correct Fix

**Point script fields to `~/.hermes/profiles/indigo/scripts/<basename>`** — the profile scripts directory. This is the directory that matches `HERMES_HOME/scripts/` when `HERMES_HOME=~/.hermes/profiles/indigo`.

Do NOT point to `~/.hermes/scripts/` — that directory is NOT under the allowed scripts dir when running under a profile.
>>>>>>> Stashed changes

## Why Previous Fixes Failed

### First Fix Attempt (2026-06-03)
<<<<<<< Updated upstream
Changed script fields from `<hermes-home>/profiles/indigo/scripts/` to `<hermes-home>/scripts/`. **This was the wrong direction** — it made things worse because `<hermes-home>/scripts/` is NOT under the profile scripts dir.

### Second Fix (2026-06-04/05)
Reverted fields back to `<hermes-home>/profiles/indigo/scripts/`. This is correct but the errors persisted due to a transient issue (likely env var not propagated during a gateway restart or parallel job race condition).
=======
Changed script fields from `~/.hermes/profiles/indigo/scripts/` to `~/.hermes/scripts/`. **This was the wrong direction** — it made things worse because `~/.hermes/scripts/` is NOT under the profile scripts dir.

### Second Fix (2026-06-04/05)
Reverted fields back to `~/.hermes/profiles/indigo/scripts/`. This is correct but the errors persisted due to a transient issue (likely env var not propagated during a gateway restart or parallel job race condition).
>>>>>>> Stashed changes

## Error Message Format

```
Blocked: script path resolves outside the scripts directory (<ALLOWED_DIR>): '<BLOCKED_PATH>'
```

<<<<<<< Updated upstream
- `<ALLOWED_DIR>` = the resolved scripts directory (e.g., `<hermes-home>/profiles/indigo/scripts`)
=======
- `<ALLOWED_DIR>` = the resolved scripts directory (e.g., `~/.hermes/profiles/indigo/scripts`)
>>>>>>> Stashed changes
- `<BLOCKED_PATH>` = the script path from the job's `script` field (before resolution)

## Detection

Scan jobs.json for jobs whose `script` field points to a path that is NOT under the expected scripts directory. The expected directory depends on `HERMES_HOME`:
<<<<<<< Updated upstream
- If `HERMES_HOME=<hermes-home>/profiles/indigo` → scripts must be under `<hermes-home>/profiles/indigo/scripts/`
- If `HERMES_HOME=<hermes-home>` (default) → scripts must be under `<hermes-home>/scripts/`
=======
- If `HERMES_HOME=~/.hermes/profiles/indigo` → scripts must be under `~/.hermes/profiles/indigo/scripts/`
- If `HERMES_HOME=~/.hermes` (default) → scripts must be under `~/.hermes/scripts/`
>>>>>>> Stashed changes

## Fix Verification

After applying a fix, test with:
```bash
HERMES_HOME=<current_value> python3 -c "
from cron.scheduler import _run_job_script
ok, output = _run_job_script('<script_path>')
print('ok:', ok, 'output:', output[:200])
"
```

## Affected Jobs (resolved 2026-06-04)

These 8 jobs were failing but pass validation when `HERMES_HOME` is correctly set:
- voyage:update, reach:update, imagine:update, spot:update, vibes:update, multipass:update
- vesper:deliver-morning (script: vesper_deliver.py)
- plaid-transaction-sync (script: plaid_sync.py)

<<<<<<< Updated upstream
Root cause: transient env var propagation issue around gateway restart. Scripts at `<hermes-home>/profiles/indigo/scripts/` are the correct location.
=======
Root cause: transient env var propagation issue around gateway restart. Scripts at `~/.hermes/profiles/indigo/scripts/` are the correct location.
>>>>>>> Stashed changes

## Tier Classification

**Tier 2** — Understanding the fix requires reading Hermes source code. The fix direction depends on which `HERMES_HOME` is active.

## Related References

- `references/cron-script-path-home-pattern.md` -- Path.home() resolution issues in scripts
- `references/known-script-auth-issues.md` -- Script auth patterns
- `references/fix-safety.md` -- Tier 1 auto-fix registry (oc_cron_dead_script_ref)
- `references/critical-pitfalls.md` -- General cron pitfalls
- `references/script-path-security-block-pattern.md` -- The fingerprint pattern for this error