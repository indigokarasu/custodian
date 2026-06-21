# Script Path Security Block Pattern

**Fingerprint:** `oc_cron_script_path_security_block`

When a cron job's `script` field points to a path outside the allowed scripts directory, the Hermes security model blocks execution with: `"Blocked: script path resolves outside the scripts directory"`. The script may physically exist at the path but is rejected by the validator.

## Fix (Tier 1 Auto-Fix)

**The fix direction depends on which `HERMES_HOME` is active.**

1. Check `HERMES_HOME`: `echo $HERMES_HOME` or check the gateway systemd service
2. If `HERMES_HOME=<hermes-root>/profiles/<profile>`:
   - Update job `script` field to `<hermes-root>/profiles/<profile>/scripts/<basename>`
   - Verify script exists at that path
3. If `HERMES_HOME=<hermes-root>` (default):
   - Update job `script` field to `<hermes-root>/scripts/<basename>`
   - Verify script exists at that path
4. Test: `HERMES_HOME=<value> python3 -c "from cron.scheduler import _run_job_script; print(_run_job_script('<path>'))"`

**IMPORTANT**: Do NOT blindly change to `<hermes-root>/scripts/` — that's wrong when running under a profile. For indigo profile, `<hermes-root>/scripts/` is OUTSIDE the allowed directory.

## Diagnostic

The error message shows the allowed directory and the blocked path:
```
Blocked: script path resolves outside the scripts directory (<ALLOWED_DIR>): '<BLOCKED_PATH>'
```

Match `<BLOCKED_PATH>` against `<ALLOWED_DIR>` — the path must start with `<ALLOWED_DIR>`.

## Distinction from `oc_cron_dead_script_ref`

| Pattern | Error | Root Cause |
|---------|-------|------------|
| `oc_cron_dead_script_ref` | "script not found" / "no such file" | File doesn't exist at the literal path |
| `oc_cron_script_path_security_block` | "Blocked: script path resolves outside..." | File exists but path is rejected by security policy |

Fix direction depends on `HERMES_HOME` env var.

## Symlink Fix for Dead Script Refs (Tier 1)

When a cron job's **prompt** (not `script` field) references a script at a path that doesn't exist, but the script DOES exist under the profile directory, create a symlink rather than rewriting the prompt:

```bash
ln -sf <hermes-root>/profiles/<profile>/scripts/<basename> <hermes-root>/scripts/<basename>
```

**When to use:** The job has `script=None` (prompt-only job) and the agent's prompt hardcodes a script path like `python3 <hermes-root>/scripts/foo.py`. The script exists at `<hermes-home>/scripts/foo.py` but not at the `<hermes-root>/scripts/` path.

**Verified example (2026-06-13):** `security:monitor` job (9bd613cd812a) prompts `python3 <hermes-root>/scripts/security_monitor.py`. Script exists at profile path. Symlink created. Job continues to work without prompt modification.

**Reversibility:** `rm <hermes-root>/scripts/<basename>` (only if no other job references it).

## Examples

| Job | Wrong Path | Correct Path | HERMES_HOME |
|-----|-----------|-------------|-------------|
| vesper:deliver-morning | `<hermes-root>/scripts/vesper_deliver.py` | `<hermes-home>/scripts/vesper_deliver.py` | `<hermes-home>` |

## Match Patterns

- `"Blocked: script path resolves outside the scripts directory"`
- `"script path resolves outside.*scripts directory"`

## Reversibility

Restore original script path from `jobs.json` backup.