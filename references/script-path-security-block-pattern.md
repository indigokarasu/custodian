# Script Path Security Block Pattern

**Fingerprint:** `oc_cron_script_path_security_block`

When a cron job's `script` field points to a path outside the allowed scripts directory, the Hermes security model blocks execution with: `"Blocked: script path resolves outside the scripts directory"`. The script may physically exist at the path but is rejected by the validator.

## Fix (Tier 1 Auto-Fix)

**The fix direction depends on which `HERMES_HOME` is active.**

1. Check `HERMES_HOME`: `echo $HERMES_HOME` or check the gateway systemd service
<<<<<<< Updated upstream
2. If `HERMES_HOME=<hermes-home>/profiles/<profile>`:
   - Update job `script` field to `<hermes-home>/profiles/<profile>/scripts/<basename>`
   - Verify script exists at that path
3. If `HERMES_HOME=<hermes-home>` (default):
   - Update job `script` field to `<hermes-home>/scripts/<basename>`
   - Verify script exists at that path
4. Test: `HERMES_HOME=<value> python3 -c "from cron.scheduler import _run_job_script; print(_run_job_script('<path>'))"`

**IMPORTANT**: Do NOT blindly change to `<hermes-home>/scripts/` — that's wrong when running under a profile. For indigo profile, `<hermes-home>/scripts/` is OUTSIDE the allowed directory.
=======
2. If `HERMES_HOME=~/.hermes/profiles/<profile>`:
   - Update job `script` field to `~/.hermes/profiles/<profile>/scripts/<basename>`
   - Verify script exists at that path
3. If `HERMES_HOME=~/.hermes` (default):
   - Update job `script` field to `~/.hermes/scripts/<basename>`
   - Verify script exists at that path
4. Test: `HERMES_HOME=<value> python3 -c "from cron.scheduler import _run_job_script; print(_run_job_script('<path>'))"`

**IMPORTANT**: Do NOT blindly change to `~/.hermes/scripts/` — that's wrong when running under a profile. For indigo profile, `~/.hermes/scripts/` is OUTSIDE the allowed directory.
>>>>>>> Stashed changes

## Diagnostic

The error message shows the allowed directory and the blocked path:
```
Blocked: script path resolves outside the scripts directory (<ALLOWED_DIR>): '<BLOCKED_PATH>'
```

Match `<BLOCKED_PATH>` against `<ALLOWED_DIR>` — the path must start with `<ALLOWED_DIR>`.

## Symlink Fix Does NOT Work For This Pattern

<<<<<<< Updated upstream
A **symlink** satisfies `oc_cron_dead_script_ref` (script-not-found — see the "Symlink Fix for Dead Script Refs" section below) but does **NOT** satisfy `oc_cron_script_path_security_block`. The security validator resolves the script's **realpath** and rejects anything whose realpath lands outside the allowed scripts directory. A symlink whose target lives in the system `<hermes-home>/scripts/` (e.g., profile symlink `deploy-site.sh -> <hermes-home>/scripts/deploy-site.sh`) realpaths to `<hermes-home>/scripts/...` → **still blocked**, even though the symlink file itself sits inside the profile dir.

**Correct fix (Tier 1):**
1. Copy a **real file** into the allowed dir — do NOT symlink:
   `cp -p <hermes-home>/scripts/<basename> <hermes-home>/profiles/<profile>/scripts/<basename> && chmod +x <hermes-home>/profiles/<profile>/scripts/<basename>`
2. Set the job `script` field to the **absolute path inside the allowed dir**:
   `<hermes-home>/profiles/<profile>/scripts/<basename>`
=======
A **symlink** satisfies `oc_cron_dead_script_ref` (script-not-found — see the "Symlink Fix for Dead Script Refs" section below) but does **NOT** satisfy `oc_cron_script_path_security_block`. The security validator resolves the script's **realpath** and rejects anything whose realpath lands outside the allowed scripts directory. A symlink whose target lives in the system `~/.hermes/scripts/` (e.g., profile symlink `deploy-site.sh -> ~/.hermes/scripts/deploy-site.sh`) realpaths to `~/.hermes/scripts/...` → **still blocked**, even though the symlink file itself sits inside the profile dir.

**Correct fix (Tier 1):**
1. Copy a **real file** into the allowed dir — do NOT symlink:
   `cp -p ~/.hermes/scripts/<basename> ~/.hermes/profiles/<profile>/scripts/<basename> && chmod +x ~/.hermes/profiles/<profile>/scripts/<basename>`
2. Set the job `script` field to the **absolute path inside the allowed dir**:
   `~/.hermes/profiles/<profile>/scripts/<basename>`
>>>>>>> Stashed changes
3. Prefer the absolute path over a bare basename so the validator checks the exact allowed path.

## Verification

Do NOT trust a prior scan's "FIXED via symlink" claim — re-verify against the LIVE `jobs.json`:

```python
import os
<<<<<<< Updated upstream
script = "<hermes-home>/profiles/<profile>/scripts/<basename>"
allowed = "<hermes-home>/profiles/<profile>/scripts"
=======
script = "~/.hermes/profiles/<profile>/scripts/<basename>"
allowed = "~/.hermes/profiles/<profile>/scripts"
>>>>>>> Stashed changes
print("is_symlink:", os.path.islink(script))            # MUST be False
print("realpath_inside:", os.path.realpath(script).startswith(allowed))  # MUST be True
```
Also run `bash -n <script>` for syntax. The job's next scheduled run is the final confirmation; if `last_error` still shows the security block, the realpath check failed and the prior fix did not hold.

## Re-Verify Prior "FIXED via symlink" Claims

A prior light/deep scan may have classified this fingerprint as `oc_no_agent_script_path_mismatch` and applied a **symlink** fix, logging `tier1_fixed` / "FIXED via symlink". That claim is **wrong for this pattern** — the symlink still fails the realpath check. Always re-derive from the LIVE `jobs.json`: if `last_error` still shows "Blocked: script path resolves outside the scripts directory", the prior symlink fix did not hold. Re-apply the real-file + absolute-path fix above. Confirmed 2026-07-07: prior scan (run_20260707_070853) marked `<agent-handle>-site-feed-refresh` FIXED via symlink; live re-derivation showed the security block still active; real-file copy + absolute path applied and verified (regular file, realpath inside allowed dir, executable, `bash -n` clean).

## Distinction from `oc_cron_dead_script_ref`

| Pattern | Error | Root Cause |
|---------|-------|------------|
| `oc_cron_dead_script_ref` | "script not found" / "no such file" | File doesn't exist at the literal path |
| `oc_cron_script_path_security_block` | "Blocked: script path resolves outside..." | File exists but path is rejected by security policy |

Fix direction depends on `HERMES_HOME` env var.

## Symlink Fix for Dead Script Refs (Tier 1)

When a cron job's **prompt** (not `script` field) references a script at a path that doesn't exist, but the script DOES exist under the profile directory, create a symlink rather than rewriting the prompt:

```bash
<<<<<<< Updated upstream
ln -sf <hermes-home>/profiles/<profile>/scripts/<basename> <hermes-home>/scripts/<basename>
```

**When to use:** The job has `script=None` (prompt-only job) and the agent's prompt hardcodes a script path like `python3 <hermes-home>/scripts/foo.py`. The script exists at `<hermes-home>/profiles/indigo/scripts/foo.py` but not at the `<hermes-home>/scripts/` path.

**Verified example (2026-06-13):** `security:monitor` job (9bd613cd812a) prompts `python3 <hermes-home>/scripts/security_monitor.py`. Script exists at profile path. Symlink created. Job continues to work without prompt modification.

**Reversibility:** `rm <hermes-home>/scripts/<basename>` (only if no other job references it).
=======
ln -sf ~/.hermes/profiles/<profile>/scripts/<basename> ~/.hermes/scripts/<basename>
```

**When to use:** The job has `script=None` (prompt-only job) and the agent's prompt hardcodes a script path like `python3 ~/.hermes/scripts/foo.py`. The script exists at `~/.hermes/profiles/indigo/scripts/foo.py` but not at the `~/.hermes/scripts/` path.

**Verified example (2026-06-13):** `security:monitor` job (9bd613cd812a) prompts `python3 ~/.hermes/scripts/security_monitor.py`. Script exists at profile path. Symlink created. Job continues to work without prompt modification.

**Reversibility:** `rm ~/.hermes/scripts/<basename>` (only if no other job references it).
>>>>>>> Stashed changes

## Examples

| Job | Wrong Path | Correct Path | HERMES_HOME |
|-----|-----------|-------------|-------------|
<<<<<<< Updated upstream
| vesper:deliver-morning | `<hermes-home>/scripts/vesper_deliver.py` | `<hermes-home>/profiles/indigo/scripts/vesper_deliver.py` | `<hermes-home>/profiles/indigo` |
=======
| vesper:deliver-morning | `~/.hermes/scripts/vesper_deliver.py` | `~/.hermes/profiles/indigo/scripts/vesper_deliver.py` | `~/.hermes/profiles/indigo` |
>>>>>>> Stashed changes

## Match Patterns

- `"Blocked: script path resolves outside the scripts directory"`
- `"script path resolves outside.*scripts directory"`

## Reversibility

Restore original script path from `jobs.json` backup.