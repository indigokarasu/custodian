# No-Agent Missing Dependency Pattern

## When to use

During light or deep scan error classification — when a `no_agent: true` cron job fails with `ModuleNotFoundError` for a Python package that exists in another venv on the system but not in the hermes-agent venv (the system Python that cron uses).

## Fingerprint

`oc_no_agent_missing_dependency`

## Distinction from other patterns

| Pattern | Key field | Meaning |
|---------|-----------|---------|
| `oc_no_agent_missing_dependency` | `ModuleNotFoundError: No module named 'X'` on a no_agent script | Script imports a package not installed in the hermes-agent venv |
| `oc_cron_script_not_found_transient` | `Script not found` but script exists at path | Write/read race condition |
| `oc_cron_no_agent_script_args` | `Script not found` with spaces/`&&` in script field | Arguments embedded in literal path |
| `oc_gateway_restart_import_window` | `ModuleNotFoundError` on agent-mode jobs (not no_agent) | Transient import failure after gateway restart |

## Classification

**Tier 2 — Auto-fix (non-destructive)** when the missing package is confirmed safe to install.

A missing Python package in the hermes-agent venv is auto-fixed (Tier 2) because:
1. Package installation is non-destructive — it only adds, never removes
2. The script demonstrably worked before (vanished dependency)
3. The package exists elsewhere on the system (confirmed safe version)
4. Multiple scripts may share the same fix

## Diagnostic steps

1. Confirm the script is `no_agent: true` — the error comes from the script's own imports, not the agent
2. Identify the missing module from `ModuleNotFoundError: No module named 'X'`
3. Check if the package exists elsewhere on the system:
   - `find <fs-root> -name "<module>" -type d 2>/dev/null` (other venvs)
   - `pip3 show <package>` (system/hermes-agent venv)
   - Check other venvs: `<hermes-home>/profiles/*/commons/data/*/venv/lib/python*/site-packages/<module>`
4. Determine when the script last succeeded vs first failed:
   - Check cron output files: `{profile}/cron/output/{job_id}/{timestamp}.md`
   - Compare timestamps to gateway restart events (mem-watchdog RSS drops)
5. Check if the package was ever in the hermes-agent venv:
   - `grep -r "<package>"<hermes-agent>/pyproject.toml <hermes-agent>/setup.py 2>/dev/null`
   - Check if it's a transitive dependency of another package
6. Verify the script's import chain — the missing module may be imported transitively (e.g., script imports `google_auth_mcp` which imports `googleapiclient`)

## Resolution: Install Procedure (2026-06-28)

**Step 1: Locate the hermes-agent venv Python.** The editable install path may differ from the plugin directory:
```bash
# Common paths (verify which one is active):
find <fs-root> -path "*/hermes-agent/.venv/bin/python3" 2>/dev/null
# Or check the source checkout path:
ls <projects-root>/hermes-agent/.venv/bin/python3
```

**Step 2: Install the missing package:**
```bash
<projects-root>/hermes-agent/.venv/bin/pip install <package>
```

**Step 3: Verify the import works:**
```bash
<projects-root>/hermes-agent/.venv/bin/python3 -c "from <module> import <thing>; print('OK')"
```

**Step 4: Run the actual script with the venv Python to confirm end-to-end:**
```bash
<projects-root>/hermes-agent/.venv/bin/python3 <script_path> --dry-run
```

## Venv Path Discovery (2026-06-28)

The Python that runs `no_agent: true` cron scripts is typically the hermes-agent venv. To definitively identify which Python:
1. Check the shebang line of known working scripts: `head -1 <hermes-home>/profiles/indigo/scripts/*.py | grep python`
2. Check `sys.path` from a cron job: `python3 -c "import sys; print('\n'.join(sys.path))"`  
3. Check pip install target: `<projects-root>/hermes-agent/.venv/bin/pip --version`

## Examples

### email:check googleapiclient (2026-06-28 — FIXED)

- **Job:** `email:check` (id: `25c06979ccc7`)
- **Script:** `email_check.py` (`no_agent: true`)
- **Error:** `ModuleNotFoundError: No module named 'googleapiclient'`
- **Import chain:** `email_check.py` → `from googleapiclient.errors import HttpError` (line 21) → also imports `google_auth_mcp.py` which imports `from googleapiclient.discovery import build`
- **Last success:** 2026-06-27 21:11
- **First failure:** 2026-06-28 00:47
- **Gateway restart:** ~00:36
- **Fix applied:** `<projects-root>/hermes-agent/.venv/bin/pip install google-api-python-client`
- **Verification:** Import OK, script --help runs successfully

## Pitfall: Don't assume the package was never installed

A script that worked yesterday but fails today with `ModuleNotFoundError` means the package WAS available and is now gone. This is not a "never installed" situation — it's a "vanished dependency" situation. The fix is the same (reinstall), but the root cause investigation differs: something removed it.

## Pitfall: Transitive imports hide the real dependency

The script may not directly import the missing module. It imports another local module (e.g., `google_auth_mcp.py`) which in turn imports the missing package. The traceback shows the actual failing import, but the dependency chain matters for understanding what to install and whether other scripts are affected.

## Pitfall: Multiple scripts may share the same missing dependency

If one no_agent script fails with a missing dependency, check other no_agent scripts that import the same local module. In the `email:check` case, any script importing `google_auth_mcp.py` would also fail. Scan for: `grep -l "google_auth_mcp" <hermes-home>/profiles/indigo/scripts/*.py <hermes-home>/scripts/*.py`

## Pitfall: Missing dependency fix may reveal token revocation

After installing a missing package on a Google-auth job (e.g., `googleapiclient` for `email:check`), the NEXT run may immediately fail with a different error: `google.auth.exceptions.RefreshError: invalid_grant: Token has been expired or revoked.`

This is NOT a new missing dependency — it's the underlying cause that was masked by the missing package. The script couldn't even attempt token refresh before the import failed.

**Rule**: After fixing a missing-dependency error on any job that accesses Google APIs (Gmail, Calendar, Tasks, Drive), immediately re-run the script to check for token revocation. If the token is dead, escalate as `oc_google_oauth_token_revoked` (Tier 3).

**Confirmed 2026-06-28**: `email:check` had `ModuleNotFoundError: No module named 'googleapiclient'` (fixed by install). Next run hit `invalid_grant: Token has been expired or revoked.` — same dead token, now visible because the import succeeded.