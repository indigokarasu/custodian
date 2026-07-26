# MCP Server Module Deleted Pattern

**Fingerprint:** `oc_mcp_server_module_deleted`

## Symptom

MCP server binary/wrapper exists and is executable, but every connection attempt fails with `No module named <module>`. The wrapper calls `python -m <module>` (e.g., `python -m main`) but the module has been deleted from the active Python environment (venv).

## Distinguishes From

| Pattern | Error | What's missing |
|---------|-------|----------------|
| `oc_workspace_mcp_binary_missing` | `No such file or directory: workspace-mcp` | The wrapper script itself is missing |
| `oc_mcp_server_module_deleted` | `No module named main` | Wrapper exists, Python module deleted |
| `oc_google_oauth_token_revoked` | `invalid_grant: Token has been expired or revoked` | Auth fails after connection succeeds |
| `oc_mcp_server_connection_refused` | `Connection refused` | Process not running at all |

## Root Cause

The MCP server was installed as a Python package (not via pip, but as a manual/editable install). The module files were later deleted — possibly by:
- A cleanup script targeting `.py` files but not `__pycache__`
- Manual removal of the package directory
- A venv rebuild/replacement that didn't include the custom package
- An update/upgrade that replaced the hermes-agent venv

The wrapper script (`/usr/local/bin/workspace-mcp-fixed` → `/usr/local/bin/workspace-mcp`) persists because it's a shell script, not a Python file, so Python-targeted cleanup misses it.

## Diagnosis

```bash
# 1. Run the wrapper directly and capture error
timeout 5 /usr/local/bin/workspace-mcp-fixed --single-user --transport stdio </dev/null 2>&1
# If "No module named main" → module deleted

# 2. Check if the module exists in the venv
python3 -c "import main"  # Should fail with ModuleNotFoundError

# 3. Check pip for the package (may not be installed via pip)
pip show workspace-mcp 2>/dev/null || echo "Not a pip package"

# 4. Count occurrences in errors.log
grep -c "No module named" <hermes-home>/profiles/indigo/logs/errors.log

# 5. Check when it started (correlate with venv changes)
grep "No module named" <hermes-home>/profiles/indigo/logs/errors.log | head -1
```

## Fix

This is a **Tier 3 escalation** — the package source is unknown (not a pip package) and cannot be auto-reinstalled.

**Investigation steps for <operator>:**
1. Check if source exists in `<fs-root>/.workspace-mcp/` (data dir, not code)
2. Look for git history: `find / -name "main.py" -path "*workspace*" 2>/dev/null`
3. Check if there's a backup of the hermes-agent venv: `ls <hermes-venv>.bak* 2>/dev/null`
4. The package may have been installed via `pip install -e /some/path` — check `pip list --editable`

**Temporary workaround (if source can be located):**
```bash
# If found in a git repo or backup:
cd /path/to/workspace-mcp-source
pip install -e .
# OR manually restore main.py to the venv's site-packages
```

## Escalation Criteria

- **Tier 3** — Cannot be auto-fixed. The package source is unknown and the module is gone from all known locations.
- **Impact:** ALL jobs depending on the affected MCP server are non-functional.
- **Journal tag:** `escalation_needed: true`

## Match Patterns

- `"No module named main"` (when wrapper runs `python -m main`)
- `"No module named <module>"` (general pattern)
- MCP server connection failures where the binary exists but Python can't find the module

## False Positive Pattern (2026-06-29)

**Confirmed:** The `oc_mcp_server_module_deleted` fingerprint can be a **false positive** when the error log contains stale entries from a transient venv state, but the module is actually present and functional.

**What happened:** Light scan at 10:05 UTC June 29 flagged `workspace-mcp-fixed` with `oc_mcp_server_module_deleted` based on error log entries. However, during the escalation run verification:
- `python3 -c "import main"` → succeeded (module present in `<hermes-venv>/lib/python3.14/site-packages/workspace_mcp/`)
- `/usr/local/bin/workspace-mcp --help` → ran successfully, showed usage
- The `workspace_mcp-1.22.0.dist-info` package metadata existed in site-packages
- The `top_level.txt` confirmed `main` is a valid top-level module

**Root cause of false positive:** The error log entries were from a transient window (possibly during a venv update or gateway restart) where the module was briefly unavailable. By the time the escalation run verified, the module was fully functional.

**Verification procedure before escalating `oc_mcp_server_module_deleted`:**
```bash
# 1. Test the import directly
<hermes-venv>/bin/python3 -c "import main; print('OK')"

# 2. Run the wrapper binary
/usr/local/bin/workspace-mcp --help 2>&1 | head -3

# 3. Check if the package metadata exists
ls <hermes-venv>/lib/python3.14/site-packages/workspace_mcp-*.dist-info/top_level.txt

# 4. If ALL of the above succeed → the error is STALE, not active
#    Classify as `oc_mcp_server_module_deleted_false_positive` (Tier 2, surface only)
#    Do NOT escalate. Do NOT write to issues.jsonl.
```

**Distinguishing real vs. false positive:**
| Check | Real failure | False positive |
|-------|-------------|----------------|
| `import main` | `ModuleNotFoundError` | Succeeds |
| `workspace-mcp --help` | `ModuleNotFoundError` | Shows usage |
| Package dist-info | Missing | Present |
| Error recency | Last error <5min ago | Last error >1h ago, no new errors since |

## History

- 2026-06-29: First detected. `workspace-mcp-fixed` wrapper at `/usr/local/bin/` exists and is executable. Underlying `main` module missing from `<hermes-venv>`. 11+ failed connection attempts in `errors.log` since 22:50 UTC June 28. Recurring every ~15 minutes (each time the gateway tries to use the MCP tool). Not a pip package — was a manual/editable install that got cleaned. Distinct from the OAuth token revocation affecting `email:check` and `monitor:list`.
- 2026-06-29 (later): **False positive confirmed.** The module was actually present and functional. Light scan flagged stale error log entries. Verification showed `import main` succeeded and the binary ran correctly. Added false positive detection procedure above.
