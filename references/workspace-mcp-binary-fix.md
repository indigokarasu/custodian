# workspace-mcp Binary Missing Entry Point

## Symptom

`workspace-mcp-fixed` wrapper exists at `/usr/local/bin/workspace-mcp-fixed` but fails with:
```
/usr/local/bin/workspace-mcp-fixed: line 7: /usr/local/bin/workspace-mcp: No such file or directory
```

The MCP server `google-workspace` fails to connect. Gateway log shows "No such file or directory: workspace-mcp-fixed" or the process starts but immediately exits.

## Root Cause

The `workspace-mcp` Python package (installed via pip) registers a console script entry point `workspace-mcp = main:main`. This entry point is created at `/usr/local/bin/workspace-mcp` during `pip install`. However, if the package was previously installed and partially removed (e.g., manual cleanup, incomplete uninstall), the entry point script may be missing even though the Python package files remain in `/usr/local/lib/python3.13/dist-packages/`.

The `workspace-mcp-fixed` wrapper is a bash script that sets environment variables and then `exec /usr/local/bin/workspace-mcp "$@"`. If the target is missing, the wrapper fails.

## Diagnosis

```bash
# Check if both binaries exist
ls -la /usr/local/bin/workspace-mcp /usr/local/bin/workspace-mcp-fixed

# Check if the Python package is installed
pip show workspace-mcp

# Check if main.py exists
ls -la /usr/local/lib/python3.13/dist-packages/main.py
```

## Fix

```bash
# Step 1: Try reinstalling (may recreate the entry point)
pip install --force-reinstall workspace-mcp

# Step 2: If still missing, create the entry point manually
cat > /usr/local/bin/workspace-mcp << 'SCRIPT'
#!/bin/bash
exec python3.13 -m main "$@"
SCRIPT
chmod +x /usr/local/bin/workspace-mcp

# Step 3: Verify both binaries work
timeout 3 /usr/local/bin/workspace-mcp --help 2>&1 | head -3
timeout 3 /usr/local/bin/workspace-mcp-fixed --help 2>&1 | head -3

# Step 4: Verify MCP server responds to JSON-RPC
echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}' | timeout 5 /usr/local/bin/workspace-mcp-fixed --single-user --transport stdio 2>/dev/null
```

## Verification

Expected output from Step 4:
```json
{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2024-11-05","capabilities":{...},"serverInfo":{"name":"google_workspace","version":"3.2.4"}}}
```

## Notes

- The `workspace-mcp` package installs `main.py` at the top level of the dist-packages directory (not in a subdirectory). This is why `python3.13 -m main` works.
- The `workspace-mcp-fixed` wrapper sets `WORKSPACE_MCP_BASE_URI`, `WORKSPACE_MCP_PORT`, `GOOGLE_OAUTH_REDIRECT_URI`, `GOOGLE_OAUTH_CLIENT_ID`, and `GOOGLE_OAUTH_CLIENT_SECRET` environment variables before exec'ing `workspace-mcp`.
- If the MCP server starts but fails auth, that's a separate OAuth issue — the binary itself is fine.
