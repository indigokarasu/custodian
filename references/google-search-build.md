# google-search MCP Server — Build Fix

## Symptoms
- Gateway logs show: `MCP server 'google-search' failed initial connection after 3 attempts`
- Error: `pydantic_core._pydantic_core.ValidationError: Invalid JSON: expected value at line 1 column 5` with input `'    }'`
- Error: `Failed to connect to MCP server 'google-search' (command=node): Connection closed`
- google-search appears in `hermes mcp list` as enabled but gateway health shows it in error state

## Root Cause
The google-search MCP server at `/opt/google-search/` is a TypeScript project that must be compiled before use. The `dist/src/mcp-server.js` file is missing or stale — the `postinstall` build step was never run (or the `dist/` dir was cleaned/not generated).

## Fix

### 1. Build the project
```bash
cd /opt/google-search && npm run build
```

### 2. Verify output exists
```bash
ls -la /opt/google-search/dist/src/mcp-server.js
```

### 3. Restart the gateway
```bash
hermes gateway restart
```

### 4. Verify recovery
```bash
hermes gateway status
# Should show node process: /opt/google-search/dist/src/mcp-server.js
# Should NOT show JSON parsing errors or connection failures
```

## Prevention
- This can recur after `git pull` updates to `/opt/google-search/` if the update doesn't include pre-built `dist/`
- After any update to the google-search repo, always run `npm run build` before restarting the gateway
- If you see `dist/src/mcp-server.js` is missing or older than `src/mcp-server.ts`, rebuild

## Notes
- The google-search MCP uses Playwright with Chromium — expect a `headless_shell` child process after startup
- Startup is slow (Chromium launch) — allow 10+ seconds before declaring failure
- Config path in `~/.hermes/config.yaml`: `mcp_servers.google-search.args = ["/opt/google-search/dist/src/mcp-server.js"]`