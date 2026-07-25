# MCP Server Files Missing Pattern

## Fingerprint: `oc_mcp_server_files_missing`

**Tier:** 2 (Surface only — requires user decision)

## Description

MCP servers are `enabled: true` in config.yaml but their server script files don't exist on disk. The MCP client tries to spawn each server, fails after 3 connection attempts, and gives up gracefully. This produces 380+ WARNING lines per startup cycle but is non-fatal.

## Affected Servers (2026-06-14)

| Server | Command | Configured Path | Status |
|--------|---------|----------------|--------|
| instagram | python3 | `<hermes-home>/mcp-servers/instagram-mcp/src/instagram_mcp_server.py` | MISSING |
| pdsx | python3 | `<hermes-home>/mcp/pdsx/src/server.py` | MISSING |
| spotify | node | `<hermes-home>/node/lib/node_modules/@darrenjaws/spotify-mcp/build/bin.js` | MISSING |
| threads | node | `<hermes-home>/mcp-servers/threads-mcp/src/index.ts` | MISSING |

## Detection

```bash
# Check if MCP server files exist
python3 -c "
import yaml, os
with open('<hermes-home>/config.yaml') as f:
    cfg = yaml.safe_load(f)
for name, srv in cfg.get('mcp_servers', {}).items():
    if not srv.get('enabled', True): continue
    args = srv.get('args', [])
    if args:
        path = args[0]
        exists = os.path.exists(path)
        print(f'{name}: {path} -> {\"EXISTS\" if exists else \"MISSING\"}')"
```

## Fix Options (User Decision Required)

**Option A — Install the missing MCP server packages** (requires user action)

**Option B — Disable the servers in config.yaml:**
```yaml
mcp_servers:
  instagram:
    enabled: false
  pdsx:
    enabled: false
  spotify:
    enabled: false
  threads:
    enabled: false
```

## Escalation Note

Do NOT auto-disable — this changes capability. Escalate for user decision.

## Match Patterns

```
"MCP server.*failed initial connection after 3 attempts"
"MCP server.*Connection closed"
"unhandled errors in a TaskGroup.*MCP"
```

## Related Patterns

- `oc_mcp_stdio_parse_error` — MCP server stdout pollution (server runs but output is non-JSON)
- `oc_mcp_google_search_connection_failure` — specific MCP server connection failure
- `oc_mcp_simultaneous_failure` — multiple MCP servers fail simultaneously (systemic issue)
