# Config Recovery from Backup

## Symptoms
- MCP servers missing from config
- Model/provider changed unexpectedly (e.g., openrouter → ollama)
- Config version downgraded (e.g., v23 → v12)
- `~/.hermes/config.yaml.restored` file present with old config

## Root Cause
Something triggers `hermes setup` or `hermes migrate` which restores an old config from a previous era. The old config gets written to `config.yaml.restored` and then replaces the current config.

## Recovery Procedure

### 1. Identify the correct backup
```bash
# List all backups with their MCP server count and model
for f in ~/.hermes/config.yaml.bak.*; do
    python3 -c "
import yaml
with open('$f') as fh:
    c = yaml.safe_load(fh)
mcp = c.get('mcp_servers', {})
model = c.get('model', {})
print(f'$f: v{c.get(\"_config_version\",\"?\")} MCP:{len(mcp)} model:{model.get(\"default\",\"?\")}')
"
done
```

### 2. Pick the right backup
- Must have the **correct model** (e.g., `openrouter/owl-alpha`)
- Must have **all expected MCP servers**
- Must have the **correct config version** (e.g., v23)
- Do NOT pick a backup just because it has all MCP servers — the model might be wrong

### 3. Restore properly
```python
import yaml, shutil
from datetime import datetime

base = 'CORRECT_BACKUP_PATH'
current = '<hermes-home>/config.yaml'

with open(base) as f:
    config = yaml.safe_load(f)

# Optionally merge in new servers added after the backup
with open(current) as f:
    cur = yaml.safe_load(f)
for name in cur.get('mcp_servers', {}):
    if name not in config.get('mcp_servers', {}):
        # Only add if user confirms it should exist
        config['mcp_servers'][name] = cur['mcp_servers'][name]

shutil.copy2(current, f'<hermes-home>/config.yaml.broken.{datetime.now().strftime("%Y%m%d_%H%M%S")}')
with open(current, 'w') as f:
    yaml.dump(config, f, default_flow_style=False, allow_unicode=True)
```

### 4. Verify
```python
import yaml
with open('<hermes-home>/config.yaml') as f:
    c = yaml.safe_load(f)
assert c['model']['default'] == 'openrouter/owl-alpha'
assert c['_config_version'] == 23
assert 'google-workspace' in c['mcp_servers']
assert 'linkedin' in c['mcp_servers']
assert 'mempalace' in c['mcp_servers']
assert 'stealth-browser' in c['mcp_servers']
```

## Known Working State (May 2026)
- Config version: 23
- Model: openrouter/owl-alpha
- Provider: openrouter
- MCP servers: google-search, google-workspace, linkedin, mempalace, stealth-browser
- tavily: NOT used, do not add back
- Fallback model: Manifest (custom, https://app.manifest.build/v1, model: auto)
- Streaming: enabled
- Web backend: searxng
- Memory provider: (empty, not mem0)
- Timezone: America/Los_Angeles

## Prevention
- After any `hermes setup` or `hermes migrate`, immediately verify config
- Check `config.yaml.restored` — if it exists, something triggered a restore
- The backup chain is in `~/.hermes/config.yaml.bak.*` — newest is not always correct
