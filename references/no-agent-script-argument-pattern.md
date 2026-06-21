# no_agent Script Argument Pattern

**Fingerprint:** `oc_cron_no_agent_script_args`

When a cron job has `no_agent: true`, the `script` field is treated as a **literal file path** by the executor — not a command line. Any arguments appended to the script filename (e.g., `active_thread_renamer.py --active`) cause a "Script not found" error because the entire string is resolved as a path.

## Detection

- Job has `no_agent: true`
- `script` field contains spaces (e.g., `foo.py --flag`)
- `last_error` contains "Script not found" or "No such file or directory"
- The script file exists at the path without the arguments

## Fix (Tier 1 Auto-Fix)

Create a wrapper script that bakes in the arguments, then point the cron job's `script` field to the wrapper:

```bash
# Create wrapper
cat > <hermes-root>/profiles/<profile>/scripts/<name>_wrapper.sh << 'EOF'
#!/bin/bash
exec python3 <hermes-root>/profiles/<profile>/scripts/<original>.py --<flag>
EOF
chmod +x <hermes-root>/profiles/<profile>/scripts/<name>_wrapper.sh

# Symlink to shared scripts dir (required by hermes cron edit --script)
ln -sf <hermes-root>/profiles/<profile>/scripts/<name>_wrapper.sh \
      <hermes-root>/scripts/<name>_wrapper.sh

# Update cron job
hermes cron edit <job_id> --script <name>_wrapper.sh
```

**Verified example (2026-06-20):** `thread-renamer:active` had `script: 'active_thread_renamer.py --active'`. Created `thread_renamer_active.sh` wrapper with `--active` baked in. Same for `thread-renamer:backfill` with `--backfill`.

## Distinction from Other Patterns

| Pattern | Error | Root Cause |
|---------|-------|------------|
| `oc_cron_dead_script_ref` | "script not found" | File doesn't exist at literal path |
| `oc_cron_script_path_security_block` | "Blocked: script path resolves outside..." | File exists but path rejected by security |
| `oc_cron_no_agent_script_args` | "Script not found" (path includes args) | `no_agent` executor treats script field as literal path, arguments embedded in filename |

## Prevention

When creating `no_agent` cron jobs that need script arguments:
- Always use a wrapper script or shell function
- Never put arguments in the `script` field
- The `script` field should be a single filename with no spaces
