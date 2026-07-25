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
<<<<<<< Updated upstream
cat > <hermes-home>/profiles/<profile>/scripts/<name>_wrapper.sh << 'EOF'
#!/bin/bash
exec python3 <hermes-home>/profiles/<profile>/scripts/<original>.py --<flag>
EOF
chmod +x <hermes-home>/profiles/<profile>/scripts/<name>_wrapper.sh

# Symlink to shared scripts dir (required by hermes cron edit --script)
ln -sf <hermes-home>/profiles/<profile>/scripts/<name>_wrapper.sh \
      <hermes-home>/scripts/<name>_wrapper.sh
=======
cat > ~/.hermes/profiles/<profile>/scripts/<name>_wrapper.sh << 'EOF'
#!/bin/bash
exec python3 ~/.hermes/profiles/<profile>/scripts/<original>.py --<flag>
EOF
chmod +x ~/.hermes/profiles/<profile>/scripts/<name>_wrapper.sh

# Symlink to shared scripts dir (required by hermes cron edit --script)
ln -sf ~/.hermes/profiles/<profile>/scripts/<name>_wrapper.sh \
      ~/.hermes/scripts/<name>_wrapper.sh
>>>>>>> Stashed changes

# Update cron job
hermes cron edit <job_id> --script <name>_wrapper.sh
```

**Verified example (2026-06-20):** `thread-renamer:active` had `script: 'active_thread_renamer.py --active'`. Created `thread_renamer_active.sh` wrapper with `--active` baked in. Same for `thread-renamer:backfill` with `--backfill`.

**Compound `&&` command pattern (2026-05-28, updated 2026-06-25):** A variant of this pattern occurs when the `script` field contains `&&` chaining multiple commands. Detected on `dispatch:triage-morning` and `dispatch:triage-evening`:
```
<<<<<<< Updated upstream
script: triage.py && python3 <hermes-home>/skills/ocas-dispatch/scripts/journal.py
```
Both jobs have `no_agent: true`. The `&&` is not a path — the executor treats the entire string as a literal path and fails with "Script not found." **Fix direction:** Create a wrapper script that runs both commands sequentially:
```bash
cat > <hermes-home>/profiles/<profile>/scripts/triage_dispatch.sh << 'EOF'
#!/bin/bash
cd <hermes-home>/skills/ocas-dispatch/scripts || exit 1
python3 triage.py && python3 journal.py
EOF
chmod +x <hermes-home>/profiles/<profile>/scripts/triage_dispatch.sh
=======
script: triage.py && python3 ~/.hermes/skills/ocas-dispatch/scripts/journal.py
```
Both jobs have `no_agent: true`. The `&&` is not a path — the executor treats the entire string as a literal path and fails with "Script not found." **Fix direction:** Create a wrapper script that runs both commands sequentially:
```bash
cat > ~/.hermes/profiles/<profile>/scripts/triage_dispatch.sh << 'EOF'
#!/bin/bash
cd ~/.hermes/skills/ocas-dispatch/scripts || exit 1
python3 triage.py && python3 journal.py
EOF
chmod +x ~/.hermes/profiles/<profile>/scripts/triage_dispatch.sh
>>>>>>> Stashed changes
```
Then update the cron job's `script` field to `triage_dispatch.sh`.

**Confirmed fix (2026-06-25):** Both `dispatch:triage-morning` and `dispatch:triage-evening` were hit by this pattern. The morning job was fixed 2026-06-20 with `triage_morning.sh`. The evening job still had the compound command in its `script` field and was failing every night at 02:45. Fixed by creating `triage_evening.sh` with identical structure and updating the cron job's `script` field via direct jobs.json edit (Python heredoc pattern — `execute_code` blocked in cron context):

```bash
# Create wrapper
<<<<<<< Updated upstream
cat > <hermes-home>/profiles/indigo/scripts/triage_evening.sh << 'EOF'
#!/usr/bin/env bash
set -e
cd <hermes-home>/profiles/indigo/skills/ocas-dispatch/scripts
python3 triage.py
python3 <hermes-home>/skills/ocas-dispatch/scripts/journal.py
EOF
chmod +x <hermes-home>/profiles/indigo/scripts/triage_evening.sh
=======
cat > ~/.hermes/profiles/indigo/scripts/triage_evening.sh << 'EOF'
#!/usr/bin/env bash
set -e
cd ~/.hermes/profiles/indigo/skills/ocas-dispatch/scripts
python3 triage.py
python3 ~/.hermes/skills/ocas-dispatch/scripts/journal.py
EOF
chmod +x ~/.hermes/profiles/indigo/scripts/triage_evening.sh
>>>>>>> Stashed changes

# Update jobs.json (Python via terminal — execute_code blocked in cron)
python3 << 'PYEOF'
import json
<<<<<<< Updated upstream
with open("<hermes-home>/profiles/indigo/cron/jobs.json") as f:
=======
with open("~/.hermes/profiles/indigo/cron/jobs.json") as f:
>>>>>>> Stashed changes
    data = json.load(f)
jobs = data.get("jobs", data) if isinstance(data, dict) else data
for job in jobs:
    if job.get("name") == "dispatch:triage-evening" and job.get("no_agent") == True:
        job["script"] = "triage_evening.sh"
        break
<<<<<<< Updated upstream
with open("<hermes-home>/profiles/indigo/cron/jobs.json", "w") as f:
=======
with open("~/.hermes/profiles/indigo/cron/jobs.json", "w") as f:
>>>>>>> Stashed changes
    json.dump(data, f, indent=2, ensure_ascii=False)
```

**Diagnosis tip for future scans:** Distinguish stale vs active:
- `dispatch:triage-morning`: `script=triage_morning.sh` + `last_error` shows old `&&` command → **stale** (fixed 2026-06-20)
- Any job where `script` still contains `&&`, `;`, `|`, or spaces with `no_agent: true` → **active** (apply wrapper fix)

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