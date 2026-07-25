# No-Agent Missing Dependency Install Procedure

When a `no_agent: true` cron job fails with `ModuleNotFoundError` and the package is confirmed missing from the hermes-agent venv:

1. Locate the active venv Python: `find /root -path "*/hermes-agent/.venv/bin/python3" 2>/dev/null` or check `<projects-root>/hermes-agent/.venv/bin/python3` (editable install path)
2. Install: `<projects-root>/hermes-agent/.venv/bin/pip install <package>`
3. Verify: `<projects-root>/hermes-agent/.venv/bin/python3 -c "from <module> import <thing>; print('OK')"`
4. Run the script with the venv Python to confirm: `<projects-root>/hermes-agent/.venv/bin/python3 <script_path> --dry-run`

**Pitfall — wrong venv path:** The hermes-agent editable install may be at `<projects-root>/hermes-agent/` (source checkout) rather than under `<hermes-home>/`. Verify which Python actually runs cron scripts by checking the shebang line of existing working scripts or by checking `sys.path` in a cron job output.