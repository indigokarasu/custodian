# Duplicate Gateway Systemd Service Pattern

## Fingerprint: `oc_gateway_duplicate_systemd_service`

## Description

When two Hermes gateway systemd services exist — one for the default profile (`hermes-gateway.service`) and one for the indigo profile (`hermes-gateway-indigo.service`) — the default profile service enters a crash loop because the indigo gateway already holds the PID file and port.

## Symptoms

- "Another gateway instance is already running (PID X)" errors at very high frequency (1000+/day)
- `systemctl --user status hermes-gateway.service` shows `activating (auto-restart) (Result: exit-code)`
- `systemctl --user status hermes-gateway-indigo.service` shows `active (running)`
- The default service has `Restart=always`, `RestartSec=5`, `StartLimitIntervalSec=0` (unlimited restarts)

## Diagnostic Procedure

```bash
# List all hermes gateway services
systemctl --user list-units --type=service | grep hermes-gateway

# Check the default profile gateway (the problematic one)
systemctl --user status hermes-gateway.service

# Check the indigo profile gateway (the healthy one)
systemctl --user status hermes-gateway-indigo.service

# Verify the indigo gateway is actually running
<<<<<<< Updated upstream
ps -p $(cat <hermes-home>/profiles/indigo/gateway.pid 2>/dev/null || echo 0) -o pid,cmd

# Count collision errors
grep -c "Another gateway instance" <hermes-home>/profiles/indigo/logs/errors.log
=======
ps -p $(cat ~/.hermes/profiles/indigo/gateway.pid 2>/dev/null || echo 0) -o pid,cmd

# Count collision errors
grep -c "Another gateway instance" ~/.hermes/profiles/indigo/logs/errors.log
>>>>>>> Stashed changes
```

## Root Cause

The `hermes-gateway.service` unit (default profile, no `--profile` flag) has:
<<<<<<< Updated upstream
- `HERMES_HOME=<hermes-home>` (not indigo)
=======
- `HERMES_HOME=~/.hermes` (not indigo)
>>>>>>> Stashed changes
- `ExecStart=...hermes_cli.main gateway run` (no `--profile indigo`)
- `Restart=always`, `RestartSec=5`

The `hermes-gateway-indigo.service` unit has:
<<<<<<< Updated upstream
- `HERMES_HOME=<hermes-home>/profiles/indigo`
=======
- `HERMES_HOME=~/.hermes/profiles/indigo`
>>>>>>> Stashed changes
- `ExecStart=...hermes_cli.main --profile indigo gateway run`

Both try to bind the same gateway port. The indigo gateway starts first (or holds the PID file), so the default gateway fails immediately and systemd restarts it every 5 seconds.

## Fix

```bash
# Stop and disable the default profile gateway
systemctl --user stop hermes-gateway.service
systemctl --user disable hermes-gateway.service

# Verify only the indigo gateway remains
systemctl --user list-units --type=service | grep hermes-gateway
# Should show only hermes-gateway-indigo.service: active (running)
```

**Cannot auto-fix in cron**: Disabling a systemd service requires user confirmation per Custodian safety envelope (Tier 2).

## First Seen

2026-06-13: 2,621 collision errors in a single day. The default profile gateway service was likely created during initial Hermes setup and never disabled after the indigo profile became the primary profile.