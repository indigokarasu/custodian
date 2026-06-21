# Gateway Instance Collision Noise Pattern

**Fingerprint:** `oc_gateway_instance_collision_noise`
**Tier:** 2 (Surface Only — Non-Fatal)

## Description

High-frequency "Another gateway instance is already running (PID X, HERMES_HOME=...)" errors in `errors.log`. The count can reach 1000+ per day.

## Root Cause

The cron scheduler's internal gateway process checker attempts to start a new gateway instance for each job run. When the gateway is already running (as it should be), this error is logged. This is expected behavior in the current Hermes architecture — the scheduler doesn't gracefully detect the existing instance.

## Diagnostic

1. Verify the gateway is actually running: `ps -p <PID> -o pid,etime,cmd`
2. Check gateway uptime: `ps -o etime= -p <PID>`
3. If the PID in the error matches a running gateway process → noise, not a failure
4. Count occurrences: `grep -c "Another gateway instance" errors.log`

## Classification

- **Gateway running, jobs executing normally** → `oc_gateway_instance_collision_noise` (Tier 2, surface only)
- **Gateway NOT running, PID stale** → `oc_gateway_process_down` (Tier 4, escalate)
- **Gateway running but health endpoint down** → `oc_gateway_health_endpoint_unreachable` (Tier 2)

## Action

Surface in report with occurrence count. No auto-fix. The noise is cosmetic but can mask real errors in the log.

## Verified Instances

| Date | Count | Gateway PID | Status |
|------|-------|-------------|--------|
| 2026-06-13 | 1489 | 688863 | Running (uptime >24h) |