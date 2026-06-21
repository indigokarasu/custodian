# Escalation Runner Reference — 2026-06-06

## Open Issues (as of 2026-06-06 08:00 PT)

### Tier 2 — Requires Planning/Investigation

1. **state.db Oversized + Disk Pressure** (`oc_state_db_oversized`)
   - state.db: 14GB (expected <1GB)
   - Disk: 93% full (7.5GB free)
   - VACUUM requires ~28GB temp — not viable
   - Suggested: Free disk space first, then VACUUM or message prune
   - Priority: HIGH — growing problem

2. **Elephas Skill Name Mismatch** (`oc_cron_skill_name_mismatch`)
   - Jobs reference `ocas-elephas`, skill dir is `elephas`
   - Scheduler logs: "Skill 'ocas-elephas' not found"
   - Skill data dirs exist at commons/data/ocas-elephas/
   - Suggested: Verify naming convention, update jobs or rename dir
   - Priority: MEDIUM — affects elephas:ingest scheduling

3. **Gateway systemd Tracking Lost** (`oc_gateway_health_endpoint_unreachable`)
   - --replace mode since Jun 5, systemd reports inactive
   - Port 8080 down, 8081/9119 healthy
   - Suggested: `hermes gateway install --force` when convenient
   - Priority: LOW — system operational

## Transient Issues (No Action Needed)
- 34 jobs with HTTP 429 (consecutive_failures=0, self-resolving)
- 2 jobs with HTTP 401 upstream errors
- next-ai-drawio MCP failures (non-fatal)
- Chronicle context engine not found (graceful fallback)
- Kanban dispatcher stuck (known pattern)
