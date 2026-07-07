# State DB Size Threshold — Contextual Interpretation

**Confirmed 2026-07-06**

## The Pattern

`state.db` commonly grows to 5-10GB in production. The SKILL.md mentions flagging as `oc_state_db_oversized` (Tier 2) when >1GB **AND** disk >80%. At lower disk usage, 5-10GB is acceptable operational cost.

## Real-World Instance

**2026-07-06**: 
- `state.db` = 12GB
- Disk usage = 66% (33GB free of 96GB)
- No performance issues observed
- Gateway logs show no SQLite contention errors

## Correct Classification

| Disk Usage | state.db Size | Classification |
|------------|---------------|----------------|
| >80% | >1GB | `oc_state_db_oversized` (Tier 2) — VACUUM or message pruning recommended |
| <80% | 5-10GB | **Not an issue** — acceptable operational cost, do not escalate |
| <80% | >10GB | Monitor — consider proactive VACUUM during low-traffic window |

## Why This Matters

Custodian scans flagged `state.db` at 12GB with 66% disk as "oversized" and created an escalated issue. This was a **false positive** — the contextual threshold (disk >80%) was not met. The alert created noise and wasted an escalation cycle.

## VACUUM Feasibility Note

VACUUM requires ~2x the DB size in temp space. At 12GB DB, that's 24GB free. With 33GB free, VACUUM is technically feasible but NOT recommended at 66% disk usage because:
1. It provides marginal benefit at this disk pressure level
2. It adds I/O load during execution
3. The DB will regrow to steady-state size within days

**Recommendation**: Only act on `oc_state_db_oversized` when disk >80%. Otherwise classify as "stable, no action."