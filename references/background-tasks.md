# Background tasks

| Job | Mechanism | Schedule | Command |
|---|---|---|---|
| `custodian:light` | heartbeat | every heartbeat cycle | `custodian.scan.light` |
| `custodian:deep` | cron | optimized 6h (initial: `0 1,7,13,19 * * *` PT) | `custodian.scan.deep` |
| `custodian:escalation-runner` | cron | `*/30 9-17 * * 1-5` (weekday mornings) | Process escalated Tier 3+ issues |
| `custodian:update` | cron | `0 0 * * *` (midnight daily) | Self-update from GitHub source |

Registration during `custodian.init` (idempotent -- check the platform scheduling registry first).

# Storage Layout

```
{agent_root}/commons/data/ocas-custodian/
  config.json                  -- ConfigBase + scan_window_minutes, optimization settings
  intents.jsonl                -- durable intent queue (recovery contract)
  evidence.jsonl               -- execution evidence log (recovery contract)
  issues.jsonl                 -- issue lifecycle records
  fixes.jsonl                  -- fix attempt records with pre/post state
  cleanup_events.jsonl         -- post-fix cleanup records
  fix_effectiveness.jsonl      -- per-fingerprint outcome tracking
  learned_issues.jsonl         -- runtime-learned fingerprints from web search
  skill_conformance.jsonl      -- per-skill background task conformance
  activity_model.json          -- rolling 14-day activity pattern (rebuilt each deep scan)
  deferred_fixes.jsonl         -- fixes queued for next quiet window
  schedule_state.json          -- current/target schedule, optimization history
  decisions.jsonl              -- DecisionRecord entries
  proposals/                   -- InsightProposal files for Vesper (cooperative read)
    {proposal_id}.json
  reports/
    YYYY-MM-DD-HHMM.md         -- deep scan summaries (7-day retention)
{agent_root}/commons/journals/ocas-custodian/
  YYYY-MM-DD/{run_id}.json
```
