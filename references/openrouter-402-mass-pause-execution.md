# OpenRouter 402 — Mass Job Pause Execution (2026-07-06)

## Summary

Paused **71 jobs** failing with OpenRouter HTTP 402 (credits exhausted) + **7 jobs** failing with model-not-found (owl-alpha). Total: **78 jobs paused**.

## Detection

```python
# Scan all jobs for 402 errors
with open('<hermes-home>/cron/jobs.json') as f:
    jobs = json.load(f).get('jobs', [])

for job in jobs:
    err = job.get('last_error') or ''
    if '402' in err and 'credits' in err.lower():
        # OpenRouter credits exhausted
        job['enabled'] = False
        job['state'] = 'paused'
    elif 'owl-alpha' in err or 'No endpoints found' in err:
        # Model not found (owl-alpha deprecated)
        job['enabled'] = False
        job['state'] = 'paused'
```

## Affected Jobs (71 OpenRouter 402)

### High-Impact OCAS Skills
- `custodian:update`, `custodian:light` ← **detection mechanism broken**
- `mentor:deep`, `mentor:update`, `mentor:light`
- `praxis:update`, `praxis:review`, `praxis:debrief`, `praxis:decay_check`
- `forge:update`, `forge:skill-audit`
- `dispatch:summary`
- `fellow:update`
- `finch:memory-guard-floor`

### Data/Ingestion Skills
- `bower:scan`, `bower:weekly-deep`
- `scout:update`, `scout:sources-refresh`
- `sift:update`
- `look:update`
- `reach:api-mine`

### Briefing/Delivery Skills
- `vesper:update`, `vesper:morning`, `vesper:evening`, `vesper:deliver-morning`, `vesper:deliver-evening`
- `sands:update`, `sands:morning-brief`, `sands:evening-brief`, `sands:chronicle-sync`
- `daily-user-context`, `daily-false-trigger-fix`

### Knowledge/Graph Skills
- `weave:update`, `weave:enrichability-recalc`, `weave:sync-contacts`, `weave:sync-google`, `weave:overnight-enrichment`
- `chronicle:daily-embed`, `Chronicle Embedding Enrichment`
- `bones:update`, `bones:research`, `bones:paper-trade`
- `rally:update`, `rally:healthcheck-pre-open`, `rally:healthcheck-pre-close`, `rally:weekend-research`

### Content/Creative Skills
- `haiku:content-review`, `haiku:haiku-post`, `haiku:engage`
- `art:studio`, `art:engagement`
- `dream-journal:morning`

### Taste/Preference Skills
- `taste:update`, `taste:scan`, `taste:sync-spotify`, `taste:historical-email`, `taste:historical-calendar`, `taste:daily-styx-enrichment`

### Autobio/Finch/Genie
- `ocas-autobio-observe`, `ocas-autobio-update`, `ocas-autobio-distill`, `ocas-autobio-grade`
- `ocas-finch:daily`, `ocas-finch:weekly`
- `genie:update`, `genie:disk-cleanup`, `genie:weekly-cleanup`
- `10khr-grind`

### Miscellaneous
- `styx:update`, `styx:enrich-new-transactions`
- `monitor:wikipedia-talk`
- `rainbow-grocery-receipts`
- `menu-monitor-weekly`
- `Backup Hermes Sessions to GitHub`
- `hermes-dojo-auto`

## Affected Jobs (7 Model Not Found — owl-alpha)

- `rally:research`
- `vesper:morning`
- `Executive Job Search — Mon/Wed/Fri`
- `Job Search Feedback Monitor`
- `genie:update`
- `soul:sync`
- `EHCS Monthly Refill Form`

## Jobs JSON Update

```python
import json

with open('<hermes-home>/cron/jobs.json') as f:
    data = json.load(f)

paused_402 = 0
paused_model_not_found = 0

for job in data.get('jobs', []):
    err = job.get('last_error') or ''
    if '402' in err and 'credits' in err.lower():
        job['enabled'] = False
        job['state'] = 'paused'
        paused_402 += 1
    elif 'owl-alpha' in err or 'No endpoints found' in err:
        if job.get('enabled', True):
            job['enabled'] = False
            job['state'] = 'paused'
            paused_model_not_found += 1

with open('<hermes-home>/cron/jobs.json', 'w') as f:
    json.dump(data, f, indent=2)

print(f"Paused {paused_402} for 402, {paused_model_not_found} for model not found")
```

## Issues.jsonl Update

```python
# Update BOTH profile and commons issues.jsonl
for issue in issues:
    if 'openrouter_402' in issue.get('issue_id', ''):
        issue['status'] = 'user_gated'
        issue['escalation_needed'] = False
        issue['resolved_at'] = datetime.now(timezone.utc).isoformat()
        issue['resolution'] = 'Paused 71 jobs failing with OpenRouter 402 credits exhausted. User must add credits to OpenRouter account to resume.'
        issue['jobs_paused'] = [...]  # list of 71 job names
```

## Resumption Procedure

When user adds OpenRouter credits:

```bash
# Re-enable all paused jobs
# Edit jobs.json: enabled=true, state='scheduled' for each paused job
# Or use: hermes cron resume <job_id> for each
```

**Do NOT resume until credits are verified** — jobs will immediately fail again and inflate error counts.

## Key Lesson

**Single provider failure cascades to ALL null-provider jobs.** The `fallback_model` with exhausted credits affected every job using `provider: null` (71 jobs). This is a systemic risk — consider per-job explicit providers or a local fallback model for critical monitoring jobs like `custodian:light`.