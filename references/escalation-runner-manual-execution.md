# Escalation Runner — Manual Execution Pattern (2026-07-06)

## Context

Executed `custodian.escalation-runner` manually (not via cron) to resolve 3 escalated issues:
1. `oc_openrouter_402_credits_exhausted_20260706` — 71 jobs paused
2. `oc_state_db_oversized_20260706` — state.db pruned + VACUUM
3. `oc_journal_gap_20260706` — verified custodian running

## Execution Steps (Manual)

### 1. Discover All Issues

```bash
# Check BOTH profile and commons issues.jsonl
<<<<<<< Updated upstream
find <hermes-home> -name "issues.jsonl" -exec cat {} \;
=======
find ~/.hermes -name "issues.jsonl" -exec cat {} \;
>>>>>>> Stashed changes
```

### 2. Classify Each Issue (Four-Bucket Model)

| Issue | Bucket | Action |
|-------|--------|--------|
| OpenRouter 402 | User-gated | Pause affected jobs, update issues.jsonl |
| state.db oversized | Actionable | Prune messages, VACUUM, update issues.jsonl |
| Journal gap | Already-resolved | Verify custodian running, update issues.jsonl |

### 3. Execute Fixes

**For User-gated (OpenRouter 402):**
```python
# Pause all jobs with 402 errors
# Edit jobs.json directly: enabled=false, state='paused'
# Record jobs_paused in issues.jsonl
```

**For Actionable (state.db):**
```bash
# 1. Remove stale snapshot (14GB)
# 2. Checkpoint WAL
# 3. Batched DELETE by timestamp
# 4. VACUUM
# 5. Final WAL checkpoint
```

**For Already-resolved (journal gap):**
```bash
# Verify custodian journals exist for today
<<<<<<< Updated upstream
ls <hermes-home>/profiles/indigo/commons/journals/ocas-custodian/2026-07-06/
=======
ls ~/.hermes/profiles/indigo/commons/journals/ocas-custodian/2026-07-06/
>>>>>>> Stashed changes
```

### 4. Update Issues (Both Locations)

```python
# Update profile AND commons issues.jsonl
# Set status, escalation_needed=false, resolved_at, resolution
```

### 5. Verify

```bash
# Check custodian jobs running
# Check state.db size
# Check disk usage
# Check journals present
```

## Key Differences from Cron Mode

| Aspect | Cron Mode | Manual Execution |
|--------|-----------|------------------|
| `execute_code` | Blocked | Works |
| `read_file` on JSONL | Blocked | Works |
| `write_file` | Works | Works |
| Pipe-to-python | Blocked | Works |
| Journal write | Python + json.dump | Python + json.dump |

## Lessons for Cron Mode

1. **Always use `terminal()` with `/tmp/` scripts** for cron execution
2. **Verify both issues.jsonl locations** — they can diverge
3. **Write journals via Python** (not heredoc) for dynamic timestamps
4. **Update both profile and commons** — escalation runner checks both
5. **Clear `escalation_needed` flag** on resolution — prevents re-escalation

## Verification Checklist

- [ ] All escalated issues have `status` in {resolved, user_gated}
- [ ] All escalated issues have `escalation_needed: false`
- [ ] All escalated issues have `resolved_at` timestamp
- [ ] All escalated issues have `resolution` text
- [ ] User-gated issues have `jobs_paused` list
- [ ] Custodian jobs show `last_status: ok` (or transient error)
- [ ] state.db < 1GB OR disk < 80%
- [ ] Today's custodian journals exist

## Related

- `escalation-runner-clean-verdict-pattern.md` — four-bucket decision tree
- `escalation-runner-pause-affected-jobs-pattern.md` — pause workflow
- `state-db-pruning-execution-procedure.md` — state.db fix details
- `custodian-self-failure-fallback-model.md` — root cause of journal gap