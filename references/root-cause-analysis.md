# Root Cause Analysis (RCA) Framework

Custodian currently treats recurring errors as independent occurrences. When `oc_cron_dead_script_ref` fires for the 5th time on the same job, it applies the same "update script path" fix without asking *why* the path keeps breaking. This document defines the methodology to break that cycle.

## The Problem: Fixation Loops

A fixation loop occurs when:
1. Error E is detected
2. Fix F is applied
3. Error E recurs (same fingerprint, same or different root cause)
4. Fix F is applied again
5. Repeat until `confidence_score` drops below threshold, then escalate

The problem: steps 1-4 can repeat dozens of times before escalation. Each cycle wastes a scan, a fix attempt, and a verify job. Meanwhile the *actual* root cause (e.g., `HERMES_HOME` path resolution in cron cwd, or a gateway update that nukes symlinks) is never addressed.

## Recurrence Pattern Taxonomy

Before applying any fix, classify the recurrence pattern:

### Pattern A — Same Root Cause, Same Fix Never Applied
The fix was never actually applied (fix record shows `outcome: fix_attempted_failed` or no fix record exists).
**Action**: Apply the fix. This is the normal case.

### Pattern B — Same Root Cause, Fix Applied But Didn't Hold
The fix was applied (`outcome: fix_applied`) but the error recurred within a time window that suggests the root cause was never addressed. Uses schedule-adjusted stickiness (see below).
**Action**: Do NOT re-apply. Escalate to Tier 3 with recurrence context. The underlying cause needs investigation.

### Pattern C — Different Root Cause, Same Symptom
Same fingerprint but different stack trace, different error message details, or different circumstances. Looks the same at the pattern level but the actual cause shifted.
**Action**: Decompose into sub-fingerprints. Treat as a new root cause investigation.

### Pattern D — Transient Error Masquerading as Persistent
Error occurs sporadically with long gaps between occurrences. The fix _did_ hold — the new occurrence is a genuinely new trigger (new provider outage, new disk pressure, etc.), not the same root cause resurfacing.
**Action**: Do not escalate. Mark as `transient_dormant` in the RCA record. Reset on next occurrence.

### Pattern E — Cascade Trigger
Error only occurs when another specific error/job fires first (temporal correlation in logs). The root cause is the upstream trigger, not the downstream symptom.
**Action**: Identify the upstream trigger via log co-occurrence analysis. Fix upstream, not downstream.

## Schedule-Adjusted Fix Stickiness

The raw calendar-day metric is a starting point but misleading:
```
fix_stickiness_raw = days_since_fix / (1 + recurrence_count)
```

Calendar days don't account for job frequency. A fix for a job that runs every 6 hours that breaks again after 2 days survived ~8 schedule cycles — that's meaningful. A fix for a weekly job that breaks after 2 days never even completed one cycle.

**Schedule-adjusted stickiness** normalizes by the job's expected run frequency:

```
cycles_survived = days_since_fix / avg_schedule_interval_days
schedule_adjusted_stickiness = cycles_survived / (1 + recurrence_count)
```

Where `avg_schedule_interval_days` is derived from the cron schedule or the job's observed run interval in jobs.json.

Interpretation:

| `schedule_adjusted_stickiness` | Meaning | Signal |
|-------------------------------|---------|--------|
| `> 2.0` | Fix survived 2+ full schedule cycles per recurrence. The fix likely addressed the root cause; new occurrences are probably Pattern D (transient/new trigger). | High confidence fix works |
| `0.5 – 2.0` | Fix survived at least one full cycle but recurred within a reasonable window. Inconclusive — could be Pattern B or Pattern D. Monitor. | No confidence adjustment |
| `< 0.5` | Fix broke before completing even one full schedule cycle per recurrence. The fix is not addressing the root cause. | Pattern B — escalate |

**Example**: A job runs every 4 hours (`avg_schedule_interval = 0.167 days`). Fix applied, error recurs after 18 hours (4.5 cycles), then again after another 30 hours.
- `cycles_survived = 18/0.167 / (1+1) = 54` → `schedule_adjusted_stickiness = 54` → well above 2.0. Pattern D territory — fix held, new triggers are independent.

**Example**: A job runs every 6 hours. Fix applied, error recurs after 4 hours (0.67 cycles).
- `cycles_survived = 4/0.167 / (1+1) = 12` → wait, that's wrong. Let me recalculate: `4 hours = 0.167 days`. `cycles_survived = 0.167/0.167 / (1+1) = 0.5`. Right at the boundary. One more recurrence at the same interval would push it below 0.5 → Pattern B.

When `schedule_adjusted_stickiness < 0.5` and `recurrence_count >= 2`, auto-escalate (Pattern B confirmed).

## Root Cause Drilldown Procedure

When a fingerprint hits `recurrence_count >= 2` with the same root cause (Pattern B), execute this drilldown before any further action:

### Step 1 — Gather the Occurrence Chain
Pull all occurrences of this fingerprint from the last 30 days. For each occurrence record:
- Timestamp
- Job ID / script path / config key involved
- Fix applied (if any)
- Fix outcome
- Time between occurrences (in schedule cycles, not just days)

### Step 2 — Identify the Variable
In the occurrence chain, find what *changes* between occurrences:
- **Path drift**: Script path, config path, or data dir changed between occurrences (check git log, file timestamps)
- **State mutation**: A value that was correct at fix-time is now wrong again (config overwritten, symlink removed, env var lost)
- **Dependency shift**: An external dependency changed (MCP server updated, package upgraded, OAuth token rotated)
- **Environmental**: Disk pressure, memory pressure, time-of-day correlation (cron clustering)

### Step 3 — Form the Root Cause Hypothesis
Based on the variable analysis, form a one-sentence root cause hypothesis:
- "Script path breaks because `HERMES_HOME` resolves differently in cron cwd vs agent cwd"
- "Config key reverts because gateway update overwrites config.yaml"
- "OAuth token expires because the refresh token has a 7-day TTL and no refresh job runs"

### Step 4 — Design a Root-Cause Fix
The fix must address the *why*, not the *what*:
- Path drift → Make the path resolution canonical (symlink, env var, or hardcoded absolute path)
- State mutation → Add a guard (post-merge hook, config validation cron, or source patch with re-application)
- Dependency shift → Pin the dependency or add a compatibility check
- Environmental → Add resource monitoring or stagger schedules

### Step 5 — Record the RCA
Write an RCA record to `rca.jsonl`. Include the hypothesis, evidence, chosen fix, and expected persistence.

## Sub-Fingerprint Decomposition

When the same fingerprint has multiple distinct root causes (Pattern C), decompose. This is the same principle as Finch's signal triage before fix — see `ocas-finch/references/signal-triage-before-fix.md` for the original methodology adapted here.

1. Take the base fingerprint (e.g., `oc_http_429_rate_limit`)
2. Extract discriminating features from the error context (provider name, error sub-type, job type)
3. Create sub-fingerprints: `oc_http_429_rate_limit:openrouter_weekly`, `oc_http_429_rate_limit:openrouter_concurrent`, `oc_http_429_rate_limit:manifest_build`
4. Track each sub-fingerprint independently in `fix_effectiveness.jsonl`
5. Apply sub-fingerprint-specific fixes

This is the same principle as Finch's signal triage before fix — don't treat a task with N affected jobs as having one root cause.

## Temporal Co-Occurrence Analysis (Pattern E)

To detect cascade triggers:

1. For each error occurrence, look at the 5-minute window in the gateway log before the error
2. Extract all other error fingerprints in that window
3. If fingerprint X is present in >60% of recurrence windows for fingerprint Y, X is a candidate trigger
4. Report the correlation in the RCA record: "Y recurs when X fires first (correlation: 0.7, N=10 occurrences)"

## Integration with Confidence Model

The existing confidence model's `recurrence_after_fix / successes > 0.5` threshold is a coarse proxy. Replace it with:

| Condition | Current Behavior | RCA-Enhanced Behavior |
|-----------|-----------------|----------------------|
| Fix succeeds, no recurrence | `successes++` | `successes++`, compute `schedule_adjusted_stickiness` |
| Fix succeeds, recurs within 1 full schedule cycle per recurrence | `failures++` | `failures++`, trigger Pattern B drilldown, create RCA record |
| Fix succeeds, recurs after 2+ cycles per recurrence | `failures++` | `successes++` (fix held), `recurrence_count` stays, log as Pattern D (new trigger) |
| Fix fails outright | `failures++` | `failures++`, log as Pattern A (fix never applied), increment `consecutive_fix_failures` |
| Same fix applied 3+ times, `schedule_adjusted_stickiness < 0.5` for all | Auto-demote to Tier 3 | Auto-demote + create RCA record with full occurrence chain + escalate with root cause hypothesis |

## What Custodian Must NOT Do

- Never apply a Tier 1 fix that has already been applied and verified if the same error has recurred since verification. Instead, escalate with the recurrence evidence.
- Never treat `fix_attempted_failed` (fix couldn't be applied) the same as `fix_applied` then recurred (fix failed to hold). The first is an execution problem, the second is a root cause problem.
- Never create an RCA record for an error that has only occurred once. RCA requires at least 2 occurrences to establish a pattern.
- Never modify `references/known_issues.json` directly. RCA findings that warrant changes to the known issues registry go through the escalation path (InsightProposal → Forge).
