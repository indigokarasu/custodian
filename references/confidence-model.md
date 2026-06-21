# Confidence Model

Custodian tracks fix outcomes per fingerprint in `fix_effectiveness.jsonl`.

```
confidence_score = sample_confidence × success_rate
where:
  sample_confidence = min(1.0, attempts / 5)   # 5+ attempts = full confidence
  success_rate = successes / (successes + failures)
```

Score range: 0.0 (no confidence) to 1.0 (fully confident the fix works).

## RCA-Enhanced Confidence

The base confidence model only tracks *whether* a fix worked. It doesn't track *why* it failed or *whether the failure was the same cause each time*. The RCA layer adds three signals:

### Schedule-Adjusted Fix Stickiness

Raw calendar-day stickiness is misleading when jobs run on different schedules. A fix for a job that fires every 6 hours breaking again after 2 days survived ~8 cycles — that's meaningful. A fix for a weekly job breaking after 2 days never completed one cycle.

```
cycles_survived = days_since_fix / avg_schedule_interval_days
schedule_adjusted_stickiness = cycles_survived / (1 + recurrence_count)
```

Where `avg_schedule_interval_days` is derived from the cron schedule (`0 */6 * * *` → 0.25 days) or observed run interval from jobs.json.

| `schedule_adjusted_stickiness` | Meaning | Signal |
|-------------------------------|---------|--------|
| `> 2.0` | Fix survived 2+ full schedule cycles per recurrence. Root cause likely addressed; new occurrences are probably Pattern D (transient/new trigger). | High confidence fix works |
| `0.5 – 2.0` | Fix survived at least one full cycle but recurred within a reasonable window. Inconclusive — could be Pattern B or Pattern D. | No confidence adjustment |
| `< 0.5` | Fix broke before completing one full schedule cycle per recurrence. Fix is not addressing the root cause. | Pattern B — escalate |

### Sub-Fingerprint Discrimination

When a fingerprint has been decomposed into sub-fingerprints (Pattern C), each sub-fingerprint gets its own `fix_effectiveness.jsonl` entry. The parent fingerprint's confidence is the **minimum** of its children's confidence scores (weakest-link model). A fix that works for `oc_http_429:openrouter_weekly` but fails for `oc_http_429:manifest_build` should not boost confidence for the latter.

### Recurrence Pattern Weighting

Not all recurrences are equal. Weight the `success_rate` by recurrence pattern:

| Pattern | Weight | Effect on confidence |
|---------|--------|---------------------|
| A (fix never applied) | 1.0 | Normal — fix hasn't had a chance |
| B (fix didn't hold) | 0.0 | Confidence → 0 regardless of `success_rate`. Fix is addressing the wrong cause. |
| C (different cause) | 0.5 | Halve the confidence — the fix may work for one sub-cause but not others. |
| D (transient) | 0.0 | Exclude from confidence calculation entirely. Transient errors don't reflect fix quality. |
| E (cascade) | 0.3 | Low confidence — fixing the downstream symptom won't help; the upstream trigger must be addressed. |

**Effective confidence**: `confidence_score × pattern_weight`

## Auto Tier Promotion/Demotion

After each scan, the confidence model is consulted before classifying new occurrences:

| Condition | Action |
|---|---|
| `attempts >= 3` AND `success_rate >= 0.85` AND `schedule_adjusted_stickiness > 2.0` | **Auto-promote to Tier 1** — fix works AND holds across schedule cycles |
| `attempts >= 3` AND `success_rate >= 0.85` AND `schedule_adjusted_stickiness < 0.5` | **Do NOT promote** — fix works short-term but doesn't survive one full cycle. Pattern B. Escalate. |
| `attempts >= 2` AND `success_rate < 0.5` | **Auto-demote to Tier 3** — stop trying, escalate |
| `recurrence_after_fix / successes > 0.5` AND `schedule_adjusted_stickiness < 0.5` | **Auto-demote to Tier 3** — fix isn't sticking (Pattern B confirmed) |
| `pattern == "B"` AND `recurrence_count >= 2` | **Auto-demote to Tier 3** — root cause not addressed by current fix |
| `attempts < 2` | **No change** — insufficient data |
| Confidence score >= 0.6 AND `schedule_adjusted_stickiness >= 0.5` | **Prefer auto-fix** over escalation |
| Confidence score >= 0.6 AND `schedule_adjusted_stickiness < 0.5` | **Do NOT auto-fix** — escalate with RCA evidence |
| **Fix-loop**: same fix applied >= 3 times, `schedule_adjusted_stickiness < 0.5` for all | **Auto-demote to Tier 3 + create RCA record** — stop the loop |

## Confidence-Gated Escalation

Before escalating a Tier 3 issue, check the confidence model:

1. If the fingerprint has `confidence_score >= 0.6` AND `recommended_tier == 1` AND `schedule_adjusted_stickiness >= 0.5`, **reclassify as Tier 1 and auto-fix** instead of escalating.
2. If the fingerprint has `confidence_score < 0.2` AND `attempts >= 3`, **escalate with high priority** — the fix is known to fail.
3. If the fingerprint has `schedule_adjusted_stickiness < 0.5` AND `recurrence_count >= 2`, **escalate with RCA evidence** — include the full occurrence chain and root cause hypothesis.
4. If `attempts == 0` (never seen before), default to Tier 3 as before.

## Initialization

On first run (or when `fix_effectiveness.jsonl` is empty/missing), backfill from `fixes.jsonl`:
```python
from collections import defaultdict
import json

effectiveness = defaultdict(lambda: {"attempts": 0, "successes": 0, "failures": 0})
for fix in fixes:
    fp = fix.get('fingerprint', 'unknown')
    e = effectiveness[fp]
    e["attempts"] += 1
    if fix.get('outcome') in ('fix_applied', 'applied', 'success', 'verified'):
        e["successes"] += 1
    elif fix.get('outcome') in ('fix_attempted_failed', 'failed'):
        e["failures"] += 1
# Compute confidence_score and recommended_tier for each
```

## OKR Impact

The confidence model directly improves two OKRs:
- `fix_success_rate`: by not retrying known-bad fixes
- `escalations`: by auto-fixing issues that have proven fix patterns

The RCA-enhanced model adds:
- `mean_time_to_fix_ms`: by reducing fix-loop iterations
- `escalation_precision`: by escalating with root cause evidence instead of raw error counts
