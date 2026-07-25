# SOUL.md Truncation Pattern

## When to Read

During deep scan Step 2 (Collect) or Step 3 (Fingerprint + Classify) — when `errors.log` shows `Context file SOUL.md TRUNCATED: N chars exceeds limit of M` warnings, OR when cron jobs run but produce subtly wrong behavior due to truncated soul content.

## Pattern: `oc_soul_md_truncation`

### SO

The `SOUL.md` file at `{profile_root}/SOUL.md` is loaded into the system prompt as a "context file." If its character count exceeds `config.yaml`'s `context_file_max_chars` value, it is silently truncated. This produces a WARNING-level log message on every single cron job run:

```
⚠️  Context file SOUL.md TRUNCATED: 10002 chars exceeds limit of 10000 — trim the file, pin a larger context_file_max_chars, or use a larger-context model!
```

### Why It Matters

Unlike a job failure (which sets `last_status=error`), truncation is a **silent degradation**. The job runs successfully (`last_status=ok`) but the agent operates with an incomplete understanding of its persona, rules, and constraints. The tail of SOUL.md — typically the most recently-added lessons and operating notes — is exactly what gets cut.

This is especially dangerous because:
1. It affects EVERY cron job simultaneously
2. It does not appear in `jobs.json` error counts
3. The warning is logged at WARNING level, not ERROR — it won't trigger failure heuristics
4. SOUL.md tends to grow over time, so the threshold is crossed gradually and silently

### Detection

**Primary signal**: Repeated "Context file SOUL.md TRUNCATED" messages in `errors.log` (one per cron job run).

```bash
# Count occurrences in recent errors.log
grep -c "SOUL.md TRUNCATED" <hermes-home>/profiles/indigo/logs/errors.log
```

**Secondary signal**: Check actual size vs config:
```bash
wc -c <hermes-home>/profiles/indigo/SOUL.md
grep "context_file_max_chars" <hermes-home>/profiles/indigo/config.yaml
```

If `wc -c` exceeds the config value, truncation is active.

### Classification: Tier 1 Auto-Fix

This is a **Tier 1 auto-fix** — apply immediately during the repair pass.

**Fix option A (preferred): Increase `context_file_max_chars`**
```python
import yaml
path = '<hermes-home>/profiles/indigo/config.yaml'
with open(path) as f:
    cfg = yaml.safe_load(f)
cfg['context_file_max_chars'] = <new_value>  # e.g. 12000 for ~10% headroom
with open(path, 'w') as f:
    yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)
```

Set the new value to `ceil(current_soul_size * 1.2)` for 20% headroom.

**Fix option B (alternative): Trim SOUL.md**
If the user prefers to keep the current limit, trim SOUL.md to 90% of the limit. This is only a temporary fix — the file will grow again.

### Verification

After fix:
```bash
# 1. Verify config value
grep context_file_max_chars <hermes-home>/profiles/indigo/config.yaml

# 2. Verify no new truncation warnings in errors.log
# (wait for next cron job to run, then check)
tail -5 <hermes-home>/profiles/indigo/logs/errors.log
```

### Pitfall: SOUL.md grows over time

SOUL.md accumulates lessons, pitfalls, and operating notes. A fix that exactly matches the current size will fail again within days/weeks. Always add headroom (20% or set to a round number like 12000 or 15000).

### See Also

- `references/config-null-key-verification.md` — PyYAML verification patterns for config changes
- `references/fix-safety.md` — Tier 1 auto-fix safety envelope
- `references/fix-effectiveness-schema-contamination.md` — fix logging patterns

---

Confirmed: 2026-06-26 — SOUL.md at 10,074 chars with limit of 10,000. Produced truncation warnings on every cron job run. Fixed by increasing to 12,000.