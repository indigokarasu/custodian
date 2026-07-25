# Escalation Runner: Journal Path Discovery & Silent-Run Schema

## Problem

Escalation runner cron jobs need to:
1. Find the **latest** esc-run journal to optimize the "check journal first" shortcut
2. Write a valid evidence record when no issues are found (the "all_clear" pattern)

Getting either wrong means either doing a full scan unnecessarily, or failing the recovery contract by not writing evidence on silent runs.

## Journal Directory Structure

Custodian journals live in **date-based subdirectories**, not flat:

```
<<<<<<< Updated upstream
<hermes-home>/profiles/indigo/commons/journals/ocas-custodian/2026-06-19/esc-run-20260619T183216Z.json
<hermes-home>/profiles/indigo/commons/journals/ocas-custodian/2026-06-19/light-scan-2026-06-19T120000-0700.json
=======
~/.hermes/profiles/indigo/commons/journals/ocas-custodian/2026-06-19/esc-run-20260619T183216Z.json
~/.hermes/profiles/indigo/commons/journals/ocas-custodian/2026-06-19/light-scan-2026-06-19T120000-0700.json
>>>>>>> Stashed changes
```

**NOT flat:**
```
<<<<<<< Updated upstream
<hermes-home>/profiles/indigo/commons/journals/ocas-custodian/esc-run-20260619T183216Z.json  ← does NOT exist
=======
~/.hermes/profiles/indigo/commons/journals/ocas-custodian/esc-run-20260619T183216Z.json  ← does NOT exist
>>>>>>> Stashed changes
```

### Path Template

```
{agent_root}/profiles/{profile}/commons/journals/ocas-custodian/{YYYY-MM-DD}/{run_id}.json
```

For the commons (non-profile) path:
```
{agent_root}/commons/journals/ocas-custodian/{YYYY-MM-DD}/{run_id}.json
```

### Discovery Pattern (for "check latest journal" optimization)

```bash
# Find the most recent esc-run journal across all paths
<<<<<<< Updated upstream
find <hermes-home> -path "*/journals/ocas-custodian/*/esc-run-*.json" -mtime -1 2>/dev/null | sort | tail -1
=======
find ~/.hermes -path "*/journals/ocas-custodian/*/esc-run-*.json" -mtime -1 2>/dev/null | sort | tail -1
>>>>>>> Stashed changes
```

Or for today's directory specifically:
```bash
TODAY=$(date -u +%Y-%m-%d)
<<<<<<< Updated upstream
ls <hermes-home>/profiles/indigo/commons/journals/ocas-custodian/$TODAY/esc-run-*.json 2>/dev/null | sort | tail -1
=======
ls ~/.hermes/profiles/indigo/commons/journals/ocas-custodian/$TODAY/esc-run-*.json 2>/dev/null | sort | tail -1
>>>>>>> Stashed changes
```

**IMPORTANT:** Use `date -u` (UTC) for the directory name, not `date` (local). The `run_id` is UTC-based, so the directory must match.

## The "all_clear" Journal Schema (Silent Runs)

When the escalation runner finds **zero** actionable issues across all sources, it must still write an evidence record per the recovery contract. The canonical schema:

```json
{
  "run_id": "20260619T183216Z",
  "timestamp": "2026-06-19T18:32:16.792686+00:00",
  "type": "escalation_runner",
  "escalation_needed": false,
  "outcome": "all_clear",
  "not_activity_reason": "No escalated or open actionable issues found across all sources.",
  "issues_checked": {
    "commons_issues_jsonl": {
      "open": 0,
      "escalated": 0,
      "fix_attempted_failed": 0
    },
    "custodian_journals_24h": "checked, no escalation_needed=true in last 24h",
    "proposals_dir": "empty — no pending proposals",
    "recent_esc_runs": "Last 3 runs all returned all_clear",
    "recent_deep_scan": "2026-06-19-0800: 0 open issues"
  },
  "system_health": "nominal"
}
```

### Field Semantics

| Field | Purpose |
|-------|---------|
| `run_id` | UTC timestamp, matches filename without extension |
| `type` | Always `"escalation_runner"` for esc-run journals |
| `escalation_needed` | `false` for all_clear (distinguishes from pre-fix schema where `true` triggers downstream) |
| `outcome` | `"all_clear"` — canonical "no issues found" verdict |
| `not_activity_reason` | Mandatory per recovery contract — explains why no action was taken |
| `issues_checked` | Breakdown of all sources checked with counts — proves the scan actually ran |
| `system_health`" | `"nominal"` when no issues; `"degraded"` when some checks failed but no fixable issues |

### Writing the all_clear Journal (cron-safe Python heredoc)

```python
import json
from datetime import timezone, datetime

now = datetime.now(timezone.utc)
run_id = now.strftime("%Y%m%dT%H%M%SZ")
date_dir = now.strftime("%Y-%m-%d")

journal = {
    "run_id": run_id,
    "timestamp": now.isoformat(),
    "type": "escalation_runner",
    "escalation_needed": False,
    "outcome": "all_clear",
    "not_activity_reason": "No escalated or open actionable issues found across all sources.",
    "issues_checked": {
        "commons_issues_jsonl": {"open": 0, "escalated": 0, "fix_attempted_failed": 0},
        "custodian_journals_24h": "checked, no escalation_needed=true in last 24h",
        "proposals_dir": "empty — no pending proposals",
        "recent_esc_runs": "Last N runs all returned all_clear",
        "recent_deep_scan": "YYYY-MM-DD-HHMM: 0 open issues"
    },
    "system_health": "nominal"
}

<<<<<<< Updated upstream
journal_dir = f"<hermes-home>/profiles/indigo/commons/journals/ocas-custodian/{date_dir}"
=======
journal_dir = f"~/.hermes/profiles/indigo/commons/journals/ocas-custodian/{date_dir}"
>>>>>>> Stashed changes
import os
os.makedirs(journal_dir, exist_ok=True)

with open(f"{journal_dir}/esc-run-{run_id}.json", "w") as f:
    json.dump(journal, f, indent=2)
```

## The "check journal first" Optimization

Before running the full multi-path issues.jsonl scan, check the latest esc-run journal:

1. Find the most recent `esc-run-*.json` (see discovery pattern above)
2. Read its `outcome` field
3. If `outcome == "all_clear"` AND the run is < 30 minutes ago → skip full scan, return `[SILENT]`
4. If `outcome == "all_clear"` but run is > 30 minutes ago → run full scan (stale evidence)

This is the single most efficient optimization for the escalation runner: a 5-second journal check can replace a 60-second full scan.

## issues.jsonl Path Hierarchy

The escalation runner checks issues from two sources:

| Path | Authority | Notes |
|------|-----------|-------|
<<<<<<< Updated upstream
| `<hermes-home>/profiles/indigo/commons/data/custodian/issues.jsonl` | **Primary** | Live source, updated by custodian plugin |
| `<hermes-home>/commons/data/ocas-custodian/issues.jsonl` | **Secondary** | Lagging copy, may contain stale entries |
=======
| `~/.hermes/profiles/indigo/commons/data/custodian/issues.jsonl` | **Primary** | Live source, updated by custodian plugin |
| `~/.hermes/commons/data/ocas-custodian/issues.jsonl` | **Secondary** | Lagging copy, may contain stale entries |
>>>>>>> Stashed changes

**Always write to the profile path.** The commons path receives data via sync.

### Parsing Pattern (reminder)

```python
def parse_jsonl(path):
    entries = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line: continue
            try:
                entries.append(json.loads(line))
                continue
            except json.JSONDecodeError:
                pass
            # Multi-object line: brace-depth parser
            depth = 0; start = None
            for i, c in enumerate(line):
                if c == '{':
                    if depth == 0: start = i
                    depth += 1
                elif c == '}':
                    depth -= 1
                    if depth == 0 and start is not None:
                        entries.append(json.loads(line[start:i+1]))
                        start = None
    return entries
```

## Quick-Reference: Full Escalation Runner Flow

```
1. Check latest esc-run journal → if all_clear & recent → [SILENT]
<<<<<<< Updated upstream
2. find <hermes-home> -name "issues.jsonl" → discover all paths
=======
2. find ~/.hermes -name "issues.jsonl" → discover all paths
>>>>>>> Stashed changes
3. Parse each with brace-depth parser
4. Filter: status NOT IN (resolved, superseded)
5. Cross-reference by description (dedup)
6. Check current live state (jobs.json, config.yaml)
7. For each actionable issue:
   a. If Tier 1 + confidence >= 0.6 → auto-fix
   b. If Tier 2 → write InsightProposal, tag escalation_needed
   c. If Tier 3 → write InsightProposal + tag escalation_needed
8. Close resolved issues + clear stale escalation_needed flags
9. Write esc-run journal (all_clear or action_taken)
10. If no issues found → write all_clear journal → return [SILENT]
```