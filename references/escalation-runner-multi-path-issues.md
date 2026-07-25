# Escalation Runner Multi-Path Issue Discovery

## Problem

Open issues with `escalation_needed: true` accumulate in **multiple** `issues.jsonl` files across the filesystem. Checking only one path leaves stale issues unresolved and causes duplicate work.

## All Known Paths (check every one)

```
<<<<<<< Updated upstream
<hermes-home>/commons/journals/ocas-custodian/issues.jsonl
<hermes-home>/profiles/indigo/commons/journals/ocas-custodian/issues.jsonl
<hermes-home>/profiles/indigo/commons/data/custodian/issues.jsonl
<hermes-home>/profiles/indigo/commons/data/ocas-custodian/issues.jsonl
<hermes-home>/profiles/indigo/commons/ocas-custodian/issues.jsonl
=======
~/.hermes/commons/journals/ocas-custodian/issues.jsonl
~/.hermes/profiles/indigo/commons/journals/ocas-custodian/issues.jsonl
~/.hermes/profiles/indigo/commons/data/custodian/issues.jsonl
~/.hermes/profiles/indigo/commons/data/ocas-custodian/issues.jsonl
~/.hermes/profiles/indigo/commons/ocas-custodian/issues.jsonl
>>>>>>> Stashed changes
```

## Discovery Pattern

```bash
<<<<<<< Updated upstream
find <hermes-home> -name "issues.jsonl" 2>/dev/null | while read f; do
=======
find ~/.hermes -name "issues.jsonl" 2>/dev/null | while read f; do
>>>>>>> Stashed changes
    grep '"escalation_needed": true' "$f" | grep -v '"status": "resolved"'
done
```

**IMPORTANT:** Use the exact pattern `"escalation_needed": true` (with the JSON key and colon) — NOT `escalation_needed.*true` which can match the word "escalation_needed" inside description text fields, producing false positives. Confirmed 2026-06-16: naive grep matched an entry whose description mentioned "escalation_needed" but whose actual boolean field was `false`.

## Critical: Deduplication Across Files

Same root cause often appears in multiple files with **different issue_id values**:

| Root Cause | ID in journals/file | ID in data/file |
|------------|---------------------|-----------------|
| Google OAuth token | `oc_google_auth_<account-identity>` | `esc-20260530-001`, `iss-20260531-001` |
| MCP server files | `oc_mcp_server_files_missing_20260614` | (same in data file) |

**Always cross-reference by description/summary, not just issue_id.**

## Stale Issue Heuristics (verified 2026-06-15, confirmed 2026-06-16)

- `last_error: null` + `status: ok` + `consecutive_failures: 0` → Resolved, close it
- OAuth token file has valid access + refresh + future expiry → Resolved
- Service missing from `systemctl list-units` → Resolved (removed, not just disabled)
- Job `last_status: ok` + `last_error: null` → Job-level issue resolved
- `status: resolved` + `escalation_needed: true` → Stale flag, clear it (systematic bug pattern)
- **`escalation_needed: true` but job is currently healthy** → Transient failure self-resolved. Verify with 2-3 consecutive successful runs before closing. Confirmed pattern: email:check flagged as escalated at 10:00 PDT, but by 12:00 PDT had 3 consecutive successful runs (11:28, 11:44, 11:53) with `last_status=ok`, `consecutive_failures=0`, `last_error=null`.

## JSONL Parsing Gotcha (2026-06-15)

issues.jsonl files can contain **multiple JSON objects concatenated on a single line** (not newline-separated). Naive `json.loads(line)` fails with `JSONDecodeError: Extra data`. Use a brace-depth parser:

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
            # Multi-object line: walk by brace depth
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

## escalation_needed Flag Cleanup (2026-06-15)

When closing stale issues, **always clear `escalation_needed: true`** on resolved entries. A systematic bug leaves `escalation_needed: true` on entries whose `status` is already `resolved`, causing false-positive escalation on every subsequent run.

After any batch close, sweep all files:
```python
if d.get('status') == 'resolved' and d.get('escalation_needed') == True:
    d['escalation_needed'] = False
```

## Corvus Proposal Staleness (2026-06-15)

<<<<<<< Updated upstream
Corvus writes InsightProposals to `<hermes-home>/proposals/` and `<hermes-home>/profiles/indigo/commons/data/ocas-corvus/proposals/`. These proposals can flag issues that **custodian has already resolved** by the time the escalation runner sees them. Example: `prop-mcp-server-files-missing-0615` proposed action on 4 MCP servers (instagram, pdsx, spotify, threads), but custodian had already verified all 4 were `enabled: false` in config.yaml.
=======
Corvus writes InsightProposals to `~/.hermes/proposals/` and `~/.hermes/profiles/indigo/commons/data/ocas-corvus/proposals/`. These proposals can flag issues that **custodian has already resolved** by the time the escalation runner sees them. Example: `prop-mcp-server-files-missing-0615` proposed action on 4 MCP servers (instagram, pdsx, spotify, threads), but custodian had already verified all 4 were `enabled: false` in config.yaml.
>>>>>>> Stashed changes

**Before acting on any Corvus proposal:**
1. Check the current live state independently (e.g., `grep "enabled:" config.yaml` for MCP servers)
2. Check the latest custodian scan journal — if the scan already classified the issue as resolved or "known pattern," skip it
3. Proposals older than 24 hours with no matching open issue in `issues.jsonl` are likely stale

## Workflow

<<<<<<< Updated upstream
1. `find <hermes-home> -name "issues.jsonl"` — discover all paths
=======
1. `find ~/.hermes -name "issues.jsonl"` — discover all paths
>>>>>>> Stashed changes
2. Parse each with the brace-depth parser (not naive `json.loads`)
3. Extract open issues; check `escalation_needed` flag independently of status
4. Deduplicate by root cause (compare descriptions, not issue_id)
5. Check current live state for each unique issue (jobs.json `last_status`/`last_error`)
6. Close resolved issues AND clear stale `escalation_needed` flags in same pass
7. Update ALL files referencing the same root cause
8. Write esc-run journal after all files updated