# issues.jsonl Parsing Pitfalls (verified 2026-07-08)

Three failure modes bite custodian reconciliation passes that read `issues.jsonl` by hand.

## Mode A — whole-file parser that resets at `depth==1` (DOUBLE-COUNTS)

A single-pass parser over the whole file that resets its accumulator when it
re-enters depth 1 **corrupts records whose `detail`/`summary` fields contain
nested `{`/`}` (embedded JSON)**. Symptom: parser reports 24 records where only
12 exist; the inflated count then makes phantom "duplicate issues" appear and
tempts a destructive delete. Confirmed 2026-07-08: this exact bug produced 8
phantom duplicate shells and nearly caused a bad deletion; the correct parser
returned the true 12.

```python
# BUG: resets obj at depth==1, wiping outer content on every inner brace.
depth = 0; obj = ""
for ch in content:
    if ch == '{' and not instr:
        depth += 1; obj += ch
        if depth == 1: obj = '{'   # <-- destroys accumulated outer record
        continue
    if ch == '}':
        depth -= 1; obj += ch
        if depth == 0: recs.append(obj); obj = ''
    obj += ch
```

## Mode B — per-line parser (MISSES multi-line records)

The parser in `references/escalation-runner-multi-path-issues.md` is correct for
multiple objects concatenated on ONE line, but iterates `for line in f`. A record
that spans multiple lines (pretty-printed JSON with internal newlines) fails
`json.loads(line)` as a partial line AND never reaches `depth==0` within one line,
so it is silently dropped.

## Mode C — the verified correct parser (USE THIS)

Whole-file, starts a record ONLY at a top-level `{` (`depth 0 -> 1`), closes at
`depth==0`. Correctly handles concatenated, multi-line, AND nested-brace records.

```python
import json

def parse_issues_jsonl(path):
    with open(path) as f:
        content = f.read()
    records, depth, obj = [], 0, ""
    for ch in content:
        if ch == '{' and depth == 0:
            depth, obj = 1, '{'
            continue
        if depth > 0:
            obj += ch
            if ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    try:
                        records.append(json.loads(obj))
                    except json.JSONDecodeError:
                        pass
            continue
    return records
```

Prefer invoking the skill's `scripts/parse_issues_jsonl.py` /
`scripts/verify_escalation_state.py` when available; hand-roll only with the
Mode C implementation above.

## Mode D — indigo profile uses per-line JSON; inline brace-depth parsers emit PHANTOM EMPTIES (2026-07-13)

The indigo deployment's issues file at
`<hermes-home>/commons/data/ocas-custodian/issues.jsonl`
is **newline-delimited**: exactly one JSON object per line, NO concatenation.

A hand-rolled brace-depth parser (even a whole-file variant that resets at the
wrong depth) read this file and emitted **24 phantom records, every one with all
`None` fields** (`issue_id=None`, `status=None`, `fingerprint=None`, ...). It
crossed line boundaries, mis-accumulated, and manufactured empty `{}` objects
that masqueraded as 24 open issues — corrupting classification counts and
threatening false escalations.

**Correct parse path for indigo — naive per-line `json.loads` is PRIMARY and
correct:**
```python
import json
recs = []
for line in open(path):
    line = line.strip()
    if not line:
        continue
    try:
        recs.append(json.loads(line))   # one object per line, no concatenation
    except json.JSONDecodeError as e:
        # Only here (genuine "Extra data" / concatenation) use Mode C.
        ...
```
Only reach for the Mode C whole-file parser when a line actually raises
`JSONDecodeError: Extra data` (true concatenation). This session's true count
was **29 real records**, not the 24 phantom empties the brace-depth parser
produced. **Confirm the true count before any delete/resolve/escalate.**

## Verify-before-acting (critical)

A double-count manufactures phantom records; a mis-accumulating brace-depth
parser manufactures phantom `None`-field records. **Before deleting or resolving
any issue entry, independently confirm the true record count** — run the
per-line parse (Mode D) for indigo, or Mode C otherwise, and cross-check each
issue's `jobs_paused` against the live `jobs.json` enrollment. If the only
evidence for a "duplicate" or "open issue" is a single parser pass, it is not
safe to act.

## YAML null-key detection — regex false positives

`grep ': $'` / `: null$` matches **any mapping header with indented children**
(e.g. `model:`, `providers:`), which in PyYAML is a `dict`, NOT `None`. A regex
scan of `config.yaml` returned 149 "null" lines; a PyYAML recursive `None` scan
returned **0**. Use PyYAML:

```python
import yaml
def find_null_keys(d, path=""):
    out = []
    if isinstance(d, dict):
        for k, v in d.items():
            p = f"{path}.{k}" if path else k
            if v is None:
                out.append(p)
            else:
                out += find_null_keys(v, p)
    elif isinstance(d, list):
        for i, v in enumerate(d):
            out += find_null_keys(v, f"{path}[{i}]")
    return out
# cfg = yaml.safe_load(open("<hermes-home>/config.yaml"))
# nulls = find_null_keys(cfg)
```
