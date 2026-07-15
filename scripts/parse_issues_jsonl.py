#!/usr/bin/env python3
"""Robust parser for custodian issues.jsonl.

Handles two malformed layouts seen in production:
  - newline-separated JSON objects (normal)
  - multiple JSON objects concatenated on ONE line (no newline between)
  - mixed (some lines concatenated, some not)

Dedupes by issue_id/id, keeping the best (non-resolved) status per entry.
Prints a summary of open issues with fingerprints.

Usage:
  python3 parse_issues_jsonl.py [path]
Default path: <hermes-home>/commons/data/ocas-custodian/issues.jsonl
(the AUTHORITATIVE profile data-path — NOT the .../journals/ocas-custodian/issues.jsonl
stale copy, which lags by days and still carries old resolved/duplicate outages).

Use this (not naive json.loads(line)) whenever reading issues.jsonl in a
scan or escalation runner — see references/escalation-runner-multi-path-issues.md.

CRITICAL PATH GOTCHA (2026-07-15): there are TWO issues.jsonl files:
  - AUTHORITATIVE:  <hermes-home>/commons/data/ocas-custodian/issues.jsonl
                     (live, written by escalation loop / custodian runs; ~80KB today)
  - STALE COPY:     <hermes-home>/commons/journals/ocas-custodian/issues.jsonl
                     (legacy path, NOT updated by current writes; ~25KB, last touched Jul 14)
Reading the stale copy made the escalation loop believe old resolved/duplicate
outage fingerprints (OpenRouter-402, Nous-401, Google-403, token_expired, etc.)
were still open. ALWAYS read the data-path. This script's DEFAULT was the stale
copy until 2026-07-15 and manufactured 7 phantom escalations in one run.
"""
import json, sys, os

DEFAULT = '<hermes-home>/commons/data/ocas-custodian/issues.jsonl'
STALE_PATH = '<hermes-home>/commons/journals/ocas-custodian/issues.jsonl'


def parse(path):
    with open(path) as f:
        raw = f.read()
    entries = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entries.append(json.loads(line))
            continue
        except json.JSONDecodeError:
            pass
        # brace-depth parse for concatenated objects on one line
        depth = 0
        cur = ''
        instr = False
        esc = False
        for ch in line:
            if ch == '\\' and not esc:
                esc = True
                cur += ch
                continue
            if ch == '"' and not esc:
                instr = not instr
            if not instr:
                if ch == '{':
                    depth += 1
                elif ch == '}':
                    depth -= 1
            cur += ch
            if depth == 0 and cur.strip():
                try:
                    entries.append(json.loads(cur))
                except json.JSONDecodeError:
                    pass
                cur = ''
        if cur.strip():
            try:
                entries.append(json.loads(cur))
            except json.JSONDecodeError:
                pass
    return entries


def dedupe(entries):
    best = {}
    for e in entries:
        key = e.get('issue_id') or e.get('id')
        if not key:
            continue
        if key not in best:
            best[key] = e
        else:
            if best[key].get('status') in ('resolved', 'closed') and e.get('status') not in ('resolved', 'closed'):
                best[key] = e
    return best


if __name__ == '__main__':
    path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT
    if os.path.abspath(path) == os.path.abspath(STALE_PATH):
        sys.stderr.write(
            'WARNING: reading the STALE issues.jsonl copy at .../journals/ocas-custodian/.\n'
            '         This file is NOT updated by current writes and still carries old resolved/duplicate\n'
            '         outages. Use the authoritative data-path instead:\n'
            '         <hermes-home>/commons/data/ocas-custodian/issues.jsonl\n')
    entries = parse(path)
    best = dedupe(entries)
    # Open-signal per escalation-loop guidance: not resolved/duplicate AND
    # (escalation_needed OR status==user_gated). 'summary' field does not exist;
    # use 'description' for the preview line.
    open_issues = [e for k, e in best.items()
                   if e.get('status') not in ('resolved', 'duplicate', 'closed')
                   and (e.get('escalation_needed') is True or e.get('status') == 'user_gated')]
    print(f'path: {path}')
    print(f'total parsed: {len(entries)}  unique: {len(best)}  open(escalation/user_gated): {len(open_issues)}')
    for e in open_issues:
        print(' ', e.get('issue_id') or e.get('id'),
              '| fp:', e.get('fingerprint'),
              '| tier:', e.get('tier'),
              '| esc:', e.get('escalation_needed'),
              '|', (e.get('description') or '')[:70])
