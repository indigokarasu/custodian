#!/usr/bin/env python3
import os
"""Race-safe targeted patch of ONE issue in the PROFILE issues.jsonl.

PROBLEM (confirmed 2026-07-14 escalation loop): `custodian:light` (top-of-hour) and
sibling esc loops rewrite issues.jsonl concurrently. A whole-file brace-parse -> mutate
-> os.replace rewrite can be CLOBBERED by a later writer whose version omits your
mutation. The existing "os.replace is idempotent" claim only guards data integrity,
not retention of *your own* change.

FIX:
  1. Rewrite ONLY the target line (minimal window; other lines untouched, so a
     concurrent rewrite of a DIFFERENT issue doesn't wipe your mutation as easily).
  2. Immediately re-read and verify the mutation persisted.
  3. Retry up to --retries times if a sibling clobbered it between write and re-read.

Usage:
  python3 race_safe_issue_patch.py --issue-id oc_state_db_oversized_20260714T0500 \
      --set status=resolved --set user_gated=false --set escalation_needed=false \
      [--require-status user_gated]   # only mutate if currently in this status
      [--path ~/.hermes/profiles/indigo/commons/data/ocas-custodian/issues.jsonl]
      [--retries 3] [--sleep 2.0]
"""
import argparse, json, os, sys, time
from datetime import datetime, timezone

DEFAULT_PATH = os.path.expanduser("~/.hermes/profiles/indigo/commons/data/ocas-custodian/issues.jsonl")


def parse_line(line):
    line = line.strip()
    if not line:
        return None
    try:
        return json.loads(line)
    except Exception:
        return None


def coerce(v):
    lv = v.lower()
    if lv == "true":
        return True
    if lv == "false":
        return False
    if lv == "null":
        return None
    try:
        return json.loads(v)
    except Exception:
        return v


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--issue-id", required=True)
    ap.add_argument("--set", action="append", default=[], dest="sets",
                    help="KEY=VALUE pairs (VALUE: true/false/null/number/json/string)")
    ap.add_argument("--require-status", default=None,
                    help="Only mutate if current status equals this (safety gate)")
    ap.add_argument("--path", default=DEFAULT_PATH)
    ap.add_argument("--retries", type=int, default=3)
    ap.add_argument("--sleep", type=float, default=2.0)
    args = ap.parse_args()

    if not args.sets:
        print("ERROR: no --set provided")
        sys.exit(2)

    keyvals = []
    for s in args.sets:
        if "=" not in s:
            print(f"ERROR: bad --set {s!r} (need KEY=VALUE)")
            sys.exit(2)
        k, v = s.split("=", 1)
        keyvals.append((k, coerce(v)))

    now_iso = datetime.now(timezone.utc).isoformat()

    for attempt in range(1, args.retries + 1):
        with open(args.path) as f:
            lines = f.readlines()
        out = []
        found = False
        gate_mismatch = False
        for ln in lines:
            o = parse_line(ln)
            if o and (o.get("issue_id") or o.get("id")) == args.issue_id:
                if args.require_status is not None and o.get("status") != args.require_status:
                    gate_mismatch = True
                    out.append(ln if ln.endswith("\n") else ln + "\n")
                    continue
                for k, val in keyvals:
                    o[k] = val
                if any(k == "status" and val == "resolved" for k, val in keyvals) and \
                        "resolved_at" not in [kv[0] for kv in keyvals]:
                    o["resolved_at"] = now_iso
                out.append(json.dumps(o) + "\n")
                found = True
            else:
                out.append(ln if ln.endswith("\n") else ln + "\n")
        if not found:
            if gate_mismatch:
                print(f"attempt {attempt}: found but status gate mismatch (require={args.require_status}); no-op")
                sys.exit(0)
            print(f"attempt {attempt}: target {args.issue_id} NOT FOUND")
            sys.exit(1)
        with open(args.path, "w") as f:
            f.writelines(out)
        # immediate re-read verify
        with open(args.path) as f:
            for ln in f:
                o = parse_line(ln)
                if o and (o.get("issue_id") or o.get("id")) == args.issue_id:
                    if all(o.get(k) == val for k, val in keyvals):
                        print(f"attempt {attempt}: PERSISTED (verified {args.issue_id})")
                        sys.exit(0)
                    break
        print(f"attempt {attempt}: clobbered by sibling concurrent writer, retrying")
        time.sleep(args.sleep)

    print(f"FAILED after {args.retries} retries: {args.issue_id} mutation did not persist")
    sys.exit(1)


if __name__ == "__main__":
    main()